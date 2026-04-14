package ai.pipestream.module.chunker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;

import ai.pipestream.module.chunker.cache.ChunkCacheService;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.config.ChunkerStepOptions;
import ai.pipestream.module.chunker.directive.DirectiveKeyComputer;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.schema.SchemaExtractorService;
import ai.pipestream.module.chunker.service.ChunkMetadataExtractor;
import ai.pipestream.module.chunker.service.NlpPreprocessor;
import ai.pipestream.module.chunker.service.OverlapChunker;
import ai.pipestream.module.chunker.service.UnicodeSanitizer;
import ai.pipestream.module.chunker.support.ChunkerSupport;
import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.LogEntrySource;
import ai.pipestream.data.v1.LogLevel;
import ai.pipestream.data.v1.ModuleLogOrigin;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.data.v1.SourceFieldAnalytics;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.server.meta.BuildInfoProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Chunker gRPC service implementation — directive-driven rewrite per DESIGN.md §7.1.
 *
 * <p>This service:
 * <ol>
 *   <li>Parses {@code ProcessConfiguration.json_config} into {@link ChunkerStepOptions}.</li>
 *   <li>Resolves {@link VectorDirective}s from
 *       {@code doc.search_metadata.vector_set_directives} — absent directives fail with
 *       {@code FAILED_PRECONDITION} (§21.1, no legacy fallback).</li>
 *   <li>For each directive and each {@link NamedChunkerConfig} on that directive, runs
 *       the chunking pipeline (Redis cache → OverlapChunker → cache writeback) and
 *       builds one {@link SemanticProcessingResult} with deterministic IDs (§21.5).</li>
 *   <li>Always emits a {@code sentences_internal} SPR per directive (§21.9 —
 *       "no opt-out knob"; the corresponding {@code always_emit_sentences} field
 *       on {@link ChunkerStepOptions} is retained for JSON back-compat but
 *       deliberately not consulted here).</li>
 *   <li>Builds {@code source_field_analytics[]} for every unique
 *       {@code (source_field, chunk_config_id)} pair (§5.1).</li>
 *   <li>Sorts {@code semantic_results[]} deterministically by
 *       {@code (source_field_name, chunk_config_id, embedding_config_id, result_id)} (§21.8).</li>
 * </ol>
 *
 * <p>Audit trail: every significant processing decision emits a plain-English log entry
 * attached to the response, per project audit-trail requirements.
 */
@Singleton
@GrpcService
public class ChunkerGrpcImpl implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(ChunkerGrpcImpl.class);

    /** Chunk config ID used for the always-emitted sentence SPR (§21.9). */
    static final String SENTENCES_INTERNAL_CONFIG_ID = "sentences_internal";

    @Inject
    ObjectMapper objectMapper;

    @Inject
    OverlapChunker overlapChunker;

    @Inject
    NlpPreprocessor nlpPreprocessor;

    @Inject
    ChunkMetadataExtractor metadataExtractor;

    @Inject
    ChunkCacheService cacheService;

    @Inject
    SchemaExtractorService schemaExtractorService;

    @Inject
    BuildInfoProvider buildInfoProvider;

    // =========================================================================
    // processData — directive-driven core flow
    // =========================================================================

    @Override
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        if (request == null) {
            LOG.error("Received null request");
            return Uni.createFrom().item(createErrorResponse("Request cannot be null", null));
        }

        boolean isTest = request.getIsTest();
        String logPrefix = isTest ? "[TEST] " : "";

        // Offload to the Quarkus worker pool so .await().indefinitely() on cache ops is safe.
        // The emitOn/runSubscriptionOn pattern is the canonical Vert.x context fix per
        // platform-coding-patterns.md. Without this the supplier runs on the Vert.x event
        // loop and blocking calls throw BlockingOperationNotAllowedException.
        return Uni.createFrom().item(() -> {
            long startTime = System.currentTimeMillis();
            ProcessDataResponse.Builder responseBuilder = ProcessDataResponse.newBuilder();
            List<LogEntry> logs = new ArrayList<>();

            try {
                if (!request.hasDocument()) {
                    LOG.info(logPrefix + "No document provided in request");
                    return ProcessDataResponse.newBuilder()
                            .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                            .addLogEntries(moduleLog("Chunker service: no document to process", LogLevel.LOG_LEVEL_INFO))
                            .build();
                }

                PipeDoc inputDoc = request.getDocument();
                ProcessConfiguration config = request.getConfig();
                ServiceMetadata metadata = request.getMetadata();
                String streamId = metadata.getStreamId();
                String pipeStepName = metadata.getPipeStepName();

                LOG.infof("%sProcessing document ID: %s for step: %s in stream: %s",
                        logPrefix,
                        inputDoc.getDocId() != null ? inputDoc.getDocId() : "unknown",
                        pipeStepName, streamId);

                // ----------------------------------------------------------------
                // Step 1: Parse ChunkerStepOptions from json_config (DESIGN.md §7.1 step 1)
                // ----------------------------------------------------------------
                ChunkerStepOptions options;
                Struct jsonConfig = config != null ? config.getJsonConfig() : null;
                if (jsonConfig == null || jsonConfig.getFieldsCount() == 0) {
                    options = ChunkerStepOptions.defaults();
                    LOG.debugf("No json_config provided — using ChunkerStepOptions defaults");
                } else {
                    try {
                        String jsonStr = JsonFormat.printer().print(jsonConfig);
                        options = objectMapper.readValue(jsonStr, ChunkerStepOptions.class);
                        LOG.debugf("Parsed ChunkerStepOptions: cacheEnabled=%b ttlSeconds=%d (always_emit_sentences is ignored per §21.9)",
                                options.effectiveCacheEnabled(),
                                options.effectiveCacheTtlSeconds());
                    } catch (Exception e) {
                        String msg = "Invalid ChunkerStepOptions JSON: " + e.getMessage();
                        LOG.errorf("INVALID_ARGUMENT: %s", msg);
                        return createErrorResponse(msg, e);
                    }
                }

                // ----------------------------------------------------------------
                // Step 2: Resolve directives — absent → FAILED_PRECONDITION (§21.1)
                // ----------------------------------------------------------------
                if (!inputDoc.hasSearchMetadata()
                        || !inputDoc.getSearchMetadata().hasVectorSetDirectives()) {
                    String msg = "Missing vector_set_directives on doc — " +
                            "chunker requires directives (DESIGN.md §21.1, no legacy fallback)";
                    LOG.warnf("FAILED_PRECONDITION: %s", msg);
                    return createErrorResponse(msg, null);
                }

                List<VectorDirective> directives = inputDoc.getSearchMetadata()
                        .getVectorSetDirectives()
                        .getDirectivesList();

                if (directives.isEmpty()) {
                    String msg = "Empty vector_set_directives — at least one directive required";
                    LOG.warnf("FAILED_PRECONDITION: %s", msg);
                    return createErrorResponse(msg, null);
                }

                logs.add(moduleLog(logPrefix + "Processing " + directives.size()
                        + " directive(s) for document " + inputDoc.getDocId(),
                        LogLevel.LOG_LEVEL_INFO));

                // ----------------------------------------------------------------
                // Step 3: Validate source_label uniqueness + compute directive_keys (§21.2)
                // ----------------------------------------------------------------
                Set<String> seenLabels = new HashSet<>();
                Map<VectorDirective, String> directiveKeys = new LinkedHashMap<>();
                for (VectorDirective d : directives) {
                    if (!seenLabels.add(d.getSourceLabel())) {
                        String msg = "Duplicate source_label '" + d.getSourceLabel()
                                + "' in vector_set_directives — INVALID_ARGUMENT";
                        LOG.warnf("INVALID_ARGUMENT: %s", msg);
                        return createErrorResponse(msg, null);
                    }
                    directiveKeys.put(d, DirectiveKeyComputer.compute(d));
                }

                // ----------------------------------------------------------------
                // Steps 4–5: Directive loop — NLP + chunker + cache
                // ----------------------------------------------------------------
                String docHash = DirectiveKeyComputer.sha256b64url(inputDoc.getDocId());
                List<SemanticProcessingResult> outputSprs = new ArrayList<>();
                // NLP cache: source text → NlpResult (computed once per unique text)
                Map<String, NlpPreprocessor.NlpResult> nlpByText = new HashMap<>();
                // DocumentAnalytics per directive source_label, so the Step 7
                // SourceFieldAnalytics loop can stamp DocumentAnalytics on every
                // SFA entry (§5.1 SourceFieldAnalytics.document_analytics, proto
                // field 3). Built here once per unique source_label so we don't
                // recompute for each chunker config on the same directive.
                Map<String, DocumentAnalytics> docAnalyticsBySourceLabel = new LinkedHashMap<>();

                // PR-H: per-directive resolution cache (sourceText, nlpResult,
                // directiveKey). Built in Step 4, reused in Step 6 so that
                // sentences_internal emission doesn't have to re-call
                // extractSourceText() and re-look-up the NLP cache. Pre-PR-H
                // Step 6 duplicated three lookups per directive that Step 4
                // had already performed. LinkedHashMap so directive iteration
                // order is preserved across the two steps.
                Map<VectorDirective, ResolvedDirective> resolvedDirectives = new LinkedHashMap<>();

                for (VectorDirective directive : directives) {
                    String sourceLabel = directive.getSourceLabel();
                    String sourceText = extractSourceText(inputDoc, directive);

                    if (sourceText == null || sourceText.isEmpty()) {
                        LOG.debugf("Directive source_label='%s' cel_selector='%s' yielded empty text — skipping",
                                sourceLabel, directive.getCelSelector());
                        logs.add(moduleLog(
                                "Skipped directive '" + sourceLabel + "': source text is empty",
                                LogLevel.LOG_LEVEL_DEBUG));
                        continue;
                    }

                    // Run NLP ONCE per unique source text within this request
                    NlpPreprocessor.NlpResult nlpResult = nlpByText.computeIfAbsent(
                            sourceText, t -> nlpPreprocessor.preprocess(t));

                    // Compute DocumentAnalytics ONCE per source_label (the same
                    // source_label will never yield different source_text in a
                    // well-formed request; §21.2 enforces source_label uniqueness).
                    final String srcLabelForDa = sourceLabel;
                    final String srcTextForDa = sourceText;
                    final NlpPreprocessor.NlpResult nlpForDa = nlpResult;
                    docAnalyticsBySourceLabel.computeIfAbsent(srcLabelForDa,
                            k -> metadataExtractor.extractDocumentAnalytics(srcTextForDa, nlpForDa));

                    String directiveKey = directiveKeys.get(directive);

                    // Cache the resolution so Step 6 can reuse it without
                    // re-extracting source text or re-looking-up the NLP cache.
                    resolvedDirectives.put(directive,
                            new ResolvedDirective(sourceText, nlpResult, directiveKey));

                    for (NamedChunkerConfig namedConfig : directive.getChunkerConfigsList()) {
                        SemanticProcessingResult spr = processOneDirectiveConfig(
                                inputDoc, docHash, sourceText, nlpResult,
                                directive, directiveKey, namedConfig,
                                options, streamId, pipeStepName, logs);
                        outputSprs.add(spr);
                    }
                }

                // ----------------------------------------------------------------
                // Step 6: Always-emit sentences_internal SPR (§21.9)
                //
                // §21.9 is explicit: "No opt-out knob in chunker/embedder/semantic-graph
                // for sentence emission." The ChunkerStepOptions.always_emit_sentences
                // field is retained in the record for JSON-config back-compat but is
                // deliberately NOT consulted here — sentences_internal is always emitted
                // regardless of what the caller puts in the options JSON. Whether the
                // sink indexes the sentence chunks is a sink-side toggle per §21.9,
                // not a chunker concern.
                // ----------------------------------------------------------------
                {
                    // Only emit if no directive config already produces sentence-level chunks
                    boolean alreadyHasSentenceSpr = outputSprs.stream()
                            .anyMatch(spr -> SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()));

                    if (!alreadyHasSentenceSpr) {
                        // PR-H: iterate the resolvedDirectives cache from
                        // Step 4 instead of re-calling extractSourceText() +
                        // nlpByText.get() + directiveKeys.get() for every
                        // directive. Pre-PR-H Step 6 was duplicating three
                        // lookups per directive; the cache makes Step 6 a
                        // simple iteration over data we already have.
                        for (Map.Entry<VectorDirective, ResolvedDirective> entry : resolvedDirectives.entrySet()) {
                            VectorDirective directive = entry.getKey();
                            ResolvedDirective resolved = entry.getValue();
                            String sourceLabel = directive.getSourceLabel();
                            NlpPreprocessor.NlpResult nlpResult = resolved.nlpResult();

                            NlpDocumentAnalysis nlpAnalysis = ChunkerSupport.buildNlpAnalysis(nlpResult);
                            String directiveKey = resolved.directiveKey();
                            List<SemanticChunk> sentenceChunks = buildSentenceChunks(
                                    docHash, sourceLabel, SENTENCES_INTERNAL_CONFIG_ID, nlpResult);

                            if (sentenceChunks.isEmpty()) {
                                LOG.debugf("No sentences extracted for directive source_label='%s' — skipping sentences_internal SPR",
                                        sourceLabel);
                                continue;
                            }

                            String resultId = "stage1:" + docHash + ":" + sourceLabel
                                    + ":" + SENTENCES_INTERNAL_CONFIG_ID + ":";

                            SemanticProcessingResult sentencesSpr = SemanticProcessingResult.newBuilder()
                                    .setResultId(resultId)
                                    .setSourceFieldName(sourceLabel)
                                    .setChunkConfigId(SENTENCES_INTERNAL_CONFIG_ID)
                                    .setEmbeddingConfigId("")   // stage 1 placeholder
                                    .addAllChunks(sentenceChunks)
                                    .putMetadata("directive_key",
                                            Value.newBuilder().setStringValue(directiveKey).build())
                                    .setNlpAnalysis(nlpAnalysis)
                                    .build();

                            outputSprs.add(sentencesSpr);
                            logs.add(moduleLog(
                                    "Emitted sentences_internal SPR for source_label='" + sourceLabel
                                            + "' with " + sentenceChunks.size() + " sentence(s)",
                                    LogLevel.LOG_LEVEL_DEBUG));
                        }
                    }
                }

                // ----------------------------------------------------------------
                // Step 7: Merge with existing SPRs and build source_field_analytics
                //
                // Preserve SPRs from prior passes that have config_ids NOT produced
                // in this pass. This allows sequential double-chunking to accumulate
                // SPRs across passes without overwriting each other.
                // ----------------------------------------------------------------
                SearchMetadata.Builder smBuilder = inputDoc.getSearchMetadata().toBuilder();

                // Build set of (sourceField, chunkConfigId) pairs produced in this pass
                Set<String> thisPassKeys = new HashSet<>();
                for (SemanticProcessingResult spr : outputSprs) {
                    thisPassKeys.add(spr.getSourceFieldName() + "|" + spr.getChunkConfigId());
                }

                // Re-use existing SPRs that are NOT being replaced by this pass
                List<SemanticProcessingResult> mergedSprs = new ArrayList<>();
                for (SemanticProcessingResult existingSpr : inputDoc.getSearchMetadata().getSemanticResultsList()) {
                    String key = existingSpr.getSourceFieldName() + "|" + existingSpr.getChunkConfigId();
                    if (!thisPassKeys.contains(key)) {
                        mergedSprs.add(existingSpr);
                    }
                }
                mergedSprs.addAll(outputSprs);

                // Rebuild source_field_analytics from ALL merged SPRs.
                // §5.1 SourceFieldAnalytics proto fields:
                //   1. source_field
                //   2. chunk_config_id
                //   3. document_analytics   ← from docAnalyticsBySourceLabel
                //   4. total_chunks         ← SPR.chunks.size()
                //   5. average_chunk_size   ← IntSummaryStatistics on chunk text length
                //   6. min_chunk_size       ← same
                //   7. max_chunk_size       ← same
                smBuilder.clearSourceFieldAnalytics();
                Set<String> analyticsKeys = new HashSet<>();
                for (SemanticProcessingResult spr : mergedSprs) {
                    String analyticsKey = spr.getSourceFieldName() + "|" + spr.getChunkConfigId();
                    if (analyticsKeys.add(analyticsKey)) {
                        List<SemanticChunk> chunksForSpr = spr.getChunksList();
                        int totalChunks = chunksForSpr.size();

                        SourceFieldAnalytics.Builder sfaBuilder = SourceFieldAnalytics.newBuilder()
                                .setSourceField(spr.getSourceFieldName())
                                .setChunkConfigId(spr.getChunkConfigId())
                                .setTotalChunks(totalChunks);

                        if (totalChunks > 0) {
                            IntSummaryStatistics sizeStats = chunksForSpr.stream()
                                    .mapToInt(c -> c.getEmbeddingInfo().getTextContent().length())
                                    .summaryStatistics();
                            sfaBuilder
                                    .setAverageChunkSize((float) sizeStats.getAverage())
                                    .setMinChunkSize(sizeStats.getMin())
                                    .setMaxChunkSize(sizeStats.getMax());
                        }

                        // document_analytics is only available for source_labels
                        // this pass actually processed. Merged-in SPRs from prior
                        // passes on other source_labels get no document_analytics
                        // stamp — that's correct: we don't have the source text
                        // for those in this request.
                        DocumentAnalytics da = docAnalyticsBySourceLabel.get(spr.getSourceFieldName());
                        if (da != null) {
                            sfaBuilder.setDocumentAnalytics(da);
                        }

                        smBuilder.addSourceFieldAnalytics(sfaBuilder.build());
                    }
                }

                // ----------------------------------------------------------------
                // Step 8: Lex sort semantic_results[] (§21.8)
                // ----------------------------------------------------------------
                mergedSprs.sort(Comparator
                        .comparing(SemanticProcessingResult::getSourceFieldName)
                        .thenComparing(SemanticProcessingResult::getChunkConfigId)
                        .thenComparing(SemanticProcessingResult::getEmbeddingConfigId)
                        .thenComparing(SemanticProcessingResult::getResultId));

                smBuilder.clearSemanticResults();
                smBuilder.addAllSemanticResults(mergedSprs);

                // ----------------------------------------------------------------
                // Step 9: Build output PipeDoc and response
                // ----------------------------------------------------------------
                PipeDoc outputDoc = inputDoc.toBuilder()
                        .setSearchMetadata(smBuilder.build())
                        .build();

                long duration = System.currentTimeMillis() - startTime;
                logs.add(moduleLog(
                        logPrefix + "Chunking completed in " + duration + "ms: produced "
                                + outputSprs.size() + " new SPR(s), "
                                + mergedSprs.size() + " total SPR(s) for document " + inputDoc.getDocId(),
                        LogLevel.LOG_LEVEL_INFO));

                return responseBuilder
                        .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                        .setOutputDoc(outputDoc)
                        .addAllLogEntries(logs)
                        .build();

            } catch (Exception e) {
                String errorMessage = "Error in ChunkerService: " + e.getMessage();
                LOG.error(errorMessage, e);
                return createErrorResponse(errorMessage, e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // =========================================================================
    // Per-(directive, config) processing
    // =========================================================================

    /**
     * Runs the cache-lookup → chunk → cache-writeback flow for a single
     * {@code (directive, NamedChunkerConfig)} pair and returns one
     * {@link SemanticProcessingResult} with a deterministic {@code result_id} and
     * empty {@code embedding_config_id} (stage-1 placeholder per DESIGN.md §4.1).
     *
     * <p>Audit log entries are appended to {@code logs}.
     */
    private SemanticProcessingResult processOneDirectiveConfig(
            PipeDoc inputDoc,
            String docHash,
            String sourceText,
            NlpPreprocessor.NlpResult nlpResult,
            VectorDirective directive,
            String directiveKey,
            NamedChunkerConfig namedConfig,
            ChunkerStepOptions options,
            String streamId,
            String pipeStepName,
            List<LogEntry> logs) {

        String sourceLabel = directive.getSourceLabel();
        String chunkerConfigId = namedConfig.getConfigId();

        // ------------------------------------------------------------------
        // Cache lookup (§7.1 step 3 — cache key per DESIGN.md §9.1)
        // ------------------------------------------------------------------
        List<SemanticChunk> chunks = null;
        if (options.effectiveCacheEnabled()) {
            List<SemanticChunk> cached = cacheService.get(sourceText, chunkerConfigId)
                    .await().indefinitely();
            if (!cached.isEmpty()) {
                chunks = cached;
                LOG.debugf("Chunk cache HIT for source_label='%s' config_id='%s' — %d chunk(s)",
                        sourceLabel, chunkerConfigId, chunks.size());
                logs.add(moduleLog(
                        "Cache HIT for source_label='" + sourceLabel
                                + "' config_id='" + chunkerConfigId
                                + "': reusing " + chunks.size() + " cached chunk(s)",
                        LogLevel.LOG_LEVEL_DEBUG));
            }
        }

        // ------------------------------------------------------------------
        // Cache miss — parse per-config struct and run chunker
        // ------------------------------------------------------------------
        if (chunks == null) {
            ChunkerConfig perConfigChunkerConfig = parseNamedChunkerConfig(namedConfig, sourceLabel);

            ChunkingResult result = overlapChunker.createChunks(
                    inputDoc, perConfigChunkerConfig, streamId, pipeStepName, nlpResult);
            List<Chunk> chunkRecords = result.chunks();

            logs.add(moduleLog(
                    "Chunked source_label='" + sourceLabel
                            + "' config_id='" + chunkerConfigId
                            + "' algorithm=" + perConfigChunkerConfig.algorithm()
                            + " → " + chunkRecords.size() + " chunk(s) from "
                            + sourceText.length() + " characters",
                    LogLevel.LOG_LEVEL_INFO));

            Map<String, String> placeholderToUrlMap = result.placeholderToUrlMap();

            // PR-I: when URL placeholder substitution happened, the doc-level
            // nlpResult's tokenSpans/sentenceSpans reference the ORIGINAL text
            // offsets, but the chunks below carry SUBSTITUTED-text offsets
            // (placeholder lengths shift positions). Slicing the doc-level NLP
            // with substituted offsets gives garbage. Pass null for slicing in
            // that case so each chunk falls back to running OpenNLP per chunk
            // (the legacy behavior). When no URLs were substituted, the
            // offsets align and we can reuse the doc-level NLP for both
            // base-counts (via slice) AND POS densities (via posTags/lemmas).
            //
            // This is the single biggest perf win in PR-I: in the common case
            // (no URLs substituted), per-chunk OpenNLP runs go from 4 to 0.
            NlpPreprocessor.NlpResult nlpForSlicing =
                    placeholderToUrlMap.isEmpty() ? nlpResult : null;

            chunks = new ArrayList<>();
            int chunkNumber = 0;
            for (Chunk c : chunkRecords) {
                // §21.5 deterministic chunk_id
                String chunkId = docHash + ":" + sourceLabel + ":" + chunkerConfigId
                        + ":" + chunkNumber + ":" + c.originalIndexStart() + ":" + c.originalIndexEnd();

                String sanitizedText = UnicodeSanitizer.sanitizeInvalidUnicode(c.text());

                boolean containsUrlPlaceholder = (perConfigChunkerConfig.preserveUrls() != null
                        && perConfigChunkerConfig.preserveUrls())
                        && !placeholderToUrlMap.isEmpty()
                        && placeholderToUrlMap.keySet().stream().anyMatch(ph -> c.text().contains(ph));

                // PR-I: compute the NLP slice ONCE per chunk and reuse it for
                // both extractAllMetadata and extractChunkAnalytics. If
                // nlpForSlicing is null (URL substitution invalidated offsets
                // for this whole pass) sliceForChunk returns null and both
                // metadata methods fall back to running OpenNLP per chunk.
                ChunkMetadataExtractor.ChunkNlpSlice nlpSlice =
                        metadataExtractor.sliceForChunk(nlpForSlicing,
                                c.originalIndexStart(), c.originalIndexEnd());

                Map<String, Value> extractedMetadata = metadataExtractor.extractAllMetadata(
                        sanitizedText, chunkNumber, chunkRecords.size(), containsUrlPlaceholder, nlpSlice);

                // §9 dedup groundwork: SHA-256 content_hash of the sanitised
                // chunk text, stamped into the chunk metadata map. Identical
                // sanitised text → identical hash, so reprocessing can skip
                // already-computed chunks, downstream embedders can use the
                // hash as a cache key, and a future OpenVINO chunker backend
                // can be byte-verified against this implementation via hash
                // equality on the same input. Same scheme the streaming impl
                // uses at ChunkerStreamingGrpcImpl.java:139-141.
                String contentHash = ChunkerSupport.sha256Hex(sanitizedText);
                extractedMetadata.put("content_hash",
                        Value.newBuilder().setStringValue(contentHash).build());

                // §4.1: chunk_analytics is ALWAYS populated. The streaming impl at
                // ChunkerStreamingGrpcImpl already does this; the non-streaming
                // rewrite must match so the output passes assertPostChunker.
                //
                // 8-arg overload: takes the pre-computed slice for base text
                // statistics AND the doc-level NlpResult for POS-density
                // slicing (posTags + lemmas binary search). Both use the
                // SAME slice work computed above — eliminates per-chunk
                // OpenNLP runs entirely in the common path.
                ai.pipestream.data.v1.ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                        sanitizedText, chunkNumber, chunkRecords.size(), containsUrlPlaceholder,
                        nlpSlice, nlpForSlicing,
                        c.originalIndexStart(), c.originalIndexEnd());

                // PR-K2: promote content_hash to the typed ChunkAnalytics field
                // (pipestream-protos PR #38 added the field). Keeps the loose-
                // map "content_hash" entry above intact for backward compat —
                // the duplication will be removed in a follow-up PR after a
                // consumer audit confirms no readers depend on the loose key.
                chunkAnalytics = chunkAnalytics.toBuilder()
                        .setContentHash(contentHash)
                        .build();

                ChunkEmbedding embedding = ChunkEmbedding.newBuilder()
                        .setTextContent(sanitizedText)
                        .setChunkId(chunkId)
                        .setOriginalCharStartOffset(c.originalIndexStart())
                        .setOriginalCharEndOffset(c.originalIndexEnd())
                        .setChunkConfigId(chunkerConfigId)
                        // vector intentionally NOT set — stage-1 placeholder (§4.1)
                        .build();

                SemanticChunk semanticChunk = SemanticChunk.newBuilder()
                        .setChunkId(chunkId)
                        .setChunkNumber(chunkNumber)
                        .setEmbeddingInfo(embedding)
                        .setChunkAnalytics(chunkAnalytics)
                        .putAllMetadata(extractedMetadata)
                        .build();

                chunks.add(semanticChunk);
                chunkNumber++;
            }

            // Cache writeback (§21.7: isRtbfSuppressed=false — future PR wires real predicate)
            if (options.effectiveCacheEnabled()) {
                cacheService.put(sourceText, chunkerConfigId, chunks,
                        options.effectiveCacheTtlSeconds(), false)
                        .await().indefinitely();
            }
        }

        // ------------------------------------------------------------------
        // Build SPR with deterministic result_id and directive_key stamp (§21.2, §21.5)
        // ------------------------------------------------------------------
        NlpDocumentAnalysis nlpAnalysis = ChunkerSupport.buildNlpAnalysis(nlpResult);

        // §21.5 deterministic result_id: stage1:{docHash}:{sourceLabel}:{chunkerConfigId}:
        String resultId = "stage1:" + docHash + ":" + sourceLabel + ":" + chunkerConfigId + ":";

        return SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .setSourceFieldName(sourceLabel)
                .setChunkConfigId(chunkerConfigId)
                .setEmbeddingConfigId("")   // KEY: empty = stage-1 placeholder (§4.1)
                .addAllChunks(chunks)
                .putMetadata("directive_key",
                        Value.newBuilder().setStringValue(directiveKey).build())
                .setNlpAnalysis(nlpAnalysis)
                .build();
    }

    // =========================================================================
    // Sentence-level SPR builder (§21.9)
    // =========================================================================

    /**
     * Builds per-sentence {@link SemanticChunk}s from the NLP result's sentence spans.
     * Uses deterministic chunk_id per §21.5:
     * {@code {docHash}:{sourceLabel}:{chunkerConfigId}:{chunkNumber}:{start}:{end}}
     */
    private List<SemanticChunk> buildSentenceChunks(
            String docHash,
            String sourceLabel,
            String chunkerConfigId,
            NlpPreprocessor.NlpResult nlpResult) {

        List<SemanticChunk> result = new ArrayList<>();
        String[] sentences = nlpResult.sentences();
        opennlp.tools.util.Span[] spans = nlpResult.sentenceSpans();

        if (sentences == null || sentences.length == 0) {
            return result;
        }

        for (int i = 0; i < sentences.length; i++) {
            String sentText = sentences[i];
            if (sentText == null || sentText.isBlank()) continue;

            int start = (spans != null && i < spans.length && spans[i] != null)
                    ? spans[i].getStart() : 0;
            int end = (spans != null && i < spans.length && spans[i] != null)
                    ? spans[i].getEnd() : sentText.length();

            String chunkId = docHash + ":" + sourceLabel + ":" + chunkerConfigId
                    + ":" + i + ":" + start + ":" + end;

            String sanitizedText = UnicodeSanitizer.sanitizeInvalidUnicode(sentText);

            // PR-I: Path B walks raw NLP sentence spans, so the doc-level
            // nlpResult is ALWAYS safe to slice here (no URL substitution
            // happens on this code path — sentences come straight from the
            // NLP run on the unmodified source text). Compute the slice
            // ONCE per sentence and pass it to both metadata methods so we
            // never re-run OpenNLP on individual sentence chunks.
            ChunkMetadataExtractor.ChunkNlpSlice nlpSlice =
                    metadataExtractor.sliceForChunk(nlpResult, start, end);

            // Legacy string-keyed metadata map — Path A populates it at
            // processOneDirectiveConfig, so Path B must too or downstream
            // consumers that read from SemanticChunk.metadata silently get
            // empty data on every sentence chunk. containsUrlPlaceholder=false
            // because Path B walks raw NLP sentence spans and never runs the
            // URL substitute/restore pipeline — any URL in sanitizedText is
            // a real URL, not a placeholder.
            Map<String, Value> extractedMetadata = metadataExtractor.extractAllMetadata(
                    sanitizedText, i, sentences.length, false, nlpSlice);

            // §9 dedup groundwork: SHA-256 content_hash of the sanitised
            // sentence text, stamped into the chunk metadata map. Same
            // scheme and key name as Path A so downstream dedup logic can
            // work uniformly across both paths. Identical sentences across
            // docs → identical hash → dedup candidate.
            String contentHash = ChunkerSupport.sha256Hex(sanitizedText);
            extractedMetadata.put("content_hash",
                    Value.newBuilder().setStringValue(contentHash).build());

            // §4.1: chunk_analytics is ALWAYS populated, including on sentences_internal chunks.
            // 8-arg overload uses the pre-computed slice for base text stats
            // AND the doc-level nlpResult for POS-density slicing. Sentence
            // spans always align with the NLP arrays so the slice is never
            // null on this path.
            ai.pipestream.data.v1.ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                    sanitizedText, i, sentences.length, false,
                    nlpSlice, nlpResult, start, end);

            // PR-K2: promote content_hash to the typed ChunkAnalytics field.
            // Same pattern as Path A — keeps the loose-map entry for
            // backward compat and adds the typed field as canonical.
            chunkAnalytics = chunkAnalytics.toBuilder()
                    .setContentHash(contentHash)
                    .build();

            ChunkEmbedding embedding = ChunkEmbedding.newBuilder()
                    .setTextContent(sanitizedText)
                    .setChunkId(chunkId)
                    .setOriginalCharStartOffset(start)
                    .setOriginalCharEndOffset(end)
                    .setChunkConfigId(chunkerConfigId)
                    // vector intentionally NOT set — stage-1 placeholder
                    .build();

            SemanticChunk chunk = SemanticChunk.newBuilder()
                    .setChunkId(chunkId)
                    .setChunkNumber(i)
                    .setEmbeddingInfo(embedding)
                    .setChunkAnalytics(chunkAnalytics)
                    .putAllMetadata(extractedMetadata)
                    .build();

            result.add(chunk);
        }

        return result;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Extracts source text from a {@link PipeDoc} using the directive's {@code cel_selector}
     * (or {@code source_label} as a fallback).
     *
     * <p>Pack-2 implements a minimal selector that covers the common dotted-path cases.
     * A real CEL evaluator is future work.
     */
    private String extractSourceText(PipeDoc doc, VectorDirective directive) {
        if (doc == null || !doc.hasSearchMetadata()) return null;

        String celSelector = directive.getCelSelector();
        String sourceLabel = directive.getSourceLabel();

        // Resolve via cel_selector (common paths)
        if (celSelector != null && !celSelector.isBlank()) {
            String lower = celSelector.toLowerCase().trim();
            if (lower.contains("body")) {
                return doc.getSearchMetadata().hasBody() ? doc.getSearchMetadata().getBody() : null;
            }
            if (lower.contains("title")) {
                return doc.getSearchMetadata().hasTitle() ? doc.getSearchMetadata().getTitle() : null;
            }
        }

        // Fallback: resolve by source_label
        return extractSourceText(doc, sourceLabel);
    }

    /**
     * Resolves a source text field by name from a {@link PipeDoc}'s {@code SearchMetadata}.
     * Kept from the original implementation for backwards-compatible label resolution.
     */
    private String extractSourceText(PipeDoc doc, String sourceField) {
        if (doc == null || !doc.hasSearchMetadata()) return null;
        if (sourceField == null) return null;
        return switch (sourceField.toLowerCase()) {
            case "body" -> doc.getSearchMetadata().hasBody() ? doc.getSearchMetadata().getBody() : null;
            case "title" -> doc.getSearchMetadata().hasTitle() ? doc.getSearchMetadata().getTitle() : null;
            case "doc_id" -> doc.getDocId();
            default -> null;
        };
    }

    /**
     * Parses a {@link NamedChunkerConfig}'s opaque {@link Struct} into a {@link ChunkerConfig}
     * POJO using Jackson + JsonFormat, and overrides {@code sourceField} with the directive's
     * {@code source_label} so the OverlapChunker reads from the correct field.
     *
     * <p>Fail-loud policy (§21.1 no-fallback rule): if the per-config Struct is
     * non-empty but fails to parse (bad field name, wrong type, malformed JSON),
     * throws {@link IllegalArgumentException} so the request returns
     * {@code PROCESSING_OUTCOME_FAILURE} with a human-readable error message.
     * An empty or missing Struct is NOT an error — it's the explicit "use
     * defaults" signal — but a malformed Struct means the caller sent garbage
     * and must be told, not silently demoted to the default token chunker.
     * Pre-R1 behavior threw on this case; R1-pack-2 accidentally demoted it to
     * a WARN-and-default, which the post-R1 correctness audit flagged.
     */
    private ChunkerConfig parseNamedChunkerConfig(NamedChunkerConfig namedConfig, String sourceLabel) {
        Struct configStruct = namedConfig.getConfig();
        ChunkerConfig base;
        if (configStruct == null || configStruct.getFieldsCount() == 0) {
            base = ChunkerConfig.createDefault();
        } else {
            try {
                String jsonStr = JsonFormat.printer().print(configStruct);
                base = objectMapper.readValue(jsonStr, ChunkerConfig.class);
            } catch (Exception e) {
                String msg = "Invalid NamedChunkerConfig '" + namedConfig.getConfigId()
                        + "' for source_label='" + sourceLabel + "': " + e.getMessage();
                LOG.warnf("INVALID_ARGUMENT: %s", msg);
                throw new IllegalArgumentException(msg, e);
            }
        }

        // Always inject source_label as the sourceField so the chunker reads the right field.
        // ChunkerConfig is immutable so we create a new one with the corrected sourceField.
        ChunkerConfig resolved = new ChunkerConfig(
                base.algorithm(),
                sourceLabel,        // override: directive source_label wins
                base.chunkSize(),
                base.chunkOverlap(),
                base.preserveUrls(),
                base.cleanText()
        );

        // Enforce structural constraints via ChunkerConfig.validate(): chunkSize
        // in [1, 10000], chunkOverlap in [0, 5000], chunkOverlap < chunkSize,
        // algorithm != SEMANTIC (not implemented). Pre-R1 relied on the REST-
        // layer JAX-RS @Min/@Max/@NotNull annotations, but the directive-driven
        // path goes through a Struct parse that bypasses JAX-RS validation —
        // so a caller could send {"chunkSize": 500, "chunkOverlap": 500} and
        // get a degenerate chunker whose token window never advances
        // (OverlapChunker clamps overlap at chunkSize-1 via
        // Math.max(1, tokens - overlap) to prevent an infinite loop, but the
        // output chunks are still unusable — pure overlap, zero novel tokens).
        //
        // Fail loud with IllegalArgumentException — the outer processData
        // catch block converts this to PROCESSING_OUTCOME_FAILURE with an
        // operator-visible log entry. Aligns with §21.1 no-silent-fallback.
        String validationError = resolved.validate();
        if (validationError != null) {
            String msg = "Invalid NamedChunkerConfig '" + namedConfig.getConfigId()
                    + "' for source_label='" + sourceLabel + "': " + validationError;
            LOG.warnf("INVALID_ARGUMENT: %s", msg);
            throw new IllegalArgumentException(msg);
        }

        return resolved;
    }

    private static LogEntry moduleLog(String message, LogLevel level) {
        return LogEntry.newBuilder()
                .setSource(LogEntrySource.LOG_ENTRY_SOURCE_MODULE)
                .setLevel(level)
                .setMessage(message)
                .setTimestampEpochMs(System.currentTimeMillis())
                .setModule(ModuleLogOrigin.newBuilder().setModuleName("chunker").build())
                .build();
    }

    /**
     * Per-directive resolution cached during Step 4 (NLP / chunker loop) so
     * Step 6 (always-emit sentences_internal) can reuse the source text,
     * NLP result, and directive key without re-extracting them. Pre-PR-H
     * Step 6 re-called {@code extractSourceText()},
     * {@code nlpByText.get()}, and {@code directiveKeys.get()} for every
     * directive on every request — pure waste because Step 4 had already
     * computed them.
     *
     * <p>Cached only for directives that actually had non-empty source
     * text in Step 4 (skipped directives don't enter the cache).
     */
    private record ResolvedDirective(
            String sourceText,
            NlpPreprocessor.NlpResult nlpResult,
            String directiveKey
    ) {}

    private ProcessDataResponse createErrorResponse(String errorMessage, Exception e) {
        ProcessDataResponse.Builder responseBuilder = ProcessDataResponse.newBuilder();
        responseBuilder.setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
        responseBuilder.addLogEntries(moduleLog(errorMessage, LogLevel.LOG_LEVEL_ERROR));

        Struct.Builder errorDetailsBuilder = Struct.newBuilder();
        errorDetailsBuilder.putFields("error_message",
                Value.newBuilder().setStringValue(errorMessage).build());
        if (e != null) {
            errorDetailsBuilder.putFields("error_type",
                    Value.newBuilder().setStringValue(e.getClass().getName()).build());
            if (e.getCause() != null) {
                errorDetailsBuilder.putFields("error_cause",
                        Value.newBuilder().setStringValue(e.getCause().getMessage()).build());
            }
        }
        responseBuilder.setErrorDetails(errorDetailsBuilder.build());
        return responseBuilder.build();
    }

    // =========================================================================
    // getServiceRegistration — unchanged from original
    // =========================================================================

    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.debug("Chunker service registration requested");

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("chunker")
                .setVersion(buildInfoProvider.getVersion())
                .putAllMetadata(buildInfoProvider.registrationMetadata())
                .setCapabilities(Capabilities.newBuilder().addTypes(CapabilityType.CAPABILITY_TYPE_UNSPECIFIED).build());

        // Use SchemaExtractorService to get a JSONForms-ready ChunkerConfig schema (refs resolved)
        Optional<String> schemaOptional = schemaExtractorService.extractChunkerConfigSchemaResolvedForJsonForms();

        if (schemaOptional.isPresent()) {
            String jsonSchema = schemaOptional.get();
            responseBuilder.setJsonConfigSchema(jsonSchema);
            LOG.debugf("Successfully extracted JSONForms-ready schema (%d characters)", jsonSchema.length());
            LOG.info("Returning JSON schema for chunker module (refs resolved).");
        } else {
            responseBuilder.setHealthCheckPassed(false);
            responseBuilder.setHealthCheckMessage("Failed to resolve ChunkerConfig schema for JSONForms");
            LOG.error("SchemaExtractorService could not resolve ChunkerConfig schema for JSONForms");
            return Uni.createFrom().item(responseBuilder.build());
        }

        // If test request is provided, perform health check
        if (request.hasTestRequest()) {
            LOG.debug("Performing health check with test request");
            return processData(request.getTestRequest())
                    .map(processResponse -> {
                        if (processResponse.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS) {
                            responseBuilder
                                    .setHealthCheckPassed(true)
                                    .setHealthCheckMessage("Chunker module is healthy - successfully processed test document");
                        } else {
                            responseBuilder
                                    .setHealthCheckPassed(false)
                                    .setHealthCheckMessage("Chunker module health check failed: " +
                                            processResponse.getLogEntriesList().stream()
                                                    .map(LogEntry::getMessage)
                                                    .reduce((a, b) -> a + "; " + b)
                                                    .orElse("unknown error"));
                        }
                        return responseBuilder.build();
                    })
                    .onFailure().recoverWithItem(error -> {
                        LOG.error("Health check failed with exception", error);
                        return responseBuilder
                                .setHealthCheckPassed(false)
                                .setHealthCheckMessage("Health check failed with exception: " + error.getMessage())
                                .build();
                    });
        } else {
            responseBuilder.setHealthCheckPassed(true);
            responseBuilder.setHealthCheckMessage("Chunker module is ready");
            return Uni.createFrom().item(responseBuilder.build());
        }
    }
}
