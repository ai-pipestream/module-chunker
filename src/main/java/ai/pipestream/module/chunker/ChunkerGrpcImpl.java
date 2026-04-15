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

        final boolean isTest = request.getIsTest();
        final String logPrefix = isTest ? "[TEST] " : "";
        final long startTime = System.currentTimeMillis();
        final List<LogEntry> logs = new ArrayList<>();

        // =============================================================
        // Phase 1: synchronous validation + NLP (no I/O)
        //
        // Everything in this phase is pure CPU work: parsing options,
        // validating directives, running OpenNLP per unique source text,
        // computing DocumentAnalytics. We do it inline in a Uni.createFrom()
        // supplier so that any failure short-circuits to the top-level
        // onFailure() recovery and produces a PROCESSING_OUTCOME_FAILURE.
        // =============================================================

        if (!request.hasDocument()) {
            LOG.info(logPrefix + "No document provided in request");
            return Uni.createFrom().item(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                    .addLogEntries(moduleLog("Chunker service: no document to process", LogLevel.LOG_LEVEL_INFO))
                    .build());
        }

        final PipeDoc inputDoc = request.getDocument();
        final ProcessConfiguration config = request.getConfig();
        final ServiceMetadata metadata = request.getMetadata();
        final String streamId = metadata.getStreamId();
        final String pipeStepName = metadata.getPipeStepName();

        LOG.infof("%sProcessing document ID: %s for step: %s in stream: %s",
                logPrefix,
                inputDoc.getDocId() != null ? inputDoc.getDocId() : "unknown",
                pipeStepName, streamId);

        // ----------------------------------------------------------------
        // Step 1: Parse ChunkerStepOptions from json_config (DESIGN.md §7.1 step 1)
        // ----------------------------------------------------------------
        final ChunkerStepOptions options;
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
                return Uni.createFrom().item(createErrorResponse(msg, e));
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
            return Uni.createFrom().item(createErrorResponse(msg, null));
        }

        final List<VectorDirective> directives = inputDoc.getSearchMetadata()
                .getVectorSetDirectives()
                .getDirectivesList();

        if (directives.isEmpty()) {
            String msg = "Empty vector_set_directives — at least one directive required";
            LOG.warnf("FAILED_PRECONDITION: %s", msg);
            return Uni.createFrom().item(createErrorResponse(msg, null));
        }

        logs.add(moduleLog(logPrefix + "Processing " + directives.size()
                + " directive(s) for document " + inputDoc.getDocId(),
                LogLevel.LOG_LEVEL_INFO));

        // ----------------------------------------------------------------
        // Step 3: Validate source_label uniqueness + compute directive_keys (§21.2)
        // ----------------------------------------------------------------
        Set<String> seenLabels = new HashSet<>();
        final Map<VectorDirective, String> directiveKeys = new LinkedHashMap<>();
        for (VectorDirective d : directives) {
            if (!seenLabels.add(d.getSourceLabel())) {
                String msg = "Duplicate source_label '" + d.getSourceLabel()
                        + "' in vector_set_directives — INVALID_ARGUMENT";
                LOG.warnf("INVALID_ARGUMENT: %s", msg);
                return Uni.createFrom().item(createErrorResponse(msg, null));
            }
            directiveKeys.put(d, DirectiveKeyComputer.compute(d));
        }

        // ----------------------------------------------------------------
        // Step 4: Synchronous NLP pass — one NlpResult per unique source text,
        // one DocumentAnalytics per source_label. Also build the ResolvedDirective
        // cache Step 6 (sentences_internal) reuses, and the per-(directive,config)
        // Uni list that drives the reactive fan-out.
        //
        // NLP is pure CPU work (OpenNLP, no I/O) so running it inline in the
        // reactive composition is safe and avoids a redundant Uni wrapper per
        // directive. Cache by source text so two directives selecting the same
        // field (rare but legal) reuse one NLP run.
        // ----------------------------------------------------------------
        final String docHash = DirectiveKeyComputer.sha256b64url(inputDoc.getDocId());
        final Map<String, NlpPreprocessor.NlpResult> nlpByText = new HashMap<>();
        final Map<String, DocumentAnalytics> docAnalyticsBySourceLabel = new LinkedHashMap<>();
        final Map<VectorDirective, ResolvedDirective> resolvedDirectives = new LinkedHashMap<>();
        final List<Uni<SemanticProcessingResult>> perConfigTasks = new ArrayList<>();

        // Synchronised wrapper so per-(directive,config) Unis completing on
        // different threads can safely append audit log entries. Only the
        // cache GET/PUT and the chunker build run off the caller thread —
        // the chunker is CPU work and these log adds happen at most once
        // per directive-config, so the synchronisation cost is negligible.
        final List<LogEntry> taskLogs = java.util.Collections.synchronizedList(new ArrayList<>());

        try {
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

                // Compute DocumentAnalytics ONCE per source_label (§21.2 enforces
                // source_label uniqueness, so one SFA per label is correct).
                final String srcLabelForDa = sourceLabel;
                final String srcTextForDa = sourceText;
                final NlpPreprocessor.NlpResult nlpForDa = nlpResult;
                docAnalyticsBySourceLabel.computeIfAbsent(srcLabelForDa,
                        k -> metadataExtractor.extractDocumentAnalytics(srcTextForDa, nlpForDa));

                String directiveKey = directiveKeys.get(directive);

                // Cache the resolution so Step 6 can reuse it.
                resolvedDirectives.put(directive,
                        new ResolvedDirective(sourceText, nlpResult, directiveKey));

                // Fan out one Uni per (directive, named chunker config) pair.
                // Each Uni is the reactive flow: cache GET → on miss run chunker
                // and cache PUT → build SPR. They run in parallel via
                // Uni.combine().all().unis(...) below.
                for (NamedChunkerConfig namedConfig : directive.getChunkerConfigsList()) {
                    perConfigTasks.add(processOneDirectiveConfigReactive(
                            inputDoc, docHash, sourceText, nlpResult,
                            directive, directiveKey, namedConfig,
                            options, streamId, pipeStepName, taskLogs));
                }
            }
        } catch (Exception e) {
            // Synchronous preparation failed (e.g. parseNamedChunkerConfig is
            // called lazily inside processOneDirectiveConfigReactive, but
            // anything thrown out of the loop above — extractSourceText,
            // NLP preprocess, DocumentAnalytics extraction — lands here).
            String errorMessage = "Error in ChunkerService: " + e.getMessage();
            LOG.error(errorMessage, e);
            return Uni.createFrom().item(createErrorResponse(errorMessage, e));
        }

        // =============================================================
        // Phase 2: reactive fan-out over per-(directive,config) tasks.
        // Fail-fast: any one failing Uni fails the combined Uni, which is
        // caught by the top-level onFailure recovery. No worker-pool
        // offload — the cache service runs on the Vert.x event loop and
        // the chunker CPU work runs on whichever thread the cache callback
        // resumes on (worker pool via Mutiny's default scheduling).
        // =============================================================
        final Uni<List<SemanticProcessingResult>> allTasksUni;
        if (perConfigTasks.isEmpty()) {
            allTasksUni = Uni.createFrom().item(java.util.Collections.emptyList());
        } else {
            allTasksUni = Uni.combine().all().unis(perConfigTasks)
                    .with(rawResults -> {
                        List<SemanticProcessingResult> collected = new ArrayList<>(rawResults.size());
                        for (Object o : rawResults) {
                            collected.add((SemanticProcessingResult) o);
                        }
                        return collected;
                    });
        }

        // =============================================================
        // Phase 3: final assembly (sync CPU work, done inside .map()).
        //
        // Steps 6–9 (sentences_internal emission, SFA rebuild, lex-sort,
        // build output doc) all need the fully-populated SPR list and have
        // no I/O, so they run inline after the combined Uni resolves.
        // =============================================================
        return allTasksUni
                .map(outputSprs -> {
                    // Transfer task-scoped log entries into the request-level log list.
                    // They were collected on whichever threads the per-config Unis
                    // resumed on; the synchronised wrapper guarantees no interleaving.
                    synchronized (taskLogs) {
                        logs.addAll(taskLogs);
                    }

                    List<SemanticProcessingResult> sprAccumulator = new ArrayList<>(outputSprs);

                    // --------------------------------------------------------
                    // Step 6: Always-emit sentences_internal SPR (§21.9)
                    //
                    // §21.9 is explicit: "No opt-out knob in chunker/embedder/semantic-graph
                    // for sentence emission." The ChunkerStepOptions.always_emit_sentences
                    // field is retained in the record for JSON-config back-compat but is
                    // deliberately NOT consulted here — sentences_internal is always emitted
                    // regardless of what the caller puts in the options JSON.
                    // --------------------------------------------------------
                    boolean alreadyHasSentenceSpr = sprAccumulator.stream()
                            .anyMatch(spr -> SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()));

                    if (!alreadyHasSentenceSpr) {
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

                            sprAccumulator.add(sentencesSpr);
                            logs.add(moduleLog(
                                    "Emitted sentences_internal SPR for source_label='" + sourceLabel
                                            + "' with " + sentenceChunks.size() + " sentence(s)",
                                    LogLevel.LOG_LEVEL_DEBUG));
                        }
                    }

                    // --------------------------------------------------------
                    // Step 7: Merge with existing SPRs and build source_field_analytics.
                    // Preserve SPRs from prior passes whose (source, config_id) pair is
                    // NOT produced in this pass — lets sequential double-chunking
                    // accumulate SPRs across passes without overwriting each other.
                    // --------------------------------------------------------
                    SearchMetadata.Builder smBuilder = inputDoc.getSearchMetadata().toBuilder();

                    Set<String> thisPassKeys = new HashSet<>();
                    for (SemanticProcessingResult spr : sprAccumulator) {
                        thisPassKeys.add(spr.getSourceFieldName() + "|" + spr.getChunkConfigId());
                    }

                    List<SemanticProcessingResult> mergedSprs = new ArrayList<>();
                    for (SemanticProcessingResult existingSpr : inputDoc.getSearchMetadata().getSemanticResultsList()) {
                        String key = existingSpr.getSourceFieldName() + "|" + existingSpr.getChunkConfigId();
                        if (!thisPassKeys.contains(key)) {
                            mergedSprs.add(existingSpr);
                        }
                    }
                    mergedSprs.addAll(sprAccumulator);

                    // Rebuild source_field_analytics from ALL merged SPRs (§5.1).
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

                            DocumentAnalytics da = docAnalyticsBySourceLabel.get(spr.getSourceFieldName());
                            if (da != null) {
                                sfaBuilder.setDocumentAnalytics(da);
                            }

                            smBuilder.addSourceFieldAnalytics(sfaBuilder.build());
                        }
                    }

                    // --------------------------------------------------------
                    // Step 8: Lex sort semantic_results[] (§21.8)
                    // --------------------------------------------------------
                    mergedSprs.sort(Comparator
                            .comparing(SemanticProcessingResult::getSourceFieldName)
                            .thenComparing(SemanticProcessingResult::getChunkConfigId)
                            .thenComparing(SemanticProcessingResult::getEmbeddingConfigId)
                            .thenComparing(SemanticProcessingResult::getResultId));

                    smBuilder.clearSemanticResults();
                    smBuilder.addAllSemanticResults(mergedSprs);

                    // --------------------------------------------------------
                    // Step 9: Build output PipeDoc and response
                    // --------------------------------------------------------
                    PipeDoc outputDoc = inputDoc.toBuilder()
                            .setSearchMetadata(smBuilder.build())
                            .build();

                    long duration = System.currentTimeMillis() - startTime;
                    logs.add(moduleLog(
                            logPrefix + "Chunking completed in " + duration + "ms: produced "
                                    + sprAccumulator.size() + " new SPR(s), "
                                    + mergedSprs.size() + " total SPR(s) for document " + inputDoc.getDocId(),
                            LogLevel.LOG_LEVEL_INFO));

                    return ProcessDataResponse.newBuilder()
                            .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                            .setOutputDoc(outputDoc)
                            .addAllLogEntries(logs)
                            .build();
                })
                .onFailure().recoverWithItem(throwable -> {
                    String errorMessage = "Error in ChunkerService: " + throwable.getMessage();
                    LOG.error(errorMessage, throwable);
                    return createErrorResponse(errorMessage,
                            throwable instanceof Exception ex ? ex : new RuntimeException(throwable));
                });
    }

    // =========================================================================
    // Per-(directive, config) processing — reactive
    // =========================================================================

    /**
     * Reactive composition for a single {@code (directive, NamedChunkerConfig)}
     * pair. Shape:
     * <pre>
     *   cache GET → (on miss: run chunker sync → cache PUT) → build SPR
     * </pre>
     * Fan-out over these Unis happens in {@link #processData}. The chunker
     * itself is synchronous CPU work (no I/O) and runs inline inside
     * {@code chain(...)} on whichever thread the cache callback resumes on.
     *
     * <p>Audit log entries are appended to {@code logs}, which is the
     * synchronised list owned by the caller so multiple per-config Unis
     * completing on different threads can safely share it.
     *
     * <p>Returns one {@link SemanticProcessingResult} with a deterministic
     * {@code result_id} and empty {@code embedding_config_id} (stage-1
     * placeholder per DESIGN.md §4.1).
     */
    private Uni<SemanticProcessingResult> processOneDirectiveConfigReactive(
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

        final String sourceLabel = directive.getSourceLabel();
        final String chunkerConfigId = namedConfig.getConfigId();

        // Per-config chunker config is parsed eagerly so a bad struct fails the
        // composed Uni (which the top-level onFailure catches). The parse is a
        // pure CPU step with no I/O — no need to defer it into chain().
        final ChunkerConfig perConfigChunkerConfig = parseNamedChunkerConfig(namedConfig, sourceLabel);

        // Chunker execution + SemanticChunk build, callable in two places
        // (cache miss, and the cache-disabled path).
        final java.util.function.Supplier<List<SemanticChunk>> runChunker = () ->
                buildChunksForConfig(inputDoc, docHash, sourceText, nlpResult,
                        sourceLabel, chunkerConfigId, perConfigChunkerConfig,
                        streamId, pipeStepName, logs);

        final Uni<List<SemanticChunk>> chunksUni;
        if (options.effectiveCacheEnabled()) {
            chunksUni = cacheService.get(sourceText, chunkerConfigId)
                    .chain(cached -> {
                        if (cached != null && !cached.isEmpty()) {
                            LOG.debugf("Chunk cache HIT for source_label='%s' config_id='%s' — %d chunk(s)",
                                    sourceLabel, chunkerConfigId, cached.size());
                            logs.add(moduleLog(
                                    "Cache HIT for source_label='" + sourceLabel
                                            + "' config_id='" + chunkerConfigId
                                            + "': reusing " + cached.size() + " cached chunk(s)",
                                    LogLevel.LOG_LEVEL_DEBUG));
                            return Uni.createFrom().item(cached);
                        }
                        // Cache miss — run chunker synchronously, then write back
                        // and replace the Uni's item with the fresh chunks.
                        List<SemanticChunk> freshChunks = runChunker.get();
                        return cacheService.put(sourceText, chunkerConfigId, freshChunks,
                                        options.effectiveCacheTtlSeconds(), false)
                                .replaceWith(freshChunks);
                    });
        } else {
            // No cache: defer the chunker run so any thrown exception lands on
            // the Uni's failure channel (not as a synchronous throw at call site).
            chunksUni = Uni.createFrom().item(runChunker);
        }

        return chunksUni.map(chunks -> {
            // Build SPR with deterministic result_id and directive_key stamp (§21.2, §21.5)
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
        });
    }

    /**
     * Runs {@link OverlapChunker#createChunks} and converts the raw
     * {@link Chunk} records into {@link SemanticChunk} protos with
     * deterministic chunk_ids (§21.5) and full chunk_analytics (§4.1,
     * including PR-K2 content_hash). Pure synchronous CPU work — safe to
     * call from inside a Uni chain callback.
     */
    private List<SemanticChunk> buildChunksForConfig(
            PipeDoc inputDoc,
            String docHash,
            String sourceText,
            NlpPreprocessor.NlpResult nlpResult,
            String sourceLabel,
            String chunkerConfigId,
            ChunkerConfig perConfigChunkerConfig,
            String streamId,
            String pipeStepName,
            List<LogEntry> logs) {

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
        NlpPreprocessor.NlpResult nlpForSlicing =
                placeholderToUrlMap.isEmpty() ? nlpResult : null;

        List<SemanticChunk> chunks = new ArrayList<>(chunkRecords.size());
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

            // PR-I: compute the NLP slice ONCE per chunk so extractChunkAnalytics
            // doesn't re-run OpenNLP on the chunk text.
            ChunkMetadataExtractor.ChunkNlpSlice nlpSlice =
                    metadataExtractor.sliceForChunk(nlpForSlicing,
                            c.originalIndexStart(), c.originalIndexEnd());

            // §4.1: chunk_analytics is ALWAYS populated.
            ai.pipestream.data.v1.ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                    sanitizedText, chunkNumber, chunkRecords.size(), containsUrlPlaceholder,
                    nlpSlice, nlpForSlicing,
                    c.originalIndexStart(), c.originalIndexEnd());

            // PR-K2 promoted content_hash to the typed ChunkAnalytics field.
            String contentHash = ChunkerSupport.sha256Hex(sanitizedText);
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

            // PR-K3: SemanticChunk.metadata is no longer populated by the chunker.
            SemanticChunk semanticChunk = SemanticChunk.newBuilder()
                    .setChunkId(chunkId)
                    .setChunkNumber(chunkNumber)
                    .setEmbeddingInfo(embedding)
                    .setChunkAnalytics(chunkAnalytics)
                    .build();

            chunks.add(semanticChunk);
            chunkNumber++;
        }

        return chunks;
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
            // ONCE per sentence so extractChunkAnalytics doesn't re-run
            // OpenNLP on individual sentence chunks.
            ChunkMetadataExtractor.ChunkNlpSlice nlpSlice =
                    metadataExtractor.sliceForChunk(nlpResult, start, end);

            // §4.1: chunk_analytics is ALWAYS populated, including on sentences_internal chunks.
            // 8-arg overload uses the pre-computed slice for base text stats
            // AND the doc-level nlpResult for POS-density slicing. Sentence
            // spans always align with the NLP arrays so the slice is never
            // null on this path.
            ai.pipestream.data.v1.ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                    sanitizedText, i, sentences.length, false,
                    nlpSlice, nlpResult, start, end);

            // PR-K2 promoted content_hash to the typed ChunkAnalytics field.
            // PR-K3 removed the duplicate write to the loose metadata map.
            String contentHash = ChunkerSupport.sha256Hex(sanitizedText);
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

            // PR-K3: SemanticChunk.metadata not populated. All fields it
            // used to hold now live exclusively on chunk_analytics.
            SemanticChunk chunk = SemanticChunk.newBuilder()
                    .setChunkId(chunkId)
                    .setChunkNumber(i)
                    .setEmbeddingInfo(embedding)
                    .setChunkAnalytics(chunkAnalytics)
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
