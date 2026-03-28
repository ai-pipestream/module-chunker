package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.service.ChunkMetadataExtractor;
import ai.pipestream.module.chunker.service.NlpPreprocessor;
import ai.pipestream.module.chunker.service.OverlapChunker;
import ai.pipestream.semantic.v1.ChunkAlgorithm;
import ai.pipestream.semantic.v1.ChunkConfigEntry;
import ai.pipestream.semantic.v1.SemanticChunkerService;
import ai.pipestream.semantic.v1.StreamChunksRequest;
import ai.pipestream.semantic.v1.StreamChunksResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Streaming gRPC service for chunking text. Implements the SemanticChunkerService
 * proto which returns a stream of chunks for a given text input.
 *
 * <p>Supports two modes:</p>
 * <ul>
 *   <li><b>Multi-config (new):</b> When {@code chunk_configs} is populated, runs NLP once
 *       and applies each config independently. Responses are tagged with chunk_config_id.
 *       NlpDocumentAnalysis is returned on the very last chunk.</li>
 *   <li><b>Legacy single-config:</b> Falls back to the original Struct-based config parsing
 *       when chunk_configs is empty.</li>
 * </ul>
 */
@Singleton
@GrpcService
public class ChunkerStreamingGrpcImpl implements SemanticChunkerService {

    private static final Logger LOG = Logger.getLogger(ChunkerStreamingGrpcImpl.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    OverlapChunker overlapChunker;

    @Inject
    ChunkMetadataExtractor metadataExtractor;

    @Inject
    NlpPreprocessor nlpPreprocessor;

    @Override
    public Multi<StreamChunksResponse> streamChunks(StreamChunksRequest request) {
        // Route to multi-config or legacy path
        if (request.getChunkConfigsCount() > 0) {
            return streamChunksMultiConfig(request);
        }
        return streamChunksLegacy(request);
    }

    // =========================================================================
    // Multi-config path: NLP runs once, each config applied independently
    // =========================================================================

    private Multi<StreamChunksResponse> streamChunksMultiConfig(StreamChunksRequest request) {
        return Multi.createFrom().emitter(emitter -> {
            try {
                String requestId = request.getRequestId();
                String docId = request.getDocId();
                String sourceFieldName = request.getSourceFieldName();
                String textContent = request.getTextContent();
                String effectiveSourceField = sourceFieldName.isEmpty() ? "body" : sourceFieldName;

                LOG.infof("StreamChunks multi-config request: requestId=%s, docId=%s, sourceField=%s, configCount=%d, textLength=%d",
                        requestId, docId, effectiveSourceField, request.getChunkConfigsCount(), textContent.length());

                // Run NLP ONCE on the full text
                NlpPreprocessor.NlpResult nlpResult = nlpPreprocessor.preprocess(textContent);

                // Compute document-level analytics once, enriched with NLP data
                DocumentAnalytics docAnalytics = metadataExtractor.extractDocumentAnalytics(textContent, nlpResult);

                // Build NlpDocumentAnalysis proto from the NLP result
                NlpDocumentAnalysis nlpAnalysis = buildNlpAnalysis(nlpResult);

                // Build a minimal PipeDoc for OverlapChunker
                PipeDoc pipeDoc = buildPipeDoc(docId, effectiveSourceField, textContent);

                // Process each config entry
                List<ChunkConfigEntry> configEntries = request.getChunkConfigsList();
                int totalConfigEntries = configEntries.size();

                for (int configIdx = 0; configIdx < totalConfigEntries; configIdx++) {
                    ChunkConfigEntry entry = configEntries.get(configIdx);
                    String chunkConfigId = entry.getChunkConfigId();
                    boolean isLastConfig = (configIdx == totalConfigEntries - 1);

                    // Convert proto ChunkerConfig to Java ChunkerConfig
                    ChunkerConfig javaConfig = convertProtoConfig(entry.getConfig(), effectiveSourceField);

                    // Create chunks using pre-computed NLP result
                    ChunkingResult result = overlapChunker.createChunks(
                            pipeDoc, javaConfig, requestId, chunkConfigId, nlpResult);
                    List<Chunk> chunks = result.chunks();

                    LOG.infof("Multi-config [%s]: produced %d chunks for requestId=%s",
                            chunkConfigId, chunks.size(), requestId);

                    // Stream each chunk for this config
                    for (int i = 0; i < chunks.size(); i++) {
                        Chunk chunk = chunks.get(i);
                        boolean isLastChunkInConfig = (i == chunks.size() - 1);
                        boolean isVeryLastChunk = isLastConfig && isLastChunkInConfig;

                        // Extract metadata (legacy map) and typed analytics
                        Map<String, com.google.protobuf.Value> chunkMetadata = metadataExtractor.extractAllMetadata(
                                chunk.text(), i, chunks.size(), false);
                        ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                                chunk.text(), i, chunks.size(), false, nlpPreprocessor);

                        // Compute SHA-256 content hash
                        String contentHash = sha256Hex(chunk.text());
                        chunkMetadata.put("content_hash",
                                com.google.protobuf.Value.newBuilder().setStringValue(contentHash).build());

                        StreamChunksResponse.Builder responseBuilder = StreamChunksResponse.newBuilder()
                                .setRequestId(requestId)
                                .setDocId(docId)
                                .setChunkId(chunk.id())
                                .setChunkNumber(i)
                                .setTextContent(chunk.text())
                                .setStartOffset(chunk.originalIndexStart())
                                .setEndOffset(chunk.originalIndexEnd())
                                .setChunkConfigId(chunkConfigId)
                                .setSourceFieldName(effectiveSourceField)
                                .setIsLast(isVeryLastChunk)
                                .putAllMetadata(chunkMetadata)
                                .setChunkAnalytics(chunkAnalytics);

                        // Populate document analytics on the last chunk of EACH config group
                        if (isLastChunkInConfig) {
                            responseBuilder.setDocumentAnalytics(docAnalytics)
                                    .setTotalChunks(chunks.size());
                        }

                        // Populate NLP analysis on the very last chunk overall
                        if (isVeryLastChunk) {
                            responseBuilder.setNlpAnalysis(nlpAnalysis);
                        }

                        emitter.emit(responseBuilder.build());
                    }
                }

                emitter.complete();

            } catch (Exception e) {
                LOG.errorf(e, "Error in StreamChunks (multi-config): %s", e.getMessage());
                emitter.fail(e);
            }
        });
    }

    // =========================================================================
    // Legacy single-config path: backward compatible with Struct-based config
    // =========================================================================

    private Multi<StreamChunksResponse> streamChunksLegacy(StreamChunksRequest request) {
        return Multi.createFrom().emitter(emitter -> {
            try {
                String requestId = request.getRequestId();
                String docId = request.getDocId();
                String sourceFieldName = request.getSourceFieldName();
                String textContent = request.getTextContent();
                String chunkConfigId = request.getChunkConfigId();

                LOG.infof("StreamChunks legacy request: requestId=%s, docId=%s, sourceField=%s, configId=%s, textLength=%d",
                        requestId, docId, sourceFieldName, chunkConfigId, textContent.length());

                // Parse chunker config from Struct
                ChunkerConfig chunkerConfig;
                if (request.hasChunkerConfig() && request.getChunkerConfig().getFieldsCount() > 0) {
                    String jsonStr = JsonFormat.printer().print(request.getChunkerConfig());
                    LOG.infof("Parsing chunker config JSON: %s", jsonStr);
                    chunkerConfig = objectMapper.readValue(jsonStr, ChunkerConfig.class);
                    LOG.infof("Parsed chunker config: algorithm=%s, chunkSize=%d, chunkOverlap=%d, sourceField=%s",
                            chunkerConfig.algorithm(), chunkerConfig.chunkSize(), chunkerConfig.chunkOverlap(), chunkerConfig.sourceField());
                } else {
                    chunkerConfig = ChunkerConfig.createDefault();
                    LOG.infof("Using default chunker config (no config in request)");
                }

                // Build a minimal PipeDoc to pass to OverlapChunker
                String effectiveSourceField = sourceFieldName.isEmpty() ? "body" : sourceFieldName;
                PipeDoc pipeDoc = buildPipeDoc(docId, effectiveSourceField, textContent);

                // Override sourceField in config to match
                ChunkerConfig effectiveConfig = new ChunkerConfig(
                        chunkerConfig.algorithm() != null ? chunkerConfig.algorithm() : ChunkingAlgorithm.TOKEN,
                        effectiveSourceField,
                        chunkerConfig.chunkSize(),
                        chunkerConfig.chunkOverlap(),
                        chunkerConfig.preserveUrls(),
                        chunkerConfig.cleanText()
                );

                // Create chunks
                ChunkingResult result = overlapChunker.createChunks(
                        pipeDoc, effectiveConfig, requestId, chunkConfigId);
                List<Chunk> chunks = result.chunks();

                LOG.infof("StreamChunks produced %d chunks for requestId=%s", chunks.size(), requestId);

                // Compute document-level analytics once for the full source text
                DocumentAnalytics docAnalytics = metadataExtractor.extractDocumentAnalytics(textContent);

                // Stream each chunk
                for (int i = 0; i < chunks.size(); i++) {
                    Chunk chunk = chunks.get(i);
                    boolean isLast = (i == chunks.size() - 1);

                    // Extract metadata (legacy map) and typed analytics
                    Map<String, com.google.protobuf.Value> chunkMetadata = metadataExtractor.extractAllMetadata(
                            chunk.text(), i, chunks.size(), false);
                    ChunkAnalytics chunkAnalytics = metadataExtractor.extractChunkAnalytics(
                            chunk.text(), i, chunks.size(), false);

                    // Compute SHA-256 content hash of the chunk text
                    String contentHash = sha256Hex(chunk.text());
                    chunkMetadata.put("content_hash",
                            com.google.protobuf.Value.newBuilder().setStringValue(contentHash).build());

                    StreamChunksResponse.Builder responseBuilder = StreamChunksResponse.newBuilder()
                            .setRequestId(requestId)
                            .setDocId(docId)
                            .setChunkId(chunk.id())
                            .setChunkNumber(i)
                            .setTextContent(chunk.text())
                            .setStartOffset(chunk.originalIndexStart())
                            .setEndOffset(chunk.originalIndexEnd())
                            .setChunkConfigId(chunkConfigId)
                            .setSourceFieldName(effectiveSourceField)
                            .setIsLast(isLast)
                            .putAllMetadata(chunkMetadata)
                            .setChunkAnalytics(chunkAnalytics);

                    // Populate document analytics and total_chunks on the last chunk only
                    if (isLast) {
                        responseBuilder.setDocumentAnalytics(docAnalytics)
                                .setTotalChunks(chunks.size());
                    }

                    emitter.emit(responseBuilder.build());
                }

                emitter.complete();

            } catch (Exception e) {
                LOG.errorf(e, "Error in StreamChunks: %s", e.getMessage());
                emitter.fail(e);
            }
        });
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Converts a proto {@link ai.pipestream.semantic.v1.ChunkerConfig} to the Java
     * {@link ChunkerConfig} record used by OverlapChunker.
     */
    private ChunkerConfig convertProtoConfig(ai.pipestream.semantic.v1.ChunkerConfig proto,
                                              String sourceField) {
        ChunkingAlgorithm algorithm = switch (proto.getAlgorithm()) {
            case CHUNK_ALGORITHM_SENTENCE -> ChunkingAlgorithm.SENTENCE;
            case CHUNK_ALGORITHM_CHARACTER -> ChunkingAlgorithm.CHARACTER;
            case CHUNK_ALGORITHM_TOKEN -> ChunkingAlgorithm.TOKEN;
            default -> ChunkingAlgorithm.TOKEN; // UNSPECIFIED defaults to TOKEN
        };

        int chunkSize = proto.getChunkSize() > 0 ? proto.getChunkSize() : ChunkerConfig.DEFAULT_CHUNK_SIZE;
        int chunkOverlap = proto.getChunkOverlap() >= 0 ? proto.getChunkOverlap() : ChunkerConfig.DEFAULT_CHUNK_OVERLAP;

        return new ChunkerConfig(
                algorithm,
                sourceField,
                chunkSize,
                chunkOverlap,
                proto.getPreserveUrls(),
                proto.getCleanText()
        );
    }

    /**
     * Builds a minimal PipeDoc with text content set on the appropriate field.
     */
    private PipeDoc buildPipeDoc(String docId, String sourceField, String textContent) {
        PipeDoc.Builder docBuilder = PipeDoc.newBuilder().setDocId(docId);
        SearchMetadata.Builder smBuilder = SearchMetadata.newBuilder();

        switch (sourceField.toLowerCase()) {
            case "title":
                smBuilder.setTitle(textContent);
                break;
            case "body":
            default:
                smBuilder.setBody(textContent);
                break;
        }
        docBuilder.setSearchMetadata(smBuilder.build());
        return docBuilder.build();
    }

    /**
     * Converts an {@link NlpPreprocessor.NlpResult} to the proto {@link NlpDocumentAnalysis}.
     * This carries the full NLP pass results back to the semantic-manager.
     */
    private NlpDocumentAnalysis buildNlpAnalysis(NlpPreprocessor.NlpResult nlpResult) {
        NlpDocumentAnalysis.Builder builder = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage(nlpResult.detectedLanguage())
                .setLanguageConfidence(nlpResult.languageConfidence())
                .setTotalTokens(nlpResult.tokens().length)
                .setNounDensity(nlpResult.nounDensity())
                .setVerbDensity(nlpResult.verbDensity())
                .setAdjectiveDensity(nlpResult.adjectiveDensity())
                .setAdverbDensity(nlpResult.adverbDensity())
                .setContentWordRatio(nlpResult.contentWordRatio())
                .setUniqueLemmaCount(nlpResult.uniqueLemmaCount())
                .setLexicalDensity(nlpResult.lexicalDensity());

        // Add sentence spans
        for (int i = 0; i < nlpResult.sentences().length; i++) {
            builder.addSentences(SentenceSpan.newBuilder()
                    .setText(nlpResult.sentences()[i])
                    .setStartOffset(nlpResult.sentenceSpans()[i].getStart())
                    .setEndOffset(nlpResult.sentenceSpans()[i].getEnd())
                    .build());
        }
        return builder.build();
    }

    /**
     * Computes the SHA-256 hash of the given text and returns it as a lowercase hex string.
     */
    private static String sha256Hex(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available in every JDK
            throw new AssertionError("SHA-256 not available", e);
        }
    }
}
