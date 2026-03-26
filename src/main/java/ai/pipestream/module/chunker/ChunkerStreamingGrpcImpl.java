package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.service.ChunkMetadataExtractor;
import ai.pipestream.module.chunker.service.OverlapChunker;
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
 * Reuses the existing OverlapChunker and ChunkMetadataExtractor beans.
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

    @Override
    public Multi<StreamChunksResponse> streamChunks(StreamChunksRequest request) {
        return Multi.createFrom().emitter(emitter -> {
            try {
                String requestId = request.getRequestId();
                String docId = request.getDocId();
                String sourceFieldName = request.getSourceFieldName();
                String textContent = request.getTextContent();
                String chunkConfigId = request.getChunkConfigId();

                LOG.infof("StreamChunks request: requestId=%s, docId=%s, sourceField=%s, configId=%s, textLength=%d",
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
                PipeDoc.Builder docBuilder = PipeDoc.newBuilder().setDocId(docId);
                SearchMetadata.Builder smBuilder = SearchMetadata.newBuilder();

                // Set the text content on the appropriate field
                switch (sourceFieldName.toLowerCase()) {
                    case "title":
                        smBuilder.setTitle(textContent);
                        break;
                    case "body":
                    default:
                        smBuilder.setBody(textContent);
                        break;
                }
                docBuilder.setSearchMetadata(smBuilder.build());

                // Override sourceField in config to match
                String effectiveSourceField = sourceFieldName.isEmpty() ? "body" : sourceFieldName;
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
                        docBuilder.build(), effectiveConfig, requestId, chunkConfigId);
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
