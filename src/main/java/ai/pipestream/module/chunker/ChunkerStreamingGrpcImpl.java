package ai.pipestream.module.chunker;

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
                    chunkerConfig = objectMapper.readValue(jsonStr, ChunkerConfig.class);
                } else {
                    chunkerConfig = ChunkerConfig.createDefault();
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

                // Stream each chunk
                for (int i = 0; i < chunks.size(); i++) {
                    Chunk chunk = chunks.get(i);
                    boolean isLast = (i == chunks.size() - 1);

                    // Extract metadata
                    Map<String, com.google.protobuf.Value> chunkMetadata = metadataExtractor.extractAllMetadata(
                            chunk.text(), i, chunks.size(), false);

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
                            .putAllMetadata(chunkMetadata);

                    emitter.emit(responseBuilder.build());
                }

                emitter.complete();

            } catch (Exception e) {
                LOG.errorf(e, "Error in StreamChunks: %s", e.getMessage());
                emitter.fail(e);
            }
        });
    }
}
