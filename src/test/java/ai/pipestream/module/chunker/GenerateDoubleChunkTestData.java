package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.module.chunker.model.ChunkerOptions;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@QuarkusTest
public class GenerateDoubleChunkTestData {
    private static final Logger log = LoggerFactory.getLogger(GenerateDoubleChunkTestData.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    public void generateDoubleChunkedTestData() throws IOException {
        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("test-double-chunk-001")
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Test Document for Double Chunking")
                .setBody("This is a comprehensive test document designed for double chunking validation. " +
                        "It contains multiple sentences and paragraphs that will be processed by the chunker service. " +
                        "The first chunking pass will create larger chunks with significant overlap. " +
                        "The second chunking pass will create smaller, more granular chunks from the larger ones. " +
                        "This approach allows for both broad context preservation and fine-grained semantic analysis. " +
                        "Each chunk will maintain metadata about its position and relationship to the original content. " +
                        "The double chunking strategy is particularly useful for complex documents where multiple " +
                        "levels of granularity are needed for effective information retrieval and processing.")
                .build())
            .build();

        log.info("Starting double chunking test data generation...");

        // First chunking: Large chunks (200 chars, 50 overlap)
        PipeDoc firstChunked = performChunking(testDoc, createLargeChunkConfig(), "first-chunking");
        if (firstChunked == null) {
            throw new RuntimeException("First chunking failed");
        }
        log.info("First chunking completed");

        // Second chunking: Small chunks (100 chars, 20 overlap)
        PipeDoc doubleChunked = performChunking(firstChunked, createSmallChunkConfig(), "second-chunking");
        if (doubleChunked == null) {
            throw new RuntimeException("Second chunking failed");
        }
        log.info("Second chunking completed");

        // Save the result with absolute path
        String projectRoot = System.getProperty("user.dir");
        Path outputDir = Paths.get(projectRoot, "modules", "chunker", "src", "test", "resources", "double_chunked_pipedocs");
        Files.createDirectories(outputDir);

        Path outputFile = outputDir.resolve("test_double_chunked_001.pb");
        Files.write(outputFile, doubleChunked.toByteArray());

        log.info("Double-chunked test data saved to: {}", outputFile.toAbsolutePath());

        // Verify the structure
        if (doubleChunked.hasSearchMetadata() && doubleChunked.getSearchMetadata().getSemanticResultsCount() == 2) {
            log.info("Verification passed: Document has 2 semantic result sets");
        } else {
            log.warn("Verification failed: Expected 2 semantic result sets, got {}",
                doubleChunked.hasSearchMetadata() ? doubleChunked.getSearchMetadata().getSemanticResultsCount() : 0);
        }
    }

    private ChunkerOptions createLargeChunkConfig() {
        return new ChunkerOptions(
            "body",                    // sourceField
            200,                       // chunkSize
            50,                        // chunkOverlap
            "%s_%s_chunk_%d",         // chunkIdTemplate
            "large_chunks_v1",        // chunkConfigId
            "first_chunks_%s_%s",     // resultSetNameTemplate
            "[LARGE-CHUNK] ",         // logPrefix
            false                     // preserveUrls
        );
    }

    private ChunkerOptions createSmallChunkConfig() {
        return new ChunkerOptions(
            "body",                    // sourceField
            100,                       // chunkSize
            20,                        // chunkOverlap
            "%s_%s_chunk_%d",         // chunkIdTemplate
            "small_chunks_v1",        // chunkConfigId
            "second_chunks_%s_%s",    // resultSetNameTemplate
            "[SMALL-CHUNK] ",         // logPrefix
            false                     // preserveUrls
        );
    }

    private PipeDoc performChunking(PipeDoc doc, ChunkerOptions config, String stepName) {
        try {
            ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("double-chunking-test")
                .setPipeStepName(stepName)
                .setStreamId(UUID.randomUUID().toString())
                .setCurrentHopNumber(1)
                .build();

            ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
                .setJsonConfig(config.toStruct())
                .build();

            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(metadata)
                .setConfig(processConfig)
                .build();

            ProcessDataResponse response = chunkerService.processData(request)
                .await().atMost(java.time.Duration.ofSeconds(30));

            if (response.getSuccess() && response.hasOutputDoc()) {
                log.info("{} successful for doc: {}", stepName, doc.getDocId());
                return response.getOutputDoc();
            } else {
                log.error("{} failed for doc: {}", stepName, doc.getDocId());
                return null;
            }
        } catch (Exception e) {
            log.error("Error in {} for doc {}: {}", stepName, doc.getDocId(), e.getMessage(), e);
            return null;
        }
    }
}
