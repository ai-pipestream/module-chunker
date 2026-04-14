package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import com.google.protobuf.Struct;
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

/**
 * Generates double-chunk test data via the directive-driven path (R1-pack-2).
 *
 * <p><b>Migration note (R1-pack-2):</b> the legacy {@code ChunkerOptions} / {@code json_config}
 * path is gone. Docs carry {@code vector_set_directives}; chunker config params live inside
 * {@link ai.pipestream.data.v1.NamedChunkerConfig#getConfig()}.
 */
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

        // First chunking: Large chunks (200 tokens, 50 overlap) from "body"
        VectorSetDirectives largeDirectives = TestDirectiveBuilder.withSingleTokenDirective("body", 200, 50);
        PipeDoc firstChunked = performChunking(testDoc, largeDirectives, "first-chunking");
        if (firstChunked == null) {
            throw new RuntimeException("First chunking failed");
        }
        log.info("First chunking completed");

        // Second chunking: Small chunks (100 tokens, 20 overlap) from "body"
        VectorSetDirectives smallDirectives = TestDirectiveBuilder.withSingleTokenDirective("body", 100, 20);
        PipeDoc doubleChunked = performChunking(firstChunked, smallDirectives, "second-chunking");
        if (doubleChunked == null) {
            throw new RuntimeException("Second chunking failed");
        }
        log.info("Second chunking completed");

        // Save the result with absolute path
        String projectRoot = System.getProperty("user.dir");
        Path outputDir = Paths.get(projectRoot, "src", "test", "resources", "double_chunked_pipedocs");
        Files.createDirectories(outputDir);

        Path outputFile = outputDir.resolve("test_double_chunked_001.pb");
        Files.write(outputFile, doubleChunked.toByteArray());

        log.info("Double-chunked test data saved to: {}", outputFile.toAbsolutePath());

        // Verify the structure
        int sprCount = doubleChunked.hasSearchMetadata()
                ? doubleChunked.getSearchMetadata().getSemanticResultsCount()
                : 0;
        if (sprCount >= 2) {
            log.info("Verification passed: Document has {} semantic result sets", sprCount);
        } else {
            log.warn("Verification note: Document has {} semantic result set(s) — expected >= 2", sprCount);
        }
    }

    /**
     * Merges {@code directives} onto the existing {@code search_metadata} of {@code doc}
     * (preserving SPRs from prior passes) and invokes the chunker service.
     *
     * @return the output document on success, {@code null} on failure
     */
    private PipeDoc performChunking(PipeDoc doc, VectorSetDirectives directives, String stepName) {
        try {
            // Merge directives onto existing search_metadata (preserve existing SPRs)
            SearchMetadata existingMeta = doc.hasSearchMetadata()
                    ? doc.getSearchMetadata()
                    : SearchMetadata.getDefaultInstance();
            SearchMetadata updatedMeta = existingMeta.toBuilder()
                    .setVectorSetDirectives(directives)
                    .build();
            PipeDoc docWithDirectives = doc.toBuilder().setSearchMetadata(updatedMeta).build();

            ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("double-chunking-test")
                .setPipeStepName(stepName)
                .setStreamId(UUID.randomUUID().toString())
                .setCurrentHopNumber(1)
                .build();

            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(docWithDirectives)
                .setMetadata(metadata)
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

            ProcessDataResponse response = chunkerService.processData(request)
                .await().atMost(java.time.Duration.ofSeconds(30));

            if (response.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS && response.hasOutputDoc()) {
                log.info("{} successful for doc: {}", stepName, doc.getDocId());
                return response.getOutputDoc();
            } else {
                log.error("{} failed for doc: {} — outcome: {}", stepName, doc.getDocId(), response.getOutcome());
                return null;
            }
        } catch (Exception e) {
            log.error("Error in {} for doc {}: {}", stepName, doc.getDocId(), e.getMessage(), e);
            return null;
        }
    }
}
