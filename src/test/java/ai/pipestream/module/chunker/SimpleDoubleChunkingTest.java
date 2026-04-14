package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.module.v1.ProcessingOutcome;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests sequential chunking of a document via the directive-driven path (R1-pack-2).
 *
 * <p><b>Migration note (R1-pack-2):</b> the legacy {@code json_config} path is gone.
 * Docs now carry {@code vector_set_directives}. Each call to {@code processData}
 * adds its SPRs on top of whatever the doc already has; downstream re-chunking of
 * the body field is represented by a second directive set with different config IDs.
 */
@QuarkusTest
public class SimpleDoubleChunkingTest {
    private static final Logger log = LoggerFactory.getLogger(SimpleDoubleChunkingTest.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    private static final String TEST_BODY =
            "This is a test document with enough content to be chunked multiple times. " +
            "It contains several sentences that will be processed by the chunker. " +
            "The first chunking will create large chunks, and the second chunking will " +
            "create smaller chunks from the large ones. This should result in multiple " +
            "semantic processing results in the final document. Each chunk will have " +
            "different characteristics based on the chunking configuration used.";

    @Test
    public void testSimpleDoubleChunking() throws IOException {
        // Create a simple test document
        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("test-double-chunk-001")
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Test Document for Double Chunking")
                .setBody(TEST_BODY)
                .build())
            .build();

        // First chunking: large chunks (200 tokens, 50 overlap)
        VectorSetDirectives firstDirectives =
                TestDirectiveBuilder.withSingleTokenDirective("body", 200, 50);
        PipeDoc firstChunked = performChunking(testDoc, firstDirectives, "first-chunking");
        assertNotNull(firstChunked, "First chunking should succeed");
        assertTrue(firstChunked.hasSearchMetadata(), "First chunked doc should have search metadata");

        int firstSprCount = firstChunked.getSearchMetadata().getSemanticResultsCount();
        log.info("First chunking created {} semantic results", firstSprCount);
        assertTrue(firstSprCount >= 1, "First chunking should produce at least 1 SPR");

        // Second chunking: small chunks (50 tokens, 10 overlap) — different config_id so IDs don't collide
        VectorSetDirectives secondDirectives =
                TestDirectiveBuilder.withSingleTokenDirective("body", 50, 10);
        PipeDoc doubleChunked = performChunking(firstChunked, secondDirectives, "second-chunking");
        assertNotNull(doubleChunked, "Second chunking should succeed");

        // Verify double chunking structure — both SPRs accumulate in search_metadata
        assertTrue(doubleChunked.hasSearchMetadata(), "Document should have search metadata");

        int totalSprCount = doubleChunked.getSearchMetadata().getSemanticResultsCount();
        log.info("After double chunking: {} total semantic results", totalSprCount);
        assertTrue(totalSprCount >= 2,
            "Document should have at least 2 semantic result sets after double chunking " +
            "(one per unique chunker config, plus sentences_internal if emitted)");

        // Save the result for inspection
        saveDoubleChunkedDocument(doubleChunked);

        log.info("Simple double chunking test completed successfully");
    }

    private PipeDoc performChunking(PipeDoc doc, VectorSetDirectives directives, String stepName) {
        // Merge directives onto the existing search_metadata (preserve existing SPRs)
        SearchMetadata existingMeta = doc.hasSearchMetadata()
                ? doc.getSearchMetadata()
                : SearchMetadata.getDefaultInstance();
        SearchMetadata updatedMeta = existingMeta.toBuilder()
                .setVectorSetDirectives(directives)
                .build();
        PipeDoc docWithDirectives = doc.toBuilder().setSearchMetadata(updatedMeta).build();

        try {
            ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("simple-double-chunking-test")
                .setPipeStepName(stepName)
                .setStreamId(UUID.randomUUID().toString())
                .setCurrentHopNumber(1)
                .build();

            ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
                .setJsonConfig(com.google.protobuf.Struct.getDefaultInstance())
                .build();

            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(docWithDirectives)
                .setMetadata(metadata)
                .setConfig(processConfig)
                .build();

            ProcessDataResponse response = chunkerService.processData(request)
                .await().indefinitely();

            if (response.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS && response.hasOutputDoc()) {
                log.info("{} successful for doc: {}", stepName, doc.getDocId());
                return response.getOutputDoc();
            } else {
                log.warn("{} failed for doc: {} — logs: {}", stepName, doc.getDocId(),
                    response.getLogEntriesList().stream()
                        .map(ai.pipestream.data.v1.LogEntry::getMessage)
                        .reduce((a, b) -> a + "; " + b).orElse("none"));
                return null;
            }
        } catch (Exception e) {
            log.error("Error in {} for doc {}: {}", stepName, doc.getDocId(), e.getMessage());
            return null;
        }
    }

    private void saveDoubleChunkedDocument(PipeDoc doc) throws IOException {
        Path outputDir = Paths.get("modules/chunker/src/test/resources/double_chunked_pipedocs");
        Files.createDirectories(outputDir);

        Path outputFile = outputDir.resolve("simple_double_chunked_001.pb");
        Files.write(outputFile, doc.toByteArray());

        log.info("Saved double-chunked document to {}", outputFile);
    }
}
