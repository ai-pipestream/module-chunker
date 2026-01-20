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

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class SimpleDoubleChunkingTest {
    private static final Logger log = LoggerFactory.getLogger(SimpleDoubleChunkingTest.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    public void testSimpleDoubleChunking() throws IOException {
        // Create a simple test document
        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("test-double-chunk-001")
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Test Document for Double Chunking")
                .setBody("This is a test document with enough content to be chunked multiple times. " +
                        "It contains several sentences that will be processed by the chunker. " +
                        "The first chunking will create large chunks, and the second chunking will " +
                        "create smaller chunks from the large ones. This should result in multiple " +
                        "semantic processing results in the final document. Each chunk will have " +
                        "different characteristics based on the chunking configuration used.")
                .build())
            .build();

        // First chunking: Large chunks (200 chars, 50 overlap)
        PipeDoc firstChunked = performChunking(testDoc, createLargeChunkConfig(), "first-chunking");
        assertNotNull(firstChunked, "First chunking should succeed");
        log.info("First chunking created {} semantic results",
            firstChunked.hasSearchMetadata() ? firstChunked.getSearchMetadata().getSemanticResultsCount() : 0);

        // Second chunking: Small chunks (100 chars, 20 overlap)
        PipeDoc doubleChunked = performChunking(firstChunked, createSmallChunkConfig(), "second-chunking");
        assertNotNull(doubleChunked, "Second chunking should succeed");

        // Verify double chunking structure
        assertTrue(doubleChunked.hasSearchMetadata(), "Document should have search metadata");
        assertEquals(2, doubleChunked.getSearchMetadata().getSemanticResultsCount(),
            "Document should have exactly 2 semantic result sets after double chunking");

        // Save the result
        saveDoubleChunkedDocument(doubleChunked);

        log.info("Simple double chunking test completed successfully");
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
                .setPipelineName("simple-double-chunking-test")
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
                .await().indefinitely();

            if (response.getSuccess() && response.hasOutputDoc()) {
                log.info("{} successful for doc: {}", stepName, doc.getDocId());
                return response.getOutputDoc();
            } else {
                log.warn("{} failed for doc: {}", stepName, doc.getDocId());
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
