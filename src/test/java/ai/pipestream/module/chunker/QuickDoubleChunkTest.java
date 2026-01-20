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
public class QuickDoubleChunkTest {
    private static final Logger log = LoggerFactory.getLogger(QuickDoubleChunkTest.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    public void createDoubleChunkedData() throws IOException {
        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("quick-test-001")
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Quick Test Document")
                .setBody("This is a quick test document for double chunking. " +
                        "It has enough content to create multiple chunks. " +
                        "The first pass will create large chunks. " +
                        "The second pass will create smaller chunks from those. " +
                        "This should result in two semantic processing results.")
                .build())
            .build();

        // First chunking
        PipeDoc firstChunked = chunk(testDoc, new ChunkerOptions(
            "body", 100, 20, "%s_%s_chunk_%d", "large_v1",
            "first_%s_%s", "[FIRST] ", false));

        // Second chunking
        PipeDoc doubleChunked = chunk(firstChunked, new ChunkerOptions(
            "body", 50, 10, "%s_%s_chunk_%d", "small_v1",
            "second_%s_%s", "[SECOND] ", false));

        // Save result
        Path outputDir = Paths.get("modules/chunker/src/test/resources/double_chunked_pipedocs");
        Files.createDirectories(outputDir);
        Files.write(outputDir.resolve("quick_double_001.pb"), doubleChunked.toByteArray());

        log.info("Created double-chunked test data");
    }

    private PipeDoc chunk(PipeDoc doc, ChunkerOptions config) {
        try {
            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                    .setPipelineName("quick-test")
                    .setPipeStepName("chunker")
                    .setStreamId(UUID.randomUUID().toString())
                    .build())
                .setConfig(ProcessConfiguration.newBuilder()
                    .setJsonConfig(config.toStruct())
                    .build())
                .build();

            ProcessDataResponse response = chunkerService.processData(request)
                .await().atMost(java.time.Duration.ofSeconds(30));

            return response.getSuccess() ? response.getOutputDoc() : doc;
        } catch (Exception e) {
            log.error("Chunking failed: {}", e.getMessage());
            return doc;
        }
    }
}
