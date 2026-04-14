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

/**
 * Generates quick double-chunk test data via the directive-driven path (R1-pack-2).
 *
 * <p><b>Migration note (R1-pack-2):</b> the legacy {@code json_config} path is gone.
 * Docs carry {@code vector_set_directives}; chunker config params live inside
 * {@link ai.pipestream.data.v1.NamedChunkerConfig#getConfig()}.
 */
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

        // First chunking: large chunks
        VectorSetDirectives firstDirectives = TestDirectiveBuilder.withSingleTokenDirective("body", 100, 20);
        PipeDoc firstChunked = chunk(testDoc, firstDirectives);

        // Second chunking: small chunks — different config_id so IDs don't collide
        VectorSetDirectives secondDirectives = TestDirectiveBuilder.withSingleTokenDirective("body", 50, 10);
        PipeDoc doubleChunked = chunk(firstChunked, secondDirectives);

        // Save result
        Path outputDir = Paths.get("modules/chunker/src/test/resources/double_chunked_pipedocs");
        Files.createDirectories(outputDir);
        Files.write(outputDir.resolve("quick_double_001.pb"), doubleChunked.toByteArray());

        log.info("Created double-chunked test data");
    }

    private PipeDoc chunk(PipeDoc doc, VectorSetDirectives directives) {
        // Merge directives onto existing search_metadata (preserve prior SPRs)
        SearchMetadata existingMeta = doc.hasSearchMetadata()
                ? doc.getSearchMetadata()
                : SearchMetadata.getDefaultInstance();
        SearchMetadata updatedMeta = existingMeta.toBuilder()
                .setVectorSetDirectives(directives)
                .build();
        PipeDoc docWithDirectives = doc.toBuilder().setSearchMetadata(updatedMeta).build();

        try {
            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(docWithDirectives)
                .setMetadata(ServiceMetadata.newBuilder()
                    .setPipelineName("quick-test")
                    .setPipeStepName("chunker")
                    .setStreamId(UUID.randomUUID().toString())
                    .build())
                .setConfig(ProcessConfiguration.newBuilder()
                    .setJsonConfig(com.google.protobuf.Struct.getDefaultInstance())
                    .build())
                .build();

            ProcessDataResponse response = chunkerService.processData(request)
                .await().atMost(java.time.Duration.ofSeconds(30));

            return response.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS
                    ? response.getOutputDoc()
                    : doc;
        } catch (Exception e) {
            log.error("Chunking failed: {}", e.getMessage());
            return doc;
        }
    }
}
