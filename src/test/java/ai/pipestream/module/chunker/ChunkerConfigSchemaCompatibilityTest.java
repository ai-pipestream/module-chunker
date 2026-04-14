package ai.pipestream.module.chunker;

import com.google.protobuf.Struct;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Verifies that the chunker service advertises the ChunkerConfig schema and that
 * directive-driven requests (R1-pack-2 path) are accepted and produce correct output.
 *
 * <p><b>Migration note (R1-pack-2):</b> the {@code processData} test now uses
 * directive-driven input per DESIGN.md §7.1. The schema advertisement test
 * ({@link #schemaShouldAdvertiseChunkerConfigFields}) is unchanged because the
 * schema registration reflects the per-config Struct format that lives inside
 * {@link ai.pipestream.data.v1.NamedChunkerConfig#getConfig()}.
 */
@QuarkusTest
public class ChunkerConfigSchemaCompatibilityTest {

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    void schemaShouldAdvertiseChunkerConfigFields() {
        var registration = chunkerService.getServiceRegistration(GetServiceRegistrationRequest.newBuilder().build())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        assertThat("Registration should include JSON schema", !registration.getJsonConfigSchema().isEmpty(), is(true));
        String schema = registration.getJsonConfigSchema();

        // Basic sanity checks for expected ChunkerConfig fields
        assertThat(schema, containsString("sourceField"));
        assertThat(schema, containsString("chunkSize"));
        assertThat(schema, containsString("chunkOverlap"));
        assertThat(schema, containsString("preserveUrls"));
        assertThat(schema, containsString("cleanText"));
        assertThat(schema, not(containsString("config_id")));

        // Ensure legacy snake_case options are not promoted by schema (guardrail)
        assertThat(schema, not(containsString("source_field")));
        assertThat(schema, not(containsString("chunk_size")));
        assertThat(schema, not(containsString("chunk_overlap")));
        assertThat(schema, not(containsString("preserve_urls")));
    }

    @Test
    void processDataShouldAcceptDirectiveDrivenRequest() {
        // R1-pack-2: doc carries vector_set_directives; NamedChunkerConfig.config
        // holds the per-config chunker parameters (token algorithm, chunkSize=120).
        String configId = "token_120_24";
        VectorSetDirectives directives = TestDirectiveBuilder
                .withSingleTokenDirective("body", 120, 24);

        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("schema-compat-doc-" + UUID.randomUUID())
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Schema Compat Title")
                .setBody("This body text will be chunked using directive-driven JSON config fields " +
                        "through the new DESIGN.md §7.1 path. Adding enough words to produce real output.")
                .setVectorSetDirectives(directives)
                .build())
            .build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
            .setPipelineName("schema-compat-pipeline")
            .setPipeStepName("schema-compat-step")
            .setStreamId(UUID.randomUUID().toString())
            .setCurrentHopNumber(1)
            .build();

        // json_config holds ChunkerStepOptions (empty → defaults)
        ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
            .setJsonConfig(Struct.getDefaultInstance())
            .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
            .setDocument(testDoc)
            .setMetadata(metadata)
            .setConfig(processConfig)
            .build();

        var response = chunkerService.processData(request)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        assertThat("Directive-driven request should be accepted",
                response.getOutcome(), is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Output document should be present", response.hasOutputDoc(), is(true));
        assertThat("Semantic results should be created",
                response.getOutputDoc().getSearchMetadata().getSemanticResultsCount(),
                is(greaterThan(0)));

        // Find the main SPR (not sentences_internal)
        var mainSpr = response.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> !"sentences_internal".equals(spr.getChunkConfigId()))
                .findFirst().orElse(null);
        assertThat("Should have at least one non-sentences_internal SPR", mainSpr, is(notNullValue()));
        assertThat("chunk_config_id should match the NamedChunkerConfig.config_id",
                mainSpr.getChunkConfigId(), is(equalTo("token_120_24")));
        assertThat("Should produce at least one chunk", mainSpr.getChunksCount(), is(greaterThan(0)));
    }
}
