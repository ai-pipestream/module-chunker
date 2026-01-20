package ai.pipestream.module.chunker;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.module.v1.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Verifies that the chunker service advertises the ChunkerConfig schema and that
 * requests constructed with ChunkerConfig-compatible JSON field names are accepted.
 * This acts as a guard to keep tests and config serialization aligned with the service contract.
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
        assertThat(schema, containsString("config_id"));

        // Ensure legacy snake_case options are not promoted by schema (guardrail)
        assertThat(schema, not(containsString("source_field")));
        assertThat(schema, not(containsString("chunk_size")));
        assertThat(schema, not(containsString("chunk_overlap")));
        assertThat(schema, not(containsString("preserve_urls")));
    }

    @Test
    void processDataShouldAcceptChunkerConfigJson() {
        // Minimal test document
        PipeDoc testDoc = PipeDoc.newBuilder()
            .setDocId("schema-compat-doc-" + UUID.randomUUID())
            .setSearchMetadata(SearchMetadata.newBuilder()
                .setTitle("Schema Compat Title")
                .setBody("This body text will be chunked using ChunkerConfig-compatible JSON fields.")
                .build())
            .build();

        // Service metadata
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
            .setPipelineName("schema-compat-pipeline")
            .setPipeStepName("schema-compat-step")
            .setStreamId(UUID.randomUUID().toString())
            .setCurrentHopNumber(1)
            .build();

        // Construct a ChunkerConfig-compatible Struct
        String forcedConfigId = "schema_compat_chunks_v1";
        Struct.Builder cfg = Struct.newBuilder()
            .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
            .putFields("sourceField", Value.newBuilder().setStringValue("body").build())
            .putFields("chunkSize", Value.newBuilder().setNumberValue(120).build())
            .putFields("chunkOverlap", Value.newBuilder().setNumberValue(24).build())
            .putFields("preserveUrls", Value.newBuilder().setBoolValue(true).build())
            .putFields("cleanText", Value.newBuilder().setBoolValue(true).build())
            .putFields("config_id", Value.newBuilder().setStringValue(forcedConfigId).build());

        ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
            .setJsonConfig(cfg.build())
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

        assertThat("ChunkerConfig JSON should be accepted", response.getSuccess(), is(true));
        assertThat("Output document should be present", response.hasOutputDoc(), is(true));
        assertThat("Semantic results should be created", response.getOutputDoc().getSearchMetadata().getSemanticResultsCount(), is(greaterThan(0)));

        var result = response.getOutputDoc().getSearchMetadata().getSemanticResults(0);
        assertThat("Result should use provided config_id", result.getChunkConfigId(), is(equalTo(forcedConfigId)));
        assertThat("Should produce at least one chunk", result.getChunksCount(), is(greaterThan(0)));
    }
}
