package ai.pipestream.module.chunker;

import com.google.protobuf.Struct;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.ServiceMetadata;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Shared abstract base for chunker service integration tests.
 *
 * <p><b>Migration note (R1-pack-2):</b> all tests that exercise
 * {@code processData} now use the directive-driven path introduced by DESIGN.md
 * §7.1. Docs carry {@code search_metadata.vector_set_directives}; the legacy
 * per-step {@code json_config} now carries {@link
 * ai.pipestream.module.chunker.config.ChunkerStepOptions} (or is left empty for
 * defaults). The old {@code algorithm}/{@code sourceField}/{@code chunkSize}
 * fields in {@code json_config} are no longer supported.
 *
 * <p>Use {@link TestDirectiveBuilder} to construct directives for test docs.
 */
public abstract class ChunkerServiceTestBase {

    protected abstract PipeStepProcessorService getChunkerService();

    // -------------------------------------------------------------------------
    // Happy-path: basic directive-driven processing
    // -------------------------------------------------------------------------

    @Test
    void testProcessData() {
        // Build a PipeDoc with vector_set_directives (directive-driven path, §21.1)
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("This is a test document body with enough words to be chunked properly " +
                                "by the directive-driven pipeline. Adding more content to ensure the chunker " +
                                "has real text to process through the new directive flow.")
                        .setTitle("Test Document")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("chunker-step")
                .setStreamId(UUID.randomUUID().toString())
                .setCurrentHopNumber(1)
                .putContextParams("tenant", "test-tenant")
                .build();

        // json_config carries ChunkerStepOptions (empty struct → defaults)
        ProcessConfiguration config = ProcessConfiguration.newBuilder()
                .setJsonConfig(Struct.getDefaultInstance())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(metadata)
                .setConfig(config)
                .build();

        var response = getChunkerService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Response should be successful", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Response should have output document", response.hasOutputDoc(), is(true));
        assertThat("Output document ID should match input",
                response.getOutputDoc().getDocId(), is(testDoc.getDocId()));
        assertThat("Should have processor logs",
                response.getLogEntriesList().stream().map(LogEntry::getMessage).toList(),
                is(not(empty())));
        assertThat("Should produce semantic results with directive-driven chunking",
                response.getOutputDoc().getSearchMetadata().getSemanticResultsCount(),
                is(greaterThan(0)));
    }

    // -------------------------------------------------------------------------
    // No-document case — unchanged behaviour
    // -------------------------------------------------------------------------

    @Test
    void testProcessDataWithoutDocument() {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("test-pipeline")
                        .setPipeStepName("chunker-step")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder().build())
                .build();

        var response = getChunkerService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Chunker should handle missing document gracefully", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("No document means no output should be produced", response.hasOutputDoc(), is(false));
        assertThat("Should log processing attempt for missing document",
                response.getLogEntriesList().stream().map(LogEntry::getMessage).toList(),
                is(not(empty())));
        assertThat("Should acknowledge no document to process",
                response.getLogEntriesList().stream().map(LogEntry::getMessage).toList(),
                hasItem(containsString("no document to process")));
    }

    // -------------------------------------------------------------------------
    // Empty body — directive present, source text empty → SPR-less success
    // -------------------------------------------------------------------------

    @Test
    void testProcessDataWithEmptyContent() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("")
                        .setTitle("Empty Document")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("chunker-step")
                .setStreamId(UUID.randomUUID().toString())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(metadata)
                .setConfig(ProcessConfiguration.newBuilder().build())
                .build();

        var response = getChunkerService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Chunker should handle empty content without failing", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Empty document should still produce output structure", response.hasOutputDoc(), is(true));
        assertThat("Document identity should be preserved through empty processing",
                response.getOutputDoc().getDocId(), is(testDoc.getDocId()));
        assertThat("Should log empty content handling",
                response.getLogEntriesList().stream().map(LogEntry::getMessage).toList(),
                is(not(empty())));
    }

    // -------------------------------------------------------------------------
    // Custom configuration — two chunker configs via directive
    // -------------------------------------------------------------------------

    @Test
    void testProcessDataWithCustomConfiguration() {
        String longBody = "This is a longer test document body that should be chunked into multiple pieces " +
                "based on the custom configuration settings. We want to ensure that the chunker " +
                "respects the custom chunk size and overlap settings. This text should be long " +
                "enough to be split into at least two chunks with the smaller chunk size setting. " +
                "Adding even more text here to ensure that we exceed the minimum valid chunk size " +
                "of fifty tokens, so that the splitting logic is actually exercised during this test.";

        // Use a small chunk size (50 tokens) to guarantee multiple chunks
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 50, 10);

        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(longBody)
                        .setTitle("Custom Config Test")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("chunker-step")
                .setStreamId(UUID.randomUUID().toString())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(metadata)
                .setConfig(ProcessConfiguration.newBuilder().build())
                .build();

        var response = getChunkerService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Custom configuration should be processed successfully", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Should produce chunked output document", response.hasOutputDoc(), is(true));

        var searchMetadata = response.getOutputDoc().getSearchMetadata();
        assertThat("Should create semantic results with custom config",
                searchMetadata.getSemanticResultsCount(), is(greaterThan(0)));
        assertThat("Should preserve original document metadata",
                response.getOutputDoc().getDocId(), is(equalTo(testDoc.getDocId())));

        // Find the main SPR (not sentences_internal)
        var mainSpr = searchMetadata.getSemanticResultsList().stream()
                .filter(spr -> !"sentences_internal".equals(spr.getChunkConfigId()))
                .findFirst().orElse(null);

        assertThat("Should have at least one non-sentences_internal SPR", mainSpr, is(notNullValue()));
        assertThat("Small chunk size should create multiple chunks",
                mainSpr.getChunksCount(), is(greaterThan(1)));

        for (int i = 0; i < mainSpr.getChunksCount(); i++) {
            var chunk = mainSpr.getChunks(i);
            assertThat(String.format("Chunk %d should have content", i),
                    chunk.getEmbeddingInfo().getTextContent(), is(not(emptyString())));
            assertThat(String.format("Chunk %d should be reasonable size", i),
                    chunk.getEmbeddingInfo().getTextContent().length(), is(lessThanOrEqualTo(500)));
        }
    }

    // -------------------------------------------------------------------------
    // URL preservation — directive-driven
    // -------------------------------------------------------------------------

    @Test
    void testProcessDataWithUrlPreservation() {
        String bodyWithUrls = "This document contains URLs like https://example.com and http://test.org/page.html " +
                "that should be preserved during chunking. The URLs should not be split across chunks.";

        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 100, 20);

        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(bodyWithUrls)
                        .setTitle("URL Test")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
                .setPipelineName("test-pipeline")
                .setPipeStepName("chunker-step")
                .setStreamId(UUID.randomUUID().toString())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setMetadata(metadata)
                .setConfig(ProcessConfiguration.newBuilder().build())
                .build();

        var response = getChunkerService().processData(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("URL preservation chunking should succeed", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Should produce output with URL handling", response.hasOutputDoc(), is(true));
        assertThat("Should create semantic results for URL content",
                response.getOutputDoc().getSearchMetadata().getSemanticResultsCount(),
                is(greaterThan(0)));

        var mainSpr = response.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> !"sentences_internal".equals(spr.getChunkConfigId()))
                .findFirst().orElse(null);
        assertThat("Should have at least one non-sentences_internal SPR", mainSpr, is(notNullValue()));
        assertThat("Should produce chunks", mainSpr.getChunksCount(), is(greaterThan(0)));
    }

    // -------------------------------------------------------------------------
    // Null request — unchanged behaviour
    // -------------------------------------------------------------------------

    @Test
    void testNullRequest() {
        var response = getChunkerService().processData(null)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Chunker should gracefully handle null input without crashing", response.getOutcome(),
                is(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS));
        assertThat("Should provide diagnostic information for null request",
                response.getLogEntriesList().stream().map(LogEntry::getMessage).toList(),
                is(not(empty())));
        assertThat("Should not attempt to process null as valid document", response.hasOutputDoc(), is(false));
    }

    // -------------------------------------------------------------------------
    // Service registration — unchanged
    // -------------------------------------------------------------------------

    @Test
    void testGetServiceRegistration() {
        var registration = getChunkerService()
                .getServiceRegistration(GetServiceRegistrationRequest.newBuilder().build())
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem();

        assertThat("Service should identify itself as chunker module",
                registration.getModuleName(), is(equalTo("chunker")));
        assertThat("Registration should include configuration schema for clients",
                !registration.getJsonConfigSchema().isEmpty(), is(true));
        assertThat("Schema should contain text chunking configuration description",
                registration.getJsonConfigSchema(),
                containsString("Configuration for text chunking operations"));
        assertThat("Schema should be valid JSON structure",
                registration.getJsonConfigSchema().length(), is(greaterThan(10)));
    }
}
