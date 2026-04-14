package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.module.chunker.directive.DirectiveKeyComputer;
import com.google.protobuf.Struct;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract-lock test for the directive-driven {@code ChunkerGrpcImpl.processData}
 * rewrite (R1-pack-2, DESIGN.md §7.1).
 *
 * <p>This test verifies all post-chunker invariants from DESIGN.md §5.1 inline
 * (rather than via {@code SemanticPipelineInvariants.assertPostChunker}) to work
 * around Quarkus classloader isolation that prevents calling a static helper loaded
 * from a file-dep jar when the helper references proto classes from the application
 * classloader.
 *
 * <p>All assertions use AssertJ with descriptive {@code .as()} messages per
 * {@code feedback-assertj-preference.md}. If any assertion fails, fix the
 * production code — not the test.
 */
@QuarkusTest
class ChunkerStepInvariantsTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    // -------------------------------------------------------------------------
    // Shared test content
    // -------------------------------------------------------------------------

    private static final String BODY_TEXT =
            "The directive-driven chunker refactor replaces the legacy single-config path " +
            "with a VectorSetDirectives-based flow per DESIGN.md §7.1. " +
            "This test validates that the output doc satisfies all post-chunker invariants. " +
            "We include enough sentences here for the NLP preprocessor to produce real results " +
            "and for the sentence-level SPR to be populated unconditionally per §21.9. " +
            "The chunker must produce deterministic chunk IDs, empty embedding_config_id, " +
            "non-empty source_field_analytics, and a directive_key in every SPR's metadata map.";

    // =========================================================================
    // Happy-path: single directive, two chunker configs, contract-lock assertion
    // =========================================================================

    @Test
    void outputShouldSatisfyPostChunkerInvariants_singleDirectiveTwoConfigs() {
        VectorSetDirectives directives = TestDirectiveBuilder.withTwoConfigDirective("body");

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("invariants-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setTitle("Invariants Test Title")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("invariants-test-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId("invariants-stream-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request)
                .await().indefinitely();

        assertThat(response.getOutcome())
                .as("processData should return PROCESSING_OUTCOME_SUCCESS for a valid directive-driven request")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        assertThat(response.hasOutputDoc())
                .as("Response must include an output document")
                .isTrue();

        PipeDoc outputDoc = response.getOutputDoc();

        // ---- Contract-lock: inline post-chunker invariants (DESIGN.md §5.1) ----
        assertPostChunkerInvariants(outputDoc);

        // ---- directive_key stamped on every SPR (§21.2) ----
        outputDoc.getSearchMetadata().getSemanticResultsList().forEach(spr -> {
            assertThat(spr.getMetadataMap())
                    .as("Every SPR must have 'directive_key' in its metadata map per DESIGN.md §21.2 " +
                        "(SPR source_field_name='" + spr.getSourceFieldName()
                        + "' chunk_config_id='" + spr.getChunkConfigId() + "')")
                    .containsKey("directive_key");

            String directiveKeyValue = spr.getMetadataMap().get("directive_key").getStringValue();
            assertThat(directiveKeyValue)
                    .as("directive_key metadata value must be non-empty in SPR for chunk_config_id='"
                        + spr.getChunkConfigId() + "'")
                    .isNotEmpty();
        });

        // ---- Deterministic chunk IDs — no UUIDs anywhere (§21.5) ----
        outputDoc.getSearchMetadata().getSemanticResultsList().forEach(spr ->
            spr.getChunksList().forEach(chunk -> {
                String chunkId = chunk.getChunkId();
                assertThat(chunkId)
                        .as("chunk_id must not be a UUID (deterministic IDs required per §21.5). " +
                            "Found: '" + chunkId + "'")
                        .doesNotMatch(
                            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

                assertThat(chunkId)
                        .as("chunk_id must be non-empty for every chunk in SPR chunk_config_id='"
                            + spr.getChunkConfigId() + "'")
                        .isNotEmpty();
            })
        );

        // ---- Deterministic result_id starts with "stage1:" (§21.5) ----
        outputDoc.getSearchMetadata().getSemanticResultsList().forEach(spr ->
            assertThat(spr.getResultId())
                    .as("result_id must follow 'stage1:{docHash}:...' pattern per §21.5. " +
                        "Found: '" + spr.getResultId() + "'")
                    .startsWith("stage1:")
        );

        // ---- No UUIDs in result_ids ----
        outputDoc.getSearchMetadata().getSemanticResultsList().forEach(spr ->
            assertThat(spr.getResultId())
                    .as("result_id must not be a UUID per §21.5. Found: '" + spr.getResultId() + "'")
                    .doesNotMatch(
                        "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        );

        // ---- embedding_config_id is empty (stage-1 placeholder) on every SPR ----
        outputDoc.getSearchMetadata().getSemanticResultsList().forEach(spr ->
            assertThat(spr.getEmbeddingConfigId())
                    .as("embedding_config_id must be empty (stage-1 placeholder) for SPR " +
                        "chunk_config_id='" + spr.getChunkConfigId() + "' per DESIGN.md §4.1")
                    .isEmpty()
        );

        // ---- Confirm at least two distinct non-sentences_internal SPRs (one per config) ----
        long mainSprCount = outputDoc.getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .count();
        assertThat(mainSprCount)
                .as("Two-config directive should produce at least 2 non-sentences_internal SPRs")
                .isGreaterThanOrEqualTo(2);
    }

    // =========================================================================
    // Deterministic chunk_id spot-check (§21.5)
    // =========================================================================

    @Test
    void chunkIdsShouldBeDeterministic() {
        String docId = "deterministic-test-doc-001";
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Sentence one for determinism. Sentence two for determinism.")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("determinism-pipeline")
                        .setPipeStepName("determinism-step")
                        .setStreamId("determinism-stream")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        // Run twice and compare
        ProcessDataResponse r1 = chunkerService.processData(request).await().indefinitely();
        ProcessDataResponse r2 = chunkerService.processData(request).await().indefinitely();

        assertThat(r1.getOutcome())
                .as("First run should succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);
        assertThat(r2.getOutcome())
                .as("Second run should succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // Extract chunk IDs from both runs
        var ids1 = r1.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .flatMap(spr -> spr.getChunksList().stream())
                .map(ai.pipestream.data.v1.SemanticChunk::getChunkId)
                .sorted()
                .toList();
        var ids2 = r2.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .flatMap(spr -> spr.getChunksList().stream())
                .map(ai.pipestream.data.v1.SemanticChunk::getChunkId)
                .sorted()
                .toList();

        assertThat(ids1)
                .as("Chunk IDs must be identical across repeated runs for the same input (§21.5 determinism)")
                .isEqualTo(ids2);

        // Spot-check the expected docHash prefix
        String expectedDocHash = DirectiveKeyComputer.sha256b64url(docId);
        ids1.stream()
            .filter(id -> !id.contains("sentences_internal"))
            .forEach(id ->
                assertThat(id)
                        .as("chunk_id must start with the SHA-256 of the doc_id per §21.5. " +
                            "Expected prefix '" + expectedDocHash + "' in chunk_id '" + id + "'")
                        .startsWith(expectedDocHash)
            );
    }

    // =========================================================================
    // Failure path: missing vector_set_directives
    // =========================================================================

    @Test
    void missingDirectivesShouldReturnFailure() {
        // Doc has no vector_set_directives — §21.1 no-fallback rule
        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("no-directives-doc-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Some body text")
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("failure-test")
                        .setPipeStepName("chunker-step")
                        .setStreamId("failure-stream")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("Missing vector_set_directives must return PROCESSING_OUTCOME_FAILURE per DESIGN.md §21.1")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        boolean hasDirectivesLog = response.getLogEntriesList().stream()
                .anyMatch(e -> e.getMessage().toLowerCase().contains("directive"));
        assertThat(hasDirectivesLog)
                .as("Error log must mention 'directive' to explain the failure to the operator")
                .isTrue();
    }

    // =========================================================================
    // Failure path: duplicate source_label
    // =========================================================================

    @Test
    void duplicateSourceLabelShouldReturnFailure() {
        // Build a directive set with two directives sharing the same source_label — §21.2
        VectorDirective dup1 = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(TestDirectiveBuilder.namedChunkerConfig("token_500_50", "token", 500, 50))
                .addEmbedderConfigs(TestDirectiveBuilder.dummyEmbedderConfig("minilm"))
                .build();
        VectorDirective dup2 = VectorDirective.newBuilder()
                .setSourceLabel("body")   // DUPLICATE
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(TestDirectiveBuilder.namedChunkerConfig("token_200_20", "token", 200, 20))
                .addEmbedderConfigs(TestDirectiveBuilder.dummyEmbedderConfig("minilm"))
                .build();
        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(dup1)
                .addDirectives(dup2)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("dup-label-doc-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Some body text")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("failure-test")
                        .setPipeStepName("chunker-step")
                        .setStreamId("failure-stream-dup")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("Duplicate source_label must return PROCESSING_OUTCOME_FAILURE per DESIGN.md §21.2")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        boolean hasDuplicateLog = response.getLogEntriesList().stream()
                .anyMatch(e -> e.getMessage().toLowerCase().contains("duplicate"));
        assertThat(hasDuplicateLog)
                .as("Error log must mention 'duplicate' to explain the failure")
                .isTrue();
    }

    // =========================================================================
    // Failure path: invalid ChunkerStepOptions JSON
    // =========================================================================

    @Test
    void unparsableChunkerStepOptionsShouldReturnFailure() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("bad-options-doc-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Some body text for invalid config test")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        // Inject a Struct that cannot be parsed into ChunkerStepOptions because
        // cache_enabled is set to a string value (should be boolean)
        Struct badConfig = Struct.newBuilder()
                .putFields("cache_enabled",
                        com.google.protobuf.Value.newBuilder().setStringValue("not-a-boolean").build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("failure-test")
                        .setPipeStepName("chunker-step")
                        .setStreamId("failure-stream-bad-opts")
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(badConfig)
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("Unparseable ChunkerStepOptions JSON must return PROCESSING_OUTCOME_FAILURE per DESIGN.md §21.1")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        boolean hasInvalidLog = response.getLogEntriesList().stream()
                .anyMatch(e -> e.getMessage().toLowerCase().contains("invalid"));
        assertThat(hasInvalidLog)
                .as("Error log must mention 'Invalid' to explain the parse failure")
                .isTrue();
    }

    // =========================================================================
    // Inline post-chunker invariants — mirrors DESIGN.md §5.1
    // (Inlined to avoid Quarkus classloader isolation issue with file-dep jars)
    // =========================================================================

    /**
     * Asserts that {@code doc} satisfies all post-chunker stage invariants per
     * DESIGN.md §5.1. Mirrors {@code SemanticPipelineInvariants.assertPostChunker}
     * but operates in the same classloader as the test proto classes.
     *
     * <p>Fix the production code, not this method, if any assertion fails.
     */
    private static void assertPostChunkerInvariants(PipeDoc doc) {
        assertThat(doc.hasSearchMetadata())
                .as("post-chunker: search_metadata must be set on the PipeDoc")
                .isTrue();

        SearchMetadata sm = doc.getSearchMetadata();
        List<SemanticProcessingResult> results = sm.getSemanticResultsList();

        for (int i = 0; i < results.size(); i++) {
            SemanticProcessingResult spr = results.get(i);
            String sprCtx = "post-chunker SPR[" + i + "] (source='" + spr.getSourceFieldName()
                    + "' config='" + spr.getChunkConfigId() + "' result_id='" + spr.getResultId() + "')";

            assertThat(spr.getEmbeddingConfigId())
                    .as(sprCtx + ": embedding_config_id must be empty (stage-1 placeholder)")
                    .isEmpty();

            assertThat(spr.getSourceFieldName())
                    .as(sprCtx + ": source_field_name must be non-empty")
                    .isNotEmpty();

            assertThat(spr.getChunkConfigId())
                    .as(sprCtx + ": chunk_config_id must be non-empty")
                    .isNotEmpty();

            assertThat(spr.getChunksList())
                    .as(sprCtx + ": chunks must be non-empty")
                    .isNotEmpty();

            assertThat(spr.getMetadataMap())
                    .as(sprCtx + ": metadata must contain 'directive_key' entry (§21.2)")
                    .containsKey("directive_key");

            for (int j = 0; j < spr.getChunksCount(); j++) {
                var chunk = spr.getChunks(j);
                String chunkCtx = sprCtx + " chunk[" + j + "] id='" + chunk.getChunkId() + "'";
                var ei = chunk.getEmbeddingInfo();

                assertThat(ei.getTextContent())
                        .as(chunkCtx + ": text_content must be non-empty")
                        .isNotEmpty();

                assertThat(ei.getVectorList())
                        .as(chunkCtx + ": vector must be empty (not yet embedded)")
                        .isEmpty();

                assertThat(chunk.getChunkId())
                        .as(chunkCtx + ": chunk_id must be non-empty")
                        .isNotEmpty();

                // §4.1: chunk_analytics is ALWAYS populated. The streaming path has
                // done this for years; the non-streaming rewrite must match or
                // downstream consumers lose the positional/POS/text-stats analytics.
                assertThat(chunk.hasChunkAnalytics())
                        .as(chunkCtx + ": chunk_analytics must be set (§4.1 always populated)")
                        .isTrue();

                int start = ei.hasOriginalCharStartOffset() ? ei.getOriginalCharStartOffset() : 0;
                int end = ei.hasOriginalCharEndOffset() ? ei.getOriginalCharEndOffset() : 0;

                assertThat(start)
                        .as(chunkCtx + ": original_char_start_offset must be >= 0")
                        .isGreaterThanOrEqualTo(0);

                assertThat(end)
                        .as(chunkCtx + ": original_char_end_offset must be >= start_offset")
                        .isGreaterThanOrEqualTo(start);
            }
        }

        // §5.1: at least one SPR per unique source_field_name must have nlp_analysis set
        Set<String> sourceFields = results.stream()
                .map(SemanticProcessingResult::getSourceFieldName)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        for (String sf : sourceFields) {
            boolean hasNlp = results.stream()
                    .filter(r -> sf.equals(r.getSourceFieldName()))
                    .anyMatch(SemanticProcessingResult::hasNlpAnalysis);
            assertThat(hasNlp)
                    .as("post-chunker: at least one SPR with source_field_name='" + sf
                        + "' must have nlp_analysis set (§5.1)")
                    .isTrue();
        }

        // §5.1: source_field_analytics[] must have one entry per unique (source_field, chunk_config_id)
        Set<String> pairsInResults = results.stream()
                .map(r -> r.getSourceFieldName() + "|" + r.getChunkConfigId())
                .collect(Collectors.toSet());

        Set<String> pairsInAnalytics = sm.getSourceFieldAnalyticsList().stream()
                .map(a -> a.getSourceField() + "|" + a.getChunkConfigId())
                .collect(Collectors.toSet());

        for (String pair : pairsInResults) {
            assertThat(pairsInAnalytics)
                    .as("post-chunker: source_field_analytics[] must contain an entry for " +
                        "(source_field, chunk_config_id)='" + pair + "' (§5.1)")
                    .contains(pair);
        }

        // §21.8: lex-sorted by (source_field_name, chunk_config_id, embedding_config_id, result_id)
        Comparator<SemanticProcessingResult> lexOrder = Comparator
                .comparing(SemanticProcessingResult::getSourceFieldName)
                .thenComparing(SemanticProcessingResult::getChunkConfigId)
                .thenComparing(SemanticProcessingResult::getEmbeddingConfigId)
                .thenComparing(SemanticProcessingResult::getResultId);

        for (int i = 1; i < results.size(); i++) {
            SemanticProcessingResult prev = results.get(i - 1);
            SemanticProcessingResult curr = results.get(i);
            assertThat(lexOrder.compare(prev, curr))
                    .as("post-chunker: semantic_results must be lex-sorted (§21.8). " +
                        "Element[" + (i - 1) + "] ('" + prev.getSourceFieldName() + "', '" +
                        prev.getChunkConfigId() + "') must sort <= element[" + i + "] ('" +
                        curr.getSourceFieldName() + "', '" + curr.getChunkConfigId() + "')")
                    .isLessThanOrEqualTo(0);
        }
    }
}
