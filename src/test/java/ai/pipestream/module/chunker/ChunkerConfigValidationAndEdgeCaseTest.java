package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.service.OverlapChunker;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closes five audit findings in one test class because they share the same
 * test infrastructure and fail for related reasons:
 *
 * <ul>
 *   <li><b>C1 (CRITICAL)</b>: {@code ChunkerConfig.validate()} is now called
 *       from {@code parseNamedChunkerConfig}. Before this PR a caller could
 *       send {@code {"chunkSize": 500, "chunkOverlap": 500}} and get a
 *       degenerate chunker whose token window never advances. Tested here
 *       with three separate invalid-config shapes to exhaustively cover the
 *       validation branches.</li>
 *   <li><b>H1 (HIGH)</b>: multi-directive body+title was completely
 *       untested via {@code processData}. {@code TestDirectiveBuilder
 *       .withTwoSourceDirectives()} existed as a zombie helper with zero
 *       callers.</li>
 *   <li><b>H2 (HIGH)</b>: the {@code alreadyHasSentenceSpr} skip branch at
 *       {@code ChunkerGrpcImpl.java:263-265} — if any directive already
 *       produced an SPR with {@code chunk_config_id == "sentences_internal"},
 *       Path B is globally skipped. Untested before this PR. The test pins
 *       the current behavior so a future regression that flips the skip
 *       direction is caught.</li>
 *   <li><b>H3 (HIGH)</b>: a directive with ZERO chunker configs. What
 *       happens? No test in the pre-PR-G suite covered this shape.</li>
 *   <li><b>H4 (HIGH)</b>: the {@code OverlapChunker.MAX_TEXT_BYTES = 40 MB}
 *       truncation path was never exercised. A 41 MB body should land on
 *       the truncation branch at {@code OverlapChunker.java:335-340} and
 *       return a clean response without crashing.</li>
 * </ul>
 *
 * <p>All assertions use AssertJ with descriptive {@code .as()} messages per
 * {@code feedback-assertj-preference.md}. Matches the inline-per-consumer
 * pattern of the other chunker test classes.
 */
@QuarkusTest
class ChunkerConfigValidationAndEdgeCaseTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    // Direct injection for the H4 MAX_TEXT_BYTES test — it exercises the
    // in-process truncation branch which is unreachable via gRPC. See the
    // test method Javadoc for the full rationale.
    @Inject
    OverlapChunker overlapChunker;

    private static final String STANDARD_BODY =
            "The directive-driven chunker refactor validates every per-config "
            + "Struct parsed from NamedChunkerConfig before building chunks. "
            + "This test uses a body long enough to produce real chunks under "
            + "a normal token or sentence chunker while keeping allocations "
            + "modest so the tests stay fast on CI runners.";

    // =========================================================================
    // C1 — ChunkerConfig.validate() is now called in parseNamedChunkerConfig
    // =========================================================================

    @Test
    void overlapGreaterThanOrEqualToChunkSizeShouldReturnFailure() {
        // Degenerate chunker: window never advances. Before PR-G this was
        // silently accepted; the OverlapChunker's Math.max(1, tokens-overlap)
        // guard prevented an infinite loop but the output was pure garbage.
        Struct degenerate = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(500).build())
                .build();

        ProcessDataResponse response = runWithRawConfig("degenerate_500_500", degenerate);

        assertThat(response.getOutcome())
                .as("chunkOverlap >= chunkSize must be rejected — pre-PR-G "
                        + "this silently fell through to a degenerate chunker")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        assertLogContains(response,
                "chunkOverlap must be less than chunkSize",
                "Error log must name the specific validation rule that failed "
                        + "so the operator can see exactly what to fix");
    }

    @Test
    void chunkSizeAbove10000ShouldReturnFailure() {
        Struct tooLarge = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(20000).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .build();

        ProcessDataResponse response = runWithRawConfig("token_20000_50", tooLarge);

        assertThat(response.getOutcome())
                .as("chunkSize > 10000 must be rejected per ChunkerConfig.validate "
                        + "upper bound")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        assertLogContains(response,
                "chunkSize must be between 1 and 10000",
                "Error log must name the upper-bound rule");
    }

    @Test
    void semanticAlgorithmShouldReturnFailure() {
        // ChunkingAlgorithm.SEMANTIC is declared in the enum but the chunker
        // implementation doesn't support it. ChunkerConfig.validate() rejects
        // it explicitly ("Semantic chunking is not yet implemented").
        Struct semantic = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("semantic").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .build();

        ProcessDataResponse response = runWithRawConfig("semantic_500_50", semantic);

        assertThat(response.getOutcome())
                .as("algorithm=semantic must be rejected — not yet implemented")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        assertLogContains(response,
                "Semantic chunking is not yet implemented",
                "Error log must name the 'not implemented' reason so the "
                        + "operator doesn't waste time debugging");
    }

    // =========================================================================
    // H1 — multi-directive (body + title) end-to-end
    // =========================================================================

    @Test
    void multiDirectiveBodyAndTitleShouldEachProduceSprs() {
        VectorSetDirectives directives = TestDirectiveBuilder.withTwoSourceDirectives();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("multi-directive-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(STANDARD_BODY)
                        .setTitle("The Directive-Driven Chunker Refactor")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "multi-directive");

        assertThat(response.getOutcome())
                .as("multi-directive request with body + title should succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        // Group SPRs by source_field_name, ignoring the always-on sentences_internal
        // SPR (Path B runs independently for each directive per §21.9).
        Set<String> directiveSourceFields = sprs.stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .map(SemanticProcessingResult::getSourceFieldName)
                .collect(Collectors.toSet());

        assertThat(directiveSourceFields)
                .as("Both 'body' and 'title' directives must have produced at "
                        + "least one directive-driven SPR")
                .contains("body", "title");

        // Both Path B sentences_internal SPRs must also appear (§21.9 union).
        Set<String> sentenceSourceFields = sprs.stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .map(SemanticProcessingResult::getSourceFieldName)
                .collect(Collectors.toSet());

        assertThat(sentenceSourceFields)
                .as("§21.9 union: Path B must emit a sentences_internal SPR "
                        + "for each directive's source_label (body AND title)")
                .contains("body", "title");

        // Verify each directive SPR has a non-empty, unique directive_key stamped per §21.2.
        Set<String> directiveKeys = sprs.stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .map(spr -> spr.getMetadataMap().get("directive_key").getStringValue())
                .collect(Collectors.toSet());

        assertThat(directiveKeys)
                .as("Two distinct directives must have two distinct directive_keys "
                        + "per §21.2 — if they collide, DirectiveKeyComputer is broken")
                .hasSize(2);

        // Spot-check: every chunk must have a non-empty chunk_id per §21.5 and
        // chunks should reflect their source label in the deterministic ID.
        for (SemanticProcessingResult spr : sprs) {
            String configId = spr.getChunkConfigId();
            if (ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(configId)) continue;
            for (SemanticChunk chunk : spr.getChunksList()) {
                assertThat(chunk.getChunkId())
                        .as("Multi-directive chunk_id must contain the source_field_name "
                                + "('%s') per §21.5 deterministic ID shape",
                                spr.getSourceFieldName())
                        .contains(":" + spr.getSourceFieldName() + ":");
            }
        }
    }

    // =========================================================================
    // H2 — alreadyHasSentenceSpr skip branch
    // =========================================================================

    @Test
    void directiveNamedSentencesInternalShouldSkipPathB() {
        // Build a directive whose NamedChunkerConfig.config_id is literally
        // "sentences_internal" — the same sentinel value Path B uses. Per
        // ChunkerGrpcImpl.java:263-265, Path B globally skips if any directive
        // already produced an SPR with that config_id. This test pins the
        // current behavior so a future regression that flips the skip
        // direction is loud.
        //
        // Note: this is a footgun. A caller SHOULD NOT name a config
        // "sentences_internal" — but nothing stops them today. Until spec
        // decides otherwise, we document the skip-behavior via this test.
        Struct config = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("sentence").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(3).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(0).build())
                .build();

        NamedChunkerConfig clashingConfig = NamedChunkerConfig.newBuilder()
                .setConfigId(ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID)
                .setConfig(config)
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(clashingConfig)
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("clashing-config-id-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(STANDARD_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "clashing-config-id");

        assertThat(response.getOutcome())
                .as("request with a clashing 'sentences_internal' config_id "
                        + "must still succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        List<SemanticProcessingResult> sentencesSprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .toList();

        // Path A produced ONE 'sentences_internal' SPR (from the directive).
        // Path B should skip because alreadyHasSentenceSpr == true.
        // The net result: exactly one 'sentences_internal' SPR, with the
        // Path A-driven sentence chunker parameters (sentence_3_0, not the
        // Path B-default sentence spans). This pins the current skip
        // behavior.
        assertThat(sentencesSprs)
                .as("Exactly one 'sentences_internal' SPR should be present — "
                        + "Path A produced it, Path B saw it and skipped "
                        + "(current behavior per ChunkerGrpcImpl.java:263-265)")
                .hasSize(1);
    }

    // =========================================================================
    // H3 — directive with zero chunker configs
    // =========================================================================

    @Test
    void directiveWithZeroChunkerConfigsShouldProduceOnlySentencesInternal() {
        // A directive with an empty chunker_configs list means: "I want no
        // token/character chunking for this source, but Path B (§21.9) still
        // runs the NLP preprocessor and emits sentences_internal because
        // §21.9 has no opt-out." This test pins that behavior.
        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                // zero chunker_configs
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("zero-configs-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(STANDARD_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "zero-configs");

        assertThat(response.getOutcome())
                .as("directive with zero chunker_configs must succeed (§21.9 "
                        + "sentence path still runs)")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        // Zero directive-driven SPRs (Path A had no configs to iterate).
        long directiveSprs = sprs.stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .count();
        assertThat(directiveSprs)
                .as("zero chunker_configs → zero directive-driven SPRs")
                .isZero();

        // Exactly one Path B sentences_internal SPR for the 'body' source.
        List<SemanticProcessingResult> sentencesSprs = sprs.stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .toList();

        assertThat(sentencesSprs)
                .as("§21.9 must still produce one sentences_internal SPR for "
                        + "the 'body' source_label even with zero chunker_configs")
                .hasSize(1);
        assertThat(sentencesSprs.get(0).getSourceFieldName())
                .as("sentences_internal SPR must carry the directive's source_label")
                .isEqualTo("body");
        assertThat(sentencesSprs.get(0).getChunksList())
                .as("sentences_internal SPR must have at least one sentence chunk")
                .isNotEmpty();
    }

    // =========================================================================
    // H4 — MAX_TEXT_BYTES truncation boundary (direct OverlapChunker call)
    // =========================================================================

    @Test
    void bodyExceedingMaxTextBytesShouldTruncateAndSucceed() {
        // WHY this test is a DIRECT in-process call, not a gRPC roundtrip:
        //
        // OverlapChunker.MAX_TEXT_BYTES = 40 MB. The chunker's HTTP layer
        // (quarkus.http.limits.max-body-size) is also capped at 40 MB to match.
        // That means a gRPC request carrying a >40 MB body is rejected at the
        // HTTP layer before it ever reaches the chunker service. The
        // truncation branch at OverlapChunker.java:335-340 is therefore ONLY
        // reachable via in-process callers — test code calling
        // overlapChunker.createChunks(...) directly, or future batch-mode
        // paths that bypass the gRPC wrapper.
        //
        // Sending 41 MB of ASCII over gRPC would hang the test for ~30s as
        // Vert.x tries, hits the HTTP body cap, drops the connection, and
        // Mutiny retries. We saw exactly that failure mode during the first
        // draft of this test. Direct invocation is the right fix: faster
        // (no Quarkus HTTP round-trip), more targeted (tests the branch we
        // care about), and avoids pathological retry behavior.
        //
        // A secondary consequence: the in-process truncation branch is
        // defensive code that protects OverlapChunker callers who may not
        // have gone through the gRPC layer. Pinning it with a direct test
        // keeps the defense working even if a future refactor changes how
        // HTTP limits are enforced.

        // Build a body just over 40 MB of ASCII via String.repeat (efficient,
        // single allocation).
        String paragraph =
                "This paragraph is part of a 41 MB body that exceeds the "
                + "chunker's MAX_TEXT_BYTES truncation limit. Every call to "
                + "OverlapChunker.createChunks that lands on this branch must "
                + "truncate the text via new String(bytes, 0, limit, UTF_8) "
                + "and continue without crashing. ";
        int targetBytes = 41 * 1024 * 1024;
        int repeatCount = (targetBytes / paragraph.length()) + 1;
        String hugeBody = paragraph.repeat(repeatCount);
        assertThat(hugeBody.getBytes().length)
                .as("Test fixture must actually exceed MAX_TEXT_BYTES (40 MB) "
                        + "to exercise the truncation branch at "
                        + "OverlapChunker.java:335-340")
                .isGreaterThan(40 * 1024 * 1024);

        // Build a minimal PipeDoc with just the huge body — no directives
        // needed because we're calling OverlapChunker directly, not the full
        // processData pipeline.
        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("max-bytes-direct-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(hugeBody)
                        .build())
                .build();

        // Use the test-only direct-config overload of OverlapChunker. Token
        // chunker, 500/50, preserveUrls=false (no point running the URL
        // substitute/restore pipeline on 40 MB of ASCII filler), cleanText=
        // true.
        ChunkerConfig config = new ChunkerConfig(
                ChunkingAlgorithm.TOKEN,
                "body",
                500,
                50,
                false,
                true
        );

        ChunkingResult result = overlapChunker.createChunks(
                inputDoc,
                config,
                "max-bytes-direct-stream",
                "max-bytes-direct-step");

        // The truncation branch should have logged a WARN and replaced the
        // body with its first 40 MB. We don't assert byte-precise truncation
        // (UTF-8 boundary split behavior is the target of a future test).
        // The must-be-true invariants:
        //   - createChunks did NOT throw
        //   - result is non-null
        //   - chunks list is non-empty (40 MB of ASCII → many token chunks)
        //   - every chunk has non-empty text_content
        assertThat(result)
                .as("OverlapChunker.createChunks must return a non-null "
                        + "ChunkingResult for a >40 MB body (truncation "
                        + "branch, not crash)")
                .isNotNull();

        assertThat(result.chunks())
                .as("Truncated 40 MB body must produce at least one token chunk")
                .isNotEmpty();

        // Spot-check: the first and last chunks must have non-empty text.
        // If truncation split a UTF-8 multi-byte char at the boundary, the
        // LAST chunk's text might have lost some content, but it should
        // still exist and be non-empty.
        assertThat(result.chunks().get(0).text())
                .as("First chunk of truncated body must have non-empty text")
                .isNotEmpty();
        assertThat(result.chunks().get(result.chunks().size() - 1).text())
                .as("Last chunk of truncated body must have non-empty text "
                        + "(even if UTF-8 boundary split lost a few bytes)")
                .isNotEmpty();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Runs processData with a single-directive single-config request where
     * the per-config Struct comes from the caller. Used by C1 tests to send
     * deliberately malformed configs.
     */
    private ProcessDataResponse runWithRawConfig(String configId, Struct rawConfig) {
        NamedChunkerConfig chunker = NamedChunkerConfig.newBuilder()
                .setConfigId(configId)
                .setConfig(rawConfig)
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(chunker)
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("c1-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(STANDARD_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        return runProcessData(inputDoc, "c1-" + configId);
    }

    private ProcessDataResponse runProcessData(PipeDoc doc, String streamIdPrefix) {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("pr-g-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId(streamIdPrefix + "-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        return chunkerService.processData(request).await().indefinitely();
    }

    private static void assertLogContains(ProcessDataResponse response, String expectedSubstring, String context) {
        boolean found = response.getLogEntriesList().stream()
                .map(LogEntry::getMessage)
                .anyMatch(msg -> msg.contains(expectedSubstring));
        assertThat(found)
                .as("%s — searching for '%s' in log entries: %s",
                        context, expectedSubstring,
                        response.getLogEntriesList().stream()
                                .map(LogEntry::getMessage)
                                .collect(Collectors.joining(" | ")))
                .isTrue();
    }
}
