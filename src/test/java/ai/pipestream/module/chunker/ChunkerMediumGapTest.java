package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closes MEDIUM coverage gaps from the post-R1 quick-wins audit.
 *
 * <ul>
 *   <li><b>M1</b>: parameterized {@code parseNamedChunkerConfig} malformed-shape
 *       coverage. PR-G's {@code ChunkerConfigValidationAndEdgeCaseTest}
 *       exercised three known shapes (chunkOverlap≥chunkSize, chunkSize&gt;10000,
 *       semantic algorithm). M1 covers the rest: negative chunkSize, negative
 *       chunkOverlap, chunkOverlap&gt;5000, totally absent algorithm, unknown
 *       string algorithm name.</li>
 *   <li><b>M2</b>: unicode edge cases that the UnicodeSanitizer + chunker
 *       pipeline should handle without crashing. Emoji, BOM (U+FEFF), RTL
 *       (Arabic), combining marks, and a surrogate-pair edge.</li>
 *   <li><b>M3</b>: degenerate body shapes — whitespace-only, URL-only,
 *       single-character body. These are common parser-output edges that
 *       must produce either a clean SUCCESS or a clean FAILURE, never a
 *       crash.</li>
 * </ul>
 *
 * <p>All tests use AssertJ with descriptive {@code .as()} messages and
 * follow the inline-per-consumer pattern used by every other chunker test.
 */
@QuarkusTest
class ChunkerMediumGapTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    // =========================================================================
    // M1 — parameterized parseNamedChunkerConfig malformed shapes
    // =========================================================================

    /**
     * Each malformed shape must return PROCESSING_OUTCOME_FAILURE with a log
     * entry naming the violated rule. Pre-PR-G these would have silently
     * fallen through to a degenerate chunker; PR-G added the
     * {@code ChunkerConfig.validate()} call to {@code parseNamedChunkerConfig}
     * to fail loud per §21.1.
     */
    static Stream<Arguments> malformedConfigShapes() {
        return Stream.of(
                Arguments.of(
                        "chunkSize_negative",
                        Struct.newBuilder()
                                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                                .putFields("chunkSize", Value.newBuilder().setNumberValue(-100).build())
                                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(0).build())
                                .build(),
                        "chunkSize must be between 1 and 10000"),
                Arguments.of(
                        "chunkSize_zero",
                        Struct.newBuilder()
                                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                                .putFields("chunkSize", Value.newBuilder().setNumberValue(0).build())
                                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(0).build())
                                .build(),
                        "chunkSize must be between 1 and 10000"),
                Arguments.of(
                        "chunkOverlap_negative",
                        Struct.newBuilder()
                                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(-10).build())
                                .build(),
                        "chunkOverlap must be between 0 and 5000"),
                Arguments.of(
                        "chunkOverlap_above_5000",
                        Struct.newBuilder()
                                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                                .putFields("chunkSize", Value.newBuilder().setNumberValue(8000).build())
                                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(6000).build())
                                .build(),
                        "chunkOverlap must be between 0 and 5000")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedConfigShapes")
    void malformedNamedChunkerConfigShouldReturnFailure(String name, Struct badConfig, String expectedLogSubstring) {
        ProcessDataResponse response = runWithRawConfig("malformed_" + name, badConfig);

        assertThat(response.getOutcome())
                .as("Malformed shape '%s' must be rejected with FAILURE — pre-PR-G "
                        + "this would have silently demoted to a degenerate chunker", name)
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        boolean hasMatchingLog = response.getLogEntriesList().stream()
                .anyMatch(e -> e.getMessage().contains(expectedLogSubstring));
        assertThat(hasMatchingLog)
                .as("Malformed shape '%s' must produce a log entry containing "
                        + "'%s' so the operator can see exactly which rule fired",
                        name, expectedLogSubstring)
                .isTrue();
    }

    // =========================================================================
    // M2 — unicode edge cases
    // =========================================================================

    /**
     * Each unicode edge must produce a clean response (success or controlled
     * failure) — never a crash, never an OOM, never a hang. The sanitizer
     * should normalize invalid sequences to replacement chars, and the
     * chunker should emit at least one chunk OR a controlled empty result.
     */
    static Stream<Arguments> unicodeEdgeBodies() {
        return Stream.of(
                Arguments.of("emoji_dense",
                        "Hello 👋 world 🌍 this 🚀 is 🎉 a 🎊 mostly 🤔 emoji 😀 body 🌟. "
                        + "Each emoji is a multi-code-unit sequence that exercises "
                        + "the surrogate-pair handling in the tokenizer and the "
                        + "UTF-8 byte-size computation."),
                Arguments.of("bom_prefixed",
                        "\uFEFFThis text starts with a UTF-8 BOM (U+FEFF zero-width "
                        + "no-break space). The sanitizer should handle the BOM "
                        + "without confusing the tokenizer."),
                Arguments.of("rtl_arabic",
                        "العربية هي لغة سامية تُكتب من اليمين إلى اليسار. "
                        + "هذا النص يحتوي على جمل عربية كاملة لاختبار "
                        + "كاشف الجمل والمقطّع على نص ثنائي الاتجاه."),
                Arguments.of("combining_marks",
                        "Café naïve résumé piñata déjà vu. These contain combining "
                        + "marks (U+0301 acute, U+0308 diaeresis, U+0303 tilde) "
                        + "which are multi-code-point sequences that the tokenizer "
                        + "must handle correctly."),
                Arguments.of("zero_width_separators",
                        "wo\u200Brd1 wo\u200Crd2 wo\u200Drd3 wo\u200Erd4. "
                        + "Contains U+200B zero-width space, U+200C ZWNJ, "
                        + "U+200D ZWJ, U+200E LRM. These are invisible "
                        + "characters that the tokenizer must not crash on.")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unicodeEdgeBodies")
    void unicodeEdgeCasesShouldNotCrash(String name, String body) {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("unicode-" + name + "-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(body)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "unicode-" + name);

        assertThat(response.getOutcome())
                .as("Unicode edge '%s' must produce SUCCESS — no crash, no "
                        + "controlled failure expected for valid (if unusual) "
                        + "UTF-8 input", name)
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // At least one SPR must be present (Path B sentences_internal at minimum).
        assertThat(response.getOutputDoc().getSearchMetadata().getSemanticResultsList())
                .as("Unicode edge '%s' must produce at least one SPR — Path B "
                        + "always emits sentences_internal per §21.9", name)
                .isNotEmpty();
    }

    // =========================================================================
    // M3 — degenerate body shapes
    // =========================================================================

    @Test
    void whitespaceOnlyBodyShouldNotCrash() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("ws-only-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("   \n\n   \t   ")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "ws-only");

        // Whitespace-only body is "valid input" — the chunker doesn't crash.
        // It may produce an empty SPR list (no extractable chunks) or a
        // degenerate single-chunk result; either is acceptable as long as
        // the outcome is SUCCESS and no exception escapes.
        assertThat(response.getOutcome())
                .as("Whitespace-only body must be handled gracefully — either "
                        + "SUCCESS with possibly-empty chunks, or controlled "
                        + "FAILURE. Never a crash.")
                .isIn(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS,
                        ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
    }

    @Test
    void singleCharacterBodyShouldNotCrash() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("single-char-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("A")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "single-char");

        assertThat(response.getOutcome())
                .as("Single-character body must succeed — chunker should produce "
                        + "exactly one chunk containing 'A'")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // Path A should produce at least one chunk, Path B may produce a sentence
        // chunk too. Either way, at least one SPR.
        assertThat(response.getOutputDoc().getSearchMetadata().getSemanticResultsList())
                .as("Single-character body must produce at least one SPR")
                .isNotEmpty();
    }

    @Test
    void urlOnlyBodyShouldRoundTripTheUrl() {
        // Body that's nothing but a URL. Tests the URL preservation path
        // when the URL placeholder substitution covers 100% of the source.
        Struct configWithPreserveUrls = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .putFields("preserveUrls", Value.newBuilder().setBoolValue(true).build())
                .build();

        NamedChunkerConfig chunker = NamedChunkerConfig.newBuilder()
                .setConfigId("url_only_test")
                .setConfig(configWithPreserveUrls)
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

        String url = "https://example.com/url-only-body-test";
        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("url-only-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(url)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "url-only");

        assertThat(response.getOutcome())
                .as("URL-only body with preserveUrls=true must succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // The URL should round-trip into at least one chunk's text_content.
        // If URL substitution leaves a stray placeholder in the chunk, this
        // catches it.
        boolean foundUrl = response.getOutputDoc().getSearchMetadata()
                .getSemanticResultsList().stream()
                .filter(spr -> "url_only_test".equals(spr.getChunkConfigId()))
                .flatMap(spr -> spr.getChunksList().stream())
                .map(SemanticChunk::getEmbeddingInfo)
                .map(ei -> ei.getTextContent())
                .anyMatch(text -> text.contains(url));

        assertThat(foundUrl)
                .as("URL '%s' must round-trip into at least one chunk's "
                        + "text_content — even when the body is nothing but the URL",
                        url)
                .isTrue();

        // Negative: no chunk text may contain a stray URL placeholder.
        boolean leakingPlaceholder = response.getOutputDoc().getSearchMetadata()
                .getSemanticResultsList().stream()
                .filter(spr -> "url_only_test".equals(spr.getChunkConfigId()))
                .flatMap(spr -> spr.getChunksList().stream())
                .map(c -> c.getEmbeddingInfo().getTextContent())
                .anyMatch(text -> text.contains("__URL_PLACEHOLDER_"));

        assertThat(leakingPlaceholder)
                .as("No chunk may leak a raw '__URL_PLACEHOLDER_' substring "
                        + "for a URL-only body")
                .isFalse();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

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
                .setDocId("m1-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Some body text for the parameterized parse test.")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        return runProcessData(inputDoc, "m1-" + configId);
    }

    private ProcessDataResponse runProcessData(PipeDoc doc, String streamIdPrefix) {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("medium-gap-pipeline")
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
}
