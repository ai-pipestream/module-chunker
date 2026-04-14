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

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers two coverage gaps flagged by the post-R1 correctness audit:
 *
 * <ol>
 *   <li><b>Union of both paths (§21.9)</b> — every pre-R1 test that touched
 *       {@code sentences_internal} did so to <em>filter it out</em> before
 *       asserting on the directive-driven SPRs, so the "always-emit sentences"
 *       path was completely untested. If a future regression deleted
 *       {@code ChunkerGrpcImpl.buildSentenceChunks} or the call to it, no
 *       existing test would fail. This test pins it: for a realistic
 *       directive-driven request, the chunker must emit BOTH the
 *       directive-named SPR AND a matching {@code sentences_internal} SPR in
 *       the same response.</li>
 *
 *   <li><b>URL preservation round-trip</b> — {@code ChunkerServiceTestBase
 *       .testProcessDataWithUrlPreservation} sends a body containing URLs but
 *       only checks that chunks were produced, not that the original URLs
 *       round-tripped into any chunk's {@code text_content}. Per
 *       {@code OverlapChunker.java:136-180, 346-358}, URLs are replaced with
 *       placeholders before token/sentence splitting and then restored
 *       inside each chunk. This test asserts the restoration actually
 *       happens end-to-end by searching chunk text for the original URL
 *       literals.</li>
 * </ol>
 *
 * <p>Both tests use AssertJ with descriptive {@code .as()} messages per
 * {@code feedback-assertj-preference.md}. If either fails, fix the production
 * path — not this test.
 */
@QuarkusTest
class ChunkerSentenceUnionAndUrlTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    // Body text chosen so:
    //   - It contains enough sentences for the NLP preprocessor to produce
    //     multiple sentence spans (Path B should have something to emit)
    //   - It is long enough to actually get chunked by a token chunker
    //     (Path A will produce >= 1 chunk)
    //   - It contains two distinct URLs at different positions so URL
    //     preservation has something non-trivial to restore in Path A
    private static final String URL_A = "https://example.com/documents/post/42";
    private static final String URL_B = "http://test.org/api/v1/resources?id=9&format=json";

    private static final String BODY_WITH_URLS =
            "The chunker preserves URLs by substituting each URL with an opaque "
            + "placeholder before tokenisation and restoring the original URL "
            + "inside every chunk whose byte range overlaps the placeholder. "
            + "The first URL to preserve is " + URL_A + " and that URL must "
            + "survive the round-trip intact. Later in the text we include a "
            + "second URL: " + URL_B + " which uses different query-string "
            + "characters and a trailing path segment to stress the placeholder "
            + "restore logic. Sentence boundaries fall naturally around both "
            + "URLs so the NLP preprocessor can produce realistic sentence "
            + "spans and the chunker can produce real token-based chunks "
            + "instead of degenerate single-chunk output.";

    // =========================================================================
    // §21.9 — the "both paths run together" invariant
    // =========================================================================

    @Test
    void processDataShouldEmitBothDirectiveSprAndSentencesInternalSpr() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("union-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_WITH_URLS)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("union-test-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId("union-stream-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("processData should succeed for a valid directive-driven request")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        assertThat(response.hasOutputDoc())
                .as("Response must include an output document")
                .isTrue();

        List<SemanticProcessingResult> sprs =
                response.getOutputDoc().getSearchMetadata().getSemanticResultsList();

        // Path A assertion: at least one directive-driven SPR whose chunk_config_id
        // is NOT the sentence-path sentinel. This is the token_500_50 SPR.
        long directiveSprCount = sprs.stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .count();
        assertThat(directiveSprCount)
                .as("§21.9 union: must find at least one directive-driven SPR "
                        + "(chunk_config_id != '%s') produced by Path A",
                        ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID)
                .isGreaterThanOrEqualTo(1);

        // Path B assertion: at least one SPR with chunk_config_id == "sentences_internal"
        // for the body source_label. This is the ONE assertion the whole suite
        // was missing before this PR. If buildSentenceChunks gets deleted or
        // its call site dropped from processData, this test fails.
        List<SemanticProcessingResult> sentenceSprs = sprs.stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .toList();

        assertThat(sentenceSprs)
                .as("§21.9 union: Path B must emit at least one '%s' SPR alongside "
                        + "the directive-driven SPR, regardless of the "
                        + "ChunkerStepOptions.always_emit_sentences field (which is "
                        + "ignored per §21.9 no-opt-out rule)",
                        ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID)
                .isNotEmpty();

        SemanticProcessingResult sentenceSpr = sentenceSprs.get(0);

        assertThat(sentenceSpr.getSourceFieldName())
                .as("§21.9 union: sentences_internal SPR must carry the directive's "
                        + "source_label ('body'), not a synthesised placeholder")
                .isEqualTo("body");

        assertThat(sentenceSpr.getChunksList())
                .as("§21.9 union: sentences_internal SPR must have at least one "
                        + "sentence chunk (BODY_WITH_URLS contains multiple "
                        + "sentence boundaries for the NLP preprocessor)")
                .isNotEmpty();

        assertThat(sentenceSpr.getEmbeddingConfigId())
                .as("§21.9 union: sentences_internal SPR is stage-1 and must have "
                        + "empty embedding_config_id")
                .isEmpty();

        assertThat(sentenceSpr.getMetadataMap())
                .as("§21.9 union: sentences_internal SPR must carry a directive_key "
                        + "in its metadata map per §21.2 (matches the directive "
                        + "it was emitted for)")
                .containsKey("directive_key");

        assertThat(sentenceSpr.hasNlpAnalysis())
                .as("§21.9 union: sentences_internal SPR must carry nlp_analysis "
                        + "(the NLP work was already done to produce the sentence "
                        + "spans, so nlp_analysis is free to attach)")
                .isTrue();

        // Every sentence chunk must pass basic stage-1 shape checks.
        for (int i = 0; i < sentenceSpr.getChunksCount(); i++) {
            SemanticChunk chunk = sentenceSpr.getChunks(i);
            String ctx = "sentences_internal chunk[" + i + "] id='" + chunk.getChunkId() + "'";

            assertThat(chunk.getChunkId())
                    .as(ctx + ": chunk_id must be non-empty")
                    .isNotEmpty();

            assertThat(chunk.getEmbeddingInfo().getTextContent())
                    .as(ctx + ": text_content must be non-empty")
                    .isNotEmpty();

            assertThat(chunk.getEmbeddingInfo().getVectorList())
                    .as(ctx + ": vector must be empty at stage 1")
                    .isEmpty();

            assertThat(chunk.hasChunkAnalytics())
                    .as(ctx + ": chunk_analytics must be populated per §4.1 "
                            + "(always populated, including on sentence chunks)")
                    .isTrue();
        }
    }

    // =========================================================================
    // URL preservation round-trip
    // =========================================================================

    @Test
    void urlsInSourceTextShouldRoundTripIntoChunkTextContent() {
        // Build a directive whose per-config Struct explicitly sets preserveUrls=true
        // rather than relying on ChunkerConfig.DEFAULT_PRESERVE_URLS. This protects
        // the test from a future default flip and makes the intent explicit.
        Struct configWithPreserveUrls = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .putFields("preserveUrls", Value.newBuilder().setBoolValue(true).build())
                .build();

        NamedChunkerConfig chunker = NamedChunkerConfig.newBuilder()
                .setConfigId("token_500_50_preserve_urls")
                .setConfig(configWithPreserveUrls)
                .build();

        NamedEmbedderConfig embedder = NamedEmbedderConfig.newBuilder()
                .setConfigId("minilm")
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(chunker)
                .addEmbedderConfigs(embedder)
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("url-preservation-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_WITH_URLS)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("url-preservation-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId("url-stream-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("processData should succeed for a directive that explicitly "
                        + "sets preserveUrls=true")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // Only inspect the directive-driven SPR (Path A is the one that runs
        // through OverlapChunker which does the URL substitution/restore).
        // Path B (sentences_internal) walks raw NLP text and does not do
        // placeholder substitution at all — it preserves URLs trivially via
        // the untouched sentence spans, which is a different code path.
        List<SemanticProcessingResult> directiveSprs = response.getOutputDoc()
                .getSearchMetadata()
                .getSemanticResultsList()
                .stream()
                .filter(spr -> "token_500_50_preserve_urls".equals(spr.getChunkConfigId()))
                .toList();

        assertThat(directiveSprs)
                .as("URL preservation: must find the directive-driven SPR "
                        + "(chunk_config_id='token_500_50_preserve_urls') in the "
                        + "response")
                .hasSize(1);

        List<SemanticChunk> chunks = directiveSprs.get(0).getChunksList();
        assertThat(chunks)
                .as("URL preservation: the directive SPR must have at least one "
                        + "chunk to search for URL round-trip")
                .isNotEmpty();

        // The heart of the assertion: walk every chunk's text_content and
        // confirm that EACH of the two original URLs is present literally
        // in at least one chunk. If OverlapChunker.restorePlaceholdersInChunk
        // regressed (e.g. the placeholder was stripped but not replaced, or
        // the original URL got truncated at a chunk boundary because overlap
        // is wrong), this assertion catches it.
        boolean foundUrlA = chunks.stream()
                .map(c -> c.getEmbeddingInfo().getTextContent())
                .anyMatch(text -> text.contains(URL_A));

        boolean foundUrlB = chunks.stream()
                .map(c -> c.getEmbeddingInfo().getTextContent())
                .anyMatch(text -> text.contains(URL_B));

        assertThat(foundUrlA)
                .as("URL preservation: URL_A ('%s') must round-trip literally "
                        + "into at least one chunk's text_content. If this fails, "
                        + "OverlapChunker.restorePlaceholdersInChunk regressed — "
                        + "check the URL substitution (line ~136-153) and "
                        + "restoration (line ~162-180) paths.",
                        URL_A)
                .isTrue();

        assertThat(foundUrlB)
                .as("URL preservation: URL_B ('%s') must round-trip literally "
                        + "into at least one chunk's text_content",
                        URL_B)
                .isTrue();

        // Negative assertion: no chunk text may leak the raw placeholder
        // format OverlapChunker uses internally. The exact prefix is
        // "__URL_PLACEHOLDER_" (see OverlapChunker.java:43). If any chunk
        // text still contains it, the restore step ran too late or not at
        // all.
        boolean anyLeakingPlaceholder = chunks.stream()
                .map(c -> c.getEmbeddingInfo().getTextContent())
                .anyMatch(text -> text.contains("__URL_PLACEHOLDER_"));

        assertThat(anyLeakingPlaceholder)
                .as("URL preservation: no chunk text_content may contain a raw "
                        + "'__URL_PLACEHOLDER_n__' placeholder — all "
                        + "placeholders must be restored to their original URLs "
                        + "before the chunk is serialised into "
                        + "SemanticChunk.embedding_info")
                .isFalse();
    }
}
