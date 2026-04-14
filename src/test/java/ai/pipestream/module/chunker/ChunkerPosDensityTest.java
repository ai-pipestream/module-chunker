package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.VectorSetDirectives;
import com.google.protobuf.Struct;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the post-R1 feature-parity fix that switched both chunker paths from
 * the 4-arg {@code extractChunkAnalytics} overload to the 7-arg overload so
 * per-chunk POS densities get populated from the doc-level {@code NlpResult}.
 *
 * <p>Before the fix, {@code ChunkerGrpcImpl} called the 4-arg overload at
 * {@code ChunkerGrpcImpl.java:473-474} (Path A, token/sentence directive
 * chunker) and {@code :563-564} (Path B, always-on sentences_internal), so
 * the six POS-related fields on every {@link ChunkAnalytics} proto were all
 * zero:
 *
 * <ul>
 *   <li>{@code noun_density}</li>
 *   <li>{@code verb_density}</li>
 *   <li>{@code adjective_density}</li>
 *   <li>{@code content_word_ratio}</li>
 *   <li>{@code unique_lemma_count}</li>
 *   <li>{@code lexical_density}</li>
 * </ul>
 *
 * <p>The 7-arg overload at {@code ChunkMetadataExtractor.java:281-331} slices
 * the doc-level {@code NlpResult.posTags()} / {@code .lemmas()} /
 * {@code .tokenSpans()} arrays on the chunk's original-text byte range to
 * compute these fields — no NLP re-run, O(log n) binary search + O(k) scan.
 * The streaming impl at {@code ChunkerStreamingGrpcImpl.java:134-136} has
 * used the 7-arg overload since it was introduced; the R1 rewrite never
 * closed the parity gap. This test pins that it is now closed.
 *
 * <p>Also asserts that the fields are populated on the sentences_internal
 * (Path B) chunks — the correctness audit specifically flagged that both
 * paths had the same 4-arg bug.
 */
@QuarkusTest
class ChunkerPosDensityTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    // Text chosen to be POS-rich: mix of common nouns, proper nouns, verbs
    // of multiple tenses, adjectives, and adverbs so that every density
    // field has something non-zero to compute. Long enough to produce
    // multiple sentence spans (so the NLP preprocessor gives real tag
    // arrays) and to hit the token chunker (so Path A produces real chunks).
    private static final String POS_RICH_BODY =
            "The experienced engineer quickly designed a new distributed system "
            + "for processing semantic documents. Her architecture elegantly "
            + "solved the notoriously difficult problem of chunk-boundary "
            + "determinism across horizontally-scaled workers. The team rapidly "
            + "adopted the approach and deployed it into production within a "
            + "single week. Operators immediately observed a dramatic reduction "
            + "in tail latency and a substantial improvement in throughput. "
            + "Stakeholders enthusiastically praised the elegant and efficient "
            + "solution, and the engineering director publicly celebrated the "
            + "team's remarkable achievement at the quarterly review meeting.";

    @Test
    void directiveDrivenChunksShouldHavePopulatedPosDensities() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("pos-density-directive-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(POS_RICH_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "pos-density-directive");

        SemanticProcessingResult directiveSpr = findDirectiveSpr(response);
        List<SemanticChunk> chunks = directiveSpr.getChunksList();

        assertThat(chunks)
                .as("Path A (directive-driven) must produce at least one chunk "
                        + "from the POS-rich body text")
                .isNotEmpty();

        // The heart of the assertion: every chunk_analytics on every Path A
        // chunk must have AT LEAST ONE of the six POS fields non-zero. If the
        // 4-arg overload regressed back, all six would be zero on every chunk.
        //
        // We allow per-chunk variation (a single-word chunk has no non-zero
        // adjective density) but require that, in aggregate across all chunks,
        // each of the six fields is populated on at least one chunk.
        assertPosDensityFieldsPopulated(chunks, "Path A directive-driven");
    }

    @Test
    void sentencesInternalChunksShouldHavePopulatedPosDensities() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("pos-density-sentences-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(POS_RICH_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "pos-density-sentences");

        List<SemanticProcessingResult> sentenceSprs = response.getOutputDoc()
                .getSearchMetadata()
                .getSemanticResultsList()
                .stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .toList();

        assertThat(sentenceSprs)
                .as("§21.9 union: sentences_internal SPR must be emitted — if "
                        + "this fails, PR-C's union invariant also broke; run "
                        + "ChunkerSentenceUnionAndUrlTest first to isolate")
                .isNotEmpty();

        List<SemanticChunk> chunks = sentenceSprs.get(0).getChunksList();

        assertThat(chunks)
                .as("Path B (sentences_internal) must produce at least one "
                        + "sentence chunk from the POS-rich body text")
                .isNotEmpty();

        // Same aggregate check on Path B chunks.
        assertPosDensityFieldsPopulated(chunks, "Path B sentences_internal");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private ProcessDataResponse runProcessData(PipeDoc doc, String streamIdPrefix) {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("pos-density-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId(streamIdPrefix + "-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("processData should succeed for POS-density test input")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        return response;
    }

    private SemanticProcessingResult findDirectiveSpr(ProcessDataResponse response) {
        return response.getOutputDoc()
                .getSearchMetadata()
                .getSemanticResultsList()
                .stream()
                .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No directive-driven SPR found in processData response — "
                                + "Path A never ran"));
    }

    /**
     * Asserts that across the supplied chunks, at least one chunk has each of
     * the six POS-related {@link ChunkAnalytics} fields non-zero. A chunk-local
     * zero is allowed (very short chunks may legitimately have zero adjectives,
     * etc.) but across the full set of chunks produced from a POS-rich body,
     * every field must land non-zero on at least one chunk.
     */
    private static void assertPosDensityFieldsPopulated(List<SemanticChunk> chunks, String pathLabel) {
        boolean anyNoun = false;
        boolean anyVerb = false;
        boolean anyAdjective = false;
        boolean anyContentWord = false;
        boolean anyUniqueLemma = false;
        boolean anyLexicalDensity = false;

        for (SemanticChunk chunk : chunks) {
            assertThat(chunk.hasChunkAnalytics())
                    .as("%s: every chunk must carry chunk_analytics (§4.1) — "
                            + "chunk_id='%s'",
                            pathLabel, chunk.getChunkId())
                    .isTrue();

            ChunkAnalytics a = chunk.getChunkAnalytics();
            if (a.getNounDensity() > 0f) anyNoun = true;
            if (a.getVerbDensity() > 0f) anyVerb = true;
            if (a.getAdjectiveDensity() > 0f) anyAdjective = true;
            if (a.getContentWordRatio() > 0f) anyContentWord = true;
            if (a.getUniqueLemmaCount() > 0) anyUniqueLemma = true;
            if (a.getLexicalDensity() > 0f) anyLexicalDensity = true;
        }

        assertThat(anyNoun)
                .as("%s: at least one chunk must have non-zero noun_density "
                        + "across %d chunks. If this fails, ChunkerGrpcImpl "
                        + "is calling the 4-arg extractChunkAnalytics overload "
                        + "and dropping POS data — check that the 7-arg overload "
                        + "is used in both Path A and Path B.",
                        pathLabel, chunks.size())
                .isTrue();

        assertThat(anyVerb)
                .as("%s: at least one chunk must have non-zero verb_density",
                        pathLabel)
                .isTrue();

        assertThat(anyAdjective)
                .as("%s: at least one chunk must have non-zero adjective_density",
                        pathLabel)
                .isTrue();

        assertThat(anyContentWord)
                .as("%s: at least one chunk must have non-zero content_word_ratio",
                        pathLabel)
                .isTrue();

        assertThat(anyUniqueLemma)
                .as("%s: at least one chunk must have unique_lemma_count > 0",
                        pathLabel)
                .isTrue();

        assertThat(anyLexicalDensity)
                .as("%s: at least one chunk must have non-zero lexical_density",
                        pathLabel)
                .isTrue();
    }
}
