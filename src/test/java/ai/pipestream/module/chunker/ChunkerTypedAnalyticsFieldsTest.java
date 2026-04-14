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
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the three new typed fields PR-K1 added to
 * {@link ai.pipestream.data.v1.ChunkAnalytics} and PR-K2 wired into the
 * chunker:
 *
 * <ol>
 *   <li>{@code content_hash} (string, field 24) — SHA-256 hex of the sanitised
 *       chunk text. Promoted from the loose {@code SemanticChunk.metadata}
 *       map where R1-pack-3 originally added it. The loose-map entry is kept
 *       for backward compat (additive landing); this test asserts the typed
 *       field is populated AND matches the loose-map value byte-for-byte.</li>
 *   <li>{@code adverb_density} (float, field 25) — chunk-local adverb ratio
 *       computed from the slice walk's adverb counter. Completes the POS
 *       density set (noun/verb/adjective/adverb).</li>
 *   <li>{@code text_byte_size} (int32, field 26) — UTF-8 byte size of the
 *       chunk text, distinct from {@code character_count} for non-ASCII
 *       input. This test exercises both the ASCII case (byte_size ==
 *       character_count) and the multi-byte UTF-8 case (CJK input where
 *       byte_size == 3 × character_count).</li>
 * </ol>
 *
 * <p>The loose-map duplication is intentional during the additive period —
 * a follow-up PR will remove the duplicates after a consumer audit confirms
 * no readers depend on the loose-map keys.
 */
@QuarkusTest
class ChunkerTypedAnalyticsFieldsTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    private static final String POS_RICH_BODY =
            "The experienced engineer quickly designed a new distributed system "
            + "for processing semantic documents. Her architecture elegantly "
            + "solved the notoriously difficult problem of chunk-boundary "
            + "determinism across horizontally-scaled workers. The team rapidly "
            + "adopted the approach and deployed it into production within a "
            + "single week. Operators immediately observed a dramatic reduction "
            + "in tail latency and a substantial improvement in throughput.";

    @Test
    void typedContentHashShouldBePopulatedAndMatchLooseMap() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("typed-hash-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(POS_RICH_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "typed-hash");
        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        int chunksChecked = 0;
        for (SemanticProcessingResult spr : sprs) {
            String pathLabel = ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId())
                    ? "Path B sentences_internal"
                    : "Path A directive-driven";

            for (SemanticChunk chunk : spr.getChunksList()) {
                chunksChecked++;
                String chunkCtx = pathLabel + " chunk_id='" + chunk.getChunkId() + "'";

                // Typed field must be populated and look like a SHA-256 hex.
                String typedHash = chunk.getChunkAnalytics().getContentHash();
                assertThat(typedHash)
                        .as("%s: ChunkAnalytics.content_hash (typed field 24) "
                                + "must be a 64-char lowercase hex SHA-256",
                                chunkCtx)
                        .matches("^[0-9a-f]{64}$");

                // Loose-map entry must STILL be present (additive landing) and
                // byte-equivalent to the typed field. After the consumer audit
                // a follow-up PR will remove this duplication.
                Value looseHashVal = chunk.getMetadataMap().get("content_hash");
                assertThat(looseHashVal)
                        .as("%s: loose-map content_hash entry must still exist "
                                + "during the additive landing window", chunkCtx)
                        .isNotNull();

                assertThat(looseHashVal.getStringValue())
                        .as("%s: typed and loose-map content_hash must agree "
                                + "byte-for-byte during the additive window — "
                                + "any drift means the producer is computing "
                                + "two different hashes for the same text",
                                chunkCtx)
                        .isEqualTo(typedHash);
            }
        }

        assertThat(chunksChecked)
                .as("test must have exercised at least 2 chunks "
                        + "(1 Path A + 1 Path B) to cover both code paths")
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void adverbDensityShouldBePopulatedOnPosRichInput() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("adverb-density-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(POS_RICH_BODY)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "adverb-density");
        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        // POS_RICH_BODY contains many adverbs (quickly, elegantly, rapidly,
        // immediately, dramatically, substantially, ...) so at least one
        // chunk in the directive-driven path should have non-zero adverb
        // density via the slice walk's POS counters.
        boolean foundAdverbDensity = false;
        for (SemanticProcessingResult spr : sprs) {
            if (ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId())) {
                continue;
            }
            for (SemanticChunk chunk : spr.getChunksList()) {
                if (chunk.getChunkAnalytics().getAdverbDensity() > 0f) {
                    foundAdverbDensity = true;
                    break;
                }
            }
            if (foundAdverbDensity) break;
        }

        assertThat(foundAdverbDensity)
                .as("at least one Path A chunk over POS-rich body must report "
                        + "non-zero adverb_density. If this fails the chunker is "
                        + "still rolling adverbs into content_word_ratio without "
                        + "exposing the dedicated adverb_density typed field on "
                        + "ChunkAnalytics — check the 8-arg extractChunkAnalytics "
                        + "POS slicing path in ChunkMetadataExtractor.")
                .isTrue();
    }

    @Test
    void textByteSizeShouldEqualCharacterCountForAsciiInput() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("byte-size-ascii-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(POS_RICH_BODY)  // Pure ASCII
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "byte-size-ascii");
        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        int chunksChecked = 0;
        for (SemanticProcessingResult spr : sprs) {
            for (SemanticChunk chunk : spr.getChunksList()) {
                chunksChecked++;
                ChunkAnalytics analytics = chunk.getChunkAnalytics();
                String chunkCtx = "chunk_id='" + chunk.getChunkId() + "'";

                assertThat(analytics.getTextByteSize())
                        .as("%s: text_byte_size must be > 0 for non-empty chunk",
                                chunkCtx)
                        .isGreaterThan(0);

                // For pure ASCII input every Java char is exactly one UTF-8
                // byte, so character_count and text_byte_size are equal.
                assertThat(analytics.getTextByteSize())
                        .as("%s: ASCII input → text_byte_size (UTF-8 bytes) must "
                                + "equal character_count (Java chars). Divergence "
                                + "means the chunker is computing one of them wrong.",
                                chunkCtx)
                        .isEqualTo(analytics.getCharacterCount());
            }
        }

        assertThat(chunksChecked)
                .as("test must have exercised at least 1 chunk")
                .isGreaterThan(0);
    }

    @Test
    void textByteSizeShouldExceedCharacterCountForMultiByteUtf8() {
        // CJK text: each character is exactly 3 bytes in UTF-8, so
        // text_byte_size == 3 × character_count is the invariant. This is
        // the test that catches a regression where someone "optimises"
        // text_byte_size to text.length() and ships it.
        // The body is intentionally structured as several sentences so the
        // chunker produces real sentence chunks for Path B; Path A may or
        // may not split it depending on the token chunker config.
        String cjkBody =
                "東京の天気は今日とても良いです。明日も晴れるでしょう。"
                + "私たちは公園で散歩をしました。子供たちは楽しそうに遊んでいました。"
                + "夕方になると、空が美しいオレンジ色に変わりました。"
                + "夜には星がたくさん見えました。本当に素晴らしい一日でした。";

        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("byte-size-cjk-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(cjkBody)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "byte-size-cjk");
        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        int chunksChecked = 0;
        for (SemanticProcessingResult spr : sprs) {
            for (SemanticChunk chunk : spr.getChunksList()) {
                chunksChecked++;
                ChunkAnalytics analytics = chunk.getChunkAnalytics();
                String chunkCtx = "chunk_id='" + chunk.getChunkId() + "'";

                assertThat(analytics.getTextByteSize())
                        .as("%s: CJK input → text_byte_size must be > 0",
                                chunkCtx)
                        .isGreaterThan(0);

                // CJK chars are 3 bytes each in UTF-8. Allow some slack for
                // ASCII punctuation in the chunk (whitespace from chunker
                // normalisation, etc.) — strictly, byte_size > char_count.
                // For pure CJK chunks the ratio is exactly 3:1.
                assertThat(analytics.getTextByteSize())
                        .as("%s: CJK input → text_byte_size (%d) must be "
                                + "STRICTLY GREATER than character_count (%d) "
                                + "because every CJK code point is 3 UTF-8 bytes. "
                                + "Equality means text_byte_size is being computed "
                                + "as text.length() instead of "
                                + "text.getBytes(UTF_8).length.",
                                chunkCtx,
                                analytics.getTextByteSize(),
                                analytics.getCharacterCount())
                        .isGreaterThan(analytics.getCharacterCount());
            }
        }

        assertThat(chunksChecked)
                .as("CJK test must have exercised at least 1 chunk")
                .isGreaterThan(0);
    }

    private ProcessDataResponse runProcessData(PipeDoc doc, String streamIdPrefix) {
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("typed-fields-pipeline")
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
                .as("processData should succeed")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);
        return response;
    }
}
