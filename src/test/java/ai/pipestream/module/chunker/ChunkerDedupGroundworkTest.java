package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.SourceFieldAnalytics;
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
 * Pins the four PR-E production changes from the post-R1 correctness audit,
 * updated through the PR-K series:
 *
 * <ol>
 *   <li><b>content_hash on every chunk</b> — both paths stamp a SHA-256 hex
 *       of the sanitised chunk text. Originally PR-E added it to the loose
 *       {@code SemanticChunk.metadata} map; PR-K2 promoted it to the typed
 *       {@code chunk_analytics.content_hash} field; <b>PR-K3 removed the
 *       loose-map duplicate</b>. The typed proto field is now the canonical
 *       and only home. Enables reprocessing dedup, content-addressed
 *       embedder cache keys, and byte-verification of a future OpenVINO
 *       chunker backend against this implementation.</li>
 *   <li><b>SourceFieldAnalytics fields 3-7</b> — {@code document_analytics},
 *       {@code total_chunks}, {@code average_chunk_size},
 *       {@code min_chunk_size}, and {@code max_chunk_size} are populated on
 *       every SFA entry instead of just {@code source_field} and
 *       {@code chunk_config_id}.</li>
 *   <li><b>Path B sentences_internal chunks have populated chunk_analytics</b>
 *       — every sentence chunk carries a fully-populated
 *       {@code ChunkAnalytics} typed proto. PR-E originally also populated
 *       a legacy string-keyed {@code SemanticChunk.metadata} map for these
 *       chunks; <b>PR-K3 removed the loose-map duplicate</b>. The typed
 *       proto is the only place to read these fields now.</li>
 *   <li><b>parseNamedChunkerConfig hard-fails</b> — a malformed per-config
 *       Struct (unknown field type) now returns {@code PROCESSING_OUTCOME_FAILURE}
 *       with an "Invalid NamedChunkerConfig" message instead of silently
 *       demoting to the default token chunker.</li>
 * </ol>
 */
@QuarkusTest
class ChunkerDedupGroundworkTest {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    private static final String BODY_TEXT =
            "Deduplication groundwork is the point of this test. "
            + "The chunker stamps a SHA-256 content_hash into every chunk "
            + "so downstream reprocessing can skip already-computed work. "
            + "SourceFieldAnalytics gets proto fields 3-7 populated so "
            + "document-level chunk-size statistics travel through the "
            + "pipeline. And sentence chunks finally carry the legacy "
            + "metadata map they were missing in R1-pack-2.";

    // =========================================================================
    // (1) content_hash on every chunk — both paths
    // =========================================================================

    @Test
    void everyChunkShouldCarrySha256ContentHash() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("content-hash-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "content-hash");

        List<SemanticProcessingResult> sprs = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList();

        assertThat(sprs)
                .as("Response must contain at least the directive SPR and the "
                        + "sentences_internal SPR (§21.9 union)")
                .isNotEmpty();

        // Every chunk in every SPR must have content_hash on the typed
        // ChunkAnalytics proto, and the hash must be a valid 64-char
        // lowercase hex string. PR-K3 removed the loose-map duplicate so
        // the typed field is the only source of truth now.
        int totalChunksSeen = 0;
        for (SemanticProcessingResult spr : sprs) {
            String pathLabel = ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId())
                    ? "Path B sentences_internal"
                    : "Path A directive-driven";

            for (SemanticChunk chunk : spr.getChunksList()) {
                totalChunksSeen++;
                String chunkCtx = pathLabel + " chunk_id='" + chunk.getChunkId() + "'";

                String hashStr = chunk.getChunkAnalytics().getContentHash();
                assertThat(hashStr)
                        .as("%s: chunk_analytics.content_hash (typed proto) "
                                + "must be a 64-char lowercase hex SHA-256",
                                chunkCtx)
                        .matches("^[0-9a-f]{64}$");

                // PR-K3: loose-map content_hash entry must be GONE. Any
                // future regression that re-introduces it (e.g. someone
                // copy-pasting from the streaming impl) lights this up.
                assertThat(chunk.getMetadataMap())
                        .as("%s: SemanticChunk.metadata must NOT contain a "
                                + "loose 'content_hash' key — PR-K3 removed "
                                + "the duplicate; the typed field is canonical",
                                chunkCtx)
                        .doesNotContainKey("content_hash");
            }
        }

        assertThat(totalChunksSeen)
                .as("Test must have exercised at least 2 chunks (1 from Path A "
                        + "+ 1 from Path B) to cover both code paths")
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void identicalChunkTextShouldProduceIdenticalContentHash() {
        // Run the SAME document twice and confirm content_hash is byte-identical
        // on chunks that share the same sanitized text. This pins the
        // deterministic-hash property — the whole point of the dedup plumbing.
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("hash-determinism-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse r1 = runProcessData(inputDoc, "hash-determinism-run-1");
        ProcessDataResponse r2 = runProcessData(inputDoc, "hash-determinism-run-2");

        List<String> hashes1 = extractContentHashes(r1);
        List<String> hashes2 = extractContentHashes(r2);

        assertThat(hashes1)
                .as("First run must produce at least one content_hash")
                .isNotEmpty();

        assertThat(hashes2)
                .as("content_hash values must be byte-identical across repeated "
                        + "runs for the same input — SHA-256 is deterministic "
                        + "and chunk_id is deterministic per §21.5, so any "
                        + "divergence means the sanitisation or hash pipeline "
                        + "became non-deterministic")
                .isEqualTo(hashes1);
    }

    // =========================================================================
    // (2) SourceFieldAnalytics fields 3-7 populated
    // =========================================================================

    @Test
    void sourceFieldAnalyticsShouldPopulateFields3Through7() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("sfa-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "sfa-test");

        List<SourceFieldAnalytics> sfas = response.getOutputDoc()
                .getSearchMetadata().getSourceFieldAnalyticsList();

        assertThat(sfas)
                .as("source_field_analytics must be populated per §5.1 — at "
                        + "least one entry for each (source_field, "
                        + "chunk_config_id) pair in semantic_results")
                .isNotEmpty();

        // Find the directive-driven SFA entry (not sentences_internal).
        SourceFieldAnalytics directiveSfa = sfas.stream()
                .filter(sfa -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(sfa.getChunkConfigId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No directive-driven SourceFieldAnalytics entry found"));

        // Field 3: document_analytics
        assertThat(directiveSfa.hasDocumentAnalytics())
                .as("SFA field 3: document_analytics must be set on the "
                        + "directive-driven SFA entry (source_field='%s' "
                        + "chunk_config_id='%s')",
                        directiveSfa.getSourceField(), directiveSfa.getChunkConfigId())
                .isTrue();

        assertThat(directiveSfa.getDocumentAnalytics().getWordCount())
                .as("SFA field 3: document_analytics.word_count must be > 0 "
                        + "for a non-empty body text")
                .isGreaterThan(0);

        // Field 4: total_chunks
        assertThat(directiveSfa.getTotalChunks())
                .as("SFA field 4: total_chunks must match the directive SPR's "
                        + "chunks.size()")
                .isGreaterThan(0);

        // Field 5: average_chunk_size
        assertThat(directiveSfa.getAverageChunkSize())
                .as("SFA field 5: average_chunk_size must be > 0 when the "
                        + "directive SPR has at least one chunk")
                .isGreaterThan(0f);

        // Field 6: min_chunk_size
        assertThat(directiveSfa.getMinChunkSize())
                .as("SFA field 6: min_chunk_size must be > 0 for a "
                        + "directive SPR with non-empty chunks")
                .isGreaterThan(0);

        // Field 7: max_chunk_size
        assertThat(directiveSfa.getMaxChunkSize())
                .as("SFA field 7: max_chunk_size must be >= min_chunk_size")
                .isGreaterThanOrEqualTo(directiveSfa.getMinChunkSize());

        // Cross-check: total_chunks equals the actual chunk count on the SPR
        SemanticProcessingResult directiveSpr = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> directiveSfa.getSourceField().equals(spr.getSourceFieldName())
                        && directiveSfa.getChunkConfigId().equals(spr.getChunkConfigId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No SPR matching the directive SFA entry"));

        assertThat(directiveSfa.getTotalChunks())
                .as("SFA field 4: total_chunks must equal the matched SPR's "
                        + "chunks.size() (%d)", directiveSpr.getChunksCount())
                .isEqualTo(directiveSpr.getChunksCount());
    }

    // =========================================================================
    // (3) Path B sentences_internal chunks have populated typed chunk_analytics
    //
    // PR-K3 inverted this test: pre-PR-K3 it asserted that the loose metadata
    // map was POPULATED on sentence chunks (mirroring Path A's behavior).
    // PR-K3 removed both Path A's AND Path B's loose-map writes — the typed
    // ChunkAnalytics proto is now the canonical and only home. This test
    // now asserts (a) the loose map is EMPTY and (b) the typed proto carries
    // every field that used to live in the loose map.
    // =========================================================================

    @Test
    void sentencesInternalChunksShouldHaveTypedAnalyticsAndEmptyLooseMap() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("path-b-typed-analytics-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "path-b-typed");

        SemanticProcessingResult sentencesSpr = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No sentences_internal SPR in response — §21.9 union broke"));

        assertThat(sentencesSpr.getChunksList())
                .as("sentences_internal SPR must have at least one chunk")
                .isNotEmpty();

        for (SemanticChunk chunk : sentencesSpr.getChunksList()) {
            String ctx = "sentences_internal chunk_id='" + chunk.getChunkId() + "'";

            // PR-K3: loose metadata map must be EMPTY for chunks emitted by
            // the chunker. Future caller-injected metadata can land here as
            // an extension point, but the chunker itself writes nothing.
            assertThat(chunk.getMetadataMap())
                    .as("%s: SemanticChunk.metadata must be EMPTY — PR-K3 "
                            + "removed all loose-map writes from the chunker. "
                            + "Any non-empty entries are a regression where "
                            + "someone is re-introducing the duplication.",
                            ctx)
                    .isEmpty();

            // The typed ChunkAnalytics proto must carry every field that
            // used to live in the loose map. Spot-check the most important
            // ones — the rest are pinned by ChunkerStepInvariantsTest's
            // post-chunker invariant check.
            assertThat(chunk.hasChunkAnalytics())
                    .as("%s: chunk_analytics must be populated (§4.1)", ctx)
                    .isTrue();

            ChunkAnalytics analytics = chunk.getChunkAnalytics();

            assertThat(analytics.getWordCount())
                    .as("%s: chunk_analytics.word_count must be > 0 for a "
                            + "non-empty sentence", ctx)
                    .isGreaterThan(0);

            assertThat(analytics.getCharacterCount())
                    .as("%s: chunk_analytics.character_count must be > 0", ctx)
                    .isGreaterThan(0);

            assertThat(analytics.getSentenceCount())
                    .as("%s: chunk_analytics.sentence_count must be > 0", ctx)
                    .isGreaterThan(0);

            // Path B never runs URL substitution, so contains_url_placeholder
            // must always be false on sentence chunks.
            assertThat(analytics.getContainsUrlPlaceholder())
                    .as("%s: chunk_analytics.contains_url_placeholder must be "
                            + "false on Path B — sentence chunks bypass the "
                            + "URL substitute/restore pipeline (they walk raw "
                            + "NLP spans)", ctx)
                    .isFalse();

            // PR-K2 added content_hash to the typed proto.
            assertThat(analytics.getContentHash())
                    .as("%s: chunk_analytics.content_hash must be a 64-char "
                            + "lowercase hex SHA-256", ctx)
                    .matches("^[0-9a-f]{64}$");
        }
    }

    // =========================================================================
    // (4) parseNamedChunkerConfig hard-fails on malformed per-config Struct
    // =========================================================================

    @Test
    void malformedNamedChunkerConfigShouldReturnFailure() {
        // Build a directive whose NamedChunkerConfig.config Struct has an
        // unknown field that will not deserialize into ChunkerConfig. The
        // current ObjectMapper is strict about unknown properties (per pack-1
        // ChunkerConfig setup) so this triggers the parseNamedChunkerConfig
        // failure path.
        Struct malformedConfig = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(500).build())
                // chunkOverlap is an int field — sending a string triggers a
                // Jackson MismatchedInputException during deserialization.
                .putFields("chunkOverlap", Value.newBuilder().setStringValue("not-an-int").build())
                .build();

        NamedChunkerConfig chunker = NamedChunkerConfig.newBuilder()
                .setConfigId("malformed_token_config")
                .setConfig(malformedConfig)
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
                .setDocId("malformed-config-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Some body text for the malformed config test.")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("malformed-config-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId("malformed-stream-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("Malformed NamedChunkerConfig.config must return "
                        + "PROCESSING_OUTCOME_FAILURE — no silent demotion to "
                        + "default token chunker per §21.1 no-fallback rule")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);

        boolean hasInvalidLog = response.getLogEntriesList().stream()
                .anyMatch(e -> e.getMessage().contains("Invalid NamedChunkerConfig"));
        assertThat(hasInvalidLog)
                .as("Error log must mention 'Invalid NamedChunkerConfig' so the "
                        + "operator can see exactly which config_id was "
                        + "malformed and why the request failed")
                .isTrue();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private ProcessDataResponse runProcessData(PipeDoc doc, String streamIdPrefix) {
        // PR-K3: disable the chunk cache so each test run computes from
        // scratch. The cache key is (sourceText, chunkerConfigId), and
        // BODY_TEXT is a constant — without this, an entry written by an
        // earlier test run (or an earlier branch's code) shadows the
        // current code path's output. Cache hits would skip the new
        // metadata-map-empty behavior and surface as confusing failures
        // that look like the production code is still writing the loose
        // map when in fact it isn't.
        Struct cacheDisabledConfig = Struct.newBuilder()
                .putFields("cache_enabled",
                        com.google.protobuf.Value.newBuilder().setBoolValue(false).build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("dedup-groundwork-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId(streamIdPrefix + "-" + UUID.randomUUID())
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(cacheDisabledConfig)
                        .build())
                .build();

        ProcessDataResponse response = chunkerService.processData(request).await().indefinitely();

        assertThat(response.getOutcome())
                .as("processData should succeed for a valid directive-driven input")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        return response;
    }

    private static List<String> extractContentHashes(ProcessDataResponse response) {
        // PR-K3: read from the typed chunk_analytics.content_hash field.
        // The loose-map "content_hash" entry was removed from the chunker
        // output in PR-K3.
        return response.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .flatMap(spr -> spr.getChunksList().stream())
                .map(chunk -> chunk.getChunkAnalytics().getContentHash())
                .filter(s -> !s.isEmpty())
                .sorted()
                .toList();
    }
}
