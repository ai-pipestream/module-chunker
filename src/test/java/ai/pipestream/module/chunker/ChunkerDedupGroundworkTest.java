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
 * Pins the four PR-E production changes from the post-R1 correctness audit:
 *
 * <ol>
 *   <li><b>content_hash on every chunk</b> — both paths stamp a SHA-256 hex
 *       of the sanitised chunk text into {@code SemanticChunk.metadata}
 *       under the {@code "content_hash"} key. Enables reprocessing dedup,
 *       content-addressed embedder cache keys, and byte-verification of a
 *       future OpenVINO chunker backend against this implementation.</li>
 *   <li><b>SourceFieldAnalytics fields 3-7</b> — {@code document_analytics},
 *       {@code total_chunks}, {@code average_chunk_size},
 *       {@code min_chunk_size}, and {@code max_chunk_size} are populated on
 *       every SFA entry instead of just {@code source_field} and
 *       {@code chunk_config_id}.</li>
 *   <li><b>Path B legacy metadata map</b> — {@code sentences_internal} chunks
 *       carry the full string-keyed metadata map (word_count, character_count,
 *       etc.) that Path A chunks already had. Before PR-E, sentence chunks
 *       had only {@code chunk_analytics} (typed proto) and an empty metadata
 *       map, silently giving downstream consumers zero data for sentence
 *       chunks read via the legacy map path.</li>
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

        // Every chunk in every SPR must have content_hash, and the hash must
        // be a valid 64-char lowercase hex string.
        int totalChunksSeen = 0;
        for (SemanticProcessingResult spr : sprs) {
            String pathLabel = ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId())
                    ? "Path B sentences_internal"
                    : "Path A directive-driven";

            for (SemanticChunk chunk : spr.getChunksList()) {
                totalChunksSeen++;
                String chunkCtx = pathLabel + " chunk_id='" + chunk.getChunkId() + "'";

                Value hashVal = chunk.getMetadataMap().get("content_hash");
                assertThat(hashVal)
                        .as("%s: metadata must contain 'content_hash' key",
                                chunkCtx)
                        .isNotNull();

                String hashStr = hashVal.getStringValue();
                assertThat(hashStr)
                        .as("%s: content_hash must be a 64-char lowercase hex "
                                + "SHA-256 digest", chunkCtx)
                        .matches("^[0-9a-f]{64}$");
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
    // (3) Path B legacy metadata map populated
    // =========================================================================

    @Test
    void sentencesInternalChunksShouldCarryLegacyMetadataMap() {
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("path-b-metadata-test-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(BODY_TEXT)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataResponse response = runProcessData(inputDoc, "path-b-metadata");

        SemanticProcessingResult sentencesSpr = response.getOutputDoc()
                .getSearchMetadata().getSemanticResultsList().stream()
                .filter(spr -> ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No sentences_internal SPR in response — §21.9 union broke"));

        assertThat(sentencesSpr.getChunksList())
                .as("sentences_internal SPR must have at least one chunk")
                .isNotEmpty();

        // Every sentence chunk must carry the legacy string-keyed metadata
        // map fields that Path A already populated — word_count, character_count,
        // sentence_count, is_first_chunk, is_last_chunk, relative_position,
        // contains_urlplaceholder, etc. Plus content_hash from change (1).
        for (SemanticChunk chunk : sentencesSpr.getChunksList()) {
            String ctx = "sentences_internal chunk_id='" + chunk.getChunkId() + "'";
            var metadata = chunk.getMetadataMap();

            assertThat(metadata)
                    .as("%s: metadata map must NOT be empty — Path B must "
                            + "populate extractAllMetadata just like Path A",
                            ctx)
                    .isNotEmpty();

            assertThat(metadata)
                    .as("%s: metadata map must contain 'word_count'", ctx)
                    .containsKey("word_count");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'character_count'", ctx)
                    .containsKey("character_count");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'sentence_count'", ctx)
                    .containsKey("sentence_count");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'is_first_chunk'", ctx)
                    .containsKey("is_first_chunk");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'is_last_chunk'", ctx)
                    .containsKey("is_last_chunk");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'relative_position'", ctx)
                    .containsKey("relative_position");

            assertThat(metadata)
                    .as("%s: metadata map must contain 'contains_urlplaceholder'", ctx)
                    .containsKey("contains_urlplaceholder");

            assertThat(metadata.get("contains_urlplaceholder").getBoolValue())
                    .as("%s: contains_urlplaceholder is always false on Path B "
                            + "(sentence chunks bypass the URL substitute/restore "
                            + "pipeline — they walk raw NLP spans)", ctx)
                    .isFalse();

            // Sanity-check the word_count is non-zero for non-trivial sentences.
            double wordCount = metadata.get("word_count").getNumberValue();
            assertThat(wordCount)
                    .as("%s: word_count must be > 0 for a non-empty sentence", ctx)
                    .isGreaterThan(0);
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
        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("dedup-groundwork-pipeline")
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
                .as("processData should succeed for a valid directive-driven input")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        return response;
    }

    private static List<String> extractContentHashes(ProcessDataResponse response) {
        return response.getOutputDoc().getSearchMetadata().getSemanticResultsList().stream()
                .flatMap(spr -> spr.getChunksList().stream())
                .map(chunk -> chunk.getMetadataMap().get("content_hash"))
                .filter(v -> v != null)
                .map(Value::getStringValue)
                .sorted()
                .toList();
    }
}
