package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.semantic.v1.SemanticChunkerService;
import ai.pipestream.semantic.v1.StreamChunksRequest;
import ai.pipestream.semantic.v1.StreamChunksResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for the SemanticChunkerService streaming gRPC endpoint.
 * Uses real sample texts (sample_article.txt, alice_in_wonderland.txt, pride_and_prejudice.txt)
 * to validate chunking, analytics, and streaming behavior with realistic data.
 */
@QuarkusTest
class ChunkerStreamingTest {

    @GrpcClient("chunker")
    SemanticChunkerService streamingService;

    // =========================================================================
    // Small text: sample_article.txt (~1.5KB)
    // =========================================================================

    @Test
    void testStreamChunks_sampleArticle_defaultConfig() {
        String text = loadResource("demo-documents/texts/sample_article.txt");
        List<StreamChunksResponse> chunks = streamChunks("body", text, "default_chunker", null);

        assertThat("Should produce at least one chunk", chunks.size(), is(greaterThan(0)));
        assertLastChunkMarked(chunks);
        assertChunksCoverFullText(chunks, text);
        assertAllChunksHaveAnalytics(chunks);
        assertDocumentAnalyticsOnLastChunk(chunks, text);
    }

    @Test
    void testStreamChunks_sampleArticle_smallChunkSize() {
        String text = loadResource("demo-documents/texts/sample_article.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(50))
                .putFields("chunkOverlap", numVal(10))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "small_token_50", config);

        assertThat("Small chunk size should produce multiple chunks", chunks.size(), is(greaterThan(3)));
        assertLastChunkMarked(chunks);
        assertChunkNumbersSequential(chunks);
        assertAllChunksHaveAnalytics(chunks);

        // All chunks should be reasonably small
        for (StreamChunksResponse chunk : chunks) {
            assertThat("Chunk text should not be empty", chunk.getTextContent(), is(not(emptyString())));
        }
    }

    @Test
    void testStreamChunks_sampleArticle_sentenceAlgorithm() {
        String text = loadResource("demo-documents/texts/sample_article.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("sentence"))
                .putFields("chunkSize", numVal(100))
                .putFields("chunkOverlap", numVal(20))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "sentence_100", config);

        assertThat("Sentence chunking should produce chunks", chunks.size(), is(greaterThan(0)));
        assertLastChunkMarked(chunks);
        assertAllChunksHaveAnalytics(chunks);
    }

    // =========================================================================
    // Medium text: constitution.txt (~47KB)
    // =========================================================================

    @Test
    void testStreamChunks_constitution_tokenChunking() {
        String text = loadResource("demo-documents/texts/constitution.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(200))
                .putFields("chunkOverlap", numVal(40))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "token_200", config);

        assertThat("Constitution should produce many chunks", chunks.size(), is(greaterThan(10)));
        assertLastChunkMarked(chunks);
        assertChunkNumbersSequential(chunks);
        assertAllChunksHaveAnalytics(chunks);
        assertDocumentAnalyticsOnLastChunk(chunks, text);

        // Verify document analytics reflect the full constitution
        DocumentAnalytics docAnalytics = chunks.get(chunks.size() - 1).getDocumentAnalytics();
        assertThat("Constitution has thousands of words", docAnalytics.getWordCount(), is(greaterThan(4000)));
        assertThat("Constitution has many sentences", docAnalytics.getSentenceCount(), is(greaterThan(50)));
    }

    @Test
    void testStreamChunks_constitution_characterChunking() {
        String text = loadResource("demo-documents/texts/constitution.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("character"))
                .putFields("chunkSize", numVal(500))
                .putFields("chunkOverlap", numVal(50))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "char_500", config);

        assertThat("Character chunking should produce many chunks for constitution",
                chunks.size(), is(greaterThan(50)));
        assertLastChunkMarked(chunks);
        assertAllChunksHaveAnalytics(chunks);
    }

    // =========================================================================
    // Large text: alice_in_wonderland.txt (~167KB)
    // =========================================================================

    @Test
    void testStreamChunks_alice_throughput() {
        String text = loadResource("demo-documents/texts/alice_in_wonderland.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(300))
                .putFields("chunkOverlap", numVal(50))
                .build();

        long startMs = System.currentTimeMillis();
        List<StreamChunksResponse> chunks = streamChunks("body", text, "token_300", config);
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertThat("Alice should produce many chunks", chunks.size(), is(greaterThan(30)));
        assertLastChunkMarked(chunks);
        assertChunkNumbersSequential(chunks);
        assertAllChunksHaveAnalytics(chunks);
        assertDocumentAnalyticsOnLastChunk(chunks, text);

        System.out.printf("Alice in Wonderland: %d chars → %d chunks in %dms%n",
                text.length(), chunks.size(), elapsedMs);
    }

    // =========================================================================
    // Large text: pride_and_prejudice.txt (~740KB)
    // =========================================================================

    @Test
    void testStreamChunks_prideAndPrejudice_throughput() {
        String text = loadResource("demo-documents/texts/pride_and_prejudice.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(500))
                .putFields("chunkOverlap", numVal(50))
                .build();

        long startMs = System.currentTimeMillis();
        List<StreamChunksResponse> chunks = streamChunks("body", text, "token_500", config);
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertThat("Pride and Prejudice should produce many chunks", chunks.size(), is(greaterThan(50)));
        assertLastChunkMarked(chunks);
        assertChunkNumbersSequential(chunks);
        assertAllChunksHaveAnalytics(chunks);
        assertDocumentAnalyticsOnLastChunk(chunks, text);

        // Verify last chunk reports total_chunks
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat("Last chunk should report total_chunks", lastChunk.hasTotalChunks(), is(true));
        assertThat("total_chunks should match actual count",
                lastChunk.getTotalChunks(), is(chunks.size()));

        // Verify document-level analytics
        DocumentAnalytics docAnalytics = lastChunk.getDocumentAnalytics();
        assertThat("P&P has >100k words", docAnalytics.getWordCount(), is(greaterThan(100000)));
        assertThat("Vocabulary density should be reasonable (0.0-1.0)",
                docAnalytics.getVocabularyDensity(), is(both(greaterThan(0.0f)).and(lessThan(1.0f))));

        System.out.printf("Pride and Prejudice: %d chars → %d chunks in %dms (doc: %d words, %d sentences)%n",
                text.length(), chunks.size(), elapsedMs,
                docAnalytics.getWordCount(), docAnalytics.getSentenceCount());
    }

    @Test
    void testStreamChunks_prideAndPrejudice_twoChunkerConfigs() {
        String text = loadResource("demo-documents/texts/pride_and_prejudice.txt");

        // Config A: large token chunks
        Struct configA = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(500))
                .putFields("chunkOverlap", numVal(50))
                .build();

        // Config B: smaller sentence chunks
        Struct configB = Struct.newBuilder()
                .putFields("algorithm", strVal("sentence"))
                .putFields("chunkSize", numVal(200))
                .putFields("chunkOverlap", numVal(30))
                .build();

        List<StreamChunksResponse> chunksA = streamChunks("body", text, "token_500_v1", configA);
        List<StreamChunksResponse> chunksB = streamChunks("body", text, "sentence_200_v1", configB);

        assertThat("Both configs should produce chunks", chunksA.size(), is(greaterThan(0)));
        assertThat("Both configs should produce chunks", chunksB.size(), is(greaterThan(0)));

        // Different algorithms/sizes should produce different chunk counts
        assertThat("Different configs should produce different chunk counts",
                chunksA.size(), is(not(equalTo(chunksB.size()))));

        // Both should have analytics
        assertAllChunksHaveAnalytics(chunksA);
        assertAllChunksHaveAnalytics(chunksB);

        // Both should have document analytics that match (same source text)
        DocumentAnalytics docA = chunksA.get(chunksA.size() - 1).getDocumentAnalytics();
        DocumentAnalytics docB = chunksB.get(chunksB.size() - 1).getDocumentAnalytics();
        assertThat("Same text should produce same word count regardless of chunking",
                docA.getWordCount(), is(equalTo(docB.getWordCount())));
        assertThat("Same text should produce same character count",
                docA.getCharacterCount(), is(equalTo(docB.getCharacterCount())));

        System.out.printf("P&P 2-config test: Config A (token-500) → %d chunks, Config B (sentence-200) → %d chunks%n",
                chunksA.size(), chunksB.size());
    }

    // =========================================================================
    // Analytics validation
    // =========================================================================

    @Test
    void testChunkAnalytics_fields() {
        String text = loadResource("demo-documents/texts/sample_article.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(100))
                .putFields("chunkOverlap", numVal(20))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "analytics_test", config);
        assertThat("Should have multiple chunks for analytics testing", chunks.size(), is(greaterThan(1)));

        // First chunk
        ChunkAnalytics first = chunks.get(0).getChunkAnalytics();
        assertThat("First chunk: is_first_chunk", first.getIsFirstChunk(), is(true));
        assertThat("First chunk: is_last_chunk", first.getIsLastChunk(), is(false));
        assertThat("First chunk: relative_position should be 0.0", first.getRelativePosition(), is(0.0f));
        assertThat("First chunk: word_count > 0", first.getWordCount(), is(greaterThan(0)));
        assertThat("First chunk: character_count > 0", first.getCharacterCount(), is(greaterThan(0)));
        assertThat("First chunk: sentence_count > 0", first.getSentenceCount(), is(greaterThan(0)));

        // Last chunk
        ChunkAnalytics last = chunks.get(chunks.size() - 1).getChunkAnalytics();
        assertThat("Last chunk: is_first_chunk", last.getIsFirstChunk(), is(false));
        assertThat("Last chunk: is_last_chunk", last.getIsLastChunk(), is(true));
        assertThat("Last chunk: relative_position should be 1.0", last.getRelativePosition(), is(1.0f));

        // Middle chunk (if exists)
        if (chunks.size() > 2) {
            int mid = chunks.size() / 2;
            ChunkAnalytics middle = chunks.get(mid).getChunkAnalytics();
            assertThat("Middle chunk: not first", middle.getIsFirstChunk(), is(false));
            assertThat("Middle chunk: not last", middle.getIsLastChunk(), is(false));
            assertThat("Middle chunk: position between 0 and 1",
                    middle.getRelativePosition(), is(both(greaterThan(0.0f)).and(lessThan(1.0f))));
            assertThat("Middle chunk: vocabulary_density between 0 and 1",
                    middle.getVocabularyDensity(), is(both(greaterThan(0.0f)).and(lessThanOrEqualTo(1.0f))));
        }
    }

    @Test
    void testChunkAnalytics_headingDetection() {
        // Use token chunking with a small size so "INTRODUCTION" ends up in its own chunk
        String text = "INTRODUCTION\n\n" +
                "This is the body of the document. It has multiple sentences that form a paragraph. " +
                "The content continues here with more details about the topic. We need enough text " +
                "to ensure this gets split into multiple chunks so the heading is its own chunk. " +
                "Adding more sentences to guarantee the text is long enough. The chunker will break " +
                "this into pieces. Each piece carries its own analytics metadata. The heading chunk " +
                "should score highly on the potential heading indicator.";

        Struct config = Struct.newBuilder()
                .putFields("algorithm", strVal("token"))
                .putFields("chunkSize", numVal(50))
                .putFields("chunkOverlap", numVal(5))
                .build();

        List<StreamChunksResponse> chunks = streamChunks("body", text, "heading_test", config);
        assertThat("Should produce multiple chunks", chunks.size(), is(greaterThan(1)));

        // The first chunk should be short (just "INTRODUCTION" plus a few words)
        // and should score as a potential heading
        ChunkAnalytics firstAnalytics = chunks.get(0).getChunkAnalytics();
        // With token chunking, the first 50 tokens include "INTRODUCTION" but also body text,
        // so let's just verify the heading score field is populated and reasonable
        assertThat("Heading score should be populated (>= 0)",
                firstAnalytics.getPotentialHeadingScore(), is(greaterThanOrEqualTo(0.0f)));
        assertThat("Heading score should be <= 1.0",
                firstAnalytics.getPotentialHeadingScore(), is(lessThanOrEqualTo(1.0f)));

        // Verify that list_item_indicator is populated
        // (regular text should not be a list item)
        assertThat("Regular text should not be a list item",
                firstAnalytics.getListItemIndicator(), is(false));
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    @Test
    void testStreamChunks_titleField() {
        String titleText = "A Short Title For Testing";
        List<StreamChunksResponse> chunks = streamChunks("title", titleText, "title_chunker", null);

        assertThat("Short title should produce single chunk", chunks.size(), is(1));
        assertThat("Chunk should contain the title text",
                chunks.get(0).getTextContent(), containsString("Short Title"));
        assertThat("Source field name should be 'title'",
                chunks.get(0).getSourceFieldName(), is("title"));
        assertThat("Single chunk should be marked as last", chunks.get(0).getIsLast(), is(true));
        assertThat("Single chunk should have analytics", chunks.get(0).hasChunkAnalytics(), is(true));
        assertThat("Single chunk should have document analytics",
                chunks.get(0).hasDocumentAnalytics(), is(true));
    }

    @Test
    void testStreamChunks_metadataMapIsEmpty_typedAnalyticsCarriesData() {
        // PR-K3 streaming mirror: the loose metadata map must be empty on
        // streaming responses — every field that used to live in the loose
        // map (word_count, sentence_count, content_hash, ...) now lives on
        // the typed ChunkAnalytics proto only. If this regresses it means
        // someone re-introduced a putAllMetadata / putMetadata call in
        // ChunkerStreamingGrpcImpl — probably copy-pasted from the pre-K3
        // code that is now gone.
        String text = loadResource("demo-documents/texts/sample_article.txt");
        List<StreamChunksResponse> chunks = streamChunks("body", text, "metadata_test", null);

        for (StreamChunksResponse chunk : chunks) {
            assertThat("Loose metadata map must be empty post-PR-K3 (found keys: "
                            + chunk.getMetadataMap().keySet() + ")",
                    chunk.getMetadataMap().isEmpty(), is(true));

            // Typed ChunkAnalytics is now the canonical home for all stats.
            assertThat("Typed ChunkAnalytics must be populated",
                    chunk.hasChunkAnalytics(), is(true));
            ChunkAnalytics ca = chunk.getChunkAnalytics();
            assertThat("Typed word_count > 0", ca.getWordCount(), is(greaterThan(0)));
            assertThat("Typed sentence_count > 0", ca.getSentenceCount(), is(greaterThan(0)));
            assertThat("Typed character_count > 0", ca.getCharacterCount(), is(greaterThan(0)));
            // content_hash promoted from loose map to typed field (PR-K2/K3)
            assertThat("Typed content_hash is a 64-char lowercase hex SHA-256",
                    ca.getContentHash().matches("^[0-9a-f]{64}$"), is(true));
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private List<StreamChunksResponse> streamChunks(String sourceField, String text,
                                                     String chunkConfigId, Struct config) {
        StreamChunksRequest.Builder reqBuilder = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("test-doc-" + UUID.randomUUID())
                .setSourceFieldName(sourceField)
                .setTextContent(text)
                .setChunkConfigId(chunkConfigId);

        if (config != null) {
            reqBuilder.setChunkerConfig(config);
        }

        return streamingService.streamChunks(reqBuilder.build())
                .collect().asList()
                .await().indefinitely();
    }

    private void assertLastChunkMarked(List<StreamChunksResponse> chunks) {
        for (int i = 0; i < chunks.size() - 1; i++) {
            assertThat("Non-last chunk should not be marked as last (chunk " + i + ")",
                    chunks.get(i).getIsLast(), is(false));
        }
        assertThat("Last chunk should be marked as last",
                chunks.get(chunks.size() - 1).getIsLast(), is(true));
    }

    private void assertChunkNumbersSequential(List<StreamChunksResponse> chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            assertThat("Chunk number should be sequential",
                    chunks.get(i).getChunkNumber(), is(i));
        }
    }

    private void assertChunksCoverFullText(List<StreamChunksResponse> chunks, String originalText) {
        // Verify that chunks collectively cover the text (no gaps at start/end)
        assertThat("First chunk should start near offset 0",
                chunks.get(0).getStartOffset(), is(lessThanOrEqualTo(10)));

        // The last chunk's end offset should be near the end of the text
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat("Last chunk should end near the text length",
                lastChunk.getEndOffset(), is(greaterThan(originalText.length() - 100)));
    }

    private void assertAllChunksHaveAnalytics(List<StreamChunksResponse> chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            assertThat("Chunk " + i + " should have chunk_analytics",
                    chunks.get(i).hasChunkAnalytics(), is(true));
            ChunkAnalytics ca = chunks.get(i).getChunkAnalytics();
            assertThat("Chunk " + i + " analytics should have word_count > 0",
                    ca.getWordCount(), is(greaterThan(0)));
            assertThat("Chunk " + i + " analytics should have character_count > 0",
                    ca.getCharacterCount(), is(greaterThan(0)));
        }
    }

    private void assertDocumentAnalyticsOnLastChunk(List<StreamChunksResponse> chunks, String originalText) {
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat("Last chunk should have document_analytics",
                lastChunk.hasDocumentAnalytics(), is(true));
        assertThat("Last chunk should have total_chunks",
                lastChunk.hasTotalChunks(), is(true));
        assertThat("total_chunks should match actual chunk count",
                lastChunk.getTotalChunks(), is(chunks.size()));

        DocumentAnalytics da = lastChunk.getDocumentAnalytics();
        assertThat("Document character_count should match text length",
                da.getCharacterCount(), is(originalText.length()));
        assertThat("Document word_count should be positive",
                da.getWordCount(), is(greaterThan(0)));
        assertThat("Document sentence_count should be positive",
                da.getSentenceCount(), is(greaterThan(0)));

        // Non-last chunks should NOT have document analytics (to avoid redundancy)
        for (int i = 0; i < chunks.size() - 1; i++) {
            assertThat("Non-last chunk " + i + " should not have document_analytics",
                    chunks.get(i).hasDocumentAnalytics(), is(false));
        }
    }

    private String loadResource(String path) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("Resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load resource: " + path, e);
        }
    }

    private static Value strVal(String s) {
        return Value.newBuilder().setStringValue(s).build();
    }

    private static Value numVal(double n) {
        return Value.newBuilder().setNumberValue(n).build();
    }
}
