package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.semantic.v1.ChunkAlgorithm;
import ai.pipestream.semantic.v1.ChunkConfigEntry;
import ai.pipestream.semantic.v1.ChunkerConfig;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-config chunking in the SemanticChunkerService.
 * Tests the new typed ChunkConfigEntry path, NlpDocumentAnalysis population,
 * and POS/language enrichment in analytics, as well as backward compatibility
 * with the legacy single-config Struct path.
 */
@QuarkusTest
class MultiConfigChunkingTest {

    @GrpcClient("chunker")
    SemanticChunkerService streamingService;

    private static final String MULTI_SENTENCE_TEXT =
            "The quick brown fox jumps over the lazy dog. " +
            "Natural language processing is a fascinating field of computer science. " +
            "Researchers develop algorithms that help machines understand human language. " +
            "Deep learning models have transformed the way we approach text analysis. " +
            "Tokenization breaks text into meaningful units for further processing. " +
            "Part-of-speech tagging assigns grammatical categories to each word. " +
            "Named entity recognition identifies proper nouns like New York and Google. " +
            "Sentiment analysis determines whether text expresses positive or negative opinions. " +
            "Machine translation converts text from one language to another automatically. " +
            "Information retrieval systems help users find relevant documents quickly.";

    // =========================================================================
    // Multi-config tests
    // =========================================================================

    @Test
    void twoConfigsSameTextShouldReturnChunksFromBoth() {
        // Config A: sentence chunking, 3 sentences per chunk, 1 overlap
        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(3)
                .setChunkOverlap(1)
                .build();

        // Config B: token chunking, 50 tokens per chunk, 10 overlap
        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(50)
                .setChunkOverlap(10)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("multi-config-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_3_1")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_50_10")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        List<StreamChunksResponse> allResponses = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(allResponses)
                .as("Should produce chunks from both configs")
                .isNotEmpty();

        // Split responses by config ID
        List<StreamChunksResponse> sentenceChunks = allResponses.stream()
                .filter(r -> "sentence_3_1".equals(r.getChunkConfigId()))
                .collect(Collectors.toList());

        List<StreamChunksResponse> tokenChunks = allResponses.stream()
                .filter(r -> "token_50_10".equals(r.getChunkConfigId()))
                .collect(Collectors.toList());

        assertThat(sentenceChunks)
                .as("Sentence config should produce at least one chunk")
                .isNotEmpty();

        assertThat(tokenChunks)
                .as("Token config should produce at least one chunk")
                .isNotEmpty();

        // Different algorithms/sizes should typically produce different chunk counts
        assertThat(sentenceChunks.size())
                .as("Sentence and token configs should produce different chunk counts for the same text")
                .isNotEqualTo(tokenChunks.size());

        // All chunks should have non-empty text
        for (StreamChunksResponse response : allResponses) {
            assertThat(response.getTextContent())
                    .as("Every chunk should have non-empty text content (chunk %s, configId=%s)",
                            response.getChunkNumber(), response.getChunkConfigId())
                    .isNotEmpty();
        }

        // Exactly one chunk should be marked as isLast=true (the very last overall)
        long isLastCount = allResponses.stream().filter(StreamChunksResponse::getIsLast).count();
        assertThat(isLastCount)
                .as("Exactly one chunk overall should be marked as isLast=true")
                .isEqualTo(1);

        // The very last chunk should be the last in the response list
        assertThat(allResponses.get(allResponses.size() - 1).getIsLast())
                .as("The last response in the stream should be the one marked isLast=true")
                .isTrue();
    }

    @Test
    void singleLegacyConfigStillWorks() {
        // Use the legacy Struct-based single config path (no chunk_configs)
        Struct config = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(100).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(20).build())
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("legacy-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .setChunkConfigId("legacy_token_100")
                .setChunkerConfig(config)
                .build();

        List<StreamChunksResponse> chunks = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(chunks)
                .as("Legacy single-config should produce at least one chunk")
                .isNotEmpty();

        // All chunks should have the legacy config ID
        for (StreamChunksResponse chunk : chunks) {
            assertThat(chunk.getChunkConfigId())
                    .as("Each chunk should carry the legacy chunk_config_id")
                    .isEqualTo("legacy_token_100");
        }

        // Last chunk should have document analytics
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat(lastChunk.getIsLast())
                .as("Last chunk should be marked as isLast=true")
                .isTrue();
        assertThat(lastChunk.hasDocumentAnalytics())
                .as("Last chunk should have document analytics")
                .isTrue();
        assertThat(lastChunk.getTotalChunks())
                .as("total_chunks should match actual chunk count")
                .isEqualTo(chunks.size());

        // Legacy path should NOT have NLP analysis (that's only on the multi-config path)
        assertThat(lastChunk.hasNlpAnalysis())
                .as("Legacy single-config path should not populate NLP analysis")
                .isFalse();
    }

    @Test
    void documentAnalyticsHasPosAndLanguageFields() {
        ChunkerConfig config = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(100)
                .setChunkOverlap(20)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("pos-analytics-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_100_20")
                        .setConfig(config)
                        .build())
                .build();

        List<StreamChunksResponse> chunks = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(chunks)
                .as("Should produce at least one chunk")
                .isNotEmpty();

        // Find the last chunk of this config group (which is also the overall last chunk)
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat(lastChunk.hasDocumentAnalytics())
                .as("Last chunk should have document analytics")
                .isTrue();

        DocumentAnalytics docAnalytics = lastChunk.getDocumentAnalytics();

        // POS/language fields should be populated (from NLP enrichment)
        assertThat(docAnalytics.getDetectedLanguage())
                .as("detected_language should be populated for English text")
                .isNotEmpty();

        assertThat(docAnalytics.getLanguageConfidence())
                .as("language_confidence should be positive for detected language")
                .isGreaterThan(0.0f);

        assertThat(docAnalytics.getNounDensity())
                .as("noun_density should be > 0 for English text with nouns")
                .isGreaterThan(0.0f);

        assertThat(docAnalytics.getVerbDensity())
                .as("verb_density should be > 0 for English text with verbs")
                .isGreaterThan(0.0f);

        assertThat(docAnalytics.getAdjectiveDensity())
                .as("adjective_density should be >= 0")
                .isGreaterThanOrEqualTo(0.0f);

        assertThat(docAnalytics.getContentWordRatio())
                .as("content_word_ratio should be between 0 and 1 for typical English text")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        assertThat(docAnalytics.getUniqueLemmaCount())
                .as("unique_lemma_count should be positive for text with diverse vocabulary")
                .isGreaterThan(0);

        assertThat(docAnalytics.getLexicalDensity())
                .as("lexical_density should be between 0 and 1")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        // Traditional analytics should also still be present
        assertThat(docAnalytics.getWordCount())
                .as("word_count should be positive")
                .isGreaterThan(0);
        assertThat(docAnalytics.getSentenceCount())
                .as("sentence_count should be positive")
                .isGreaterThan(0);
    }

    @Test
    void nlpAnalysisHasSentencesWithSpans() {
        ChunkerConfig config = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("nlp-analysis-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_5_1")
                        .setConfig(config)
                        .build())
                .build();

        List<StreamChunksResponse> chunks = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(chunks)
                .as("Should produce at least one chunk")
                .isNotEmpty();

        // NLP analysis should be on the very last chunk
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat(lastChunk.getIsLast())
                .as("Last chunk should be marked isLast=true")
                .isTrue();
        assertThat(lastChunk.hasNlpAnalysis())
                .as("Last chunk should have NLP analysis")
                .isTrue();

        NlpDocumentAnalysis nlpAnalysis = lastChunk.getNlpAnalysis();

        // Check sentence spans
        assertThat(nlpAnalysis.getSentencesCount())
                .as("NLP analysis should have detected sentences from the multi-sentence text")
                .isGreaterThan(0);

        // Each sentence span should have text and offsets
        for (int i = 0; i < nlpAnalysis.getSentencesCount(); i++) {
            SentenceSpan span = nlpAnalysis.getSentences(i);
            assertThat(span.getText())
                    .as("Sentence span %d should have non-empty text", i)
                    .isNotEmpty();
            assertThat(span.getStartOffset())
                    .as("Sentence span %d start_offset should be >= 0", i)
                    .isGreaterThanOrEqualTo(0);
            assertThat(span.getEndOffset())
                    .as("Sentence span %d end_offset should be > start_offset", i)
                    .isGreaterThan(span.getStartOffset());
        }

        // Check POS/language stats on NLP analysis
        assertThat(nlpAnalysis.getDetectedLanguage())
                .as("NLP analysis detected_language should be populated")
                .isNotEmpty();

        assertThat(nlpAnalysis.getTotalTokens())
                .as("NLP analysis total_tokens should be positive")
                .isGreaterThan(0);

        assertThat(nlpAnalysis.getNounDensity())
                .as("NLP analysis noun_density should be > 0 for English text")
                .isGreaterThan(0.0f);

        assertThat(nlpAnalysis.getVerbDensity())
                .as("NLP analysis verb_density should be > 0 for English text")
                .isGreaterThan(0.0f);

        assertThat(nlpAnalysis.getContentWordRatio())
                .as("NLP analysis content_word_ratio should be between 0 and 1")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        assertThat(nlpAnalysis.getUniqueLemmaCount())
                .as("NLP analysis unique_lemma_count should be positive")
                .isGreaterThan(0);

        assertThat(nlpAnalysis.getLexicalDensity())
                .as("NLP analysis lexical_density should be between 0 and 1")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        // Non-last chunks should NOT have NLP analysis
        for (int i = 0; i < chunks.size() - 1; i++) {
            assertThat(chunks.get(i).hasNlpAnalysis())
                    .as("Non-last chunk %d should not have NLP analysis", i)
                    .isFalse();
        }
    }

    @Test
    void chunkAnalyticsHasPosFields() {
        ChunkerConfig config = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(50)
                .setChunkOverlap(10)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("chunk-pos-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_50_10")
                        .setConfig(config)
                        .build())
                .build();

        List<StreamChunksResponse> chunks = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(chunks)
                .as("Should produce multiple chunks for POS testing")
                .hasSizeGreaterThan(1);

        // Every chunk should have POS-enriched chunk analytics
        for (int i = 0; i < chunks.size(); i++) {
            StreamChunksResponse response = chunks.get(i);
            assertThat(response.hasChunkAnalytics())
                    .as("Chunk %d should have chunk analytics", i)
                    .isTrue();

            ChunkAnalytics ca = response.getChunkAnalytics();

            // Traditional fields
            assertThat(ca.getWordCount())
                    .as("Chunk %d word_count should be positive", i)
                    .isGreaterThan(0);

            // POS-derived fields (from multi-config path which runs NLP on each chunk)
            assertThat(ca.getNounDensity())
                    .as("Chunk %d noun_density should be >= 0", i)
                    .isGreaterThanOrEqualTo(0.0f);

            assertThat(ca.getVerbDensity())
                    .as("Chunk %d verb_density should be >= 0", i)
                    .isGreaterThanOrEqualTo(0.0f);

            assertThat(ca.getContentWordRatio())
                    .as("Chunk %d content_word_ratio should be between 0 and 1", i)
                    .isGreaterThanOrEqualTo(0.0f)
                    .isLessThanOrEqualTo(1.0f);

            assertThat(ca.getLexicalDensity())
                    .as("Chunk %d lexical_density should be between 0 and 1", i)
                    .isGreaterThanOrEqualTo(0.0f)
                    .isLessThanOrEqualTo(1.0f);
        }

        // At least one chunk should have non-zero noun density (English text with nouns)
        boolean anyNonZeroNounDensity = chunks.stream()
                .anyMatch(c -> c.getChunkAnalytics().getNounDensity() > 0.0f);
        assertThat(anyNonZeroNounDensity)
                .as("At least one chunk should have non-zero noun_density for English text")
                .isTrue();
    }

    @Test
    void multiConfigWithRealDocumentSampleArticle() {
        String text = loadResource("demo-documents/texts/sample_article.txt");

        // Two configs: sentence and token
        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(200)
                .setChunkOverlap(40)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("sample-article-multi-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(text)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_5_1")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_200_40")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        List<StreamChunksResponse> allResponses = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        List<StreamChunksResponse> sentenceChunks = allResponses.stream()
                .filter(r -> "sentence_5_1".equals(r.getChunkConfigId()))
                .collect(Collectors.toList());

        List<StreamChunksResponse> tokenChunks = allResponses.stream()
                .filter(r -> "token_200_40".equals(r.getChunkConfigId()))
                .collect(Collectors.toList());

        assertThat(sentenceChunks)
                .as("Sentence config should produce chunks for sample article")
                .isNotEmpty();

        assertThat(tokenChunks)
                .as("Token config should produce chunks for sample article")
                .isNotEmpty();

        // Both config groups should have document analytics on their last chunk
        StreamChunksResponse lastSentenceChunk = sentenceChunks.get(sentenceChunks.size() - 1);
        StreamChunksResponse lastTokenChunk = tokenChunks.get(tokenChunks.size() - 1);

        assertThat(lastSentenceChunk.hasDocumentAnalytics())
                .as("Last sentence chunk should have document analytics")
                .isTrue();
        assertThat(lastTokenChunk.hasDocumentAnalytics())
                .as("Last token chunk should have document analytics")
                .isTrue();

        // Both should report the same word count (same source text, same NLP pass)
        assertThat(lastSentenceChunk.getDocumentAnalytics().getWordCount())
                .as("Both configs should report the same word count since they share the same source text")
                .isEqualTo(lastTokenChunk.getDocumentAnalytics().getWordCount());

        // NLP analysis should only be on the very last overall chunk
        assertThat(lastTokenChunk.hasNlpAnalysis())
                .as("The very last chunk (from last config) should have NLP analysis")
                .isTrue();

        // The sentence config's last chunk should NOT have NLP analysis (it's not the overall last)
        assertThat(lastSentenceChunk.hasNlpAnalysis())
                .as("The last chunk of the first config should NOT have NLP analysis")
                .isFalse();

        System.out.printf("Sample article multi-config: sentence=%d chunks, token=%d chunks, total=%d%n",
                sentenceChunks.size(), tokenChunks.size(), allResponses.size());
    }

    @Test
    void unspecifiedAlgorithmDefaultsToToken() {
        // Use CHUNK_ALGORITHM_UNSPECIFIED
        ChunkerConfig config = ChunkerConfig.newBuilder()
                .setChunkSize(100)
                .setChunkOverlap(20)
                .build();
        // algorithm defaults to CHUNK_ALGORITHM_UNSPECIFIED (0)

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("unspecified-algo-test-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(MULTI_SENTENCE_TEXT)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("unspecified_100_20")
                        .setConfig(config)
                        .build())
                .build();

        List<StreamChunksResponse> chunks = streamingService.streamChunks(request)
                .collect().asList()
                .await().indefinitely();

        assertThat(chunks)
                .as("Unspecified algorithm should default to TOKEN and produce chunks")
                .isNotEmpty();

        // Should behave like token chunking - verify chunks have reasonable properties
        for (StreamChunksResponse chunk : chunks) {
            assertThat(chunk.getTextContent())
                    .as("Each chunk should have non-empty text")
                    .isNotEmpty();
            assertThat(chunk.getChunkConfigId())
                    .as("Each chunk should carry the config ID")
                    .isEqualTo("unspecified_100_20");
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

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
}
