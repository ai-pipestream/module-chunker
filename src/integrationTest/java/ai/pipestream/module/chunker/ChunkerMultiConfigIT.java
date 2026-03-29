package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.semantic.v1.ChunkAlgorithm;
import ai.pipestream.semantic.v1.ChunkConfigEntry;
import ai.pipestream.semantic.v1.ChunkerConfig;
import ai.pipestream.semantic.v1.SemanticChunkerServiceGrpc;
import ai.pipestream.semantic.v1.StreamChunksRequest;
import ai.pipestream.semantic.v1.StreamChunksResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the chunker module running as a packaged JAR.
 * <p>
 * Tests multi-config chunking, NLP analytics, backward compatibility,
 * and throughput with real documents against the built artifact.
 */
@QuarkusIntegrationTest
@TestProfile(ChunkerIntegrationTestProfile.class)
class ChunkerMultiConfigIT {

    private static final Logger LOG = Logger.getLogger(ChunkerMultiConfigIT.class);

    private ManagedChannel channel;
    private SemanticChunkerServiceGrpc.SemanticChunkerServiceBlockingStub stub;

    @BeforeEach
    void setUp() {
        int port = ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
        LOG.infof("Connecting gRPC client to localhost:%d", port);

        channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        stub = SemanticChunkerServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // Test 1: Multi-config with sample article
    // =========================================================================

    @Test
    void multiConfigWithSampleArticle() {
        String text = loadResource("test-data/sample_article.txt");

        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(2)
                .setChunkOverlap(0)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(50)
                .setChunkOverlap(5)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("sample-article-it-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(text)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_2_0")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_50_5")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        List<StreamChunksResponse> allResponses = collectResponses(stub.streamChunks(request));

        // Split by config ID
        List<StreamChunksResponse> sentenceChunks = filterByConfigId(allResponses, "sentence_2_0");
        List<StreamChunksResponse> tokenChunks = filterByConfigId(allResponses, "token_50_5");

        assertThat(sentenceChunks)
                .as("Sentence config should produce chunks for sample article")
                .isNotEmpty();

        assertThat(tokenChunks)
                .as("Token config should produce chunks for sample article")
                .isNotEmpty();

        // All chunks should have non-empty text
        for (StreamChunksResponse r : allResponses) {
            assertThat(r.getTextContent())
                    .as("Chunk %d (configId=%s) should have non-empty text",
                            r.getChunkNumber(), r.getChunkConfigId())
                    .isNotEmpty();
        }

        // Exactly one chunk should be marked isLast
        long isLastCount = allResponses.stream().filter(StreamChunksResponse::getIsLast).count();
        assertThat(isLastCount)
                .as("Exactly one chunk overall should be marked isLast=true")
                .isEqualTo(1);

        // Last response should be the one marked isLast
        StreamChunksResponse lastChunk = allResponses.get(allResponses.size() - 1);
        assertThat(lastChunk.getIsLast())
                .as("The final streamed response should be marked isLast=true")
                .isTrue();

        // NLP analysis on last chunk
        assertThat(lastChunk.hasNlpAnalysis())
                .as("Last chunk should carry NlpDocumentAnalysis")
                .isTrue();

        NlpDocumentAnalysis nlp = lastChunk.getNlpAnalysis();
        assertThat(nlp.getSentencesCount())
                .as("NLP analysis should detect sentences in the article")
                .isGreaterThan(0);
        assertThat(nlp.getDetectedLanguage())
                .as("Detected language should be English (eng)")
                .isEqualTo("eng");
        assertThat(nlp.getNounDensity())
                .as("Noun density should be > 0 for English text")
                .isGreaterThan(0.0f);
        assertThat(nlp.getVerbDensity())
                .as("Verb density should be > 0 for English text")
                .isGreaterThan(0.0f);
        assertThat(nlp.getContentWordRatio())
                .as("Content word ratio should be between 0 and 1")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        // Document analytics on last chunk
        assertThat(lastChunk.hasDocumentAnalytics())
                .as("Last chunk should carry DocumentAnalytics")
                .isTrue();

        DocumentAnalytics docAnalytics = lastChunk.getDocumentAnalytics();
        assertThat(docAnalytics.getDetectedLanguage())
                .as("DocumentAnalytics detected_language should be populated")
                .isNotEmpty();
        assertThat(docAnalytics.getNounDensity())
                .as("DocumentAnalytics noun_density should be > 0")
                .isGreaterThan(0.0f);
        assertThat(docAnalytics.getVerbDensity())
                .as("DocumentAnalytics verb_density should be > 0")
                .isGreaterThan(0.0f);
        assertThat(docAnalytics.getLanguageConfidence())
                .as("DocumentAnalytics language_confidence should be positive")
                .isGreaterThan(0.0f);

        // Chunk analytics on every chunk
        for (int i = 0; i < allResponses.size(); i++) {
            StreamChunksResponse r = allResponses.get(i);
            assertThat(r.hasChunkAnalytics())
                    .as("Chunk %d (configId=%s) should have ChunkAnalytics", i, r.getChunkConfigId())
                    .isTrue();

            ChunkAnalytics ca = r.getChunkAnalytics();
            assertThat(ca.getWordCount())
                    .as("Chunk %d word_count should be positive", i)
                    .isGreaterThan(0);
            assertThat(ca.getNounDensity())
                    .as("Chunk %d noun_density should be >= 0", i)
                    .isGreaterThanOrEqualTo(0.0f);
            assertThat(ca.getVerbDensity())
                    .as("Chunk %d verb_density should be >= 0", i)
                    .isGreaterThanOrEqualTo(0.0f);
        }

        LOG.infof("Sample article: sentence=%d chunks, token=%d chunks, total=%d",
                sentenceChunks.size(), tokenChunks.size(), allResponses.size());
    }

    // =========================================================================
    // Test 2: Multi-config with US Constitution
    // =========================================================================

    @Test
    void multiConfigWithConstitution() {
        String text = loadResource("test-data/constitution.txt");

        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(200)
                .setChunkOverlap(20)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("constitution-it-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(text)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_5_1")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_200_20")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        List<StreamChunksResponse> allResponses = collectResponses(stub.streamChunks(request));

        List<StreamChunksResponse> sentenceChunks = filterByConfigId(allResponses, "sentence_5_1");
        List<StreamChunksResponse> tokenChunks = filterByConfigId(allResponses, "token_200_20");

        assertThat(sentenceChunks)
                .as("Sentence config should produce chunks for the Constitution")
                .isNotEmpty();
        assertThat(tokenChunks)
                .as("Token config should produce chunks for the Constitution")
                .isNotEmpty();

        // Both configs should report the same sentence count (same NLP pass)
        StreamChunksResponse lastSentence = sentenceChunks.get(sentenceChunks.size() - 1);
        StreamChunksResponse lastToken = tokenChunks.get(tokenChunks.size() - 1);

        assertThat(lastSentence.hasDocumentAnalytics())
                .as("Last sentence chunk should have DocumentAnalytics")
                .isTrue();
        assertThat(lastToken.hasDocumentAnalytics())
                .as("Last token chunk should have DocumentAnalytics")
                .isTrue();

        int sentenceSentenceCount = lastSentence.getDocumentAnalytics().getSentenceCount();
        int tokenSentenceCount = lastToken.getDocumentAnalytics().getSentenceCount();
        assertThat(sentenceSentenceCount)
                .as("Both configs should report the same sentence count (same NLP pass)")
                .isEqualTo(tokenSentenceCount);

        // NLP analysis sentences should match DocumentAnalytics sentence_count
        StreamChunksResponse overallLast = allResponses.get(allResponses.size() - 1);
        assertThat(overallLast.hasNlpAnalysis())
                .as("Overall last chunk should have NlpDocumentAnalysis")
                .isTrue();

        int nlpSentenceCount = overallLast.getNlpAnalysis().getSentencesCount();
        assertThat(nlpSentenceCount)
                .as("NLP sentence spans count should match DocumentAnalytics sentence_count")
                .isEqualTo(tokenSentenceCount);

        LOG.infof("Constitution: sentence=%d chunks, token=%d chunks, sentences=%d",
                sentenceChunks.size(), tokenChunks.size(), nlpSentenceCount);
    }

    // =========================================================================
    // Test 3: Multi-config with large document (Alice in Wonderland)
    // =========================================================================

    @Test
    void multiConfigWithLargeDocument() {
        String text = loadResource("test-data/alice_in_wonderland.txt");

        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(200)
                .setChunkOverlap(20)
                .build();

        ChunkerConfig charConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_CHARACTER)
                .setChunkSize(1000)
                .setChunkOverlap(100)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("alice-it-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(text)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_5_1")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_200_20")
                        .setConfig(tokenConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("char_1000_100")
                        .setConfig(charConfig)
                        .build())
                .build();

        long startMs = System.currentTimeMillis();
        List<StreamChunksResponse> allResponses = collectResponses(stub.streamChunks(request));
        long elapsedMs = System.currentTimeMillis() - startMs;

        List<StreamChunksResponse> sentenceChunks = filterByConfigId(allResponses, "sentence_5_1");
        List<StreamChunksResponse> tokenChunks = filterByConfigId(allResponses, "token_200_20");
        List<StreamChunksResponse> charChunks = filterByConfigId(allResponses, "char_1000_100");

        assertThat(sentenceChunks)
                .as("Sentence config should produce chunks for Alice in Wonderland")
                .isNotEmpty();
        assertThat(tokenChunks)
                .as("Token config should produce chunks for Alice in Wonderland")
                .isNotEmpty();
        assertThat(charChunks)
                .as("Character config should produce chunks for Alice in Wonderland")
                .isNotEmpty();

        // NLP ran once: document analytics should be identical across all configs
        DocumentAnalytics sentenceDocAnalytics = sentenceChunks.get(sentenceChunks.size() - 1).getDocumentAnalytics();
        DocumentAnalytics tokenDocAnalytics = tokenChunks.get(tokenChunks.size() - 1).getDocumentAnalytics();
        DocumentAnalytics charDocAnalytics = charChunks.get(charChunks.size() - 1).getDocumentAnalytics();

        assertThat(sentenceDocAnalytics.getWordCount())
                .as("All configs should report the same word count (single NLP pass)")
                .isEqualTo(tokenDocAnalytics.getWordCount())
                .isEqualTo(charDocAnalytics.getWordCount());

        assertThat(sentenceDocAnalytics.getSentenceCount())
                .as("All configs should report the same sentence count (single NLP pass)")
                .isEqualTo(tokenDocAnalytics.getSentenceCount())
                .isEqualTo(charDocAnalytics.getSentenceCount());

        assertThat(sentenceDocAnalytics.getDetectedLanguage())
                .as("All configs should report the same detected language")
                .isEqualTo(tokenDocAnalytics.getDetectedLanguage())
                .isEqualTo(charDocAnalytics.getDetectedLanguage());

        LOG.infof("Alice in Wonderland (%d chars): sentence=%d, token=%d, char=%d chunks in %dms",
                text.length(), sentenceChunks.size(), tokenChunks.size(), charChunks.size(), elapsedMs);
    }

    // =========================================================================
    // Test 4: Court opinions throughput
    // =========================================================================

    @Test
    void courtOpinionsThroughput() throws Exception {
        List<CourtOpinion> opinions = loadOpinions(10);

        assertThat(opinions)
                .as("Should load at least 10 court opinions from JSONL")
                .hasSizeGreaterThanOrEqualTo(10);

        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(200)
                .setChunkOverlap(20)
                .build();

        long totalMs = 0;
        for (int i = 0; i < opinions.size(); i++) {
            CourtOpinion opinion = opinions.get(i);

            // Skip opinions with no text
            if (opinion.plainText == null || opinion.plainText.isBlank()) {
                LOG.warnf("Skipping opinion %d (%s) — no plain_text", i, opinion.caseName);
                continue;
            }

            StreamChunksRequest request = StreamChunksRequest.newBuilder()
                    .setRequestId(UUID.randomUUID().toString())
                    .setDocId("opinion-" + i + "-" + UUID.randomUUID())
                    .setSourceFieldName("body")
                    .setTextContent(opinion.plainText)
                    .addChunkConfigs(ChunkConfigEntry.newBuilder()
                            .setChunkConfigId("sentence_5_1")
                            .setConfig(sentenceConfig)
                            .build())
                    .addChunkConfigs(ChunkConfigEntry.newBuilder()
                            .setChunkConfigId("token_200_20")
                            .setConfig(tokenConfig)
                            .build())
                    .build();

            long startMs = System.currentTimeMillis();
            List<StreamChunksResponse> responses = collectResponses(stub.streamChunks(request));
            long elapsedMs = System.currentTimeMillis() - startMs;
            totalMs += elapsedMs;

            assertThat(responses)
                    .as("Opinion %d (%s) should produce chunks", i, opinion.caseName)
                    .isNotEmpty();

            // Verify language detection ran
            StreamChunksResponse lastChunk = responses.get(responses.size() - 1);
            assertThat(lastChunk.hasDocumentAnalytics())
                    .as("Opinion %d last chunk should have DocumentAnalytics", i)
                    .isTrue();
            assertThat(lastChunk.getDocumentAnalytics().getDetectedLanguage())
                    .as("Opinion %d should have detected language (expected eng)", i)
                    .isEqualTo("eng");

            // Verify NLP analysis present on last chunk
            assertThat(lastChunk.hasNlpAnalysis())
                    .as("Opinion %d last chunk should have NlpDocumentAnalysis", i)
                    .isTrue();

            LOG.infof("Opinion %d (%s): %d chars, %d chunks in %dms",
                    i, opinion.caseName, opinion.plainText.length(), responses.size(), elapsedMs);
        }

        LOG.infof("Court opinions throughput: %d opinions processed in %dms total (avg %dms/opinion)",
                opinions.size(), totalMs, totalMs / opinions.size());
    }

    // =========================================================================
    // Test 5: Legacy single-config backward compatibility
    // =========================================================================

    @Test
    void legacySingleConfigStillWorks() {
        String text = loadResource("test-data/sample_article.txt");

        Struct config = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(100).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(20).build())
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("legacy-it-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(text)
                .setChunkConfigId("legacy_token_100")
                .setChunkerConfig(config)
                .build();

        List<StreamChunksResponse> chunks = collectResponses(stub.streamChunks(request));

        assertThat(chunks)
                .as("Legacy single-config should produce at least one chunk")
                .isNotEmpty();

        // All chunks should carry the legacy config ID
        for (StreamChunksResponse chunk : chunks) {
            assertThat(chunk.getChunkConfigId())
                    .as("Each chunk should carry the legacy chunk_config_id")
                    .isEqualTo("legacy_token_100");
            assertThat(chunk.getTextContent())
                    .as("Each chunk should have non-empty text")
                    .isNotEmpty();
        }

        // Last chunk should be marked isLast and have document analytics
        StreamChunksResponse lastChunk = chunks.get(chunks.size() - 1);
        assertThat(lastChunk.getIsLast())
                .as("Last chunk should be marked isLast=true")
                .isTrue();
        assertThat(lastChunk.hasDocumentAnalytics())
                .as("Last chunk should have DocumentAnalytics")
                .isTrue();
        assertThat(lastChunk.getTotalChunks())
                .as("total_chunks should match actual chunk count")
                .isEqualTo(chunks.size());

        // Legacy path should NOT have NLP analysis (that's only on the multi-config path)
        assertThat(lastChunk.hasNlpAnalysis())
                .as("Legacy single-config path should not populate NLP analysis")
                .isFalse();

        LOG.infof("Legacy config: %d chunks produced", chunks.size());
    }

    // =========================================================================
    // Test 6: Empty text returns no chunks
    // =========================================================================

    @Test
    void emptyTextReturnsNoChunks() {
        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(100)
                .setChunkOverlap(10)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("empty-text-it-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent("")
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_100_10")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        List<StreamChunksResponse> chunks = collectResponses(stub.streamChunks(request));

        assertThat(chunks)
                .as("Empty text should return zero chunks (no error)")
                .isEmpty();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private List<StreamChunksResponse> collectResponses(Iterator<StreamChunksResponse> iterator) {
        List<StreamChunksResponse> responses = new ArrayList<>();
        iterator.forEachRemaining(responses::add);
        return responses;
    }

    private List<StreamChunksResponse> filterByConfigId(List<StreamChunksResponse> responses, String configId) {
        return responses.stream()
                .filter(r -> configId.equals(r.getChunkConfigId()))
                .collect(Collectors.toList());
    }

    private String loadResource(String path) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("Resource not found on classpath: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load resource: " + path, e);
        }
    }

    /**
     * Load the first N court opinions from the JSONL file.
     * Each line is a JSON object with "plain_text", "case_name", etc.
     * We parse minimally using string manipulation to avoid adding a JSON dependency.
     */
    private List<CourtOpinion> loadOpinions(int count) throws IOException {
        List<CourtOpinion> opinions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("test-data/opinions_1000.jsonl"),
                StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null && opinions.size() < count) {
                CourtOpinion opinion = new CourtOpinion();
                opinion.plainText = extractJsonField(line, "plain_text");
                opinion.caseName = extractJsonField(line, "case_name");
                opinions.add(opinion);
            }
        }
        return opinions;
    }

    /**
     * Extract a string field from a JSON line using simple string parsing.
     * This avoids adding Jackson/Gson as a test dependency just for JSONL parsing.
     * Handles both "key": and "key" : (with optional spaces around the colon).
     */
    private static String extractJsonField(String json, String fieldName) {
        String quotedKey = "\"" + fieldName + "\"";
        int keyIdx = json.indexOf(quotedKey);
        if (keyIdx < 0) {
            return null;
        }
        // Find the colon after the closing quote of the key
        int valueStart = keyIdx + quotedKey.length();
        // Skip whitespace before colon
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') {
            valueStart++;
        }
        // Skip the colon
        if (valueStart >= json.length() || json.charAt(valueStart) != ':') {
            return null;
        }
        valueStart++;
        // Skip whitespace
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') {
            valueStart++;
        }
        if (valueStart >= json.length() || json.charAt(valueStart) != '"') {
            return null; // not a string value
        }
        valueStart++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        for (int i = valueStart; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\' && i + 1 < json.length()) {
                char next = json.charAt(i + 1);
                switch (next) {
                    case '"': sb.append('"'); i++; break;
                    case '\\': sb.append('\\'); i++; break;
                    case 'n': sb.append('\n'); i++; break;
                    case 'r': sb.append('\r'); i++; break;
                    case 't': sb.append('\t'); i++; break;
                    default: sb.append(c); break;
                }
            } else if (c == '"') {
                break; // end of string
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static class CourtOpinion {
        String plainText;
        String caseName;
    }
}
