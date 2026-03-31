package ai.pipestream.module.chunker;

import ai.pipestream.module.chunker.service.NlpPreprocessor;
import ai.pipestream.module.chunker.service.NlpPreprocessor.NlpResult;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class NlpPreprocessorTest {

    @Inject
    NlpPreprocessor preprocessor;

    private static final String ENGLISH_TEXT =
            "The quick brown fox jumps over the lazy dog. " +
            "Natural language processing is a fascinating field of computer science. " +
            "Researchers develop algorithms that help machines understand human language.";

    @Test
    void preprocessShouldRunAllNlpSteps() {
        NlpResult result = preprocessor.preprocess(ENGLISH_TEXT);

        assertThat(result.tokens())
                .as("Tokens should be non-empty for English text")
                .isNotEmpty();

        assertThat(result.tokenSpans())
                .as("Token spans should be non-empty for English text")
                .isNotEmpty();

        assertThat(result.sentences())
                .as("Sentences should be detected from multi-sentence English text")
                .isNotEmpty();

        assertThat(result.sentenceSpans())
                .as("Sentence spans should be detected from multi-sentence English text")
                .isNotEmpty();

        assertThat(result.posTags())
                .as("POS tags should be non-empty when POS tagger model is available")
                .isNotEmpty();

        assertThat(result.lemmas())
                .as("Lemmas should be non-empty when lemmatizer model is available")
                .isNotEmpty();

        assertThat(result.detectedLanguage())
                .as("Detected language should not be empty when language detector is available")
                .isNotEmpty();

        assertThat(result.languageConfidence())
                .as("Language confidence should be positive when language detector is available")
                .isGreaterThan(0.0f);
    }

    @Test
    void preprocessShouldComputePosRatios() {
        NlpResult result = preprocessor.preprocess(ENGLISH_TEXT);

        assertThat(result.nounDensity())
                .as("Noun density should be > 0 for typical English text with nouns")
                .isGreaterThan(0.0f);

        assertThat(result.verbDensity())
                .as("Verb density should be > 0 for typical English text with verbs")
                .isGreaterThan(0.0f);

        assertThat(result.adjectiveDensity())
                .as("Adjective density should be > 0 for text containing adjectives like 'quick', 'brown', 'lazy'")
                .isGreaterThan(0.0f);

        assertThat(result.contentWordRatio())
                .as("Content word ratio should be between 0 and 1 (exclusive)")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);

        assertThat(result.lexicalDensity())
                .as("Lexical density should be between 0 and 1 (exclusive)")
                .isGreaterThan(0.0f)
                .isLessThan(1.0f);
    }

    @Test
    void preprocessShouldDetectLanguage() {
        // Use longer English text for reliable detection
        String longerText =
                "The quick brown fox jumps over the lazy dog on a warm summer afternoon. " +
                "Natural language processing uses computational techniques to analyze and understand text. " +
                "Researchers have developed many sophisticated algorithms for machine translation. " +
                "These methods enable computers to process human language with increasing accuracy.";

        NlpResult result = preprocessor.preprocess(longerText);

        assertThat(result.detectedLanguage())
                .as("Language should be detected as English (ISO-639-3 code 'eng') for English text")
                .isEqualTo("eng");

        assertThat(result.languageConfidence())
                .as("Language detection confidence should be reasonably high for clear English text")
                .isGreaterThan(0.5f);
    }

    @Test
    void preprocessEmptyTextShouldReturnEmptyResult() {
        NlpResult emptyResult = preprocessor.preprocess("");
        NlpResult nullResult = preprocessor.preprocess(null);
        NlpResult staticEmpty = NlpResult.empty();

        assertThat(emptyResult.tokens())
                .as("Empty text should produce zero tokens")
                .isEmpty();

        assertThat(emptyResult.sentences())
                .as("Empty text should produce zero sentences")
                .isEmpty();

        assertThat(emptyResult.posTags())
                .as("Empty text should produce zero POS tags")
                .isEmpty();

        assertThat(emptyResult.lemmas())
                .as("Empty text should produce zero lemmas")
                .isEmpty();

        assertThat(emptyResult.detectedLanguage())
                .as("Empty text should have empty detected language")
                .isEmpty();

        assertThat(emptyResult.languageConfidence())
                .as("Empty text should have zero language confidence")
                .isEqualTo(0.0f);

        assertThat(emptyResult.nounDensity())
                .as("Empty text should have zero noun density")
                .isEqualTo(0.0f);

        assertThat(emptyResult.contentWordRatio())
                .as("Empty text should have zero content word ratio")
                .isEqualTo(0.0f);

        assertThat(emptyResult.uniqueLemmaCount())
                .as("Empty text should have zero unique lemma count")
                .isEqualTo(0);

        // Verify null text gives same empty result
        assertThat(nullResult.tokens())
                .as("Null text should produce zero tokens, same as empty text")
                .isEmpty();

        assertThat(nullResult.detectedLanguage())
                .as("Null text should have empty detected language, same as empty text")
                .isEmpty();

        // Verify static factory empty() matches
        assertThat(staticEmpty.tokens())
                .as("NlpResult.empty() should produce zero tokens")
                .isEmpty();

        assertThat(staticEmpty.detectedLanguage())
                .as("NlpResult.empty() should have empty detected language")
                .isEmpty();
    }

    @Test
    void preprocessShouldHaveMatchingArrayLengths() {
        NlpResult result = preprocessor.preprocess(ENGLISH_TEXT);

        int tokenCount = result.tokens().length;

        assertThat(tokenCount)
                .as("Token count should be positive for non-empty English text")
                .isGreaterThan(0);

        assertThat(result.tokenSpans())
                .as("Token spans length should match tokens length")
                .hasSize(tokenCount);

        assertThat(result.posTags())
                .as("POS tags length should match tokens length")
                .hasSize(tokenCount);

        assertThat(result.lemmas())
                .as("Lemmas length should match tokens length")
                .hasSize(tokenCount);
    }

    @Test
    void preprocessShouldCountUniqueLemmas() {
        NlpResult result = preprocessor.preprocess(ENGLISH_TEXT);

        assertThat(result.uniqueLemmaCount())
                .as("Unique lemma count should be positive for text with diverse vocabulary")
                .isGreaterThan(0);

        // Unique lemma count should be less than or equal to token count
        assertThat(result.uniqueLemmaCount())
                .as("Unique lemma count should not exceed token count")
                .isLessThanOrEqualTo(result.tokens().length);
    }

    @Test
    void preprocessShouldDetectMultipleSentences() {
        NlpResult result = preprocessor.preprocess(ENGLISH_TEXT);

        assertThat(result.sentences())
                .as("Should detect 3 sentences from the test text with 3 periods")
                .hasSize(3);

        assertThat(result.sentenceSpans())
                .as("Sentence spans count should match sentences count")
                .hasSize(result.sentences().length);
    }
}
