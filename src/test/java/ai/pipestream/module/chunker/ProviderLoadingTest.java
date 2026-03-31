package ai.pipestream.module.chunker;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class ProviderLoadingTest {

    @Inject
    Tokenizer tokenizer;

    @Inject
    SentenceDetector sentenceDetector;

    @Inject
    POSTagger posTagger;

    @Inject
    Lemmatizer lemmatizer;

    @Inject
    LanguageDetector languageDetector;

    @Test
    void tokenizerShouldLoadAndTokenize() {
        assertThat(tokenizer)
                .as("Tokenizer should be injected and not null")
                .isNotNull();

        String[] tokens = tokenizer.tokenize("Hello world.");
        assertThat(tokens)
                .as("Tokenizer should produce tokens from 'Hello world.'")
                .isNotEmpty()
                .contains("Hello", "world");
    }

    @Test
    void sentenceDetectorShouldLoadAndDetect() {
        assertThat(sentenceDetector)
                .as("SentenceDetector should be injected and not null")
                .isNotNull();

        String[] sentences = sentenceDetector.sentDetect("Hello world. How are you?");
        assertThat(sentences)
                .as("Should detect 2 sentences from 'Hello world. How are you?'")
                .hasSize(2);
    }

    @Test
    void posTaggerShouldLoadAndTag() {
        assertThat(posTagger)
                .as("POS tagger should be injected and not null")
                .isNotNull();

        String[] tokens = {"The", "cat", "sat"};
        String[] tags = posTagger.tag(tokens);
        assertThat(tags)
                .as("POS tags should have same count as input tokens")
                .hasSize(3);
        assertThat(tags[0])
                .as("'The' should be tagged as a determiner (DET in UD tagset)")
                .isEqualTo("DET");
    }

    @Test
    void lemmatizerShouldLoadAndLemmatize() {
        assertThat(lemmatizer)
                .as("Lemmatizer should be injected and not null")
                .isNotNull();

        // Use UD tagset tags (VERB, NOUN, ADJ) matching the OpenNLP 3.0 model
        String[] tokens = {"running", "cats", "better"};
        String[] tags = {"VERB", "NOUN", "ADJ"};
        String[] lemmas = lemmatizer.lemmatize(tokens, tags);
        assertThat(lemmas)
                .as("Lemmas should have same count as input tokens")
                .hasSize(3);
        // LemmatizerME returns lemma forms based on the trained UD model
    }

    @Test
    void languageDetectorShouldLoadAndDetect() {
        assertThat(languageDetector)
                .as("Language detector should be injected and not null")
                .isNotNull();

        var result = languageDetector.predictLanguage(
                "The quick brown fox jumps over the lazy dog on a sunny afternoon. "
                + "This is a much longer piece of English text to ensure the language "
                + "detector has enough content to make a confident prediction about "
                + "the language being used in this passage.");
        assertThat(result.getLang())
                .as("Should detect English (eng) for English text")
                .isEqualTo("eng");
        assertThat(result.getConfidence())
                .as("Language detection confidence should be positive")
                .isGreaterThan(0.0);
    }
}
