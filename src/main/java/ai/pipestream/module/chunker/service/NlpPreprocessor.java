package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Single NLP pass preprocessor that runs tokenization, sentence detection,
 * POS tagging, lemmatization, and language detection once per document
 * and caches all results in an NlpResult record.
 *
 * <p>Optional providers (POS tagger, lemmatizer, language detector) use CDI
 * {@link Instance} for graceful degradation when models are unavailable.</p>
 */
@ApplicationScoped
public class NlpPreprocessor {

    private static final Logger LOG = Logger.getLogger(NlpPreprocessor.class);

    // Universal Dependencies POS tags for content word classification (OpenNLP 3.0)
    private static final Set<String> NOUN_TAGS = Set.of("NOUN", "PROPN");
    private static final Set<String> VERB_TAGS = Set.of("VERB", "AUX");
    private static final Set<String> ADJ_TAGS = Set.of("ADJ");
    private static final Set<String> ADV_TAGS = Set.of("ADV");

    /** Sentinel value returned by DictionaryLemmatizer for unknown words. */
    private static final String LEMMA_UNKNOWN = "O";

    private final Tokenizer tokenizer;
    private final SentenceDetector sentenceDetector;
    private final Instance<POSTagger> posTaggerInstance;
    private final Instance<Lemmatizer> lemmatizerInstance;
    private final Instance<LanguageDetector> languageDetectorInstance;

    @Inject
    public NlpPreprocessor(
            Tokenizer tokenizer,
            SentenceDetector sentenceDetector,
            Instance<POSTagger> posTaggerInstance,
            Instance<Lemmatizer> lemmatizerInstance,
            Instance<LanguageDetector> languageDetectorInstance) {
        this.tokenizer = tokenizer;
        this.sentenceDetector = sentenceDetector;
        this.posTaggerInstance = posTaggerInstance;
        this.lemmatizerInstance = lemmatizerInstance;
        this.languageDetectorInstance = languageDetectorInstance;
    }

    /**
     * Immutable record containing all NLP analysis results for a document.
     */
    public record NlpResult(
            String[] tokens,
            Span[] tokenSpans,
            String[] sentences,
            Span[] sentenceSpans,
            String[] posTags,
            String[] lemmas,
            String detectedLanguage,
            float languageConfidence,
            float nounDensity,
            float verbDensity,
            float adjectiveDensity,
            float adverbDensity,
            float contentWordRatio,
            float lexicalDensity,
            int uniqueLemmaCount
    ) {
        /**
         * Returns an empty NlpResult for null or empty text.
         */
        public static NlpResult empty() {
            return new NlpResult(
                    new String[0],
                    new Span[0],
                    new String[0],
                    new Span[0],
                    new String[0],
                    new String[0],
                    "",
                    0.0f,
                    0.0f,
                    0.0f,
                    0.0f,
                    0.0f,
                    0.0f,
                    0.0f,
                    0
            );
        }
    }

    /**
     * Runs all NLP analysis steps on the given text in a single pass.
     *
     * @param text the document text to preprocess
     * @return an NlpResult containing all cached NLP data
     */
    public NlpResult preprocess(String text) {
        if (text == null || text.isBlank()) {
            return NlpResult.empty();
        }

        // Step 1: Tokenization
        String[] tokens = tokenizer.tokenize(text);
        Span[] tokenSpans = tokenizer.tokenizePos(text);

        // Step 2: Sentence detection
        String[] sentences = sentenceDetector.sentDetect(text);
        Span[] sentenceSpans = sentenceDetector.sentPosDetect(text);

        // Step 3: POS tagging (optional)
        String[] posTags = tagTokens(tokens);

        // Step 4: Lemmatization (optional, requires POS tags)
        String[] lemmas = lemmatizeTokens(tokens, posTags);

        // Step 5: Language detection (optional)
        String detectedLanguage = "";
        float languageConfidence = 0.0f;
        if (languageDetectorInstance.isResolvable()) {
            LanguageDetector detector = languageDetectorInstance.get();
            if (detector != null) {
                try {
                    Language lang = detector.predictLanguage(text);
                    detectedLanguage = lang.getLang();
                    languageConfidence = (float) lang.getConfidence();
                } catch (Exception e) {
                    LOG.warn("Language detection failed", e);
                }
            }
        } else {
            LOG.debug("Language detector not available, skipping language detection");
        }

        // Step 6: Compute POS-derived ratios
        int totalTokens = tokens.length;
        int nounCount = 0;
        int verbCount = 0;
        int adjCount = 0;
        int advCount = 0;

        for (String tag : posTags) {
            if (NOUN_TAGS.contains(tag)) nounCount++;
            else if (VERB_TAGS.contains(tag)) verbCount++;
            else if (ADJ_TAGS.contains(tag)) adjCount++;
            else if (ADV_TAGS.contains(tag)) advCount++;
        }

        int contentWordCount = nounCount + verbCount + adjCount + advCount;

        float nounDensity = totalTokens > 0 ? (float) nounCount / totalTokens : 0.0f;
        float verbDensity = totalTokens > 0 ? (float) verbCount / totalTokens : 0.0f;
        float adjectiveDensity = totalTokens > 0 ? (float) adjCount / totalTokens : 0.0f;
        float adverbDensity = totalTokens > 0 ? (float) advCount / totalTokens : 0.0f;
        float contentWordRatio = totalTokens > 0 ? (float) contentWordCount / totalTokens : 0.0f;

        // Step 7: Compute unique lemma count (exclude "O" sentinel from DictionaryLemmatizer)
        int uniqueLemmaCount = (int) Arrays.stream(lemmas)
                .filter(l -> !LEMMA_UNKNOWN.equals(l))
                .collect(Collectors.toSet())
                .size();

        float lexicalDensity = totalTokens > 0
                ? (float) contentWordCount / totalTokens
                : 0.0f;

        return new NlpResult(
                tokens,
                tokenSpans,
                sentences,
                sentenceSpans,
                posTags,
                lemmas,
                detectedLanguage,
                languageConfidence,
                nounDensity,
                verbDensity,
                adjectiveDensity,
                adverbDensity,
                contentWordRatio,
                lexicalDensity,
                uniqueLemmaCount
        );
    }

    private String[] tagTokens(String[] tokens) {
        if (tokens.length == 0) {
            return new String[0];
        }
        if (posTaggerInstance.isResolvable()) {
            POSTagger tagger = posTaggerInstance.get();
            if (tagger != null) {
                try {
                    return tagger.tag(tokens);
                } catch (Exception e) {
                    LOG.warn("POS tagging failed, returning empty tags", e);
                }
            }
        } else {
            LOG.debug("POS tagger not available, returning empty tags");
        }
        // Fallback: empty strings for each token
        String[] empty = new String[tokens.length];
        Arrays.fill(empty, "");
        return empty;
    }

    private String[] lemmatizeTokens(String[] tokens, String[] posTags) {
        if (tokens.length == 0) {
            return new String[0];
        }
        if (lemmatizerInstance.isResolvable()) {
            Lemmatizer lemmatizer = lemmatizerInstance.get();
            if (lemmatizer != null) {
                try {
                    return lemmatizer.lemmatize(tokens, posTags);
                } catch (Exception e) {
                    LOG.warn("Lemmatization failed, returning raw tokens", e);
                }
            }
        } else {
            LOG.debug("Lemmatizer not available, returning raw tokens");
        }
        // Fallback: raw tokens as lemmas
        return Arrays.copyOf(tokens, tokens.length);
    }
}
