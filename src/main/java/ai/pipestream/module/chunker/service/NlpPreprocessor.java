package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    /** Number of virtual threads for parallel sentence tagging. */
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();

    private final Tokenizer tokenizer;
    private final SentenceDetector sentenceDetector;
    private final Instance<POSTagger> posTaggerInstance;
    private final Instance<Lemmatizer> lemmatizerInstance;
    private final Instance<LanguageDetector> languageDetectorInstance;
    private final POSTaggerProvider posTaggerProvider;

    @Inject
    public NlpPreprocessor(
            Tokenizer tokenizer,
            SentenceDetector sentenceDetector,
            Instance<POSTagger> posTaggerInstance,
            Instance<Lemmatizer> lemmatizerInstance,
            Instance<LanguageDetector> languageDetectorInstance,
            POSTaggerProvider posTaggerProvider) {
        this.tokenizer = tokenizer;
        this.sentenceDetector = sentenceDetector;
        this.posTaggerInstance = posTaggerInstance;
        this.lemmatizerInstance = lemmatizerInstance;
        this.languageDetectorInstance = languageDetectorInstance;
        this.posTaggerProvider = posTaggerProvider;
        LOG.infof("NlpPreprocessor initialized with parallelism=%d", PARALLELISM);
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

        // Step 1: Tokenization — OpenNLP can return null entries or out-of-bounds spans
        // with messy text (parsed PDFs, HTML). Sanitize both arrays.
        String[] rawTokens = tokenizer.tokenize(text);
        Span[] rawTokenSpans = tokenizer.tokenizePos(text);
        int textLen = text.length();

        // Build sanitized parallel arrays: drop null spans and out-of-bounds offsets
        int validCount = 0;
        for (int i = 0; i < rawTokenSpans.length && i < rawTokens.length; i++) {
            Span s = rawTokenSpans[i];
            if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                validCount++;
            }
        }
        String[] tokens;
        Span[] tokenSpans;
        if (validCount == rawTokens.length && validCount == rawTokenSpans.length) {
            tokens = rawTokens;
            tokenSpans = rawTokenSpans;
        } else {
            LOG.warnf("OpenNLP tokenizer returned %d/%d valid spans for text of length %d — sanitizing",
                    validCount, rawTokenSpans.length, textLen);
            tokens = new String[validCount];
            tokenSpans = new Span[validCount];
            int idx = 0;
            for (int i = 0; i < rawTokenSpans.length && i < rawTokens.length; i++) {
                Span s = rawTokenSpans[i];
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    tokens[idx] = rawTokens[i];
                    tokenSpans[idx] = s;
                    idx++;
                }
            }
        }

        // Step 2: Sentence detection — call sentPosDetect() first (returns Span[]),
        // then derive sentence strings from spans. Do NOT call sentDetect() — it internally
        // calls Span.spansToStrings() which NPEs on null spans from messy text.
        Span[] rawSentenceSpans;
        String[] rawSentences;
        try {
            rawSentenceSpans = sentenceDetector.sentPosDetect(text);
            // Derive sentence strings from spans manually (avoids Span.spansToStrings NPE)
            rawSentences = new String[rawSentenceSpans.length];
            for (int i = 0; i < rawSentenceSpans.length; i++) {
                if (rawSentenceSpans[i] != null
                        && rawSentenceSpans[i].getStart() >= 0
                        && rawSentenceSpans[i].getEnd() <= textLen) {
                    rawSentences[i] = text.substring(rawSentenceSpans[i].getStart(), rawSentenceSpans[i].getEnd());
                } else {
                    rawSentences[i] = "";
                }
            }
        } catch (Exception e) {
            LOG.warnf("OpenNLP sentence detection failed for text of length %d: %s — falling back to single sentence",
                    textLen, e.getMessage());
            rawSentences = new String[]{text};
            rawSentenceSpans = new Span[]{new Span(0, textLen)};
        }

        // Sanitize sentence spans
        int validSentences = 0;
        for (int i = 0; i < rawSentenceSpans.length && i < rawSentences.length; i++) {
            Span s = rawSentenceSpans[i];
            if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                validSentences++;
            }
        }
        String[] sentences;
        Span[] sentenceSpans;
        if (validSentences == rawSentences.length && validSentences == rawSentenceSpans.length) {
            sentences = rawSentences;
            sentenceSpans = rawSentenceSpans;
        } else {
            LOG.warnf("OpenNLP sentence detector returned %d/%d valid spans for text of length %d — sanitizing",
                    validSentences, rawSentenceSpans.length, textLen);
            sentences = new String[validSentences];
            sentenceSpans = new Span[validSentences];
            int idx = 0;
            for (int i = 0; i < rawSentenceSpans.length && i < rawSentences.length; i++) {
                Span s = rawSentenceSpans[i];
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    sentences[idx] = rawSentences[i];
                    sentenceSpans[idx] = s;
                    idx++;
                }
            }
        }

        // Step 3 + 4: POS tagging + lemmatization PER SENTENCE (not whole document).
        // POS tagger uses Viterbi beam search — O(n × k²) per sequence.
        // Tagging 800K tokens as one sequence is catastrophic; tagging 30K sentences
        // of ~25 tokens each is fast.
        String[] posTags = new String[tokens.length];
        String[] lemmas = new String[tokens.length];
        tagAndLemmatizePerSentence(text, sentences, sentenceSpans, tokens, tokenSpans, posTags, lemmas);

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

    /**
     * Represents a sentence's tokens and their position in the document-level arrays.
     */
    private record SentenceWork(int firstTokenIdx, int tokenCount, String[] sentTokens) {}

    /**
     * POS tags and lemmatizes tokens per sentence, using parallel virtual threads.
     * <p>
     * POSTaggerME uses Viterbi beam search — O(n × k²) per sequence. Tagging 800K tokens
     * as one sequence takes minutes; tagging 30K sentences of ~25 tokens each takes seconds.
     * <p>
     * POSTaggerME is NOT thread-safe (stores bestSequence as instance state), so each virtual
     * thread creates its own tagger from the shared thread-safe POSModel.
     */
    private void tagAndLemmatizePerSentence(String text, String[] sentences, Span[] sentenceSpans,
                                             String[] tokens, Span[] tokenSpans,
                                             String[] posTags, String[] lemmas) {
        // Initialize defaults
        Arrays.fill(posTags, "");
        for (int i = 0; i < lemmas.length; i++) {
            lemmas[i] = tokens[i];
        }

        POSModel model = posTaggerProvider.getModel();
        Lemmatizer sharedLemmatizer = null;
        if (lemmatizerInstance.isResolvable()) {
            sharedLemmatizer = lemmatizerInstance.get();
        }

        if (model == null && sharedLemmatizer == null) {
            return;
        }

        // Build sentence work items: map tokens to sentences by character offsets.
        // Guard against OpenNLP returning inconsistent token/sentence spans.
        SentenceWork[] work = new SentenceWork[sentenceSpans.length];
        int tokenIdx = 0;
        for (int s = 0; s < sentenceSpans.length; s++) {
            int sentEnd = sentenceSpans[s].getEnd();
            int firstToken = tokenIdx;
            while (tokenIdx < tokenSpans.length) {
                Span ts = tokenSpans[tokenIdx];
                if (ts == null || ts.getStart() >= sentEnd) break;
                tokenIdx++;
            }
            int tokenCount = tokenIdx - firstToken;
            // Clamp to actual array bounds to prevent arraycopy overflow
            if (firstToken + tokenCount > tokens.length) {
                tokenCount = Math.max(0, tokens.length - firstToken);
            }
            if (tokenCount == 0) {
                work[s] = new SentenceWork(firstToken, 0, new String[0]);
                continue;
            }
            String[] sentTokens = new String[tokenCount];
            System.arraycopy(tokens, firstToken, sentTokens, 0, tokenCount);
            work[s] = new SentenceWork(firstToken, tokenCount, sentTokens);
        }

        // For small documents (< 100 sentences), process sequentially — no thread overhead
        if (sentenceSpans.length < 100) {
            POSTagger tagger = model != null ? new POSTaggerME(model) : null;
            for (SentenceWork sw : work) {
                tagAndLemmatizeSentence(sw, tagger, sharedLemmatizer, posTags, lemmas);
            }
            return;
        }

        // For large documents, parallel process with virtual threads.
        // Each thread gets its own POSTaggerME (not thread-safe) from shared POSModel (thread-safe).
        final POSModel finalModel = model;
        final Lemmatizer finalLemmatizer = sharedLemmatizer;

        LOG.infof("Parallel NLP: %d sentences across virtual threads", sentenceSpans.length);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (SentenceWork sw : work) {
                if (sw.tokenCount == 0) continue;
                futures.add(executor.submit(() -> {
                    // Each virtual thread gets its own POSTaggerME — not thread-safe
                    POSTagger threadTagger = finalModel != null ? new POSTaggerME(finalModel) : null;
                    tagAndLemmatizeSentence(sw, threadTagger, finalLemmatizer, posTags, lemmas);
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Parallel NLP interrupted", e);
        } catch (Exception e) {
            LOG.warn("Parallel NLP failed, results may be partial", e);
        }
    }

    private void tagAndLemmatizeSentence(SentenceWork sw, POSTagger tagger,
                                          Lemmatizer lemmatizer,
                                          String[] posTags, String[] lemmas) {
        if (sw.tokenCount == 0) return;

        String[] sentTags = null;
        if (tagger != null) {
            try {
                sentTags = tagger.tag(sw.sentTokens);
                System.arraycopy(sentTags, 0, posTags, sw.firstTokenIdx, sw.tokenCount);
            } catch (Exception e) {
                LOG.warnf("POS tagging failed for sentence at token %d (%d tokens): %s",
                        sw.firstTokenIdx, sw.tokenCount, e.getMessage());
            }
        }

        if (lemmatizer != null && sentTags != null) {
            try {
                String[] sentLemmas = lemmatizer.lemmatize(sw.sentTokens, sentTags);
                System.arraycopy(sentLemmas, 0, lemmas, sw.firstTokenIdx, sw.tokenCount);
            } catch (Exception e) {
                LOG.warnf("Lemmatization failed for sentence at token %d: %s",
                        sw.firstTokenIdx, e.getMessage());
            }
        }
    }
}
