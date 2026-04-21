package ai.pipestream.module.chunker.nlp;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Runs one full OpenNLP pass over a piece of text and returns an immutable
 * {@link NlpResult}. Stateless and thread-safe: every call constructs fresh
 * ME instances from {@link OpenNlpModels} so concurrent callers never share
 * mutable ME state.
 *
 * <p>No caches. No fallbacks. If OpenNLP throws on pathological input the
 * exception propagates — callers decide how to respond.
 */
@ApplicationScoped
public class NlpAnalyzer {

    private static final Logger LOG = Logger.getLogger(NlpAnalyzer.class);

    private static final Set<String> NOUN_TAGS = Set.of("NOUN", "PROPN");
    private static final Set<String> VERB_TAGS = Set.of("VERB", "AUX");
    private static final Set<String> ADJ_TAGS  = Set.of("ADJ");
    private static final Set<String> ADV_TAGS  = Set.of("ADV");
    private static final String LEMMA_UNKNOWN = "O";

    private final OpenNlpModels models;

    @Inject
    public NlpAnalyzer(OpenNlpModels models) {
        this.models = models;
    }

    public NlpResult analyze(String text) {
        if (text == null || text.isBlank()) {
            return NlpResult.empty();
        }

        final int textLen = text.length();

        // Fresh ME instances — OpenNLP ME classes carry mutable state.
        TokenizerME tokenizer = new TokenizerME(models.tokenizerModel());
        SentenceDetectorME sentDetect = new SentenceDetectorME(models.sentenceModel());
        POSTaggerME posTagger = new POSTaggerME(models.posModel());
        LemmatizerME lemmatizer = new LemmatizerME(models.lemmatizerModel());
        LanguageDetectorME langDetect = new LanguageDetectorME(models.languageDetectorModel());

        // Tokenize via positions only — deriving strings manually avoids the
        // NPE hazard in Span.spansToStrings() when OpenNLP emits a null span.
        Span[] rawTokenSpans = tokenizer.tokenizePos(text);
        int validTokens = 0;
        for (Span s : rawTokenSpans) {
            if (isValidSpan(s, textLen)) validTokens++;
        }
        String[] tokens = new String[validTokens];
        Span[] tokenSpans = new Span[validTokens];
        {
            int idx = 0;
            for (Span s : rawTokenSpans) {
                if (isValidSpan(s, textLen)) {
                    tokens[idx] = text.substring(s.getStart(), s.getEnd());
                    tokenSpans[idx] = s;
                    idx++;
                }
            }
        }

        Span[] rawSentSpans = sentDetect.sentPosDetect(text);
        int validSents = 0;
        for (Span s : rawSentSpans) {
            if (isValidSpan(s, textLen)) validSents++;
        }
        String[] sentences = new String[validSents];
        Span[] sentenceSpans = new Span[validSents];
        {
            int idx = 0;
            for (Span s : rawSentSpans) {
                if (isValidSpan(s, textLen)) {
                    sentences[idx] = text.substring(s.getStart(), s.getEnd());
                    sentenceSpans[idx] = s;
                    idx++;
                }
            }
        }

        String[] posTags = new String[tokens.length];
        String[] lemmas = new String[tokens.length];
        tagAndLemmatize(posTagger, lemmatizer, tokens, tokenSpans, sentenceSpans, posTags, lemmas);

        Language lang = langDetect.predictLanguage(text);
        String detectedLang = lang != null ? lang.getLang() : "";
        float langConfidence = lang != null ? (float) lang.getConfidence() : 0f;

        int nounCount = 0, verbCount = 0, adjCount = 0, advCount = 0;
        for (String tag : posTags) {
            if (NOUN_TAGS.contains(tag))      nounCount++;
            else if (VERB_TAGS.contains(tag)) verbCount++;
            else if (ADJ_TAGS.contains(tag))  adjCount++;
            else if (ADV_TAGS.contains(tag))  advCount++;
        }
        int total = tokens.length;
        int contentWords = nounCount + verbCount + adjCount + advCount;
        float nounDensity = ratio(nounCount, total);
        float verbDensity = ratio(verbCount, total);
        float adjDensity  = ratio(adjCount, total);
        float advDensity  = ratio(advCount, total);
        float contentWordRatio = ratio(contentWords, total);
        float lexicalDensity   = contentWordRatio;

        Set<String> uniqueLemmas = new HashSet<>();
        for (String lemma : lemmas) {
            if (lemma != null && !LEMMA_UNKNOWN.equals(lemma)) {
                uniqueLemmas.add(lemma);
            }
        }

        return new NlpResult(
                tokens, tokenSpans,
                sentences, sentenceSpans,
                posTags, lemmas,
                detectedLang, langConfidence,
                nounDensity, verbDensity, adjDensity, advDensity,
                contentWordRatio, lexicalDensity,
                uniqueLemmas.size());
    }

    private static void tagAndLemmatize(POSTaggerME tagger, LemmatizerME lemmatizer,
                                        String[] tokens, Span[] tokenSpans,
                                        Span[] sentenceSpans,
                                        String[] posTags, String[] lemmas) {
        Arrays.fill(posTags, "");
        System.arraycopy(tokens, 0, lemmas, 0, tokens.length);
        if (tokens.length == 0) return;

        // Walk tokens in sentence-sized batches. POS tagging scales O(n) per
        // sentence but worse than linear across a giant single sequence.
        int tokenIdx = 0;
        if (sentenceSpans.length == 0) {
            tagBatch(tagger, lemmatizer, tokens, 0, tokens.length, posTags, lemmas);
            return;
        }
        for (Span sent : sentenceSpans) {
            int firstToken = tokenIdx;
            while (tokenIdx < tokenSpans.length
                    && tokenSpans[tokenIdx].getStart() < sent.getEnd()) {
                tokenIdx++;
            }
            int count = tokenIdx - firstToken;
            if (count > 0) {
                tagBatch(tagger, lemmatizer, tokens, firstToken, count, posTags, lemmas);
            }
        }
        // Trailing tokens that didn't land in any sentence span.
        if (tokenIdx < tokens.length) {
            tagBatch(tagger, lemmatizer, tokens, tokenIdx, tokens.length - tokenIdx, posTags, lemmas);
        }
    }

    private static void tagBatch(POSTaggerME tagger, LemmatizerME lemmatizer,
                                 String[] tokens, int start, int count,
                                 String[] posTags, String[] lemmas) {
        String[] slice = new String[count];
        System.arraycopy(tokens, start, slice, 0, count);
        String[] tags = tagger.tag(slice);
        System.arraycopy(tags, 0, posTags, start, count);
        String[] lem = lemmatizer.lemmatize(slice, tags);
        System.arraycopy(lem, 0, lemmas, start, count);
    }

    private static boolean isValidSpan(Span s, int textLen) {
        return s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd();
    }

    private static float ratio(int num, int denom) {
        return denom == 0 ? 0f : (float) num / denom;
    }
}
