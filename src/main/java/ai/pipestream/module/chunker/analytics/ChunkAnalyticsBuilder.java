package ai.pipestream.module.chunker.analytics;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.util.Span;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Builds per-chunk {@link ChunkAnalytics} from a chunk's text + the document-level
 * {@link NlpResult}. POS counts and lemma counts are sliced directly from the
 * NlpResult's parallel arrays by token offset — no per-chunk OpenNLP re-run.
 *
 * <p>Pure function, no state.
 */
@ApplicationScoped
public class ChunkAnalyticsBuilder {

    private static final Set<String> NOUN_TAGS = Set.of("NOUN", "PROPN");
    private static final Set<String> VERB_TAGS = Set.of("VERB", "AUX");
    private static final Set<String> ADJ_TAGS  = Set.of("ADJ");
    private static final Set<String> ADV_TAGS  = Set.of("ADV");
    private static final String LEMMA_UNKNOWN = "O";

    private static final Pattern LIST_ITEM = Pattern.compile("^\\s*([*\\-+•]|[0-9]+[.)])\\s+.*");

    public ChunkAnalytics build(String chunkText,
                                int chunkIndex, int totalChunks,
                                int charStart, int charEnd,
                                int docCharLength,
                                NlpResult docNlp,
                                boolean containsUrl,
                                String contentHashHex) {

        TextStats.Result stats = TextStats.compute(chunkText);

        TokenSlice slice = sliceTokens(docNlp, charStart, charEnd);
        int tokenCount = slice.count;
        int sumTokenLen = slice.sumTokenLengths;
        int sentenceCount = countSentences(docNlp, charStart, charEnd);

        float avgWordLen = tokenCount == 0 ? 0f : (float) sumTokenLen / tokenCount;
        float avgSentLen = sentenceCount == 0 ? 0f : (float) tokenCount / sentenceCount;

        Set<String> types = new HashSet<>();
        for (int i = slice.start; i < slice.start + slice.count; i++) {
            types.add(docNlp.tokens()[i]);
        }
        float vocabDensity = tokenCount == 0 ? 0f : (float) types.size() / tokenCount;

        int nouns = 0, verbs = 0, adjs = 0, advs = 0;
        Set<String> uniqueLemmas = new HashSet<>();
        String[] posTags = docNlp.posTags();
        String[] lemmas = docNlp.lemmas();
        for (int i = slice.start; i < slice.start + slice.count; i++) {
            String tag = posTags[i];
            if (NOUN_TAGS.contains(tag))      nouns++;
            else if (VERB_TAGS.contains(tag)) verbs++;
            else if (ADJ_TAGS.contains(tag))  adjs++;
            else if (ADV_TAGS.contains(tag))  advs++;
            String lemma = lemmas[i];
            if (lemma != null && !LEMMA_UNKNOWN.equals(lemma)) {
                uniqueLemmas.add(lemma);
            }
        }
        int contentWords = nouns + verbs + adjs + advs;
        float nounDensity = ratio(nouns, tokenCount);
        float verbDensity = ratio(verbs, tokenCount);
        float adjDensity  = ratio(adjs, tokenCount);
        float advDensity  = ratio(advs, tokenCount);
        float contentRatio = ratio(contentWords, tokenCount);
        float lexicalDensity = contentRatio;

        float relativePosition = docCharLength <= 0 ? 0f
                : Math.min(1f, Math.max(0f, (float) charStart / docCharLength));
        boolean isFirst = chunkIndex == 0;
        boolean isLast = chunkIndex == totalChunks - 1;
        boolean isListItem = LIST_ITEM.matcher(chunkText).matches();
        float headingScore = headingScore(chunkText, sentenceCount);

        int byteSize = chunkText.getBytes(StandardCharsets.UTF_8).length;

        ChunkAnalytics.Builder b = ChunkAnalytics.newBuilder()
                .setWordCount(tokenCount)
                .setCharacterCount(stats.chars())
                .setSentenceCount(sentenceCount)
                .setAverageWordLength(avgWordLen)
                .setAverageSentenceLength(avgSentLen)
                .setVocabularyDensity(vocabDensity)
                .setWhitespacePercentage(stats.whitespacePct())
                .setAlphanumericPercentage(stats.alphanumericPct())
                .setDigitPercentage(stats.digitPct())
                .setUppercasePercentage(stats.uppercasePct())
                .setIsFirstChunk(isFirst)
                .setIsLastChunk(isLast)
                .setRelativePosition(relativePosition)
                .setPotentialHeadingScore(headingScore)
                .setListItemIndicator(isListItem)
                .setContainsUrlPlaceholder(containsUrl)
                .setNounDensity(nounDensity)
                .setVerbDensity(verbDensity)
                .setAdjectiveDensity(adjDensity)
                .setAdverbDensity(advDensity)
                .setContentWordRatio(contentRatio)
                .setUniqueLemmaCount(uniqueLemmas.size())
                .setLexicalDensity(lexicalDensity)
                .setContentHash(contentHashHex == null ? "" : contentHashHex)
                .setTextByteSize(byteSize);
        b.putAllPunctuationCounts(stats.punctuation());
        return b.build();
    }

    private record TokenSlice(int start, int count, int sumTokenLengths) {}

    private static TokenSlice sliceTokens(NlpResult nlp, int charStart, int charEnd) {
        Span[] spans = nlp.tokenSpans();
        String[] tokens = nlp.tokens();
        if (spans.length == 0) return new TokenSlice(0, 0, 0);

        int lo = lowerBound(spans, charStart);
        int hi = upperBound(spans, charEnd);
        if (hi <= lo) return new TokenSlice(lo, 0, 0);
        int sum = 0;
        for (int i = lo; i < hi; i++) sum += tokens[i].length();
        return new TokenSlice(lo, hi - lo, sum);
    }

    private static int lowerBound(Span[] spans, int charStart) {
        int lo = 0, hi = spans.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans[mid].getStart() < charStart) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int upperBound(Span[] spans, int charEndExclusive) {
        int lo = 0, hi = spans.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans[mid].getEnd() <= charEndExclusive) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int countSentences(NlpResult nlp, int charStart, int charEnd) {
        int count = 0;
        for (Span s : nlp.sentenceSpans()) {
            if (s.getStart() < charEnd && s.getEnd() > charStart) count++;
        }
        return count;
    }

    private static float ratio(int num, int denom) {
        return denom == 0 ? 0f : (float) num / denom;
    }

    private static float headingScore(String text, int sentenceCount) {
        if (text == null || text.isBlank()) return 0f;
        String trimmed = text.trim();
        float score = 0f;
        if (trimmed.length() <= 80) score += 0.4f;
        if (sentenceCount <= 1) score += 0.3f;
        char last = trimmed.charAt(trimmed.length() - 1);
        if (".!?:".indexOf(last) < 0) score += 0.15f;
        long upper = trimmed.chars().filter(Character::isUpperCase).count();
        long letters = trimmed.chars().filter(Character::isLetter).count();
        if (letters > 0 && (float) upper / letters > 0.6f) score += 0.15f;
        return Math.min(1f, score);
    }
}
