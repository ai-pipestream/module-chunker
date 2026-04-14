package ai.pipestream.module.chunker.service;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.DocumentAnalytics;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;

import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Extracts metadata from text chunks to provide additional context and information.
 * This includes statistics like word count, sentence count, and various text characteristics.
 */
@Singleton
public class ChunkMetadataExtractor {

    private static final Logger LOG = Logger.getLogger(ChunkMetadataExtractor.class);
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private static final Pattern LIST_ITEM_PATTERN = Pattern.compile("^\\s*([*\\-+•]|[0-9]+[.)])\\s+.*");

    private final SentenceDetector sentenceDetector;
    private final Tokenizer tokenizer;

    @Inject
    public ChunkMetadataExtractor(SentenceDetector sentenceDetector, Tokenizer tokenizer) {
        this.sentenceDetector = sentenceDetector;
        this.tokenizer = tokenizer;
    }

    /**
     * Pre-computed per-chunk NLP data sliced from a doc-level {@link NlpPreprocessor.NlpResult}.
     *
     * <p>The whole point of this record is to let callers compute the NLP slice
     * <em>once</em> per chunk and reuse it across both {@link #extractAllMetadata}
     * and {@link #extractChunkAnalytics}. Without this, both metadata methods
     * call {@link #safeTokenize} and {@link #safeSentDetect} on the chunk text
     * — which means OpenNLP runs <b>4 times per chunk</b> (2 in extractAllMetadata
     * + 2 in extractChunkAnalytics's 4-arg base form). On a 200-chunk doc that's
     * 800 OpenNLP calls per request, easily the chunker's hot spot.
     *
     * <p>Use {@link #sliceForChunk} to build a slice from a doc-level NLP result
     * + the chunk's [start, end] offsets. If the doc-level NLP is missing or
     * the slice is empty (e.g. URL substitution invalidated the offsets),
     * {@link #sliceForChunk} returns {@code null} and callers should pass
     * {@code null} through — the metadata methods then fall back to running
     * OpenNLP per chunk (current behavior).
     *
     * <p><b>Sum of token lengths</b> is pre-computed so the average-word-length
     * calculation doesn't need a second pass over the sliced array.
     */
    public static final class ChunkNlpSlice {
        public final String[] tokens;
        public final int sentenceCount;
        public final int sumTokenLengths;

        ChunkNlpSlice(String[] tokens, int sentenceCount, int sumTokenLengths) {
            this.tokens = tokens;
            this.sentenceCount = sentenceCount;
            this.sumTokenLengths = sumTokenLengths;
        }
    }

    /**
     * Slices a doc-level {@link NlpPreprocessor.NlpResult} for one chunk's
     * [chunkStart, chunkEnd] byte range. Returns {@code null} if the slice
     * cannot be safely produced — caller falls back to running OpenNLP on
     * the chunk text directly.
     *
     * <p><b>O(log n + k)</b>: binary search to find the first token at or
     * after {@code chunkStart}, then a linear walk to copy tokens whose
     * spans fall within the chunk range. Sentence count is computed by
     * walking the doc-level sentenceSpans once and counting overlaps with
     * the chunk's range.
     *
     * <p><b>When this returns null:</b>
     * <ul>
     *   <li>{@code docNlp} is {@code null} (caller didn't precompute NLP).</li>
     *   <li>{@code docNlp.tokens()} is empty (NLP ran on empty input).</li>
     *   <li>The binary search finds no tokens in the chunk's range
     *       (e.g. chunk offsets are stale because URL substitution
     *       shifted the underlying text — see
     *       {@link ai.pipestream.module.chunker.service.OverlapChunker}'s
     *       URL substitution logic at line 346-358).</li>
     * </ul>
     *
     * <p><b>Caller responsibility for URL safety:</b> if the chunker ran
     * URL placeholder substitution on this doc, the chunk offsets reference
     * the substituted text but {@code docNlp} was computed on the original
     * text. The two coordinate systems do not align. Callers MUST pass
     * {@code null} for {@code docNlp} when URL substitution happened, OR
     * detect the stale-offset case via the binary search returning
     * invalid indices (which this method does correctly — it returns
     * {@code null} and the fallback fires). Pass {@code null} explicitly
     * if you know URLs were substituted; relying on the indirect detection
     * works but produces a few wasted slice attempts per request.
     */
    public ChunkNlpSlice sliceForChunk(
            NlpPreprocessor.NlpResult docNlp, int chunkStart, int chunkEnd) {
        if (docNlp == null || docNlp.tokens().length == 0) {
            return null;
        }
        opennlp.tools.util.Span[] tokenSpans = docNlp.tokenSpans();
        if (tokenSpans == null || tokenSpans.length == 0) {
            return null;
        }
        int firstToken = findFirstTokenAtOrAfter(tokenSpans, chunkStart);
        int lastToken = findLastTokenBefore(tokenSpans, chunkEnd);
        if (firstToken < 0 || lastToken < firstToken) {
            return null;
        }

        String[] docTokens = docNlp.tokens();
        // Defensive: doc tokens array length should match span array length,
        // but if it doesn't, clamp to whichever is shorter to avoid
        // ArrayIndexOutOfBoundsException.
        int safeLast = Math.min(lastToken, docTokens.length - 1);
        if (safeLast < firstToken) {
            return null;
        }
        String[] sliced = Arrays.copyOfRange(docTokens, firstToken, safeLast + 1);
        int sumLen = 0;
        for (String t : sliced) {
            if (t != null) sumLen += t.length();
        }

        // Sentence count: walk the doc-level sentence spans once, count
        // those whose [start, end) overlaps the chunk's [chunkStart, chunkEnd).
        // This is O(s) where s is the doc-level sentence count — typically
        // small (< 1000 even for huge docs).
        opennlp.tools.util.Span[] sentenceSpans = docNlp.sentenceSpans();
        int sentCount = 0;
        if (sentenceSpans != null) {
            for (opennlp.tools.util.Span ss : sentenceSpans) {
                if (ss == null) continue;
                // Half-open [start, end) overlap test
                if (ss.getStart() < chunkEnd && ss.getEnd() > chunkStart) {
                    sentCount++;
                }
            }
        }
        // A non-empty chunk always contains at least 1 sentence by definition
        // (the chunker either splits on sentence boundaries or accumulates
        // tokens within a single sentence). If our sentence-span walk
        // returned 0 due to malformed spans, treat the chunk as 1 sentence
        // so downstream avg-sentence-length math doesn't divide by zero.
        if (sentCount == 0) sentCount = 1;

        return new ChunkNlpSlice(sliced, sentCount, sumLen);
    }

    /**
     * Single-pass character statistics over a text region. Replaces five
     * separate {@code chars().filter(...).count()} passes plus a sixth
     * {@code toCharArray()} pass for punctuation that the legacy code did
     * per metadata call. On a 200-chunk doc with both
     * {@link #extractAllMetadata} and {@link #extractChunkAnalytics} called
     * per chunk, that's 12 linear passes over chunk text per chunk; one
     * fused loop drops it to 2.
     *
     * <p>Uses {@link String#charAt} directly which avoids the
     * {@code IntStream} boxing overhead of the legacy
     * {@code chars().filter(...).count()} pattern.
     *
     * <p>Counters semantics match the legacy code:
     * <ul>
     *   <li>{@code whitespace}: {@link Character#isWhitespace}</li>
     *   <li>{@code alphanumeric}: {@link Character#isLetterOrDigit}</li>
     *   <li>{@code digits}: {@link Character#isDigit} (subset of alphanumeric)</li>
     *   <li>{@code uppercase}: {@link Character#isUpperCase} (subset of alphanumeric)</li>
     *   <li>{@code punctuationCounts}: ASCII-printable chars (0x20-0x7E) that
     *       are NOT whitespace and NOT letter-or-digit, keyed by the char</li>
     * </ul>
     */
    private static final class CharStats {
        long whitespace;
        long alphanumeric;
        long digits;
        long uppercase;
        final Map<Character, Integer> punctuationCounts = new HashMap<>();
    }

    /**
     * Computes {@link CharStats} for {@code text} in a single pass.
     */
    private static CharStats computeCharStats(String text) {
        CharStats stats = new CharStats();
        if (text == null) return stats;
        int len = text.length();
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) {
                stats.whitespace++;
                continue;
            }
            if (Character.isLetterOrDigit(c)) {
                stats.alphanumeric++;
                if (Character.isDigit(c)) stats.digits++;
                if (Character.isUpperCase(c)) stats.uppercase++;
                continue;
            }
            // Not whitespace, not letter-or-digit. ASCII-printable range
            // (0x20-0x7E) excluding whitespace gives us punctuation. The
            // legacy code used StringUtils.isAsciiPrintable(String.valueOf(c))
            // which allocates a new String per char — direct range check
            // avoids that allocation.
            if (c >= 0x20 && c <= 0x7E) {
                stats.punctuationCounts.merge(c, 1, Integer::sum);
            }
        }
        return stats;
    }

    /**
     * Extracts comprehensive metadata from a text chunk.
     *
     * <p>Convenience overload that runs {@link #safeSentDetect} and
     * {@link #safeTokenize} on the chunk text. Use the
     * {@link #extractAllMetadata(String, int, int, boolean, ChunkNlpSlice)
     * 5-arg overload} when you have a doc-level NLP result available — that
     * version slices the doc tokens/spans instead of re-running OpenNLP per
     * chunk, eliminating the chunker's #1 hot-path cost.
     *
     * @param chunkText The text content of the chunk
     * @param chunkNumber The position of this chunk in the sequence (0-based)
     * @param totalChunksInDocument Total number of chunks in the document
     * @param containsUrlPlaceholder Whether the chunk contains URL placeholders
     * @return A map of metadata key-value pairs
     */
    public Map<String, Value> extractAllMetadata(String chunkText, int chunkNumber, int totalChunksInDocument, boolean containsUrlPlaceholder) {
        return extractAllMetadata(chunkText, chunkNumber, totalChunksInDocument, containsUrlPlaceholder, null);
    }

    /**
     * Extracts comprehensive metadata from a text chunk, using a pre-computed
     * {@link ChunkNlpSlice} to avoid re-running OpenNLP on the chunk text.
     *
     * <p>If {@code slice} is {@code null}, this method falls back to running
     * {@link #safeSentDetect} and {@link #safeTokenize} on the chunk text
     * (current pre-PR-I behavior). When {@code slice} is non-null, the slice's
     * pre-sliced tokens, sentence count, and sum-of-token-lengths are used
     * directly — no per-chunk OpenNLP execution.
     *
     * @param chunkText The text content of the chunk
     * @param chunkNumber The position of this chunk in the sequence (0-based)
     * @param totalChunksInDocument Total number of chunks in the document
     * @param containsUrlPlaceholder Whether the chunk contains URL placeholders
     * @param slice Pre-computed NLP slice from {@link #sliceForChunk}, or
     *              {@code null} to compute on the fly
     * @return A map of metadata key-value pairs
     */
    public Map<String, Value> extractAllMetadata(String chunkText, int chunkNumber, int totalChunksInDocument, boolean containsUrlPlaceholder, ChunkNlpSlice slice) {
        Map<String, Value> metadataMap = new HashMap<>();

        if (StringUtils.isBlank(chunkText)) {
            metadataMap.put("word_count", Value.newBuilder().setNumberValue(0).build());
            metadataMap.put("character_count", Value.newBuilder().setNumberValue(0).build());
            metadataMap.put("sentence_count", Value.newBuilder().setNumberValue(0).build());
            return metadataMap;
        }

        int characterCount = chunkText.length();
        metadataMap.put("character_count", Value.newBuilder().setNumberValue(characterCount).build());

        // Use slice if available; otherwise run OpenNLP per-chunk (legacy path).
        // Both branches produce the same shape for downstream consumers.
        String[] tokens;
        int sentenceCount;
        int sumTokenLengths;
        if (slice != null) {
            tokens = slice.tokens;
            sentenceCount = slice.sentenceCount;
            sumTokenLengths = slice.sumTokenLengths;
        } else {
            String[] sentences = safeSentDetect(chunkText);
            sentenceCount = sentences.length;
            tokens = safeTokenize(chunkText);
            // Compute sum of token lengths the same way the legacy path did
            sumTokenLengths = 0;
            for (String t : tokens) {
                if (t != null) sumTokenLengths += t.length();
            }
        }
        metadataMap.put("sentence_count", Value.newBuilder().setNumberValue(sentenceCount).build());

        int wordCount = tokens.length;
        metadataMap.put("word_count", Value.newBuilder().setNumberValue(wordCount).build());

        double avgWordLength = wordCount > 0 ? (double) sumTokenLengths / wordCount : 0;
        metadataMap.put("average_word_length", Value.newBuilder().setNumberValue(Double.parseDouble(DECIMAL_FORMAT.format(avgWordLength))).build());

        double avgSentenceLength = sentenceCount > 0 ? (double) wordCount / sentenceCount : 0;
        metadataMap.put("average_sentence_length", Value.newBuilder().setNumberValue(Double.parseDouble(DECIMAL_FORMAT.format(avgSentenceLength))).build());

        if (wordCount > 0) {
            Set<String> uniqueTokens = new HashSet<>(Arrays.asList(tokens));
            double ttr = (double) uniqueTokens.size() / wordCount;
            metadataMap.put("vocabulary_density", Value.newBuilder().setNumberValue(Double.parseDouble(DECIMAL_FORMAT.format(ttr))).build());
        } else {
            metadataMap.put("vocabulary_density", Value.newBuilder().setNumberValue(0).build());
        }

        // PR-H: single fused pass over chunkText for all 5 char-class counters
        // (whitespace, alphanumeric, digits, uppercase, punctuation) instead
        // of 5 separate chars().filter().count() calls + a 6th toCharArray()
        // loop. ~6x reduction in linear scans of chunk text per call.
        CharStats charStats = computeCharStats(chunkText);

        metadataMap.put("whitespace_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) charStats.whitespace / characterCount)) : 0).build());
        metadataMap.put("alphanumeric_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) charStats.alphanumeric / characterCount)) : 0).build());
        metadataMap.put("digit_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) charStats.digits / characterCount)) : 0).build());
        metadataMap.put("uppercase_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) charStats.uppercase / characterCount)) : 0).build());

        Struct.Builder punctuationStruct = Struct.newBuilder();
        for (Map.Entry<Character, Integer> entry : charStats.punctuationCounts.entrySet()) {
            punctuationStruct.putFields(String.valueOf(entry.getKey()), Value.newBuilder().setNumberValue(entry.getValue()).build());
        }
        metadataMap.put("punctuation_counts", Value.newBuilder().setStructValue(punctuationStruct).build());

        metadataMap.put("is_first_chunk", Value.newBuilder().setBoolValue(chunkNumber == 0).build());
        metadataMap.put("is_last_chunk", Value.newBuilder().setBoolValue(chunkNumber == totalChunksInDocument - 1).build());
        if (totalChunksInDocument > 0) {
            double relativePosition = (totalChunksInDocument == 1) ? 0.0 : (double) chunkNumber / (totalChunksInDocument - 1);
            metadataMap.put("relative_position", Value.newBuilder().setNumberValue(Double.parseDouble(DECIMAL_FORMAT.format(relativePosition))).build());
        } else {
            metadataMap.put("relative_position", Value.newBuilder().setNumberValue(0).build());
        }

        metadataMap.put("contains_urlplaceholder", Value.newBuilder().setBoolValue(containsUrlPlaceholder).build());
        metadataMap.put("list_item_indicator", Value.newBuilder().setBoolValue(LIST_ITEM_PATTERN.matcher(chunkText).matches()).build());
        metadataMap.put("potential_heading_score", Value.newBuilder().setNumberValue(calculatePotentialHeadingScore(chunkText, tokens, sentenceCount)).build());

        return metadataMap;
    }

    /**
     * Extracts typed ChunkAnalytics proto for a chunk.
     *
     * <p>Convenience overload that runs OpenNLP on the chunk text. Use the
     * {@link #extractChunkAnalytics(String, int, int, boolean, ChunkNlpSlice)
     * 5-arg overload} when you have a pre-computed {@link ChunkNlpSlice}
     * available — that version reuses the slice instead of re-running
     * sentence detection and tokenization per chunk.
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks, boolean containsUrlPlaceholder) {
        return extractChunkAnalytics(chunkText, chunkNumber, totalChunks, containsUrlPlaceholder, null);
    }

    /**
     * Extracts typed ChunkAnalytics proto for a chunk, optionally using a
     * pre-computed {@link ChunkNlpSlice} to avoid re-running OpenNLP on the
     * chunk text.
     *
     * <p>If {@code slice} is {@code null}, this method falls back to running
     * {@link #safeSentDetect} and {@link #safeTokenize} on the chunk text
     * (current pre-PR-I behavior). When {@code slice} is non-null, the slice's
     * pre-computed tokens, sentence count, and sum-of-token-lengths are used
     * directly.
     *
     * @param chunkText The text content of the chunk
     * @param chunkNumber Position of this chunk in the sequence (0-based)
     * @param totalChunks Total number of chunks in the document
     * @param containsUrlPlaceholder Whether the chunk contains URL placeholders
     * @param slice Pre-computed NLP slice from {@link #sliceForChunk}, or
     *              {@code null} to compute on the fly
     * @return ChunkAnalytics proto with text-statistics fields populated
     *         (POS densities still require the
     *         {@link #extractChunkAnalytics(String, int, int, boolean,
     *         ChunkNlpSlice, NlpPreprocessor.NlpResult, int, int) 8-arg form})
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks, boolean containsUrlPlaceholder, ChunkNlpSlice slice) {
        ChunkAnalytics.Builder builder = ChunkAnalytics.newBuilder();
        if (StringUtils.isBlank(chunkText)) {
            return builder.build();
        }

        int characterCount = chunkText.length();
        String[] tokens;
        int sentenceCount;
        int sumTokenLengths;
        if (slice != null) {
            tokens = slice.tokens;
            sentenceCount = slice.sentenceCount;
            sumTokenLengths = slice.sumTokenLengths;
        } else {
            String[] sentences = safeSentDetect(chunkText);
            sentenceCount = sentences.length;
            tokens = safeTokenize(chunkText);
            sumTokenLengths = 0;
            for (String t : tokens) {
                if (t != null) sumTokenLengths += t.length();
            }
        }
        int wordCount = tokens.length;

        builder.setWordCount(wordCount)
                .setCharacterCount(characterCount)
                .setSentenceCount(sentenceCount);

        if (wordCount > 0) {
            double avgWordLen = (double) sumTokenLengths / wordCount;
            builder.setAverageWordLength((float) avgWordLen);
            Set<String> unique = new HashSet<>(Arrays.asList(tokens));
            builder.setVocabularyDensity((float) unique.size() / wordCount);
        }
        if (sentenceCount > 0) {
            builder.setAverageSentenceLength((float) wordCount / sentenceCount);
        }

        // PR-H: single fused pass over chunkText for all 5 char-class counters
        // (whitespace, alphanumeric, digits, uppercase, punctuation).
        CharStats charStats = computeCharStats(chunkText);

        builder.setWhitespacePercentage(characterCount > 0 ? (float) charStats.whitespace / characterCount : 0)
                .setAlphanumericPercentage(characterCount > 0 ? (float) charStats.alphanumeric / characterCount : 0)
                .setDigitPercentage(characterCount > 0 ? (float) charStats.digits / characterCount : 0)
                .setUppercasePercentage(characterCount > 0 ? (float) charStats.uppercase / characterCount : 0);

        for (Map.Entry<Character, Integer> entry : charStats.punctuationCounts.entrySet()) {
            builder.putPunctuationCounts(String.valueOf(entry.getKey()), entry.getValue());
        }

        // Positional fields
        builder.setIsFirstChunk(chunkNumber == 0)
                .setIsLastChunk(chunkNumber == totalChunks - 1)
                .setContainsUrlPlaceholder(containsUrlPlaceholder)
                .setListItemIndicator(LIST_ITEM_PATTERN.matcher(chunkText).matches())
                .setPotentialHeadingScore((float) calculatePotentialHeadingScore(chunkText, tokens, sentenceCount));

        if (totalChunks > 1) {
            builder.setRelativePosition((float) chunkNumber / (totalChunks - 1));
        }

        return builder.build();
    }

    /**
     * Extracts typed DocumentAnalytics proto for a full source text.
     */
    public DocumentAnalytics extractDocumentAnalytics(String fullText) {
        DocumentAnalytics.Builder builder = DocumentAnalytics.newBuilder();
        if (StringUtils.isBlank(fullText)) {
            return builder.build();
        }

        int characterCount = fullText.length();
        String[] sentences = safeSentDetect(fullText);
        String[] tokens = safeTokenize(fullText);
        int wordCount = tokens.length;
        int sentenceCount = sentences.length;

        builder.setWordCount(wordCount)
                .setCharacterCount(characterCount)
                .setSentenceCount(sentenceCount);

        if (wordCount > 0) {
            double avgWordLen = (double) Arrays.stream(tokens).mapToInt(String::length).sum() / wordCount;
            builder.setAverageWordLength((float) avgWordLen);
            Set<String> unique = new HashSet<>(Arrays.asList(tokens));
            builder.setVocabularyDensity((float) unique.size() / wordCount);
        }
        if (sentenceCount > 0) {
            builder.setAverageSentenceLength((float) wordCount / sentenceCount);
        }

        // PR-H: single fused pass — same fix as the chunk-level metadata
        // methods. extractDocumentAnalytics runs once per source_label per
        // request so the win here is small per call, but it adds up on
        // multi-directive requests with large source texts.
        CharStats charStats = computeCharStats(fullText);

        builder.setWhitespacePercentage(characterCount > 0 ? (float) charStats.whitespace / characterCount : 0)
                .setAlphanumericPercentage(characterCount > 0 ? (float) charStats.alphanumeric / characterCount : 0)
                .setDigitPercentage(characterCount > 0 ? (float) charStats.digits / characterCount : 0)
                .setUppercasePercentage(characterCount > 0 ? (float) charStats.uppercase / characterCount : 0);

        for (Map.Entry<Character, Integer> entry : charStats.punctuationCounts.entrySet()) {
            builder.putPunctuationCounts(String.valueOf(entry.getKey()), entry.getValue());
        }

        return builder.build();
    }

    /**
     * Extracts typed DocumentAnalytics proto for a full source text, enriched with NLP data.
     * Uses pre-computed NlpResult to populate POS/language fields without re-running NLP.
     *
     * @param fullText The full source text
     * @param nlpResult Pre-computed NLP analysis results
     * @return DocumentAnalytics proto with POS/language fields populated
     */
    public DocumentAnalytics extractDocumentAnalytics(String fullText, NlpPreprocessor.NlpResult nlpResult) {
        // Start with the base analytics (word count, sentence count, etc.)
        DocumentAnalytics base = extractDocumentAnalytics(fullText);

        if (nlpResult == null || nlpResult.tokens().length == 0) {
            return base;
        }

        // Enrich with NLP-derived fields
        return base.toBuilder()
                .setDetectedLanguage(nlpResult.detectedLanguage())
                .setLanguageConfidence(nlpResult.languageConfidence())
                .setNounDensity(nlpResult.nounDensity())
                .setVerbDensity(nlpResult.verbDensity())
                .setAdjectiveDensity(nlpResult.adjectiveDensity())
                .setContentWordRatio(nlpResult.contentWordRatio())
                .setUniqueLemmaCount(nlpResult.uniqueLemmaCount())
                .setLexicalDensity(nlpResult.lexicalDensity())
                .build();
    }

    /**
     * Extracts chunk analytics by slicing the document-level NLP arrays.
     * No NLP re-execution — just finds the token range for this chunk's character
     * offsets and computes POS ratios from that slice. O(log n) binary search + O(k) scan.
     *
     * <p>This 7-arg overload computes the {@link ChunkNlpSlice} internally and
     * passes it to the {@link #extractChunkAnalytics(String, int, int, boolean,
     * ChunkNlpSlice, NlpPreprocessor.NlpResult, int, int) 8-arg form}. Callers
     * that compute the slice once and want to share it with
     * {@link #extractAllMetadata(String, int, int, boolean, ChunkNlpSlice)}
     * should use the 8-arg form directly to avoid the extra slice work.
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks,
                                                 boolean containsUrlPlaceholder,
                                                 NlpPreprocessor.NlpResult docNlpResult,
                                                 int chunkStartOffset, int chunkEndOffset) {
        ChunkNlpSlice slice = sliceForChunk(docNlpResult, chunkStartOffset, chunkEndOffset);
        return extractChunkAnalytics(chunkText, chunkNumber, totalChunks, containsUrlPlaceholder,
                slice, docNlpResult, chunkStartOffset, chunkEndOffset);
    }

    /**
     * Extracts typed ChunkAnalytics proto for a chunk using a pre-computed
     * {@link ChunkNlpSlice} (for base text statistics) AND the doc-level
     * {@link NlpPreprocessor.NlpResult} (for POS densities derived from
     * posTags/lemmas slices).
     *
     * <p>This is the form ChunkerGrpcImpl Path A and Path B use: the slice
     * is computed once per chunk via {@link #sliceForChunk} and shared
     * between this call and the matching {@link #extractAllMetadata(String,
     * int, int, boolean, ChunkNlpSlice)} call so the slice work is paid for
     * once instead of twice. Eliminates the chunker's hot-path 4 OpenNLP
     * runs per chunk (2 in extractAllMetadata + 2 in extractChunkAnalytics's
     * 4-arg base).
     *
     * <p>If {@code slice} is {@code null}, falls back to running OpenNLP
     * on the chunk text (legacy 4-arg behavior). If {@code docNlpResult} is
     * {@code null}, the POS density fields stay at proto defaults (zero) —
     * this is the same fallback as the pre-PR-I 7-arg overload.
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks,
                                                 boolean containsUrlPlaceholder,
                                                 ChunkNlpSlice slice,
                                                 NlpPreprocessor.NlpResult docNlpResult,
                                                 int chunkStartOffset, int chunkEndOffset) {
        ChunkAnalytics base = extractChunkAnalytics(chunkText, chunkNumber, totalChunks,
                containsUrlPlaceholder, slice);

        if (docNlpResult == null || docNlpResult.tokens().length == 0 || StringUtils.isBlank(chunkText)) {
            return base;
        }

        // Binary search for first token at or after chunkStartOffset.
        // Same logic as the 7-arg form — POS densities still need the
        // doc-level posTags / lemmas arrays, which the slice doesn't carry
        // (carrying them would balloon the slice memory by 2× per chunk
        // for a relatively small POS-density gain).
        opennlp.tools.util.Span[] spans = docNlpResult.tokenSpans();
        String[] posTags = docNlpResult.posTags();
        String[] lemmas = docNlpResult.lemmas();

        int firstToken = findFirstTokenAtOrAfter(spans, chunkStartOffset);
        int lastToken = findLastTokenBefore(spans, chunkEndOffset);

        if (firstToken < 0 || lastToken < firstToken) {
            return base;
        }

        // Count POS tags in this chunk's token range
        int total = lastToken - firstToken + 1;
        int nouns = 0, verbs = 0, adjectives = 0, adverbs = 0;
        Set<String> uniqueLemmas = new HashSet<>();

        for (int i = firstToken; i <= lastToken; i++) {
            if (i >= posTags.length || i >= lemmas.length) break;
            String tag = posTags[i];
            if ("NOUN".equals(tag) || "PROPN".equals(tag)) nouns++;
            else if ("VERB".equals(tag) || "AUX".equals(tag)) verbs++;
            else if ("ADJ".equals(tag)) adjectives++;
            else if ("ADV".equals(tag)) adverbs++;

            String lemma = lemmas[i];
            if (lemma != null && !"O".equals(lemma) && !lemma.isBlank()) {
                uniqueLemmas.add(lemma.toLowerCase());
            }
        }

        int contentWords = nouns + verbs + adjectives + adverbs;

        return base.toBuilder()
                .setNounDensity(total > 0 ? (float) nouns / total : 0)
                .setVerbDensity(total > 0 ? (float) verbs / total : 0)
                .setAdjectiveDensity(total > 0 ? (float) adjectives / total : 0)
                .setContentWordRatio(total > 0 ? (float) contentWords / total : 0)
                .setUniqueLemmaCount(uniqueLemmas.size())
                .setLexicalDensity(total > 0 ? (float) contentWords / total : 0)
                .build();
    }

    /** Finds the first token whose start offset is >= targetOffset. */
    private int findFirstTokenAtOrAfter(opennlp.tools.util.Span[] spans, int targetOffset) {
        int lo = 0, hi = spans.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans[mid].getStart() < targetOffset) lo = mid + 1;
            else hi = mid;
        }
        return lo < spans.length ? lo : -1;
    }

    /** Finds the last token whose start offset is < targetOffset. */
    private int findLastTokenBefore(opennlp.tools.util.Span[] spans, int targetOffset) {
        int lo = 0, hi = spans.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans[mid].getStart() < targetOffset) lo = mid + 1;
            else hi = mid;
        }
        return lo - 1;
    }

    /**
     * Calculates a score indicating how likely the text is to be a heading.
     * Higher scores (closer to 1.0) indicate greater likelihood of being a heading.
     *
     * @param chunkText The text content of the chunk
     * @param tokens The tokens in the chunk
     * @param sentenceCount The number of sentences in the chunk
     * @return A score between 0.0 and 1.0
     */
    /**
     * Safely tokenize text, avoiding OpenNLP's internal NPE on null spans.
     * tokenize() calls Span.spansToStrings() internally which NPEs on null spans.
     */
    private String[] safeTokenize(String text) {
        try {
            opennlp.tools.util.Span[] spans = tokenizer.tokenizePos(text);
            int textLen = text.length();
            int valid = 0;
            for (opennlp.tools.util.Span s : spans) {
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    valid++;
                }
            }
            String[] result = new String[valid];
            int idx = 0;
            for (opennlp.tools.util.Span s : spans) {
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    result[idx++] = text.substring(s.getStart(), s.getEnd());
                }
            }
            return result;
        } catch (Exception e) {
            return new String[]{text};
        }
    }

    /**
     * Safely detect sentences, avoiding OpenNLP's internal NPE on null spans.
     * sentDetect() calls Span.spansToStrings() internally which NPEs when the model
     * produces null spans (common with parsed PDFs/HTML containing funky formatting).
     */
    private String[] safeSentDetect(String text) {
        try {
            opennlp.tools.util.Span[] spans = sentenceDetector.sentPosDetect(text);
            int textLen = text.length();
            // Count valid spans, derive strings manually
            int valid = 0;
            for (opennlp.tools.util.Span s : spans) {
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    valid++;
                }
            }
            String[] result = new String[valid];
            int idx = 0;
            for (opennlp.tools.util.Span s : spans) {
                if (s != null && s.getStart() >= 0 && s.getEnd() <= textLen && s.getStart() < s.getEnd()) {
                    result[idx++] = text.substring(s.getStart(), s.getEnd());
                }
            }
            return result;
        } catch (Exception e) {
            // Last resort fallback — treat entire text as one sentence
            return new String[]{text};
        }
    }

    private double calculatePotentialHeadingScore(String chunkText, String[] tokens, int sentenceCount) {
        double score = 0.0;
        if (tokens.length == 0) return 0.0;

        if (tokens.length < 10) score += 0.2;
        if (tokens.length < 5) score += 0.2;
        if (sentenceCount == 1) score += 0.3;

        if (!chunkText.isEmpty()) {
            char lastChar = chunkText.charAt(chunkText.length() - 1);
            if (Character.isLetterOrDigit(lastChar)) {
                score += 0.2;
            }
        }

        long uppercaseWords = Arrays.stream(tokens)
                .filter(token -> token.length() > 0 && Character.isUpperCase(token.charAt(0)))
                .count();
        if (tokens.length > 0 && (double) uppercaseWords / tokens.length > 0.7) {
            score += 0.2;
        }
        if (StringUtils.isAllUpperCase(chunkText.replaceAll("\\s+", ""))) {
            score += 0.2;
        }
        return Math.min(1.0, Double.parseDouble(DECIMAL_FORMAT.format(score)));
    }
}