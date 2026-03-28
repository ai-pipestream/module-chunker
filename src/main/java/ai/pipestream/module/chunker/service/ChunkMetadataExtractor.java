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
     * Extracts comprehensive metadata from a text chunk.
     *
     * @param chunkText The text content of the chunk
     * @param chunkNumber The position of this chunk in the sequence (0-based)
     * @param totalChunksInDocument Total number of chunks in the document
     * @param containsUrlPlaceholder Whether the chunk contains URL placeholders
     * @return A map of metadata key-value pairs
     */
    public Map<String, Value> extractAllMetadata(String chunkText, int chunkNumber, int totalChunksInDocument, boolean containsUrlPlaceholder) {
        Map<String, Value> metadataMap = new HashMap<>();

        if (StringUtils.isBlank(chunkText)) {
            metadataMap.put("word_count", Value.newBuilder().setNumberValue(0).build());
            metadataMap.put("character_count", Value.newBuilder().setNumberValue(0).build());
            metadataMap.put("sentence_count", Value.newBuilder().setNumberValue(0).build());
            return metadataMap;
        }

        int characterCount = chunkText.length();
        metadataMap.put("character_count", Value.newBuilder().setNumberValue(characterCount).build());

        String[] sentences = sentenceDetector.sentDetect(chunkText);
        int sentenceCount = sentences.length;
        metadataMap.put("sentence_count", Value.newBuilder().setNumberValue(sentenceCount).build());

        String[] tokens = tokenizer.tokenize(chunkText);
        int wordCount = tokens.length;
        metadataMap.put("word_count", Value.newBuilder().setNumberValue(wordCount).build());

        double avgWordLength = wordCount > 0 ? (double) Arrays.stream(tokens).mapToInt(String::length).sum() / wordCount : 0;
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

        long whitespaceChars = chunkText.chars().filter(Character::isWhitespace).count();
        long alphanumericChars = chunkText.chars().filter(Character::isLetterOrDigit).count();
        long digitChars = chunkText.chars().filter(Character::isDigit).count();
        long uppercaseChars = chunkText.chars().filter(Character::isUpperCase).count();

        metadataMap.put("whitespace_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) whitespaceChars / characterCount)) : 0).build());
        metadataMap.put("alphanumeric_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) alphanumericChars / characterCount)) : 0).build());
        metadataMap.put("digit_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) digitChars / characterCount)) : 0).build());
        metadataMap.put("uppercase_percentage", Value.newBuilder().setNumberValue(characterCount > 0 ? Double.parseDouble(DECIMAL_FORMAT.format((double) uppercaseChars / characterCount)) : 0).build());

        Struct.Builder punctuationStruct = Struct.newBuilder();
        Map<Character, Integer> puncCounts = new HashMap<>();
        for (char c : chunkText.toCharArray()) {
            if (StringUtils.isAsciiPrintable(String.valueOf(c)) && !Character.isLetterOrDigit(c) && !Character.isWhitespace(c)) {
                puncCounts.put(c, puncCounts.getOrDefault(c, 0) + 1);
            }
        }
        for (Map.Entry<Character, Integer> entry : puncCounts.entrySet()) {
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
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks, boolean containsUrlPlaceholder) {
        ChunkAnalytics.Builder builder = ChunkAnalytics.newBuilder();
        if (StringUtils.isBlank(chunkText)) {
            return builder.build();
        }

        int characterCount = chunkText.length();
        String[] sentences = sentenceDetector.sentDetect(chunkText);
        String[] tokens = tokenizer.tokenize(chunkText);
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

        long whitespace = chunkText.chars().filter(Character::isWhitespace).count();
        long alphanumeric = chunkText.chars().filter(Character::isLetterOrDigit).count();
        long digits = chunkText.chars().filter(Character::isDigit).count();
        long uppercase = chunkText.chars().filter(Character::isUpperCase).count();

        builder.setWhitespacePercentage(characterCount > 0 ? (float) whitespace / characterCount : 0)
                .setAlphanumericPercentage(characterCount > 0 ? (float) alphanumeric / characterCount : 0)
                .setDigitPercentage(characterCount > 0 ? (float) digits / characterCount : 0)
                .setUppercasePercentage(characterCount > 0 ? (float) uppercase / characterCount : 0);

        Map<Character, Integer> puncCounts = new HashMap<>();
        for (char c : chunkText.toCharArray()) {
            if (StringUtils.isAsciiPrintable(String.valueOf(c)) && !Character.isLetterOrDigit(c) && !Character.isWhitespace(c)) {
                puncCounts.merge(c, 1, Integer::sum);
            }
        }
        for (Map.Entry<Character, Integer> entry : puncCounts.entrySet()) {
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
        String[] sentences = sentenceDetector.sentDetect(fullText);
        String[] tokens = tokenizer.tokenize(fullText);
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

        long whitespace = fullText.chars().filter(Character::isWhitespace).count();
        long alphanumeric = fullText.chars().filter(Character::isLetterOrDigit).count();
        long digits = fullText.chars().filter(Character::isDigit).count();
        long uppercase = fullText.chars().filter(Character::isUpperCase).count();

        builder.setWhitespacePercentage(characterCount > 0 ? (float) whitespace / characterCount : 0)
                .setAlphanumericPercentage(characterCount > 0 ? (float) alphanumeric / characterCount : 0)
                .setDigitPercentage(characterCount > 0 ? (float) digits / characterCount : 0)
                .setUppercasePercentage(characterCount > 0 ? (float) uppercase / characterCount : 0);

        Map<Character, Integer> puncCounts = new HashMap<>();
        for (char c : fullText.toCharArray()) {
            if (StringUtils.isAsciiPrintable(String.valueOf(c)) && !Character.isLetterOrDigit(c) && !Character.isWhitespace(c)) {
                puncCounts.merge(c, 1, Integer::sum);
            }
        }
        for (Map.Entry<Character, Integer> entry : puncCounts.entrySet()) {
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
     * Extracts typed ChunkAnalytics proto for a chunk, enriched with NLP POS data.
     * Runs a lightweight NLP pass on the chunk text to compute chunk-level POS ratios.
     *
     * @param chunkText The text content of the chunk
     * @param chunkNumber The position of this chunk in the sequence (0-based)
     * @param totalChunks Total number of chunks in the document
     * @param containsUrlPlaceholder Whether the chunk contains URL placeholders
     * @param nlpPreprocessor NlpPreprocessor to run on the chunk text
     * @return ChunkAnalytics proto with POS fields populated
     */
    public ChunkAnalytics extractChunkAnalytics(String chunkText, int chunkNumber, int totalChunks,
                                                 boolean containsUrlPlaceholder,
                                                 NlpPreprocessor nlpPreprocessor) {
        // Start with the base chunk analytics
        ChunkAnalytics base = extractChunkAnalytics(chunkText, chunkNumber, totalChunks, containsUrlPlaceholder);

        if (nlpPreprocessor == null || StringUtils.isBlank(chunkText)) {
            return base;
        }

        // Run NLP on the chunk text for chunk-level POS ratios
        NlpPreprocessor.NlpResult chunkNlp = nlpPreprocessor.preprocess(chunkText);

        if (chunkNlp.tokens().length == 0) {
            return base;
        }

        // Compute unique lemma count for this chunk
        int chunkUniqueLemmaCount = (int) Arrays.stream(chunkNlp.lemmas())
                .filter(l -> !"O".equals(l))
                .collect(Collectors.toSet())
                .size();

        return base.toBuilder()
                .setNounDensity(chunkNlp.nounDensity())
                .setVerbDensity(chunkNlp.verbDensity())
                .setAdjectiveDensity(chunkNlp.adjectiveDensity())
                .setContentWordRatio(chunkNlp.contentWordRatio())
                .setUniqueLemmaCount(chunkUniqueLemmaCount)
                .setLexicalDensity(chunkNlp.lexicalDensity())
                .build();
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