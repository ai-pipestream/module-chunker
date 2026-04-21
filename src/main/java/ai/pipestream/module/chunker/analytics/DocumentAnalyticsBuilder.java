package ai.pipestream.module.chunker.analytics;

import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.util.Span;

import java.util.HashSet;
import java.util.Set;

/**
 * Builds the document-level proto messages consumed downstream:
 * {@link DocumentAnalytics} (stored on each {@code SourceFieldAnalytics})
 * and {@link NlpDocumentAnalysis} (stored on each {@code SemanticProcessingResult}).
 *
 * <p>Pure function over {@link NlpResult} + raw text. No caching, no state.
 */
@ApplicationScoped
public class DocumentAnalyticsBuilder {

    public DocumentAnalytics buildDocumentAnalytics(String text, NlpResult nlp) {
        TextStats.Result s = TextStats.compute(text);
        int tokenCount = nlp.tokens().length;
        int sentenceCount = nlp.sentences().length;

        int sumTokenLen = 0;
        Set<String> types = new HashSet<>();
        for (String t : nlp.tokens()) {
            sumTokenLen += t.length();
            types.add(t);
        }
        float avgWordLen = tokenCount == 0 ? 0f : (float) sumTokenLen / tokenCount;
        float avgSentLen = sentenceCount == 0 ? 0f : (float) tokenCount / sentenceCount;
        float vocabDensity = tokenCount == 0 ? 0f : (float) types.size() / tokenCount;

        DocumentAnalytics.Builder b = DocumentAnalytics.newBuilder()
                .setWordCount(tokenCount)
                .setCharacterCount(s.chars())
                .setSentenceCount(sentenceCount)
                .setAverageWordLength(avgWordLen)
                .setAverageSentenceLength(avgSentLen)
                .setVocabularyDensity(vocabDensity)
                .setWhitespacePercentage(s.whitespacePct())
                .setAlphanumericPercentage(s.alphanumericPct())
                .setDigitPercentage(s.digitPct())
                .setUppercasePercentage(s.uppercasePct())
                .setDetectedLanguage(nlp.detectedLanguage())
                .setLanguageConfidence(nlp.languageConfidence())
                .setNounDensity(nlp.nounDensity())
                .setVerbDensity(nlp.verbDensity())
                .setAdjectiveDensity(nlp.adjectiveDensity())
                .setContentWordRatio(nlp.contentWordRatio())
                .setUniqueLemmaCount(nlp.uniqueLemmaCount())
                .setLexicalDensity(nlp.lexicalDensity());
        b.putAllPunctuationCounts(s.punctuation());
        return b.build();
    }

    public NlpDocumentAnalysis buildNlpAnalysis(NlpResult nlp) {
        NlpDocumentAnalysis.Builder b = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage(nlp.detectedLanguage())
                .setLanguageConfidence(nlp.languageConfidence())
                .setTotalTokens(nlp.tokens().length)
                .setNounDensity(nlp.nounDensity())
                .setVerbDensity(nlp.verbDensity())
                .setAdjectiveDensity(nlp.adjectiveDensity())
                .setAdverbDensity(nlp.adverbDensity())
                .setContentWordRatio(nlp.contentWordRatio())
                .setUniqueLemmaCount(nlp.uniqueLemmaCount())
                .setLexicalDensity(nlp.lexicalDensity());

        String[] sentences = nlp.sentences();
        Span[] spans = nlp.sentenceSpans();
        for (int i = 0; i < sentences.length; i++) {
            int start = spans[i].getStart();
            int end = spans[i].getEnd();
            b.addSentences(SentenceSpan.newBuilder()
                    .setText(sentences[i])
                    .setStartOffset(start)
                    .setEndOffset(end)
                    .build());
        }
        return b.build();
    }
}
