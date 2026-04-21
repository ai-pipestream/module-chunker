package ai.pipestream.module.chunker.nlp;

import opennlp.tools.util.Span;

/**
 * Immutable result of a single NLP pass over one piece of source text.
 *
 * <p>Every field is computed at analyze time. There is no lazy, no cache,
 * no mutability. Two NlpResults for the same text are byte-identical.
 *
 * <p>{@code tokens} and {@code tokenSpans} are parallel arrays: {@code tokens[i]}
 * is the surface string for {@code tokenSpans[i]}. Same for sentences.
 * {@code posTags[i]} and {@code lemmas[i]} correspond to {@code tokens[i]}.
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
    private static final Span[] EMPTY_SPANS = new Span[0];
    private static final String[] EMPTY_STRINGS = new String[0];

    public static NlpResult empty() {
        return new NlpResult(
                EMPTY_STRINGS, EMPTY_SPANS,
                EMPTY_STRINGS, EMPTY_SPANS,
                EMPTY_STRINGS, EMPTY_STRINGS,
                "", 0f,
                0f, 0f, 0f, 0f, 0f, 0f, 0);
    }

    public boolean isEmpty() {
        return tokens.length == 0;
    }
}
