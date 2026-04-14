package ai.pipestream.module.chunker.support;

import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.SentenceSpan;
import ai.pipestream.module.chunker.service.NlpPreprocessor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Shared static helpers used by both {@code ChunkerGrpcImpl} (the
 * directive-driven unary path) and {@code ChunkerStreamingGrpcImpl} (the
 * legacy streaming path). Extracted in PR-J after the same code lived
 * verbatim in both classes — small enough to repeat per the
 * "microservice pattern" guidance, but big enough (~45 LOC of proto
 * builder + 15 LOC of digest plumbing) that one shared module-local
 * helper is warranted.
 *
 * <p>Stays inside {@code module-chunker} — explicitly NOT extracted to
 * {@code pipestream-test-support} or any other shared library because
 * those bundle proto-generated classes into Quarkus extension jars and
 * cause classloader hazards for every consumer that also generates its
 * own protos. See PR #51 / PR #54 in pipestream-platform for the
 * historical incident.
 */
public final class ChunkerSupport {

    private ChunkerSupport() {}

    /**
     * Converts a {@link NlpPreprocessor.NlpResult} into the proto
     * {@link NlpDocumentAnalysis} message.
     *
     * <p>Pre-PR-J this conversion was duplicated verbatim in both
     * {@code ChunkerGrpcImpl} and {@code ChunkerStreamingGrpcImpl}. The
     * code is identical: same field set, same sentence-span loop, same
     * null-defensive offset reads.
     *
     * <p>Returns an empty {@link NlpDocumentAnalysis} (not null) if
     * {@code nlpResult} is null — caller doesn't need to null-check.
     */
    public static NlpDocumentAnalysis buildNlpAnalysis(NlpPreprocessor.NlpResult nlpResult) {
        if (nlpResult == null) {
            return NlpDocumentAnalysis.getDefaultInstance();
        }

        NlpDocumentAnalysis.Builder builder = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage(nlpResult.detectedLanguage())
                .setLanguageConfidence(nlpResult.languageConfidence())
                .setTotalTokens(nlpResult.tokens().length)
                .setNounDensity(nlpResult.nounDensity())
                .setVerbDensity(nlpResult.verbDensity())
                .setAdjectiveDensity(nlpResult.adjectiveDensity())
                .setAdverbDensity(nlpResult.adverbDensity())
                .setContentWordRatio(nlpResult.contentWordRatio())
                .setUniqueLemmaCount(nlpResult.uniqueLemmaCount())
                .setLexicalDensity(nlpResult.lexicalDensity());

        String[] sentences = nlpResult.sentences();
        opennlp.tools.util.Span[] spans = nlpResult.sentenceSpans();
        if (sentences != null) {
            for (int i = 0; i < sentences.length; i++) {
                if (sentences[i] == null) continue;
                int start = (spans != null && i < spans.length && spans[i] != null)
                        ? spans[i].getStart() : 0;
                int end = (spans != null && i < spans.length && spans[i] != null)
                        ? spans[i].getEnd() : sentences[i].length();
                builder.addSentences(SentenceSpan.newBuilder()
                        .setText(sentences[i])
                        .setStartOffset(start)
                        .setEndOffset(end)
                        .build());
            }
        }
        return builder.build();
    }

    /**
     * Computes the SHA-256 hash of the given text and returns it as a
     * lowercase hex string (64 chars, no separators).
     *
     * <p>Used as the chunk-level {@code content_hash} stamped into both
     * the typed {@code ChunkAnalytics.content_hash} field (PR-K2) and the
     * loose {@code SemanticChunk.metadata} map (legacy, kept for backward
     * compat during the additive window).
     *
     * <p>Identical sanitised text always produces the identical hash —
     * enables reprocessing dedup, content-addressed embedder cache keys,
     * and byte-verification of alternative chunker backends (e.g. an
     * OpenVINO tokenizer swap) against this implementation.
     *
     * @throws AssertionError if SHA-256 is not available, which cannot
     *         happen on a conformant JDK
     */
    public static String sha256Hex(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every JDK conforming to JLS.
            throw new AssertionError("SHA-256 not available", e);
        }
    }
}
