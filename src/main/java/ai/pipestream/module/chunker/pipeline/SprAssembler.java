package ai.pipestream.module.chunker.pipeline;

import ai.pipestream.data.v1.ChunkAnalytics;
import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.module.chunker.analytics.ChunkAnalyticsBuilder;
import ai.pipestream.module.chunker.analytics.DocumentAnalyticsBuilder;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpResult;
import ai.pipestream.module.chunker.service.UnicodeSanitizer;
import com.google.protobuf.Value;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles {@link SemanticProcessingResult} messages from the output of the
 * chunking stage. Applies DESIGN §21.5 deterministic chunk/result ids and
 * §4.1 stage-1 placeholder (embedding_config_id=""). Stateless.
 */
@ApplicationScoped
public class SprAssembler {

    public static final String SENTENCES_INTERNAL_CONFIG_ID = "sentences_internal";

    private final ChunkAnalyticsBuilder chunkAnalyticsBuilder;
    private final DocumentAnalyticsBuilder docAnalyticsBuilder;

    @Inject
    public SprAssembler(ChunkAnalyticsBuilder chunkAnalyticsBuilder,
                        DocumentAnalyticsBuilder docAnalyticsBuilder) {
        this.chunkAnalyticsBuilder = chunkAnalyticsBuilder;
        this.docAnalyticsBuilder = docAnalyticsBuilder;
    }

    public SemanticProcessingResult assemble(String docHash,
                                             String sourceLabel,
                                             String chunkerConfigId,
                                             String directiveKey,
                                             List<Chunk> chunks,
                                             NlpResult nlp,
                                             int sourceCharLen,
                                             List<Span> urlSpans) {

        NlpDocumentAnalysis nlpAnalysis = docAnalyticsBuilder.buildNlpAnalysis(nlp);
        List<SemanticChunk> semanticChunks = new ArrayList<>(chunks.size());

        int i = 0;
        for (Chunk c : chunks) {
            String sanitized = UnicodeSanitizer.sanitizeInvalidUnicode(c.text());
            String chunkId = docHash + ":" + sourceLabel + ":" + chunkerConfigId
                    + ":" + i + ":" + c.originalIndexStart() + ":" + c.originalIndexEnd();
            String contentHash = Hashing.sha256Hex(sanitized);
            boolean containsUrl = spanContainsAnyUrl(c.originalIndexStart(), c.originalIndexEnd() + 1, urlSpans);

            ChunkAnalytics analytics = chunkAnalyticsBuilder.build(
                    sanitized, i, chunks.size(),
                    c.originalIndexStart(), c.originalIndexEnd() + 1,
                    sourceCharLen, nlp, containsUrl, contentHash);

            ChunkEmbedding embedding = ChunkEmbedding.newBuilder()
                    .setTextContent(sanitized)
                    .setChunkId(chunkId)
                    .setOriginalCharStartOffset(c.originalIndexStart())
                    .setOriginalCharEndOffset(c.originalIndexEnd())
                    .setChunkConfigId(chunkerConfigId)
                    .build();

            semanticChunks.add(SemanticChunk.newBuilder()
                    .setChunkId(chunkId)
                    .setChunkNumber(i)
                    .setEmbeddingInfo(embedding)
                    .setChunkAnalytics(analytics)
                    .build());
            i++;
        }

        String resultId = "stage1:" + docHash + ":" + sourceLabel + ":" + chunkerConfigId + ":";

        return SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .setSourceFieldName(sourceLabel)
                .setChunkConfigId(chunkerConfigId)
                .setEmbeddingConfigId("")
                .addAllChunks(semanticChunks)
                .putMetadata("directive_key", Value.newBuilder().setStringValue(directiveKey).build())
                .setNlpAnalysis(nlpAnalysis)
                .build();
    }

    /**
     * Builds the sentences_internal SPR for §21.9. One chunk per detected
     * sentence, each with full ChunkAnalytics.
     */
    public SemanticProcessingResult assembleSentencesInternal(String docHash,
                                                              String sourceLabel,
                                                              String directiveKey,
                                                              NlpResult nlp,
                                                              int sourceCharLen) {
        String[] sentences = nlp.sentences();
        Span[] spans = nlp.sentenceSpans();
        NlpDocumentAnalysis nlpAnalysis = docAnalyticsBuilder.buildNlpAnalysis(nlp);

        List<SemanticChunk> semanticChunks = new ArrayList<>(sentences.length);
        for (int i = 0; i < sentences.length; i++) {
            String sanitized = UnicodeSanitizer.sanitizeInvalidUnicode(sentences[i]);
            int start = spans[i].getStart();
            int end = spans[i].getEnd();
            String chunkId = docHash + ":" + sourceLabel + ":" + SENTENCES_INTERNAL_CONFIG_ID
                    + ":" + i + ":" + start + ":" + (end - 1);
            String contentHash = Hashing.sha256Hex(sanitized);

            ChunkAnalytics analytics = chunkAnalyticsBuilder.build(
                    sanitized, i, sentences.length,
                    start, end,
                    sourceCharLen, nlp, false, contentHash);

            ChunkEmbedding embedding = ChunkEmbedding.newBuilder()
                    .setTextContent(sanitized)
                    .setChunkId(chunkId)
                    .setOriginalCharStartOffset(start)
                    .setOriginalCharEndOffset(end - 1)
                    .setChunkConfigId(SENTENCES_INTERNAL_CONFIG_ID)
                    .build();

            semanticChunks.add(SemanticChunk.newBuilder()
                    .setChunkId(chunkId)
                    .setChunkNumber(i)
                    .setEmbeddingInfo(embedding)
                    .setChunkAnalytics(analytics)
                    .build());
        }

        String resultId = "stage1:" + docHash + ":" + sourceLabel + ":" + SENTENCES_INTERNAL_CONFIG_ID + ":";
        return SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .setSourceFieldName(sourceLabel)
                .setChunkConfigId(SENTENCES_INTERNAL_CONFIG_ID)
                .setEmbeddingConfigId("")
                .addAllChunks(semanticChunks)
                .putMetadata("directive_key", Value.newBuilder().setStringValue(directiveKey).build())
                .setNlpAnalysis(nlpAnalysis)
                .build();
    }

    private static boolean spanContainsAnyUrl(int start, int endExclusive, List<Span> urlSpans) {
        if (urlSpans == null) return false;
        for (Span u : urlSpans) {
            if (u.getStart() < endExclusive && u.getEnd() > start) return true;
        }
        return false;
    }
}
