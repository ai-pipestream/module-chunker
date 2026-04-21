package ai.pipestream.module.chunker.chunk;

import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;

/**
 * Sentence-windowed chunker. {@code chunkSize} = sentences per chunk,
 * {@code chunkOverlap} = sentence overlap. Text is substring of original
 * source so offsets are exact.
 */
@ApplicationScoped
public class SentenceChunker implements Chunker {

    @Override
    public List<Chunk> chunk(String text, NlpResult nlp, List<Span> urlSpans, ChunkerConfig config) {
        Span[] spans = nlp.sentenceSpans();
        if (spans.length == 0) return List.of();

        int sentencesPer = config.chunkSize();
        int overlap = config.chunkOverlap();
        int advance = Math.max(1, sentencesPer - overlap);

        List<Chunk> chunks = new ArrayList<>();
        int start = 0;
        while (start < spans.length) {
            int endExclusive = Math.min(start + sentencesPer, spans.length);
            int charStart = spans[start].getStart();
            int charEnd   = spans[endExclusive - 1].getEnd();
            chunks.add(new Chunk("sent-" + chunks.size(), text.substring(charStart, charEnd), charStart, charEnd - 1));
            if (endExclusive >= spans.length) break;
            int nextStart = start + advance;
            if (nextStart <= start) nextStart = start + 1;
            start = nextStart;
        }
        return chunks;
    }
}
