package ai.pipestream.module.chunker.chunk;

import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;

/**
 * Character-windowed chunker. {@code chunkSize} and {@code chunkOverlap} are
 * character counts. Cuts inside words / URLs by construction — intended for
 * callers that already know what they're asking for.
 */
@ApplicationScoped
public class CharacterChunker implements Chunker {

    @Override
    public List<Chunk> chunk(String text, NlpResult nlp, List<Span> urlSpans, ChunkerConfig config) {
        if (text == null || text.isEmpty()) return List.of();

        int size = config.chunkSize();
        int overlap = config.chunkOverlap();
        int advance = Math.max(1, size - overlap);
        int len = text.length();

        List<Chunk> chunks = new ArrayList<>();
        int start = 0;
        while (start < len) {
            int end = Math.min(start + size, len);
            chunks.add(new Chunk("char-" + chunks.size(), text.substring(start, end), start, end - 1));
            if (end >= len) break;
            start += advance;
        }
        return chunks;
    }
}
