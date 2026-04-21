package ai.pipestream.module.chunker.chunk;

import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;

/**
 * Token-windowed chunker. {@code chunkSize} and {@code chunkOverlap} are
 * counted in tokens. Chunk text is reconstructed as a substring of the
 * original text (inclusive of inter-token whitespace) so offsets are exact
 * and reversible — no smart-spacing reconstruction.
 *
 * <p>When {@code preserveUrls} is true and a chunk's end boundary falls inside
 * a URL span, the boundary is extended forward to the end of that URL so URLs
 * are never split. This replaces the old placeholder-substitution scheme and
 * keeps offsets aligned with the source text throughout.
 */
@ApplicationScoped
public class TokenChunker implements Chunker {

    @Override
    public List<Chunk> chunk(String text, NlpResult nlp, List<Span> urlSpans, ChunkerConfig config) {
        Span[] spans = nlp.tokenSpans();
        if (spans.length == 0) return List.of();

        int chunkSize = config.chunkSize();
        int overlap = config.chunkOverlap();
        int advance = Math.max(1, chunkSize - overlap);
        boolean preserveUrls = Boolean.TRUE.equals(config.preserveUrls());

        List<Chunk> chunks = new ArrayList<>();
        int startToken = 0;
        while (startToken < spans.length) {
            int endTokenExclusive = Math.min(startToken + chunkSize, spans.length);
            int charStart = spans[startToken].getStart();
            int charEnd   = spans[endTokenExclusive - 1].getEnd();

            if (preserveUrls) {
                Span straddled = findStraddlingUrl(urlSpans, charEnd);
                if (straddled != null) {
                    charEnd = straddled.getEnd();
                    endTokenExclusive = advanceTokenCursor(spans, endTokenExclusive, charEnd);
                }
            }

            String chunkText = text.substring(charStart, charEnd);
            // Internal id; SPR assembler stamps the canonical §21.5 chunk_id later.
            chunks.add(new Chunk("tok-" + chunks.size(), chunkText, charStart, charEnd - 1));

            if (endTokenExclusive >= spans.length) break;
            int nextStart = startToken + advance;
            if (nextStart <= startToken) nextStart = startToken + 1;
            startToken = nextStart;
        }
        return chunks;
    }

    private static Span findStraddlingUrl(List<Span> urlSpans, int charEnd) {
        for (Span u : urlSpans) {
            if (u.getStart() < charEnd && charEnd < u.getEnd()) {
                return u;
            }
        }
        return null;
    }

    private static int advanceTokenCursor(Span[] spans, int cursor, int targetCharEnd) {
        while (cursor < spans.length && spans[cursor - 1].getEnd() < targetCharEnd) {
            cursor++;
        }
        return cursor;
    }
}
