package ai.pipestream.module.chunker.chunk;

import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpResult;
import opennlp.tools.util.Span;

import java.util.List;

/**
 * Strategy for turning one piece of source text into a list of {@link Chunk}
 * records whose offsets always refer to the original text. Implementations are
 * stateless and thread-safe.
 */
public interface Chunker {
    List<Chunk> chunk(String text, NlpResult nlp, List<Span> urlSpans, ChunkerConfig config);
}
