package ai.pipestream.module.chunker.chunk;

import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.nlp.NlpResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import opennlp.tools.util.Span;

import java.util.List;

/**
 * Dispatches to the strategy chosen by {@link ChunkingAlgorithm}. SEMANTIC is
 * reserved in the proto but not implemented here — callers receive an
 * {@link UnsupportedOperationException}, which the gRPC layer turns into a
 * {@code FAILED_PRECONDITION} response.
 */
@ApplicationScoped
public class ChunkerRouter {

    private final TokenChunker tokenChunker;
    private final SentenceChunker sentenceChunker;
    private final CharacterChunker characterChunker;

    @Inject
    public ChunkerRouter(TokenChunker tokenChunker,
                         SentenceChunker sentenceChunker,
                         CharacterChunker characterChunker) {
        this.tokenChunker = tokenChunker;
        this.sentenceChunker = sentenceChunker;
        this.characterChunker = characterChunker;
    }

    public List<Chunk> chunk(String text, NlpResult nlp, List<Span> urlSpans, ChunkerConfig config) {
        ChunkingAlgorithm alg = config.algorithm();
        return switch (alg) {
            case TOKEN     -> tokenChunker.chunk(text, nlp, urlSpans, config);
            case SENTENCE  -> sentenceChunker.chunk(text, nlp, urlSpans, config);
            case CHARACTER -> characterChunker.chunk(text, nlp, urlSpans, config);
            case SEMANTIC  -> throw new UnsupportedOperationException(
                    "Semantic chunking is not implemented — use TOKEN or SENTENCE");
        };
    }
}
