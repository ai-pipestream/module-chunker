package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provider for OpenNLP Tokenizer model and instances.
 * <p>
 * TokenizerME is NOT thread-safe (mutable internal state: newTokens list).
 * The TokenizerModel IS thread-safe. Callers needing concurrency should
 * call {@link #getModel()} and create their own TokenizerME per thread.
 */
@ApplicationScoped
public class TokenizerProvider {

    private static final Logger LOG = Logger.getLogger(TokenizerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-tokens-1.3-2.5.4.bin";

    private volatile TokenizerModel tokenizerModel;

    /**
     * Produces a singleton Tokenizer for simple single-threaded use (e.g., REST endpoints).
     * Do NOT share across concurrent threads — use {@link #getModel()} instead.
     */
    @Produces
    @Singleton
    public Tokenizer createTokenizer() {
        TokenizerModel model = getModel();
        if (model != null) {
            return new TokenizerME(model);
        }
        LOG.warn("Using simple tokenizer fallback");
        return SimpleTokenizer.INSTANCE;
    }

    /**
     * Returns the thread-safe TokenizerModel, or null if unavailable.
     * Create a new {@code new TokenizerME(model)} per thread for concurrent use.
     */
    public TokenizerModel getModel() {
        if (tokenizerModel == null) {
            synchronized (this) {
                if (tokenizerModel == null) {
                    try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
                        if (modelIn == null) {
                            LOG.warn("Tokenizer model not found at " + MODEL_PATH);
                            return null;
                        }
                        tokenizerModel = new TokenizerModel(modelIn);
                        LOG.info("Loaded OpenNLP tokenizer model");
                    } catch (IOException e) {
                        LOG.error("Error loading tokenizer model", e);
                    }
                }
            }
        }
        return tokenizerModel;
    }
}
