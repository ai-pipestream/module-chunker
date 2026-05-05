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
 * Provider for OpenNLP tokenizer model and a shared {@link TokenizerME}.
 * <p>
 * With OpenNLP 3.0.0-SNAPSHOT+, {@code TokenizerME} is thread-safe; this module
 * exposes one instance for the whole application.
 */
@ApplicationScoped
public class TokenizerProvider {

    private static final Logger LOG = Logger.getLogger(TokenizerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-tokens-1.3-2.5.4.bin";

    private volatile TokenizerModel tokenizerModel;

    /** Application-wide tokenizer (UD model-backed {@link TokenizerME}, or {@link SimpleTokenizer}). */
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

    /** Returns the loaded {@link TokenizerModel}, or null if unavailable. */
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
