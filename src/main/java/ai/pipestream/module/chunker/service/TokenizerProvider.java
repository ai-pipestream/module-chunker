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
 * Provider for OpenNLP Tokenizer.
 * This class creates and provides a singleton instance of the OpenNLP Tokenizer.
 */
@ApplicationScoped
public class TokenizerProvider {

    private static final Logger LOG = Logger.getLogger(TokenizerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-tokens-1.3-2.5.4.bin";

    /**
     * Creates a simple fallback tokenizer when the model is not available.
     * 
     * @return A basic Tokenizer implementation
     */
    private Tokenizer createFallbackTokenizer() {
        LOG.warn("Using simple tokenizer fallback");
        return SimpleTokenizer.INSTANCE;
    }

    /**
     * Produces a singleton instance of the OpenNLP Tokenizer.
     * 
     * @return A Tokenizer instance
     */
    @Produces
    @Singleton
    public Tokenizer createTokenizer() {
        try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
            if (modelIn == null) {
                LOG.warn("Tokenizer model not found at " + MODEL_PATH + ". Using simple tokenizer.");
                return createFallbackTokenizer();
            }

            TokenizerModel model = new TokenizerModel(modelIn);
            return new TokenizerME(model);
        } catch (IOException e) {
            LOG.error("Error loading tokenizer model", e);
            return createFallbackTokenizer();
        }
    }
}