package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provider for OpenNLP POS Tagger.
 * Loads the English POS model from the Maven model artifact on the classpath.
 * Returns null if the model is unavailable (graceful degradation).
 */
@ApplicationScoped
public class POSTaggerProvider {

    private static final Logger LOG = Logger.getLogger(POSTaggerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-pos-1.3-2.5.4.bin";

    /**
     * Produces a singleton instance of the OpenNLP POS Tagger.
     *
     * @return A POSTagger instance, or null if the model is unavailable
     */
    @Produces
    @Singleton
    public POSTagger createPOSTagger() {
        try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
            if (modelIn == null) {
                LOG.warn("POS tagger model not found at " + MODEL_PATH + ". POS tagging will be unavailable.");
                return null;
            }

            POSModel model = new POSModel(modelIn);
            return new POSTaggerME(model);
        } catch (IOException e) {
            LOG.error("Error loading POS tagger model", e);
            return null;
        }
    }
}
