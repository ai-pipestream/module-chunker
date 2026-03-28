package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provider for OpenNLP Lemmatizer.
 * Loads the English lemmatizer model from the Maven model artifact on the classpath.
 * Returns null if the model is unavailable (graceful degradation).
 */
@ApplicationScoped
public class LemmatizerProvider {

    private static final Logger LOG = Logger.getLogger(LemmatizerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-lemmas-1.3-2.5.4.bin";

    /**
     * Produces a singleton instance of the OpenNLP Lemmatizer.
     *
     * @return A Lemmatizer instance, or null if the model is unavailable
     */
    @Produces
    @Singleton
    public Lemmatizer createLemmatizer() {
        try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
            if (modelIn == null) {
                LOG.warn("Lemmatizer model not found at " + MODEL_PATH + ". Lemmatization will be unavailable.");
                return null;
            }

            LemmatizerModel model = new LemmatizerModel(modelIn);
            return new LemmatizerME(model);
        } catch (IOException e) {
            LOG.error("Error loading lemmatizer model", e);
            return null;
        }
    }
}
