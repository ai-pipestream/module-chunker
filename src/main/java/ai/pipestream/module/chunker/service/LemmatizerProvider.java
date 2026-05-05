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
 * Provider for OpenNLP lemmatizer: loads the English model once and exposes a shared
 * {@link LemmatizerME}. With OpenNLP 3.0.0-SNAPSHOT+, {@code LemmatizerME} is thread-safe.
 */
@ApplicationScoped
public class LemmatizerProvider {

    private static final Logger LOG = Logger.getLogger(LemmatizerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-lemmas-1.3-2.5.4.bin";

    private volatile Lemmatizer lemmatizer;

    @Produces
    @Singleton
    public Lemmatizer createLemmatizer() {
        if (lemmatizer == null) {
            synchronized (this) {
                if (lemmatizer == null) {
                    try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
                        if (modelIn == null) {
                            LOG.warn("Lemmatizer model not found at " + MODEL_PATH + ". Lemmatization will be unavailable.");
                            return null;
                        }
                        LemmatizerModel model = new LemmatizerModel(modelIn);
                        lemmatizer = new LemmatizerME(model);
                        LOG.info("Loaded OpenNLP lemmatizer model");
                    } catch (IOException e) {
                        LOG.error("Error loading lemmatizer model", e);
                    }
                }
            }
        }
        return lemmatizer;
    }
}
