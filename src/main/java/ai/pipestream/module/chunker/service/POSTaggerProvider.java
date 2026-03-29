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
 * Provider for OpenNLP POS Tagger model and instances.
 * Loads the English POS model once from the Maven model artifact.
 * Exposes the POSModel (thread-safe) so callers can create per-thread POSTaggerME instances.
 */
@ApplicationScoped
public class POSTaggerProvider {

    private static final Logger LOG = Logger.getLogger(POSTaggerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-pos-1.3-2.5.4.bin";

    private volatile POSModel posModel;

    /**
     * Produces a singleton POSTagger for simple single-threaded use.
     */
    @Produces
    @Singleton
    public POSTagger createPOSTagger() {
        POSModel model = getModel();
        return model != null ? new POSTaggerME(model) : null;
    }

    /**
     * Returns the thread-safe POSModel, or null if unavailable.
     * Callers needing thread-safety should create their own POSTaggerME(model) per thread.
     */
    public POSModel getModel() {
        if (posModel == null) {
            synchronized (this) {
                if (posModel == null) {
                    try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
                        if (modelIn == null) {
                            LOG.warn("POS tagger model not found at " + MODEL_PATH);
                            return null;
                        }
                        posModel = new POSModel(modelIn);
                        LOG.info("Loaded POS tagger model");
                    } catch (IOException e) {
                        LOG.error("Error loading POS tagger model", e);
                    }
                }
            }
        }
        return posModel;
    }
}
