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
 * Provider for OpenNLP POS model and a shared {@link POSTaggerME}.
 * <p>
 * With OpenNLP 3.0.0-SNAPSHOT+, {@code POSTaggerME} is thread-safe; this module
 * exposes one tagger for the application (or null if the model is missing).
 */
@ApplicationScoped
public class POSTaggerProvider {

    private static final Logger LOG = Logger.getLogger(POSTaggerProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-pos-1.3-2.5.4.bin";

    private volatile POSModel posModel;

    /** Application-wide POS tagger, or null when the model is not on the classpath. */
    @Produces
    @Singleton
    public POSTagger createPOSTagger() {
        POSModel model = getModel();
        return model != null ? new POSTaggerME(model) : null;
    }

    /** Returns the loaded {@link POSModel}, or null if unavailable. */
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
