package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provider for OpenNLP language detection: loads the model once and exposes a shared
 * {@link LanguageDetectorME}. With OpenNLP 3.0.0-SNAPSHOT+, {@code LanguageDetectorME} is thread-safe.
 */
@ApplicationScoped
public class LanguageDetectorProvider {

    private static final Logger LOG = Logger.getLogger(LanguageDetectorProvider.class);
    private static final String MODEL_PATH = "langdetect-183.bin";

    private volatile LanguageDetector languageDetector;

    @Produces
    @Singleton
    public LanguageDetector createLanguageDetector() {
        if (languageDetector == null) {
            synchronized (this) {
                if (languageDetector == null) {
                    try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
                        if (modelIn == null) {
                            LOG.warn("Language detector model not found at " + MODEL_PATH + ". Language detection will be unavailable.");
                            return null;
                        }
                        LanguageDetectorModel model = new LanguageDetectorModel(modelIn);
                        languageDetector = new LanguageDetectorME(model);
                        LOG.info("Loaded OpenNLP language detector model");
                    } catch (IOException e) {
                        LOG.error("Error loading language detector model", e);
                    }
                }
            }
        }
        return languageDetector;
    }
}
