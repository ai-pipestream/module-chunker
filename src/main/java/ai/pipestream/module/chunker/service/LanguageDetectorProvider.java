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
 * Provider for OpenNLP Language Detector.
 * Loads the language detection model from the Maven model artifact on the classpath.
 * Returns null if the model is unavailable (graceful degradation).
 */
@ApplicationScoped
public class LanguageDetectorProvider {

    private static final Logger LOG = Logger.getLogger(LanguageDetectorProvider.class);
    private static final String MODEL_PATH = "langdetect-183.bin";

    /**
     * Produces a singleton instance of the OpenNLP Language Detector.
     *
     * @return A LanguageDetector instance, or null if the model is unavailable
     */
    @Produces
    @Singleton
    public LanguageDetector createLanguageDetector() {
        try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
            if (modelIn == null) {
                LOG.warn("Language detector model not found at " + MODEL_PATH + ". Language detection will be unavailable.");
                return null;
            }

            LanguageDetectorModel model = new LanguageDetectorModel(modelIn);
            return new LanguageDetectorME(model);
        } catch (IOException e) {
            LOG.error("Error loading language detector model", e);
            return null;
        }
    }
}
