package ai.pipestream.module.chunker.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provider for OpenNLP SentenceDetector.
 * This class creates and provides a singleton instance of the OpenNLP SentenceDetector.
 */
@ApplicationScoped
public class SentenceDetectorProvider {

    private static final Logger LOG = Logger.getLogger(SentenceDetectorProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-sentence-1.3-2.5.4.bin";

    /**
     * Creates a simple fallback sentence detector when the model is not available.
     * 
     * @return A basic SentenceDetector implementation
     */
    private SentenceDetector createFallbackDetector() {
        return new SentenceDetector() {
            @Override
            public String[] sentDetect(CharSequence text) {
                return text.toString().split("(?<=\\.)\\s+");
            }

            @Override
            public Span[] sentPosDetect(CharSequence text) {
                String[] sentences = sentDetect(text);
                Span[] spans = new Span[sentences.length];
                int start = 0;
                for (int i = 0; i < sentences.length; i++) {
                    spans[i] = new Span(start, start + sentences[i].length());
                    start += sentences[i].length() + 1; // +1 for the space
                }
                return spans;
            }
        };
    }

    /**
     * Produces a singleton instance of the OpenNLP SentenceDetector.
     * 
     * @return A SentenceDetector instance
     */
    @Produces
    @Singleton
    public SentenceDetector createSentenceDetector() {
        try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
            if (modelIn == null) {
                LOG.warn("Sentence detector model not found at " + MODEL_PATH + ". Using simple sentence detector.");
                return createFallbackDetector();
            }

            SentenceModel model = new SentenceModel(modelIn);
            return new SentenceDetectorME(model);
        } catch (IOException e) {
            LOG.error("Error loading sentence detector model", e);
            return createFallbackDetector();
        }
    }
}