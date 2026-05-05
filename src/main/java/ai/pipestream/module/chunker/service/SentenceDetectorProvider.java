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
 * Provider for OpenNLP sentence model and a shared {@link SentenceDetectorME}.
 * <p>
 * With OpenNLP 3.0.0-SNAPSHOT+, {@code SentenceDetectorME} is thread-safe; this module
 * exposes one instance (or a single stateless fallback) for the application.
 */
@ApplicationScoped
public class SentenceDetectorProvider {

    private static final Logger LOG = Logger.getLogger(SentenceDetectorProvider.class);
    private static final String MODEL_PATH = "opennlp-en-ud-ewt-sentence-1.3-2.5.4.bin";

    private volatile SentenceModel sentenceModel;

    /**
     * Creates a simple fallback sentence detector when the model is not available.
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
                    start += sentences[i].length() + 1;
                }
                return spans;
            }
        };
    }

    /** Application-wide sentence detector (model-backed {@link SentenceDetectorME} or fallback). */
    @Produces
    @Singleton
    public SentenceDetector createSentenceDetector() {
        SentenceModel model = getModel();
        if (model != null) {
            return new SentenceDetectorME(model);
        }
        LOG.warn("Using simple sentence detector fallback");
        return createFallbackDetector();
    }

    /** Returns the loaded {@link SentenceModel}, or null if unavailable. */
    public SentenceModel getModel() {
        if (sentenceModel == null) {
            synchronized (this) {
                if (sentenceModel == null) {
                    try (InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(MODEL_PATH)) {
                        if (modelIn == null) {
                            LOG.warn("Sentence detector model not found at " + MODEL_PATH);
                            return null;
                        }
                        sentenceModel = new SentenceModel(modelIn);
                        LOG.info("Loaded OpenNLP sentence detector model");
                    } catch (IOException e) {
                        LOG.error("Error loading sentence detector model", e);
                    }
                }
            }
        }
        return sentenceModel;
    }
}
