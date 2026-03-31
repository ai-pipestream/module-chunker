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
 * Provider for OpenNLP SentenceDetector model and instances.
 * <p>
 * SentenceDetectorME is NOT thread-safe (mutable internal state: sentProbs list).
 * The SentenceModel IS thread-safe. Callers needing concurrency should
 * call {@link #getModel()} and create their own SentenceDetectorME per thread.
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

    /**
     * Produces a singleton SentenceDetector for simple single-threaded use.
     * Do NOT share across concurrent threads — use {@link #getModel()} instead.
     */
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

    /**
     * Returns the thread-safe SentenceModel, or null if unavailable.
     * Create a new {@code new SentenceDetectorME(model)} per thread for concurrent use.
     */
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
