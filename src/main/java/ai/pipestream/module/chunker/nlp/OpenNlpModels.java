package ai.pipestream.module.chunker.nlp;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Loads the five OpenNLP models used by the chunker exactly once at startup.
 * Every model is mandatory. If any is missing the application fails to start —
 * no silent regex fallback, no degraded-mode, no "analytics will be unavailable".
 *
 * <p>OpenNLP {@code *Model} classes are immutable and thread-safe; the
 * corresponding {@code *ME} classes are not. Callers obtain ME instances via
 * {@link NlpAnalyzer} which constructs a fresh one per request.
 */
@Startup
@ApplicationScoped
public class OpenNlpModels {

    private static final Logger LOG = Logger.getLogger(OpenNlpModels.class);

    private static final String SENTENCE_MODEL   = "opennlp-en-ud-ewt-sentence-1.3-2.5.4.bin";
    private static final String TOKENIZER_MODEL  = "opennlp-en-ud-ewt-tokens-1.3-2.5.4.bin";
    private static final String POS_MODEL        = "opennlp-en-ud-ewt-pos-1.3-2.5.4.bin";
    private static final String LEMMA_MODEL      = "opennlp-en-ud-ewt-lemmas-1.3-2.5.4.bin";
    private static final String LANGDETECT_MODEL = "langdetect-183.bin";

    private SentenceModel sentenceModel;
    private TokenizerModel tokenizerModel;
    private POSModel posModel;
    private LemmatizerModel lemmatizerModel;
    private LanguageDetectorModel languageDetectorModel;

    @PostConstruct
    void load() {
        long t0 = System.currentTimeMillis();
        sentenceModel         = load(SENTENCE_MODEL,   SentenceModel::new);
        tokenizerModel        = load(TOKENIZER_MODEL,  TokenizerModel::new);
        posModel              = load(POS_MODEL,        POSModel::new);
        lemmatizerModel       = load(LEMMA_MODEL,      LemmatizerModel::new);
        languageDetectorModel = load(LANGDETECT_MODEL, LanguageDetectorModel::new);
        LOG.infof("OpenNlpModels: loaded 5 models in %d ms", System.currentTimeMillis() - t0);
    }

    public SentenceModel sentenceModel()                { return sentenceModel; }
    public TokenizerModel tokenizerModel()              { return tokenizerModel; }
    public POSModel posModel()                          { return posModel; }
    public LemmatizerModel lemmatizerModel()            { return lemmatizerModel; }
    public LanguageDetectorModel languageDetectorModel(){ return languageDetectorModel; }

    @FunctionalInterface
    private interface ModelLoader<M> {
        M load(InputStream in) throws IOException;
    }

    private static <M> M load(String resource, ModelLoader<M> loader) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream in = cl.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException(
                        "Required OpenNLP model '" + resource + "' not found on classpath. "
                                + "The chunker cannot start without it — there is no silent fallback.");
            }
            return loader.load(in);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load OpenNLP model '" + resource + "': " + e.getMessage(), e);
        }
    }
}
