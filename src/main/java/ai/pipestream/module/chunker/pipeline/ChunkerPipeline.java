package ai.pipestream.module.chunker.pipeline;

import ai.pipestream.data.v1.DocumentAnalytics;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.SourceFieldAnalytics;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.module.chunker.analytics.DocumentAnalyticsBuilder;
import ai.pipestream.module.chunker.chunk.ChunkerRouter;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.directive.DirectiveKeyComputer;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.nlp.NlpAnalyzer;
import ai.pipestream.module.chunker.nlp.NlpResult;
import ai.pipestream.module.chunker.text.TextCleaner;
import ai.pipestream.module.chunker.text.UrlFinder;
import ai.pipestream.module.chunker.service.UnicodeSanitizer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Synchronous, single-threaded orchestrator that runs the chunker stages for
 * one request. Callers invoke {@link #process(PipeDoc)} from inside a Mutiny
 * {@code Uni.createFrom().item(...).runSubscriptionOn(workerPool)} so the CPU
 * work happens off the gRPC event loop; the pipeline itself holds no mutable
 * state across calls.
 *
 * <p>Per-request invariants:
 * <ul>
 *   <li>Each unique source text is NLP-analyzed exactly once.</li>
 *   <li>Every chunk_id follows §21.5.</li>
 *   <li>{@code semantic_results[]} on the output doc is lex-sorted per §21.8.</li>
 *   <li>{@code source_field_analytics[]} is rebuilt from the merged SPR set.</li>
 *   <li>Every directive with non-empty source text emits a sentences_internal
 *       SPR (§21.9).</li>
 * </ul>
 */
@ApplicationScoped
public class ChunkerPipeline {

    private static final Logger LOG = Logger.getLogger(ChunkerPipeline.class);

    private final ConfigParser configParser;
    private final SourceTextResolver sourceTextResolver;
    private final NlpAnalyzer nlpAnalyzer;
    private final ChunkerRouter chunkerRouter;
    private final SprAssembler sprAssembler;
    private final DocumentAnalyticsBuilder documentAnalyticsBuilder;

    @Inject
    public ChunkerPipeline(ConfigParser configParser,
                           SourceTextResolver sourceTextResolver,
                           NlpAnalyzer nlpAnalyzer,
                           ChunkerRouter chunkerRouter,
                           SprAssembler sprAssembler,
                           DocumentAnalyticsBuilder documentAnalyticsBuilder) {
        this.configParser = configParser;
        this.sourceTextResolver = sourceTextResolver;
        this.nlpAnalyzer = nlpAnalyzer;
        this.chunkerRouter = chunkerRouter;
        this.sprAssembler = sprAssembler;
        this.documentAnalyticsBuilder = documentAnalyticsBuilder;
    }

    public record Outcome(PipeDoc outputDoc, int newSprCount, int totalSprCount) {}

    public Outcome process(PipeDoc inputDoc) {
        if (!inputDoc.hasSearchMetadata()
                || !inputDoc.getSearchMetadata().hasVectorSetDirectives()) {
            throw new IllegalArgumentException(
                    "Missing vector_set_directives on doc — chunker requires directives (DESIGN §21.1)");
        }
        List<VectorDirective> directives = inputDoc.getSearchMetadata()
                .getVectorSetDirectives()
                .getDirectivesList();
        if (directives.isEmpty()) {
            throw new IllegalArgumentException(
                    "Empty vector_set_directives — at least one directive required");
        }

        Set<String> seenLabels = new HashSet<>();
        Map<VectorDirective, String> directiveKeys = new LinkedHashMap<>();
        for (VectorDirective d : directives) {
            if (!seenLabels.add(d.getSourceLabel())) {
                throw new IllegalArgumentException(
                        "Duplicate source_label '" + d.getSourceLabel() + "' in vector_set_directives");
            }
            directiveKeys.put(d, DirectiveKeyComputer.compute(d));
        }

        String docHash = DirectiveKeyComputer.sha256b64url(inputDoc.getDocId());

        Map<String, NlpResult> nlpByText = new HashMap<>();
        Map<String, String> cleanedTextByLabel = new LinkedHashMap<>();
        Map<String, List<Span>> urlSpansByLabel = new LinkedHashMap<>();
        Map<String, DocumentAnalytics> docAnalyticsByLabel = new LinkedHashMap<>();

        List<SemanticProcessingResult> newSprs = new ArrayList<>();

        for (VectorDirective directive : directives) {
            String label = directive.getSourceLabel();
            if (directive.getChunkerConfigsList().isEmpty()) {
                throw new IllegalArgumentException(
                        "Directive source_label='" + label + "' has no chunker_configs — at least one is required");
            }
            String rawText = sourceTextResolver.resolve(inputDoc, directive);
            if (rawText == null || rawText.isEmpty()) {
                throw new IllegalArgumentException(
                        "Directive source_label='" + label + "' resolved to empty text — "
                                + "fix the source document, the chunker does not silently skip");
            }
            String sanitized = UnicodeSanitizer.sanitizeInvalidUnicode(rawText);

            // Parse every config up front so a malformed one fails the whole
            // request before any NLP / chunking work runs. The first pass
            // also tells us whether any config wants raw text (cleanText=false).
            List<ChunkerConfig> parsedConfigs = new ArrayList<>(directive.getChunkerConfigsCount());
            boolean cleanText = true;
            for (NamedChunkerConfig ncc : directive.getChunkerConfigsList()) {
                ChunkerConfig cfg = configParser.parseNamedConfig(ncc, label);
                parsedConfigs.add(cfg);
                if (!Boolean.TRUE.equals(cfg.cleanText())) {
                    cleanText = false;
                }
            }

            String sourceText = cleanText ? TextCleaner.clean(sanitized) : sanitized;
            cleanedTextByLabel.put(label, sourceText);

            NlpResult nlp = nlpByText.computeIfAbsent(sourceText, nlpAnalyzer::analyze);
            List<Span> urlSpans = UrlFinder.find(sourceText);
            urlSpansByLabel.put(label, urlSpans);

            docAnalyticsByLabel.computeIfAbsent(label,
                    k -> documentAnalyticsBuilder.buildDocumentAnalytics(sourceText, nlp));

            String directiveKey = directiveKeys.get(directive);

            for (int i = 0; i < parsedConfigs.size(); i++) {
                NamedChunkerConfig ncc = directive.getChunkerConfigs(i);
                ChunkerConfig cfg = parsedConfigs.get(i);
                List<Chunk> chunks = chunkerRouter.chunk(sourceText, nlp, urlSpans, cfg);
                SemanticProcessingResult spr = sprAssembler.assemble(
                        docHash, label, ncc.getConfigId(), directiveKey,
                        chunks, nlp, sourceText.length(), urlSpans);
                newSprs.add(spr);
            }

            // §21.9 — always emit sentences_internal per directive when the
            // source text produced at least one sentence.
            if (nlp.sentences().length > 0) {
                newSprs.add(sprAssembler.assembleSentencesInternal(
                        docHash, label, directiveKey, nlp, sourceText.length()));
            }
        }

        PipeDoc output = mergeIntoDoc(inputDoc, newSprs, docAnalyticsByLabel);
        int totalSprs = output.getSearchMetadata().getSemanticResultsCount();
        return new Outcome(output, newSprs.size(), totalSprs);
    }

    private PipeDoc mergeIntoDoc(PipeDoc inputDoc,
                                 List<SemanticProcessingResult> newSprs,
                                 Map<String, DocumentAnalytics> docAnalyticsByLabel) {
        SearchMetadata.Builder sm = inputDoc.getSearchMetadata().toBuilder();

        Set<String> newKeys = new HashSet<>();
        for (SemanticProcessingResult s : newSprs) {
            newKeys.add(s.getSourceFieldName() + "|" + s.getChunkConfigId());
        }
        List<SemanticProcessingResult> merged = new ArrayList<>();
        for (SemanticProcessingResult existing : inputDoc.getSearchMetadata().getSemanticResultsList()) {
            String key = existing.getSourceFieldName() + "|" + existing.getChunkConfigId();
            if (!newKeys.contains(key)) merged.add(existing);
        }
        merged.addAll(newSprs);

        sm.clearSourceFieldAnalytics();
        Set<String> sfaKeys = new HashSet<>();
        for (SemanticProcessingResult spr : merged) {
            String key = spr.getSourceFieldName() + "|" + spr.getChunkConfigId();
            if (!sfaKeys.add(key)) continue;
            int total = spr.getChunksCount();
            SourceFieldAnalytics.Builder sfa = SourceFieldAnalytics.newBuilder()
                    .setSourceField(spr.getSourceFieldName())
                    .setChunkConfigId(spr.getChunkConfigId())
                    .setTotalChunks(total);
            if (total > 0) {
                IntSummaryStatistics stats = spr.getChunksList().stream()
                        .mapToInt(c -> c.getEmbeddingInfo().getTextContent().length())
                        .summaryStatistics();
                sfa.setAverageChunkSize((float) stats.getAverage())
                   .setMinChunkSize(stats.getMin())
                   .setMaxChunkSize(stats.getMax());
            }
            DocumentAnalytics da = docAnalyticsByLabel.get(spr.getSourceFieldName());
            if (da != null) sfa.setDocumentAnalytics(da);
            sm.addSourceFieldAnalytics(sfa.build());
        }

        merged.sort(Comparator
                .comparing(SemanticProcessingResult::getSourceFieldName)
                .thenComparing(SemanticProcessingResult::getChunkConfigId)
                .thenComparing(SemanticProcessingResult::getEmbeddingConfigId)
                .thenComparing(SemanticProcessingResult::getResultId));
        sm.clearSemanticResults();
        sm.addAllSemanticResults(merged);

        return inputDoc.toBuilder().setSearchMetadata(sm.build()).build();
    }
}
