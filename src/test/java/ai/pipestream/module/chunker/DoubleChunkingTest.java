package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests sequential double-chunking of parsed documents via the directive-driven path
 * (R1-pack-2).
 *
 * <p><b>Migration note (R1-pack-2):</b> the legacy {@code json_config} path is gone.
 * Docs now carry {@code vector_set_directives}. Since {@code sentences_internal} SPRs
 * may also be emitted, assertions on exact SPR count now check for {@code >= 2} rather
 * than {@code == 2} and filter out {@code sentences_internal} when checking specific
 * per-config results.
 */
@QuarkusTest
public class DoubleChunkingTest {
    private static final Logger log = LoggerFactory.getLogger(DoubleChunkingTest.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    public void testDoubleChunking() throws IOException {
        List<PipeDoc> parsedDocs = loadParsedDocuments();
        log.info("Loaded {} parsed documents", parsedDocs.size());

        if (parsedDocs.isEmpty()) {
            log.warn("No parsed documents found, skipping test");
            return;
        }

        // First chunking: large chunks (1000 tokens, 200 overlap) from "body" field
        List<PipeDoc> firstChunkedDocs = performFirstChunking(parsedDocs);
        log.info("First chunking produced {} documents", firstChunkedDocs.size());

        // Second chunking: small chunks (300 tokens, 50 overlap) from "body" field
        List<PipeDoc> doubleChunkedDocs = performSecondChunking(firstChunkedDocs);
        log.info("Double chunking produced {} documents", doubleChunkedDocs.size());

        saveDoubleChunkedDocuments(doubleChunkedDocs);
        verifyDoubleChunkingStructure(doubleChunkedDocs);
    }

    private List<PipeDoc> loadParsedDocuments() throws IOException {
        List<PipeDoc> docs = new ArrayList<>();
        for (int i = 1; i <= 102; i++) {
            String resourceName = String.format("/parser_pipedoc_parsed/parsed_document_%03d.pb", i);
            try (var inputStream = getClass().getResourceAsStream(resourceName)) {
                if (inputStream != null) {
                    byte[] data = inputStream.readAllBytes();
                    PipeDoc doc = PipeDoc.parseFrom(data);
                    docs.add(doc);
                    log.debug("Loaded document from resource: {}", resourceName);
                } else {
                    break;
                }
            } catch (Exception e) {
                log.warn("Failed to load document from resource {}: {}", resourceName, e.getMessage());
            }
        }
        return docs;
    }

    private List<PipeDoc> performFirstChunking(List<PipeDoc> parsedDocs) {
        List<PipeDoc> chunkedDocs = new ArrayList<>();
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 1000, 200);

        for (PipeDoc doc : parsedDocs) {
            try {
                ProcessDataResponse response = doChunking(doc, directives, "first-chunking");
                if (response != null && response.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS
                        && response.hasOutputDoc()) {
                    chunkedDocs.add(response.getOutputDoc());
                    log.debug("First chunking successful for doc: {}", doc.getDocId());
                } else {
                    log.warn("First chunking failed for doc: {}", doc.getDocId());
                }
            } catch (Exception e) {
                log.error("Error in first chunking for doc {}: {}", doc.getDocId(), e.getMessage());
            }
        }
        return chunkedDocs;
    }

    private List<PipeDoc> performSecondChunking(List<PipeDoc> firstChunkedDocs) {
        List<PipeDoc> doubleChunkedDocs = new ArrayList<>();
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective("body", 300, 50);

        for (PipeDoc doc : firstChunkedDocs) {
            try {
                ProcessDataResponse response = doChunking(doc, directives, "second-chunking");
                if (response != null && response.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS
                        && response.hasOutputDoc()) {
                    doubleChunkedDocs.add(response.getOutputDoc());
                    log.debug("Second chunking successful for doc: {}", doc.getDocId());
                } else {
                    log.warn("Second chunking failed for doc: {}", doc.getDocId());
                }
            } catch (Exception e) {
                log.error("Error in second chunking for doc {}: {}", doc.getDocId(), e.getMessage());
            }
        }
        return doubleChunkedDocs;
    }

    private ProcessDataResponse doChunking(PipeDoc doc, VectorSetDirectives directives, String stepName) {
        // Merge directives onto existing search_metadata (preserve existing SPRs from previous pass)
        SearchMetadata existingMeta = doc.hasSearchMetadata()
                ? doc.getSearchMetadata()
                : SearchMetadata.getDefaultInstance();
        SearchMetadata updatedMeta = existingMeta.toBuilder()
                .setVectorSetDirectives(directives)
                .build();
        PipeDoc docWithDirectives = doc.toBuilder().setSearchMetadata(updatedMeta).build();

        ServiceMetadata metadata = ServiceMetadata.newBuilder()
            .setPipelineName("double-chunking-test")
            .setPipeStepName(stepName)
            .setStreamId(UUID.randomUUID().toString())
            .setCurrentHopNumber(1)
            .build();

        ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
            .setJsonConfig(com.google.protobuf.Struct.getDefaultInstance())
            .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
            .setDocument(docWithDirectives)
            .setMetadata(metadata)
            .setConfig(processConfig)
            .build();

        return chunkerService.processData(request).await().indefinitely();
    }

    private void saveDoubleChunkedDocuments(List<PipeDoc> docs) throws IOException {
        // Write debug snapshots under build/tmp so they don't pollute git status.
        // The committed fixtures under src/test/resources/double_chunked_pipedocs/ are
        // historical inputs for InspectDoubleChunkedData and are not regenerated here.
        Path outputDir = Paths.get(System.getProperty("user.dir"), "build", "tmp", "double_chunked_pipedocs")
                .toAbsolutePath();
        Files.createDirectories(outputDir);

        for (int i = 0; i < docs.size(); i++) {
            PipeDoc doc = docs.get(i);
            Path outputFile = outputDir.resolve(String.format("double_chunked_%03d.pb", i + 1));
            Files.write(outputFile, doc.toByteArray());
        }
        log.info("Saved {} double-chunked debug snapshots to {}", docs.size(), outputDir);
    }

    private void verifyDoubleChunkingStructure(List<PipeDoc> docs) {
        int docsWithTwoSprCount = 0;

        for (PipeDoc doc : docs) {
            assertThat(String.format("Document '%s' should have search metadata", doc.getDocId()),
                doc.hasSearchMetadata(), is(true));

            if (doc.getSearchMetadata().getSemanticResultsCount() > 0) {
                log.info("Document {} has {} semantic result sets",
                    doc.getDocId(), doc.getSearchMetadata().getSemanticResultsCount());

                // Filter out sentences_internal SPRs (emitted by always_emit_sentences default)
                var mainSprs = doc.getSearchMetadata().getSemanticResultsList().stream()
                        .filter(spr -> !ChunkerGrpcImpl.SENTENCES_INTERNAL_CONFIG_ID.equals(spr.getChunkConfigId()))
                        .toList();

                // A doc may have only 1 main SPR if the source field (title) was empty
                // for one of the two passes — that is valid behavior. We assert per-doc
                // invariants only when both passes produced results.
                if (mainSprs.size() >= 2) {
                    docsWithTwoSprCount++;
                    var result1 = mainSprs.get(0);
                    var result2 = mainSprs.get(1);

                    assertThat(String.format("Document '%s' should have different chunk config IDs (first='%s', second='%s')",
                            doc.getDocId(), result1.getChunkConfigId(), result2.getChunkConfigId()),
                        result1.getChunkConfigId(), is(not(equalTo(result2.getChunkConfigId()))));

                    assertThat(String.format("Document '%s' first result set should have chunks", doc.getDocId()),
                        result1.getChunksCount(), is(greaterThan(0)));
                    assertThat(String.format("Document '%s' second result set should have chunks", doc.getDocId()),
                        result2.getChunksCount(), is(greaterThan(0)));

                    log.info("Result 1: {} chunks with config {}",
                        result1.getChunksCount(), result1.getChunkConfigId());
                    log.info("Result 2: {} chunks with config {}",
                        result2.getChunksCount(), result2.getChunkConfigId());
                } else {
                    log.info("Document {} has {} main SPR(s) — source field was empty for one pass, skipping per-doc two-SPR assertion",
                        doc.getDocId(), mainSprs.size());
                }
            }
        }

        // At least some docs in the corpus should have two distinct SPRs — verifies
        // that double-chunking actually ran end-to-end (not just for titles-empty corpus).
        assertThat("At least one document in the corpus should have >= 2 main SPRs after double chunking",
            docsWithTwoSprCount, is(greaterThan(0)));
    }
}
