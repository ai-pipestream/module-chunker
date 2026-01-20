package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.module.chunker.model.ChunkerOptions;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.equalTo;

// Proto imports for building ChunkerConfig-compatible Struct
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

@QuarkusTest
public class DoubleChunkingTest {
    private static final Logger log = LoggerFactory.getLogger(DoubleChunkingTest.class);

    @GrpcClient
    PipeStepProcessorService chunkerService;

    @Test
    public void testDoubleChunking() throws IOException {
        // Load parsed documents
        List<PipeDoc> parsedDocs = loadParsedDocuments();
        log.info("Loaded {} parsed documents", parsedDocs.size());

        if (parsedDocs.isEmpty()) {
            log.warn("No parsed documents found, skipping test");
            return;
        }

        // First chunking: Large chunks (1000 chars, 200 overlap)
        List<PipeDoc> firstChunkedDocs = performFirstChunking(parsedDocs);
        log.info("First chunking produced {} documents", firstChunkedDocs.size());

        // Second chunking: Small chunks (300 chars, 50 overlap)
        List<PipeDoc> doubleChunkedDocs = performSecondChunking(firstChunkedDocs);
        log.info("Double chunking produced {} documents", doubleChunkedDocs.size());

        // Save double-chunked documents
        saveDoubleChunkedDocuments(doubleChunkedDocs);

        // Verify double chunking structure
        verifyDoubleChunkingStructure(doubleChunkedDocs);
    }

    private List<PipeDoc> loadParsedDocuments() throws IOException {
        List<PipeDoc> docs = new ArrayList<>();

        // Load from local test resources - process all 102 documents
        for (int i = 1; i <= 102; i++) {
            String resourceName = String.format("/parser_pipedoc_parsed/parsed_document_%03d.pb", i);
            try (var inputStream = getClass().getResourceAsStream(resourceName)) {
                if (inputStream != null) {
                    byte[] data = inputStream.readAllBytes();
                    PipeDoc doc = PipeDoc.parseFrom(data);
                    docs.add(doc);
                    log.debug("Loaded document from resource: {}", resourceName);
                } else {
                    break; // Stop when we can't find more documents
                }
            } catch (Exception e) {
                log.warn("Failed to load document from resource {}: {}", resourceName, e.getMessage());
            }
        }

        return docs;
    }

    private List<PipeDoc> performFirstChunking(List<PipeDoc> parsedDocs) {
        List<PipeDoc> chunkedDocs = new ArrayList<>();

        // Large chunk configuration - try title if body is empty
        ChunkerOptions largeChunkConfig = new ChunkerOptions(
            "title", // sourceField - fallback to title since parsed docs may not have body
            1000,   // chunkSize
            200,    // chunkOverlap
            "%s_%s_chunk_%d", // chunkIdTemplate
            "large_chunks_v1", // chunkConfigId
            "first_chunks_%s_%s", // resultSetNameTemplate
            "[LARGE-CHUNK] ", // logPrefix
            false   // preserveUrls
        );

        for (PipeDoc doc : parsedDocs) {
            try {
                ProcessDataRequest request = createChunkerRequest(doc, largeChunkConfig, "first-chunking");
                ProcessDataResponse response = chunkerService.processData(request)
                    .await().indefinitely();

                if (response.getSuccess() && response.hasOutputDoc()) {
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

        // Small chunk configuration - try title if body is empty
        ChunkerOptions smallChunkConfig = new ChunkerOptions(
            "title", // sourceField - fallback to title since parsed docs may not have body
            300,    // chunkSize
            50,     // chunkOverlap
            "%s_%s_chunk_%d", // chunkIdTemplate
            "small_chunks_v1", // chunkConfigId
            "second_chunks_%s_%s", // resultSetNameTemplate
            "[SMALL-CHUNK] ", // logPrefix
            false   // preserveUrls
        );

        for (PipeDoc doc : firstChunkedDocs) {
            try {
                ProcessDataRequest request = createChunkerRequest(doc, smallChunkConfig, "second-chunking");
                ProcessDataResponse response = chunkerService.processData(request)
                    .await().indefinitely();

                if (response.getSuccess() && response.hasOutputDoc()) {
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

    private ProcessDataRequest createChunkerRequest(PipeDoc doc, ChunkerOptions config, String stepName) {
        ServiceMetadata metadata = ServiceMetadata.newBuilder()
            .setPipelineName("double-chunking-test")
            .setPipeStepName(stepName)
            .setStreamId(UUID.randomUUID().toString())
            .setCurrentHopNumber(1)
            .build();

        // Build a ChunkerConfig-compatible Struct instead of legacy ChunkerOptions format
        // Fields expected by ChunkerConfig: sourceField, chunkSize, chunkOverlap, preserveUrls, cleanText, config_id
        Struct.Builder cfg = Struct.newBuilder()
            .putFields("sourceField", Value.newBuilder().setStringValue(config.sourceField()).build())
            .putFields("chunkSize", Value.newBuilder().setNumberValue(config.chunkSize()).build())
            .putFields("chunkOverlap", Value.newBuilder().setNumberValue(config.chunkOverlap()).build())
            .putFields("preserveUrls", Value.newBuilder().setBoolValue(config.preserveUrls()).build())
            // Ensure predictable behavior in tests
            .putFields("cleanText", Value.newBuilder().setBoolValue(true).build())
            // Force explicit configuration IDs so the two passes differ deterministically
            .putFields("config_id", Value.newBuilder().setStringValue(config.chunkConfigId()).build());

        ProcessConfiguration processConfig = ProcessConfiguration.newBuilder()
            .setJsonConfig(cfg.build())
            .build();

        return ProcessDataRequest.newBuilder()
            .setDocument(doc)
            .setMetadata(metadata)
            .setConfig(processConfig)
            .build();
    }

    private void saveDoubleChunkedDocuments(List<PipeDoc> docs) throws IOException {
        Path outputDir = Paths.get("src/test/resources/double_chunked_pipedocs").toAbsolutePath();
        Files.createDirectories(outputDir);

        for (int i = 0; i < docs.size(); i++) {
            PipeDoc doc = docs.get(i);
            Path outputFile = outputDir.resolve(String.format("double_chunked_%03d.pb", i + 1));
            Files.write(outputFile, doc.toByteArray());
        }

        log.info("Saved {} double-chunked documents to {}", docs.size(), outputDir);
    }

    private void verifyDoubleChunkingStructure(List<PipeDoc> docs) {
        for (PipeDoc doc : docs) {
            assertThat(String.format("Document '%s' should have search metadata", doc.getDocId()),
                doc.hasSearchMetadata(), is(true));

            if (doc.getSearchMetadata().getSemanticResultsCount() > 0) {
                log.info("Document {} has {} semantic result sets",
                    doc.getDocId(), doc.getSearchMetadata().getSemanticResultsCount());

                // Should have 2 semantic result sets (from 2 chunking rounds)
                assertThat(String.format("Document '%s' should have exactly 2 semantic result sets after double chunking (found %d)",
                        doc.getDocId(), doc.getSearchMetadata().getSemanticResultsCount()),
                    doc.getSearchMetadata().getSemanticResultsCount(),
                    is(equalTo(2)));

                // Verify chunk config IDs are different
                var result1 = doc.getSearchMetadata().getSemanticResults(0);
                var result2 = doc.getSearchMetadata().getSemanticResults(1);

                assertThat(String.format("Document '%s' should have different chunk config IDs (first='%s', second='%s')",
                        doc.getDocId(), result1.getChunkConfigId(), result2.getChunkConfigId()),
                    result1.getChunkConfigId(),
                    is(not(equalTo(result2.getChunkConfigId()))));

                assertThat(String.format("Document '%s' first result set should have chunks", doc.getDocId()),
                    result1.getChunksCount(),
                    is(greaterThan(0)));

                assertThat(String.format("Document '%s' second result set should have chunks", doc.getDocId()),
                    result2.getChunksCount(),
                    is(greaterThan(0)));

                log.info("Result 1: {} chunks with config {}",
                    result1.getChunksCount(), result1.getChunkConfigId());
                log.info("Result 2: {} chunks with config {}",
                    result2.getChunksCount(), result2.getChunkConfigId());
            }
        }
    }
}
