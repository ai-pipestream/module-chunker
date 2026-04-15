package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in fixture generator that runs the <b>real</b> chunker against the
 * committed {@code opinions_1000.jsonl} corpus and writes the resulting
 * Stage 1 {@link PipeDoc}s as length-delimited protobuf files on disk.
 * Downstream modules (notably {@code module-embedder}) consume these
 * dumps as golden fixtures so their load tests exercise real chunker
 * output rather than an in-memory fake.
 *
 * <h2>Why dump real chunker output</h2>
 *
 * <p>The embedder's load test needs representative Stage 1 PipeDocs:
 * realistic chunk count distributions, correct {@code directive_key}
 * metadata, {@code chunk_analytics} populated by the real analytics
 * pass, §21.5-canonical {@code chunk_id} / {@code result_id} formats,
 * and whatever else the chunker decides to stamp. A hand-rolled
 * in-memory chunker drifts from these invariants over time — as soon
 * as the chunker learns a new analytic or tweaks a chunk_id format, the
 * hand-rolled version lies about what production looks like and the
 * embedder tests pass against stale shapes.
 *
 * <p>Instead, this IT boots the built chunker JAR, fires real
 * {@code processData} RPCs at it, and serialises the response docs
 * straight to disk. Regenerate the fixtures any time the chunker's
 * output shape changes materially — the embedder test can then load
 * the updated {@code .pb} files without ever depending on chunker
 * internals.
 *
 * <h2>Opt-in only</h2>
 *
 * <p>Tagged {@code @Tag("fixture-dump")} and excluded from the default
 * {@code quarkusIntTest} run. Opt in via:
 *
 * <pre>{@code
 * ./gradlew quarkusIntTest -PrunFixtureDump -PfixtureOutDir=/tmp/chunker-fixtures
 * ./gradlew quarkusIntTest -PrunFixtureDump -PfixtureOutDir=/tmp/chunker-fixtures -PfixtureOpinions=1000
 * }</pre>
 *
 * <p>The {@code fixtureOpinions} project property (default 100, max
 * 1000 — the committed corpus size) controls how many docs are
 * written. {@code fixtureOutDir} is the directory the {@code .pb}
 * files land in; the test creates it if it doesn't exist and will
 * overwrite any existing files there.
 *
 * <h2>Output shape</h2>
 *
 * <p>Three files are written, one per chunker algorithm variant, each
 * containing {@code fixtureOpinions} PipeDocs in length-delimited
 * protobuf format (read back via {@code PipeDoc.parseDelimitedFrom(in)}
 * in a loop):
 *
 * <ol>
 *   <li>{@code stage1_sentence_N.pb} — sentence chunker (5 sentences
 *       per chunk, 1 sentence overlap). Low chunk count per doc,
 *       large-ish chunks.</li>
 *   <li>{@code stage1_token_N.pb} — token chunker (200 tokens per
 *       chunk, 20 token overlap). This is the shape
 *       {@code ChunkerCourtOpinionsPerfIT} uses; the most realistic
 *       setting for embedding-oriented chunking per DESIGN.md §5.1.</li>
 *   <li>{@code stage1_paragraph_N.pb} — character chunker (500 chars,
 *       50 char overlap). "Paragraph" here means "medium-sized char
 *       window" — the real chunker has no paragraph algorithm per se,
 *       but this variant produces the block-sized chunks that downstream
 *       modules need for coarse-granularity embeddings.</li>
 * </ol>
 *
 * <p>Every doc carries one {@link VectorDirective} with the config id
 * used above so the embedder test can match placeholders back to their
 * directive by {@code source_label} + {@code metadata["directive_key"]}.
 * Each doc's {@code search_metadata.body} is preserved from the source
 * opinion so the embedder can re-chunk if ever needed; the real
 * attraction is the already-populated {@code semantic_results[]} with
 * Stage 1 chunks in place.
 *
 * <h2>What this does NOT do</h2>
 *
 * <ul>
 *   <li>Assert anything about chunker performance — that's
 *       {@link ChunkerCourtOpinionsPerfIT}'s job.</li>
 *   <li>Commit the output files to version control automatically —
 *       you copy them into {@code module-embedder/src/integrationTest/
 *       resources/fixtures/court/} by hand after running this test.
 *       The module-chunker repo doesn't own the embedder's fixture
 *       directory; the generated files are a build artifact, not a
 *       versioned asset of this module.</li>
 * </ul>
 */
@QuarkusIntegrationTest
@TestProfile(ChunkerIntegrationTestProfile.class)
@Tag("fixture-dump")
class ChunkerFixtureDumpIT {

    private static final Logger LOG = Logger.getLogger(ChunkerFixtureDumpIT.class);

    /** Default number of opinions written per fixture if {@code fixture.opinions} is unset. */
    private static final int DEFAULT_FIXTURE_OPINIONS = 1000;

    /** Hard cap — committed corpus has exactly 1000 opinions. */
    private static final int MAX_FIXTURE_OPINIONS_FILE_CAP = 1000;

    /**
     * When {@code true} (default), the raw input body text is stripped
     * from each output PipeDoc before serialization. The embedder only
     * reads the Stage 1 SPR chunks (each chunk carries its own
     * {@code text_content}), so keeping the full raw body is pure
     * overhead — roughly doubles the compressed file size on the court
     * opinion corpus without helping any downstream consumer. Override
     * via {@code -PfixtureKeepBody=true} if a future consumer needs it.
     */
    private static final boolean STRIP_BODY_FROM_OUTPUT =
            !Boolean.parseBoolean(System.getProperty("fixture.keep-body", "false"));

    private ManagedChannel channel;
    private PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub unaryStub;

    @BeforeEach
    void setUp() {
        int port = ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
        LOG.infof("ChunkerFixtureDumpIT: connecting gRPC client to localhost:%d", port);

        channel = io.grpc.netty.NettyChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .flowControlWindow(100 * 1024 * 1024)
                .build();

        unaryStub = PipeStepProcessorServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.MINUTES);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // The test
    // =========================================================================

    @Test
    void dumpsThreeChunkerFixtureSetsFromRealCourtOpinions() throws Exception {
        String outDirProp = System.getProperty("fixture.dump.dir");
        assertThat(outDirProp)
                .as("-PfixtureOutDir=/some/path must be set so the dump has somewhere to land")
                .isNotBlank();
        Path outDir = Paths.get(outDirProp).toAbsolutePath();
        Files.createDirectories(outDir);

        int requested = Integer.parseInt(System.getProperty("fixture.opinions",
                String.valueOf(DEFAULT_FIXTURE_OPINIONS)));
        int count = Math.min(Math.max(1, requested), MAX_FIXTURE_OPINIONS_FILE_CAP);

        LOG.infof("Loading %d court opinions from test-data/opinions_1000.jsonl", count);
        List<CourtOpinion> opinions = loadOpinions(count);
        assertThat(opinions)
                .as("loader must return %d opinions (committed corpus has 1000)", count)
                .hasSizeGreaterThanOrEqualTo(count);

        // Three variant configs — see class javadoc for rationale on each.
        // Output is ONE gzipped length-delimited protobuf stream per variant.
        // Downstream consumers wrap the file in GZIPInputStream and call
        // PipeDoc.parseDelimitedFrom(in) in a loop until it returns null.
        FixtureVariant sentence = new FixtureVariant(
                "sentence_5_1",
                "sentence",
                5,
                1,
                outDir.resolve("stage1_sentence_" + count + ".pb.gz"));
        FixtureVariant token = new FixtureVariant(
                "token_200_20",
                "token",
                200,
                20,
                outDir.resolve("stage1_token_" + count + ".pb.gz"));
        FixtureVariant paragraph = new FixtureVariant(
                "paragraph_500_50",
                "character",
                500,
                50,
                outDir.resolve("stage1_paragraph_" + count + ".pb.gz"));

        int totalChunksWritten = 0;
        for (FixtureVariant variant : List.of(sentence, token, paragraph)) {
            LOG.infof("Generating fixture [%s] → %s (algorithm=%s chunkSize=%d overlap=%d)",
                    variant.configId, variant.outFile, variant.algorithm,
                    variant.chunkSize, variant.chunkOverlap);

            int variantChunks = 0;
            int variantDocs = 0;
            int variantSkipped = 0;

            try (OutputStream rawOut = new BufferedOutputStream(Files.newOutputStream(variant.outFile));
                 GZIPOutputStream gzOut = new GZIPOutputStream(rawOut, 64 * 1024)) {
                for (int i = 0; i < opinions.size(); i++) {
                    CourtOpinion op = opinions.get(i);
                    if (op.plainText == null || op.plainText.isBlank()) {
                        variantSkipped++;
                        continue;
                    }

                    ProcessDataResponse response = runChunker(op, variant, i);
                    if (response.getOutcome() != ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS) {
                        LOG.warnf("Chunker returned %s for opinion %d (%s) — skipping",
                                response.getOutcome(), i, op.caseName);
                        variantSkipped++;
                        continue;
                    }
                    if (!response.hasOutputDoc()) {
                        variantSkipped++;
                        continue;
                    }

                    PipeDoc outputDoc = response.getOutputDoc();
                    if (STRIP_BODY_FROM_OUTPUT) {
                        // Remove the raw input body from search_metadata; the
                        // Stage 1 chunks already carry text_content per chunk,
                        // which is the only text the embedder reads.
                        SearchMetadata originalSm = outputDoc.getSearchMetadata();
                        SearchMetadata slimSm = originalSm.toBuilder()
                                .clearBody()
                                .build();
                        outputDoc = outputDoc.toBuilder().setSearchMetadata(slimSm).build();
                    }

                    outputDoc.writeDelimitedTo(gzOut);
                    variantDocs++;
                    for (var spr : outputDoc.getSearchMetadata().getSemanticResultsList()) {
                        variantChunks += spr.getChunksCount();
                    }
                }
            }

            long fileSize = Files.size(variant.outFile);
            LOG.infof("  → wrote %d docs (%d skipped) / %d total chunks / %.1f KB to %s",
                    variantDocs, variantSkipped, variantChunks,
                    fileSize / 1024.0, variant.outFile.getFileName());
            totalChunksWritten += variantChunks;

            // Basic sanity: file must exist, be non-empty, and at least one doc
            // must have been written for this variant to be useful downstream.
            assertThat(Files.exists(variant.outFile))
                    .as("fixture file %s must exist", variant.outFile).isTrue();
            assertThat(fileSize)
                    .as("fixture file %s must be non-empty", variant.outFile)
                    .isGreaterThan(0L);
            assertThat(variantDocs)
                    .as("fixture file %s must have at least one PipeDoc", variant.outFile)
                    .isGreaterThan(0);
            assertThat(variantChunks)
                    .as("fixture variant %s must produce at least one chunk across all docs",
                            variant.configId)
                    .isGreaterThan(0);
        }

        LOG.infof("Fixture dump complete: %d total chunks across 3 variants in %s",
                totalChunksWritten, outDir);
    }

    // =========================================================================
    // Chunker invocation — mirrors ChunkerCourtOpinionsPerfIT.runUnary
    // =========================================================================

    private ProcessDataResponse runChunker(CourtOpinion opinion, FixtureVariant variant, int idx) {
        Struct configStruct = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue(variant.algorithm).build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(variant.chunkSize).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(variant.chunkOverlap).build())
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(NamedChunkerConfig.newBuilder()
                        .setConfigId(variant.configId)
                        .setConfig(configStruct)
                        .build())
                // Embedder config is required on the directive for the chunker
                // to stamp directive_key correctly — the embedder config id is
                // what the embedder-side test will reference. Use "minilm" so
                // downstream tests don't have to reconfigure.
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder()
                        .setConfigId("minilm")
                        .build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("opinion-" + idx + "-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(opinion.plainText)
                        .setTitle(opinion.caseName == null ? "" : opinion.caseName)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("fixture-dump")
                        .setPipeStepName("chunker-step")
                        .setStreamId("fixture-stream-" + variant.configId)
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        return unaryStub.processData(request);
    }

    // =========================================================================
    // Court opinions loader — duplicated from ChunkerCourtOpinionsPerfIT per
    // the project convention (repeat small test code rather than extract a
    // shared helper until the third consumer shows up).
    // =========================================================================

    private List<CourtOpinion> loadOpinions(int count) throws IOException {
        List<CourtOpinion> opinions = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("test-data/opinions_1000.jsonl")) {
            if (is == null) {
                throw new IllegalStateException(
                        "test-data/opinions_1000.jsonl not found on classpath");
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null && opinions.size() < count) {
                    CourtOpinion op = new CourtOpinion();
                    op.plainText = extractJsonField(line, "plain_text");
                    op.caseName = extractJsonField(line, "case_name");
                    opinions.add(op);
                }
            }
        }
        return opinions;
    }

    private static String extractJsonField(String json, String fieldName) {
        String quotedKey = "\"" + fieldName + "\"";
        int keyIdx = json.indexOf(quotedKey);
        if (keyIdx < 0) return null;
        int valueStart = keyIdx + quotedKey.length();
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') valueStart++;
        if (valueStart >= json.length() || json.charAt(valueStart) != ':') return null;
        valueStart++;
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') valueStart++;
        if (valueStart >= json.length() || json.charAt(valueStart) != '"') return null;
        valueStart++;
        StringBuilder sb = new StringBuilder();
        for (int i = valueStart; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\' && i + 1 < json.length()) {
                char next = json.charAt(i + 1);
                switch (next) {
                    case '"':  sb.append('"');  i++; break;
                    case '\\': sb.append('\\'); i++; break;
                    case 'n':  sb.append('\n'); i++; break;
                    case 'r':  sb.append('\r'); i++; break;
                    case 't':  sb.append('\t'); i++; break;
                    default:   sb.append(c); break;
                }
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** One chunker-algorithm variant — name, params, and output file path. */
    private record FixtureVariant(String configId, String algorithm,
                                  int chunkSize, int chunkOverlap, Path outFile) {}

    private static class CourtOpinion {
        String plainText;
        String caseName;
    }
}
