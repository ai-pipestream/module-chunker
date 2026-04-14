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
import ai.pipestream.semantic.v1.ChunkAlgorithm;
import ai.pipestream.semantic.v1.ChunkConfigEntry;
import ai.pipestream.semantic.v1.ChunkerConfig;
import ai.pipestream.semantic.v1.SemanticChunkerServiceGrpc;
import ai.pipestream.semantic.v1.StreamChunksRequest;
import ai.pipestream.semantic.v1.StreamChunksResponse;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in performance integration test that chunks real court opinions through
 * BOTH the streaming path ({@code SemanticChunkerService.streamChunks}) and the
 * unary directive-driven path ({@code PipeStepProcessorService.processData}),
 * measures per-doc wall-clock latency, computes p50/p95/p99/max, and appends a
 * single CSV row per run to {@code build/chunker-perf.csv} for manual trend
 * analysis.
 *
 * <h2>Why this test exists</h2>
 *
 * <p>The chunker's post-R1 correctness audit identified several quick-win
 * perf opportunities (fused char-stream passes, eliminating per-chunk OpenNLP
 * re-runs, single-pass URL placeholder restore). Before shipping those wins
 * we need a realistic baseline — this IT establishes it against a 22 MB
 * committed fixture of real court opinions at
 * {@code src/integrationTest/resources/test-data/opinions_1000.jsonl}.
 *
 * <p>The test runs both chunker entry points side by side because the
 * streaming path ({@link SemanticChunkerServiceGrpc}) and the unary
 * directive-driven path ({@link PipeStepProcessorServiceGrpc}) share most of
 * the chunking internals but differ in the request-shape wrapper, NLP handoff,
 * and response streaming mechanics. Measuring both catches regressions in
 * either path and makes it possible to benchmark the two approaches against
 * each other.
 *
 * <h2>Opt-in only</h2>
 *
 * <p>Tagged {@code @Tag("perf")}. The default
 * {@code ./gradlew quarkusIntTest} invocation excludes this tag (see
 * {@code build.gradle}'s {@code quarkusIntTest.useJUnitPlatform} config) so
 * CI does not pay the 30-60s runtime. Run explicitly via:
 *
 * <pre>{@code
 * ./gradlew quarkusIntTest -PrunPerf
 * ./gradlew quarkusIntTest -PrunPerf -PperfOpinions=500
 * }</pre>
 *
 * <p>The {@code perfOpinions} property (forwarded as the system property
 * {@code perf.opinions}) controls how many opinions are timed (default 100).
 * A warm-up of the first 5 opinions is always discarded regardless.
 *
 * <h2>CSV output format</h2>
 *
 * <p>Two rows appended per run (one for streaming, one for unary), schema:
 *
 * <pre>
 * timestamp_iso,git_sha,n_opinions,path,total_ms,mean_ms,p50_ms,p95_ms,p99_ms,max_ms,total_chunks
 * </pre>
 *
 * <p>The file is created with a header if missing. Intended use: open in a
 * spreadsheet / plot over time to spot regressions row-over-row. Tight
 * assertion thresholds are intentionally NOT used because shared-runner
 * latency is noisy; the assertions below are generous enough to only fire on
 * catastrophic regressions.
 */
@QuarkusIntegrationTest
@TestProfile(ChunkerIntegrationTestProfile.class)
@Tag("perf")
class ChunkerCourtOpinionsPerfIT {

    private static final Logger LOG = Logger.getLogger(ChunkerCourtOpinionsPerfIT.class);

    private static final int WARMUP_OPINIONS = 5;
    private static final int DEFAULT_MEASURED_OPINIONS = 100;

    // Generous per-opinion latency bounds. The primary value of this test is
    // the CSV trend log; the assertions only fire on catastrophic regressions.
    // Shared CI runners have too much latency noise for tight bounds.
    private static final long MAX_P95_MS_PER_OPINION = 10_000L;
    private static final long MAX_P99_MS_PER_OPINION = 30_000L;

    private ManagedChannel channel;
    private SemanticChunkerServiceGrpc.SemanticChunkerServiceBlockingStub streamingStub;
    private PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub unaryStub;

    @BeforeEach
    void setUp() {
        int port = ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
        LOG.infof("ChunkerCourtOpinionsPerfIT: connecting gRPC client to localhost:%d", port);

        channel = io.grpc.netty.NettyChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .flowControlWindow(100 * 1024 * 1024)
                .build();

        streamingStub = SemanticChunkerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.MINUTES);
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
    void courtOpinionsPerfBothPaths() throws Exception {
        int measuredN = Integer.parseInt(System.getProperty("perf.opinions",
                String.valueOf(DEFAULT_MEASURED_OPINIONS)));

        // Need warmup + measured, but cap at the total available (1000).
        int loadCount = Math.min(WARMUP_OPINIONS + measuredN, 1000);

        List<CourtOpinion> opinions = loadOpinions(loadCount);
        assertThat(opinions)
                .as("opinions_1000.jsonl must load at least %d opinions "
                        + "(warmup=%d + measured=%d)",
                        loadCount, WARMUP_OPINIONS, measuredN)
                .hasSizeGreaterThanOrEqualTo(loadCount);

        LOG.infof("Loaded %d opinions (warmup=%d, measured=%d)",
                opinions.size(), WARMUP_OPINIONS, measuredN);

        // Warm-up: run first N opinions through the streaming path so JIT /
        // NLP model loading settles before we start timing. Discard results.
        // OpenNLP is loaded at module boot but the first few calls still show
        // JIT-driven warmup effects.
        for (int i = 0; i < WARMUP_OPINIONS && i < opinions.size(); i++) {
            CourtOpinion op = opinions.get(i);
            if (op.plainText == null || op.plainText.isBlank()) continue;
            runStreaming(op, "warmup-" + i);
        }
        LOG.infof("Warm-up complete (%d opinions)", WARMUP_OPINIONS);

        // -----------------------------------------------------------------
        // Streaming path run
        // -----------------------------------------------------------------
        long[] streamingNanos = new long[measuredN];
        int streamingSamples = 0;
        long streamingChunks = 0;
        for (int i = 0; i < measuredN; i++) {
            int idx = WARMUP_OPINIONS + i;
            if (idx >= opinions.size()) break;
            CourtOpinion op = opinions.get(idx);
            if (op.plainText == null || op.plainText.isBlank()) continue;

            long start = System.nanoTime();
            int chunks = runStreaming(op, "streaming-" + i);
            long elapsed = System.nanoTime() - start;

            streamingNanos[streamingSamples++] = elapsed;
            streamingChunks += chunks;
        }
        PerfStats streamingStats = PerfStats.compute(streamingNanos, streamingSamples, streamingChunks);
        LOG.infof("Streaming: samples=%d total=%dms mean=%dms p50=%dms p95=%dms p99=%dms max=%dms chunks=%d",
                streamingStats.samples, streamingStats.totalMs, streamingStats.meanMs,
                streamingStats.p50Ms, streamingStats.p95Ms, streamingStats.p99Ms,
                streamingStats.maxMs, streamingStats.totalChunks);

        // -----------------------------------------------------------------
        // Unary directive-driven path run
        // -----------------------------------------------------------------
        long[] unaryNanos = new long[measuredN];
        int unarySamples = 0;
        long unaryChunks = 0;
        for (int i = 0; i < measuredN; i++) {
            int idx = WARMUP_OPINIONS + i;
            if (idx >= opinions.size()) break;
            CourtOpinion op = opinions.get(idx);
            if (op.plainText == null || op.plainText.isBlank()) continue;

            long start = System.nanoTime();
            int chunks = runUnary(op, "unary-" + i);
            long elapsed = System.nanoTime() - start;

            unaryNanos[unarySamples++] = elapsed;
            unaryChunks += chunks;
        }
        PerfStats unaryStats = PerfStats.compute(unaryNanos, unarySamples, unaryChunks);
        LOG.infof("Unary:     samples=%d total=%dms mean=%dms p50=%dms p95=%dms p99=%dms max=%dms chunks=%d",
                unaryStats.samples, unaryStats.totalMs, unaryStats.meanMs,
                unaryStats.p50Ms, unaryStats.p95Ms, unaryStats.p99Ms,
                unaryStats.maxMs, unaryStats.totalChunks);

        // -----------------------------------------------------------------
        // Append CSV rows for manual trend analysis
        // -----------------------------------------------------------------
        writeCsvRow("streaming", measuredN, streamingStats);
        writeCsvRow("unary", measuredN, unaryStats);

        // -----------------------------------------------------------------
        // Generous assertions — primary value is the CSV trend log
        // -----------------------------------------------------------------
        assertThat(streamingStats.samples)
                .as("streaming path must have measured at least 1 opinion "
                        + "(n=%d requested)", measuredN)
                .isGreaterThan(0);
        assertThat(unaryStats.samples)
                .as("unary path must have measured at least 1 opinion "
                        + "(n=%d requested)", measuredN)
                .isGreaterThan(0);

        assertThat(streamingStats.p95Ms)
                .as("streaming p95 latency must stay under %d ms per opinion "
                        + "(observed %d ms over %d samples). Generous bound — "
                        + "a failure here means a catastrophic regression, "
                        + "not a normal variance.",
                        MAX_P95_MS_PER_OPINION, streamingStats.p95Ms,
                        streamingStats.samples)
                .isLessThan(MAX_P95_MS_PER_OPINION);

        assertThat(streamingStats.p99Ms)
                .as("streaming p99 latency must stay under %d ms per opinion",
                        MAX_P99_MS_PER_OPINION)
                .isLessThan(MAX_P99_MS_PER_OPINION);

        assertThat(unaryStats.p95Ms)
                .as("unary p95 latency must stay under %d ms per opinion "
                        + "(observed %d ms over %d samples)",
                        MAX_P95_MS_PER_OPINION, unaryStats.p95Ms, unaryStats.samples)
                .isLessThan(MAX_P95_MS_PER_OPINION);

        assertThat(unaryStats.p99Ms)
                .as("unary p99 latency must stay under %d ms per opinion",
                        MAX_P99_MS_PER_OPINION)
                .isLessThan(MAX_P99_MS_PER_OPINION);

        assertThat(streamingStats.totalChunks)
                .as("streaming path must have produced chunks "
                        + "(0 chunks = chunker returned empty for every opinion)")
                .isGreaterThan(0);
        assertThat(unaryStats.totalChunks)
                .as("unary path must have produced chunks")
                .isGreaterThan(0);
    }

    // =========================================================================
    // Streaming path request/response
    // =========================================================================

    private int runStreaming(CourtOpinion opinion, String label) {
        ChunkerConfig sentenceConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_SENTENCE)
                .setChunkSize(5)
                .setChunkOverlap(1)
                .build();

        ChunkerConfig tokenConfig = ChunkerConfig.newBuilder()
                .setAlgorithm(ChunkAlgorithm.CHUNK_ALGORITHM_TOKEN)
                .setChunkSize(200)
                .setChunkOverlap(20)
                .build();

        StreamChunksRequest request = StreamChunksRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setDocId("perf-streaming-" + label + "-" + UUID.randomUUID())
                .setSourceFieldName("body")
                .setTextContent(opinion.plainText)
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("sentence_5_1")
                        .setConfig(sentenceConfig)
                        .build())
                .addChunkConfigs(ChunkConfigEntry.newBuilder()
                        .setChunkConfigId("token_200_20")
                        .setConfig(tokenConfig)
                        .build())
                .build();

        Iterator<StreamChunksResponse> iter = streamingStub.streamChunks(request);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        return count;
    }

    // =========================================================================
    // Unary directive-driven path request/response
    // =========================================================================

    private int runUnary(CourtOpinion opinion, String label) {
        // Build the same two-config directive as the streaming path so the
        // CSV rows are apples-to-apples.
        Struct sentenceStruct = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("sentence").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(5).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(1).build())
                .build();

        Struct tokenStruct = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(200).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(20).build())
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(NamedChunkerConfig.newBuilder()
                        .setConfigId("sentence_5_1")
                        .setConfig(sentenceStruct)
                        .build())
                .addChunkerConfigs(NamedChunkerConfig.newBuilder()
                        .setConfigId("token_200_20")
                        .setConfig(tokenStruct)
                        .build())
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder()
                        .setConfigId("minilm")
                        .build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(directive)
                .build();

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("perf-unary-" + label + "-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody(opinion.plainText)
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("perf-pipeline")
                        .setPipeStepName("chunker-step")
                        .setStreamId("perf-stream-" + label)
                        .setCurrentHopNumber(1)
                        .build())
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(Struct.getDefaultInstance())
                        .build())
                .build();

        ProcessDataResponse response = unaryStub.processData(request);

        assertThat(response.getOutcome())
                .as("unary perf run for %s must return SUCCESS", label)
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        // Count all chunks across all SPRs so the comparison with the
        // streaming count is apples-to-apples.
        int count = 0;
        for (var spr : response.getOutputDoc().getSearchMetadata().getSemanticResultsList()) {
            count += spr.getChunksCount();
        }
        return count;
    }

    // =========================================================================
    // CSV output
    // =========================================================================

    private static void writeCsvRow(String pathLabel, int measuredN, PerfStats stats) throws IOException {
        Path csvPath = Paths.get("build", "chunker-perf.csv");
        Files.createDirectories(csvPath.getParent());

        boolean writeHeader = !Files.exists(csvPath) || Files.size(csvPath) == 0;

        StringBuilder sb = new StringBuilder();
        if (writeHeader) {
            sb.append("timestamp_iso,git_sha,n_opinions,path,samples,total_ms,mean_ms,"
                    + "p50_ms,p95_ms,p99_ms,max_ms,total_chunks\n");
        }

        String gitSha = System.getenv().getOrDefault("GIT_COMMIT", "local");

        sb.append(Instant.now().toString()).append(',')
                .append(gitSha).append(',')
                .append(measuredN).append(',')
                .append(pathLabel).append(',')
                .append(stats.samples).append(',')
                .append(stats.totalMs).append(',')
                .append(stats.meanMs).append(',')
                .append(stats.p50Ms).append(',')
                .append(stats.p95Ms).append(',')
                .append(stats.p99Ms).append(',')
                .append(stats.maxMs).append(',')
                .append(stats.totalChunks).append('\n');

        Files.writeString(csvPath, sb.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        LOG.infof("Appended CSV row to %s: path=%s samples=%d p95=%dms",
                csvPath.toAbsolutePath(), pathLabel, stats.samples, stats.p95Ms);
    }

    // =========================================================================
    // Stats computation
    // =========================================================================

    private static final class PerfStats {
        final int samples;
        final long totalChunks;
        final long totalMs;
        final long meanMs;
        final long p50Ms;
        final long p95Ms;
        final long p99Ms;
        final long maxMs;

        private PerfStats(int samples, long totalChunks, long totalMs, long meanMs,
                          long p50Ms, long p95Ms, long p99Ms, long maxMs) {
            this.samples = samples;
            this.totalChunks = totalChunks;
            this.totalMs = totalMs;
            this.meanMs = meanMs;
            this.p50Ms = p50Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.maxMs = maxMs;
        }

        static PerfStats compute(long[] nanos, int samples, long totalChunks) {
            if (samples == 0) {
                return new PerfStats(0, totalChunks, 0, 0, 0, 0, 0, 0);
            }
            // Sort only the prefix actually populated — the backing array may
            // be oversized if some opinions were skipped.
            long[] sorted = Arrays.copyOf(nanos, samples);
            Arrays.sort(sorted);

            long totalNs = 0;
            for (int i = 0; i < samples; i++) {
                totalNs += sorted[i];
            }

            long totalMs = TimeUnit.NANOSECONDS.toMillis(totalNs);
            long meanMs = totalMs / samples;
            long p50Ms = TimeUnit.NANOSECONDS.toMillis(percentile(sorted, 50));
            long p95Ms = TimeUnit.NANOSECONDS.toMillis(percentile(sorted, 95));
            long p99Ms = TimeUnit.NANOSECONDS.toMillis(percentile(sorted, 99));
            long maxMs = TimeUnit.NANOSECONDS.toMillis(sorted[samples - 1]);
            return new PerfStats(samples, totalChunks, totalMs, meanMs,
                    p50Ms, p95Ms, p99Ms, maxMs);
        }

        /**
         * Nearest-rank percentile for the sorted nanos array. For a small
         * sample set this is simpler and more deterministic than linear
         * interpolation.
         */
        private static long percentile(long[] sorted, int pct) {
            if (sorted.length == 0) return 0;
            int rank = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
            if (rank < 0) rank = 0;
            if (rank >= sorted.length) rank = sorted.length - 1;
            return sorted[rank];
        }
    }

    // =========================================================================
    // Court opinions fixture loader — copy of ChunkerMultiConfigIT helpers
    //
    // Duplicated here (not extracted) per the project preference to repeat
    // small amounts of test code between classes rather than pulling a shared
    // helper out. If a third consumer ever appears, the threshold for
    // extraction is worth revisiting.
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

    private static class CourtOpinion {
        String plainText;
        String caseName;
    }
}
