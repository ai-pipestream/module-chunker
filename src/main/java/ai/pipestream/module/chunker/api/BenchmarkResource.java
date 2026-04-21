package ai.pipestream.module.chunker.api;

import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.module.chunker.pipeline.ChunkerPipeline;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parallel direct-call benchmark for the chunker pipeline. Dispatches N calls
 * to {@link ChunkerPipeline#process(PipeDoc)} across a fixed thread pool that
 * defaults to the number of available CPUs, so every core can be driven hard
 * — same dispatch shape the engine uses when many gRPC callers hit the
 * module concurrently, just without the gRPC / engine / OpenSearch layers
 * in the measurement.
 *
 * <p>Document pool loaded once per request into memory:
 * <ul>
 *   <li>{@code opinions} — the 1000 real court opinions from
 *       {@code /work/sample-documents/.../opinions_1000.jsonl}, same corpus
 *       the JDBC crawl uses.</li>
 *   <li>{@code samples} — the 6 demo-documents bundled in the module.</li>
 *   <li>{@code both} — union of the two pools.</li>
 * </ul>
 *
 * <p>The benchmark performs no disk writes. Input corpora are read-once at the
 * start; the pipeline itself operates entirely on in-memory protobuf builders.
 */
@Path("/internal/benchmark")
public class BenchmarkResource {

    private static final String[] SAMPLE_RESOURCES = {
            "demo-documents/texts/constitution.txt",
            "demo-documents/texts/sample_article.txt",
            "demo-documents/texts/lorem_ipsum.txt",
            "demo-documents/texts/alice_in_wonderland.txt",
            "demo-documents/texts/pride_and_prejudice.txt",
            "demo-documents/texts/cath_and_brazz.txt"
    };

    private static final String OPINIONS_PATH =
            "/work/sample-documents/sample-documents/courtlistener-seed/src/main/resources/courtlistener/opinions_1000.jsonl";

    @Inject
    ChunkerPipeline pipeline;

    @Inject
    ObjectMapper objectMapper;

    @GET
    @Path("/pipeline")
    public Response runBenchmark(@QueryParam("n") Integer nParam,
                                 @QueryParam("warmup") Integer warmupParam,
                                 @QueryParam("concurrency") Integer concParam,
                                 @QueryParam("source") String sourceParam) {
        String source = sourceParam == null ? "opinions" : sourceParam.toLowerCase();
        int defaultN = "opinions".equals(source) ? 1000 : 100;
        int n = nParam != null && nParam > 0 ? nParam : defaultN;
        int warmup = warmupParam != null && warmupParam >= 0 ? warmupParam : 10;
        int concurrency = concParam != null && concParam > 0
                ? concParam
                : Runtime.getRuntime().availableProcessors();

        List<String> pool;
        try {
            pool = switch (source) {
                case "opinions" -> loadOpinions();
                case "samples"  -> loadSamples();
                case "both"     -> {
                    List<String> combined = new ArrayList<>();
                    combined.addAll(loadOpinions());
                    combined.addAll(loadSamples());
                    yield combined;
                }
                default -> throw new IllegalArgumentException("unknown source: " + source);
            };
        } catch (Exception e) {
            return Response.status(500)
                    .entity(Map.of("error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                                   "source", source))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        if (pool.isEmpty()) {
            return Response.status(500)
                    .entity(Map.of("error", "empty document pool", "source", source))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        long minBytes = Long.MAX_VALUE, maxBytes = 0, totalPoolBytes = 0;
        for (String body : pool) {
            int b = body.getBytes(StandardCharsets.UTF_8).length;
            totalPoolBytes += b;
            if (b < minBytes) minBytes = b;
            if (b > maxBytes) maxBytes = b;
        }

        // Warm-up on the request thread — primes JIT + ensures NLP models are hot.
        for (int i = 0; i < warmup; i++) {
            pipeline.process(doc("warmup-" + i, pool.get(i % pool.size())));
        }

        long[] perDocNs = new long[n];
        AtomicLong totalChunks = new AtomicLong();
        AtomicLong totalSprs = new AtomicLong();
        AtomicLong totalBodyBytes = new AtomicLong();

        ExecutorService pooled = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r, "chunker-bench");
            t.setDaemon(true);
            return t;
        });

        List<Callable<Void>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final int idx = i;
            tasks.add(() -> {
                String body = pool.get(idx % pool.size());
                totalBodyBytes.addAndGet(body.getBytes(StandardCharsets.UTF_8).length);
                PipeDoc d = doc("bench-" + idx, body);
                long t0 = System.nanoTime();
                ChunkerPipeline.Outcome out = pipeline.process(d);
                perDocNs[idx] = System.nanoTime() - t0;
                long localSprs = 0, localChunks = 0;
                for (var spr : out.outputDoc().getSearchMetadata().getSemanticResultsList()) {
                    localSprs++;
                    localChunks += spr.getChunksCount();
                }
                totalSprs.addAndGet(localSprs);
                totalChunks.addAndGet(localChunks);
                return null;
            });
        }

        long startAll = System.nanoTime();
        List<Future<Void>> futures;
        try {
            futures = pooled.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Response.status(500)
                    .entity(Map.of("error", "interrupted during invokeAll"))
                    .type(MediaType.APPLICATION_JSON).build();
        }
        long wallNs = System.nanoTime() - startAll;
        pooled.shutdown();
        try { pooled.awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}

        // Surface any task failure as a 500.
        for (Future<Void> f : futures) {
            try { f.get(); }
            catch (Exception e) {
                return Response.status(500)
                        .entity(Map.of("error", "task failure: " + e.getCause()))
                        .type(MediaType.APPLICATION_JSON).build();
            }
        }

        long[] sorted = perDocNs.clone();
        Arrays.sort(sorted);
        long sum = 0;
        for (long v : sorted) sum += v;
        long mean = sum / n;
        double wallSec = wallNs / 1e9;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", source);
        result.put("poolSize", pool.size());
        result.put("docs", n);
        result.put("warmupDocs", warmup);
        result.put("concurrency", concurrency);
        result.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        result.put("minBodyBytes", minBytes);
        result.put("maxBodyBytes", maxBytes);
        result.put("avgPoolBodyBytes", totalPoolBytes / pool.size());
        result.put("totalBodyBytesProcessed", totalBodyBytes.get());
        result.put("wallSeconds", round(wallSec, 4));
        result.put("docsPerSec", round(n / wallSec, 2));
        result.put("MBPerSec", round(totalBodyBytes.get() / wallSec / (1024.0 * 1024.0), 2));
        result.put("totalSprs", totalSprs.get());
        result.put("totalChunks", totalChunks.get());
        result.put("sprsPerDoc", round((double) totalSprs.get() / n, 2));
        result.put("chunksPerDoc", round((double) totalChunks.get() / n, 2));

        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("meanMs", round(mean / 1e6, 3));
        latency.put("p50Ms",  round(sorted[n / 2] / 1e6, 3));
        latency.put("p90Ms",  round(sorted[(int)(n * 0.90)] / 1e6, 3));
        latency.put("p95Ms",  round(sorted[(int)(n * 0.95)] / 1e6, 3));
        latency.put("p99Ms",  round(sorted[(int)(n * 0.99)] / 1e6, 3));
        latency.put("maxMs",  round(sorted[n - 1] / 1e6, 3));
        latency.put("minMs",  round(sorted[0] / 1e6, 3));
        result.put("perDocLatency", latency);

        return Response.ok(result).type(MediaType.APPLICATION_JSON).build();
    }

    private List<String> loadOpinions() throws IOException {
        List<String> out = new ArrayList<>(1000);
        for (String line : Files.readAllLines(Paths.get(OPINIONS_PATH), StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            JsonNode node = objectMapper.readTree(line);
            JsonNode pt = node.get("plain_text");
            if (pt != null && pt.isTextual() && !pt.asText().isBlank()) {
                out.add(pt.asText());
            }
        }
        return out;
    }

    private static List<String> loadSamples() throws IOException {
        List<String> out = new ArrayList<>(SAMPLE_RESOURCES.length);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (String path : SAMPLE_RESOURCES) {
            try (InputStream in = cl.getResourceAsStream(path)) {
                if (in == null) throw new IOException("missing resource: " + path);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                in.transferTo(bos);
                out.add(bos.toString(StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static PipeDoc doc(String id, String body) {
        Struct tokCfg = Struct.newBuilder()
                .putFields("algorithm",    Value.newBuilder().setStringValue("token").build())
                .putFields("chunkSize",    Value.newBuilder().setNumberValue(500).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .putFields("preserveUrls", Value.newBuilder().setBoolValue(true).build())
                .putFields("cleanText",    Value.newBuilder().setBoolValue(true).build())
                .build();

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("body")
                .addChunkerConfigs(NamedChunkerConfig.newBuilder()
                        .setConfigId("body-token-500")
                        .setConfig(tokCfg)
                        .build())
                .build();

        SearchMetadata sm = SearchMetadata.newBuilder()
                .setBody(body)
                .setVectorSetDirectives(VectorSetDirectives.newBuilder()
                        .addDirectives(directive)
                        .build())
                .build();

        return PipeDoc.newBuilder()
                .setDocId(id)
                .setSearchMetadata(sm)
                .build();
    }

    private static double round(double v, int places) {
        double scale = Math.pow(10, places);
        return Math.round(v * scale) / scale;
    }
}
