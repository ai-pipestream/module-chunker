package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.data.v1.CentroidMetadata;
import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.VectorSetDirectives;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * R1-pack-3 end-to-end integration test: the real chunker's output flows into
 * the {@code pipestream-wiremock-server} testcontainer's {@code EmbedderStepMock}
 * via gRPC, and the wiremock's canned stage-2 response satisfies the post-embedder
 * invariants.
 *
 * <p>This test proves four things:
 * <ol>
 *   <li>The real {@code ChunkerGrpcImpl.processData} produces a valid stage-1
 *       {@link PipeDoc} from a directive-driven request (structural check via
 *       {@link #assertPostChunkerInvariants(PipeDoc)}).</li>
 *   <li>The {@code pipestream-wiremock-server:0.1.55} docker image is pullable
 *       and launches a gRPC server we can talk to.</li>
 *   <li>Header-scoped gRPC routing on {@code x-module-name} works end-to-end:
 *       the same {@code PipeStepProcessorService.ProcessData} method on the
 *       wiremock responds with an {@code EmbedderStepMock} result when called
 *       with {@code x-module-name: embedder}.</li>
 *   <li>The wiremock's canned stage-2 {@link PipeDoc} satisfies the post-embedder
 *       stage invariants (structural check via {@link #assertPostEmbedderInvariants(PipeDoc)}).</li>
 * </ol>
 *
 * <p>This test does NOT prove that a real embedder would produce that stage-2 —
 * only a real embedder module can do that (R2's own tests). It's a contract-lock
 * smoke test: "the chunker's output can be fed into a downstream embedder-shaped
 * gRPC endpoint and round-trip cleanly."
 *
 * <p><b>Inline invariants:</b> both {@code assertPostChunkerInvariants} and
 * {@code assertPostEmbedderInvariants} are private methods in this test class.
 * Not shared via a library — see {@code ChunkerStepInvariantsTest}'s class
 * Javadoc for the full rationale (bundling shared proto-aware assertion code
 * into a Quarkus extension jar creates classloader hazards for every consumer
 * that also generates its own proto classes).
 *
 * <p><b>Testcontainer lifecycle:</b> via {@code @QuarkusTestResource} on the
 * {@code ai.pipestream.test.support.WireMockTestResource} from
 * {@code pipestream-test-support}. That resource pulls {@code TestContainerConstants.WireMock.DEFAULT_IMAGE}
 * which currently resolves to {@code docker.io/pipestreamai/pipestream-wiremock-server:0.1.55}.
 *
 * <p>Every assertion uses AssertJ with descriptive {@code .as()} messages per
 * {@code feedback-assertj-preference.md}.
 */
@QuarkusTest
@QuarkusTestResource(value = ChunkerWireMockTestResource.class, restrictToAnnotatedClass = true)
class ChunkerEmbedderIntegrationTest {

    /**
     * The real chunker bean, in-process via CDI injection. Invokes the chunker's
     * {@code processData} directly — no gRPC round-trip to a separate JVM.
     */
    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    /**
     * Host of the wiremock testcontainer, published by
     * {@link ChunkerWireMockTestResource} on the {@code pipestream.test.wiremock.*}
     * key namespace (matching the existing {@code WireMockContainerTestResource}
     * convention used in {@code platform-registration-service}).
     *
     * <p>{@code defaultValue} is required so that other test classes in this
     * module — which do NOT activate {@link ChunkerWireMockTestResource} — can
     * still boot Quarkus without {@code @ConfigProperty} validation failing on
     * a missing required key. The default is unused at runtime: this test only
     * dereferences the field after the resource has published the real value.
     */
    @ConfigProperty(name = "pipestream.test.wiremock.host", defaultValue = "localhost")
    String wiremockHost;

    /**
     * Mapped HTTP port (host port for container's 8080) — the WireMock admin
     * HTTP server with the wiremock-grpc extension loaded. Unary gRPC calls
     * against {@code PipeStepProcessorService.ProcessData} land on stubs
     * registered here by the three header-scoped step mocks
     * ({@code ChunkerStepMock}, {@code EmbedderStepMock}, {@code SemanticGraphStepMock}).
     */
    @ConfigProperty(name = "pipestream.test.wiremock.http-port", defaultValue = "0")
    int wiremockGrpcPort;

    private static ManagedChannel wiremockChannel;

    @AfterAll
    static void tearDownAll() {
        if (wiremockChannel != null) {
            wiremockChannel.shutdownNow();
        }
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    void chunkerOutputFlowsIntoEmbedderStepMockViaWiremock() {
        // 1. Build a directive-driven stage-0 PipeDoc via the shared pack-2 helper.
        VectorSetDirectives directives = TestDirectiveBuilder.withSingleTokenDirective(
                "body", 500, 50);

        PipeDoc inputDoc = PipeDoc.newBuilder()
                .setDocId("pack3-fixture-" + UUID.randomUUID())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Pack 3 integration test body. The real chunker processes this text, " +
                                "emits a stage-1 PipeDoc with directive-driven semantic_results, and " +
                                "the test hands that doc over to the wiremock embedder mock via gRPC. " +
                                "Enough text here to exercise NLP and produce multiple sentence spans.")
                        .setVectorSetDirectives(directives)
                        .build())
                .build();

        ProcessDataRequest chunkerRequest = ProcessDataRequest.newBuilder()
                .setDocument(inputDoc)
                .setConfig(ProcessConfiguration.newBuilder().build())
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("pack3-pipeline")
                        .setPipeStepName("chunker")
                        .setStreamId("pack3-stream-" + UUID.randomUUID())
                        .build())
                .build();

        // 2. Invoke the real chunker via CDI and assert its output is a valid stage-1 doc.
        ProcessDataResponse chunkerResponse = chunkerService.processData(chunkerRequest)
                .await().indefinitely();

        assertThat(chunkerResponse.getOutcome())
                .as("real chunker should return PROCESSING_OUTCOME_SUCCESS for a valid directive-driven doc")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        assertThat(chunkerResponse.hasOutputDoc())
                .as("real chunker response must include output_doc")
                .isTrue();

        PipeDoc stage1Doc = chunkerResponse.getOutputDoc();
        assertPostChunkerInvariants(stage1Doc);

        // 3. Hand the chunker's stage-1 output to the wiremock's EmbedderStepMock.
        PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub embedderStub =
                getWiremockStub("embedder");

        ProcessDataRequest embedderRequest = ProcessDataRequest.newBuilder()
                .setDocument(stage1Doc)
                .setConfig(ProcessConfiguration.newBuilder().build())
                .setMetadata(ServiceMetadata.newBuilder()
                        .setPipelineName("pack3-pipeline")
                        .setPipeStepName("embedder")
                        .setStreamId("pack3-stream-embedder")
                        .build())
                .build();

        ProcessDataResponse embedderResponse = assertThatGrpcSucceeds(
                () -> embedderStub.processData(embedderRequest),
                "wiremock EmbedderStepMock round-trip");

        // 4. Assert the wiremock's canned stage-2 response passes post-embedder invariants.
        assertThat(embedderResponse.getOutcome())
                .as("wiremock embedder step mock should return PROCESSING_OUTCOME_SUCCESS")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        assertThat(embedderResponse.hasOutputDoc())
                .as("wiremock embedder step mock response must include output_doc")
                .isTrue();

        PipeDoc stage2Doc = embedderResponse.getOutputDoc();
        assertPostEmbedderInvariants(stage2Doc);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub getWiremockStub(String moduleName) {
        if (wiremockChannel == null) {
            wiremockChannel = ManagedChannelBuilder.forAddress(wiremockHost, wiremockGrpcPort)
                    .usePlaintext()
                    .build();
        }
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-module-name", Metadata.ASCII_STRING_MARSHALLER), moduleName);
        return PipeStepProcessorServiceGrpc.newBlockingStub(wiremockChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    /** Runs the given gRPC call and returns the result; fails the test with context on any exception. */
    private static <T> T assertThatGrpcSucceeds(java.util.function.Supplier<T> call, String context) {
        try {
            return call.get();
        } catch (Exception e) {
            throw new AssertionError(context + " failed: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    // =========================================================================
    // Inline post-chunker invariants — mirrors DESIGN.md §5.1
    // Owned by this test class (not shared via a library). See
    // ChunkerStepInvariantsTest Javadoc for the classloader-hazard rationale.
    // =========================================================================

    private static void assertPostChunkerInvariants(PipeDoc doc) {
        assertThat(doc.hasSearchMetadata())
                .as("post-chunker: search_metadata must be set on the PipeDoc")
                .isTrue();

        SearchMetadata sm = doc.getSearchMetadata();
        List<SemanticProcessingResult> results = sm.getSemanticResultsList();

        for (int i = 0; i < results.size(); i++) {
            SemanticProcessingResult spr = results.get(i);
            String ctx = "post-chunker SPR[" + i + "] (source='" + spr.getSourceFieldName()
                    + "' config='" + spr.getChunkConfigId() + "')";

            assertThat(spr.getEmbeddingConfigId())
                    .as(ctx + ": embedding_config_id must be empty (stage-1 placeholder)")
                    .isEmpty();
            assertThat(spr.getSourceFieldName()).as(ctx + ": source_field_name non-empty").isNotEmpty();
            assertThat(spr.getChunkConfigId()).as(ctx + ": chunk_config_id non-empty").isNotEmpty();
            assertThat(spr.getChunksList()).as(ctx + ": chunks non-empty").isNotEmpty();
            assertThat(spr.getMetadataMap())
                    .as(ctx + ": metadata must contain 'directive_key' (§21.2)")
                    .containsKey("directive_key");

            for (int j = 0; j < spr.getChunksCount(); j++) {
                var chunk = spr.getChunks(j);
                String chunkCtx = ctx + " chunk[" + j + "] id='" + chunk.getChunkId() + "'";
                var ei = chunk.getEmbeddingInfo();

                assertThat(ei.getTextContent()).as(chunkCtx + ": text_content non-empty").isNotEmpty();
                assertThat(ei.getVectorList())
                        .as(chunkCtx + ": vector must be empty (not yet embedded)")
                        .isEmpty();
                assertThat(chunk.getChunkId()).as(chunkCtx + ": chunk_id non-empty").isNotEmpty();
                assertThat(chunk.hasChunkAnalytics())
                        .as(chunkCtx + ": chunk_analytics must be set (§4.1 always populated)")
                        .isTrue();

                int start = ei.hasOriginalCharStartOffset() ? ei.getOriginalCharStartOffset() : 0;
                int end = ei.hasOriginalCharEndOffset() ? ei.getOriginalCharEndOffset() : 0;
                assertThat(start).as(chunkCtx + ": start_offset >= 0").isGreaterThanOrEqualTo(0);
                assertThat(end).as(chunkCtx + ": end_offset >= start_offset").isGreaterThanOrEqualTo(start);
            }
        }

        // §5.1: nlp_analysis preservation per unique source_field_name
        Set<String> sourceFields = results.stream()
                .map(SemanticProcessingResult::getSourceFieldName)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        for (String sf : sourceFields) {
            boolean hasNlp = results.stream()
                    .filter(r -> sf.equals(r.getSourceFieldName()))
                    .anyMatch(SemanticProcessingResult::hasNlpAnalysis);
            assertThat(hasNlp)
                    .as("post-chunker: at least one SPR for source_field_name='" + sf + "' must have nlp_analysis (§5.1)")
                    .isTrue();
        }

        // §5.1: source_field_analytics pair coverage
        Set<String> pairsInResults = results.stream()
                .map(r -> r.getSourceFieldName() + "|" + r.getChunkConfigId())
                .collect(Collectors.toSet());
        Set<String> pairsInAnalytics = sm.getSourceFieldAnalyticsList().stream()
                .map(a -> a.getSourceField() + "|" + a.getChunkConfigId())
                .collect(Collectors.toSet());
        for (String pair : pairsInResults) {
            assertThat(pairsInAnalytics)
                    .as("post-chunker: source_field_analytics[] must contain " + pair + " (§5.1)")
                    .contains(pair);
        }

        assertLexSorted(results, "post-chunker");
    }

    // =========================================================================
    // Inline post-embedder invariants — mirrors DESIGN.md §5.2
    // =========================================================================

    private static void assertPostEmbedderInvariants(PipeDoc doc) {
        assertThat(doc.hasSearchMetadata())
                .as("post-embedder: search_metadata must be set")
                .isTrue();

        SearchMetadata sm = doc.getSearchMetadata();
        List<SemanticProcessingResult> results = sm.getSemanticResultsList();

        assertThat(results)
                .as("post-embedder: semantic_results must be non-empty")
                .isNotEmpty();

        for (int i = 0; i < results.size(); i++) {
            SemanticProcessingResult spr = results.get(i);
            String ctx = "post-embedder SPR[" + i + "] (source='" + spr.getSourceFieldName()
                    + "' config='" + spr.getChunkConfigId()
                    + "' embedder='" + spr.getEmbeddingConfigId() + "')";

            // §5.2: zero placeholders remain at stage 2
            assertThat(spr.getEmbeddingConfigId())
                    .as(ctx + ": embedding_config_id must be non-empty (stage-2 fully embedded, §5.2)")
                    .isNotEmpty();
            assertThat(spr.getSourceFieldName()).as(ctx + ": source_field_name non-empty").isNotEmpty();
            assertThat(spr.getChunkConfigId()).as(ctx + ": chunk_config_id non-empty").isNotEmpty();
            assertThat(spr.getChunksList()).as(ctx + ": chunks non-empty").isNotEmpty();
            assertThat(spr.getMetadataMap())
                    .as(ctx + ": metadata must contain 'directive_key' (preserved from stage 1, §21.2)")
                    .containsKey("directive_key");

            for (int j = 0; j < spr.getChunksCount(); j++) {
                var chunk = spr.getChunks(j);
                String chunkCtx = ctx + " chunk[" + j + "] id='" + chunk.getChunkId() + "'";
                var ei = chunk.getEmbeddingInfo();

                assertThat(ei.getTextContent())
                        .as(chunkCtx + ": text_content preserved from stage 1 — non-empty")
                        .isNotEmpty();
                assertThat(ei.getVectorList())
                        .as(chunkCtx + ": vector must be populated at stage 2 (§5.2)")
                        .isNotEmpty();
                assertThat(chunk.getChunkId()).as(chunkCtx + ": chunk_id non-empty").isNotEmpty();
            }
        }

        assertLexSorted(results, "post-embedder");
    }

    private static void assertLexSorted(List<SemanticProcessingResult> results, String stageLabel) {
        Comparator<SemanticProcessingResult> lexOrder = Comparator
                .comparing(SemanticProcessingResult::getSourceFieldName)
                .thenComparing(SemanticProcessingResult::getChunkConfigId)
                .thenComparing(SemanticProcessingResult::getEmbeddingConfigId)
                .thenComparing(SemanticProcessingResult::getResultId);

        for (int i = 1; i < results.size(); i++) {
            SemanticProcessingResult prev = results.get(i - 1);
            SemanticProcessingResult curr = results.get(i);
            assertThat(lexOrder.compare(prev, curr))
                    .as(stageLabel + ": semantic_results must be lex-sorted (§21.8). "
                            + "Element[" + (i - 1) + "] ('" + prev.getSourceFieldName() + "', '"
                            + prev.getChunkConfigId() + "') must sort <= element[" + i + "] ('"
                            + curr.getSourceFieldName() + "', '" + curr.getChunkConfigId() + "')")
                    .isLessThanOrEqualTo(0);
        }
    }
}
