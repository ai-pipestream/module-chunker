package ai.pipestream.module.chunker;

import ai.pipestream.test.support.BaseWireMockTestResource;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

/**
 * Local {@link BaseWireMockTestResource} subclass for the R1-pack-3 integration
 * test that spins up {@code pipestream-wiremock-server} as a testcontainer.
 *
 * <p>Mirrors the existing {@code WireMockContainerTestResource} pattern from
 * {@code platform-registration-service}: a thin per-consumer subclass living in
 * the test sources of the consuming module that exposes both the WireMock HTTP
 * server (port 8080, hosting the wiremock-grpc extension and the header-scoped
 * step mocks) and the {@code DirectWireMockGrpcServer} (port 50052, used for
 * streaming RPCs). Tests inject the host/port pair via {@code @ConfigProperty}
 * on the {@code pipestream.test.wiremock.*} keys this class publishes.
 *
 * <p>Why a per-consumer subclass and not the generic
 * {@code ai.pipestream.test.support.WireMockTestResource}: the stock generic
 * resource exposes only port 50052 (the direct streaming server) and waits on
 * its log pattern. The chunker→embedder round-trip test calls
 * {@code PipeStepProcessorService.ProcessData} as a unary gRPC against the
 * header-scoped {@code EmbedderStepMock}, which is registered with the
 * wiremock-grpc extension on port 8080 — not the streaming server. Following
 * the {@code platform-registration-service} convention keeps the per-consumer
 * subclass under 30 lines.
 */
public class ChunkerWireMockTestResource extends BaseWireMockTestResource {

    @Override
    protected String readyLogPattern() {
        // Wait on the direct streaming server log line: it's the last server to
        // come up, so seeing it guarantees BOTH the WireMock HTTP server (port
        // 8080, where the step mock stubs live) AND the direct streaming server
        // (port 50052) are ready.
        return ".*Direct Streaming gRPC Server started.*";
    }

    @Override
    protected Map<String, String> buildConfig(GenericContainer<?> container) {
        String host = getHost();
        String httpPort = String.valueOf(getMappedPort(DEFAULT_HTTP_PORT));
        String directPort = String.valueOf(getMappedPort(DEFAULT_GRPC_PORT));

        return Map.of(
                "pipestream.test.wiremock.host", host,
                "pipestream.test.wiremock.http-port", httpPort,
                "pipestream.test.wiremock.direct-port", directPort
        );
    }
}
