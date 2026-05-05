package ai.pipestream.module.chunker;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/**
 * Test profile for chunker integration tests.
 * <p>
 * The JAR runs with prod profile by default. We must disable:
 * - Service registration (no Consul available in test)
 * - Separate gRPC server (share HTTP port so @TestHTTPResource gives the right port)
 * - Set HTTP port to 0 for random port assignment
 */
public class ChunkerIntegrationTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                // Disable Consul-based service registration
                "pipestream.registration.enabled", "false",
                // Share HTTP port for gRPC so ConfigProvider gives the correct port
                "quarkus.grpc.server.use-separate-server", "false",
                // Random port for the HTTP/gRPC server
                "quarkus.http.port", "0",
                "quarkus.http.test-port", "0"
        );
    }
}
