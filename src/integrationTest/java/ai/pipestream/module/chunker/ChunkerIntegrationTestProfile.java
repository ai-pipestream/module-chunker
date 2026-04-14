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
                "quarkus.http.test-port", "0",
                // ChunkCacheService injects ReactiveRedisDataSource directly
                // (not Instance<>), so the Redis bean must be "active" or the
                // packaged JAR refuses to boot. quarkus.redis.devservices
                // doesn't help here because devservices is a build-time
                // feature and the IT runs a packaged prod JAR.
                //
                // Setting a bogus host keeps the bean active (ArC is happy)
                // and doesn't trigger eager connection — ChunkCacheService's
                // @PostConstruct only calls redis.value(byte[].class) which
                // returns a lazy command-API wrapper. At runtime the first
                // real GET/PUT fails with a connection error, which the
                // service catches and treats as a cache miss per §9.3, so
                // every chunk just computes through. Cache is effectively
                // off for this IT, which is the right behavior for perf
                // measurement anyway (we want to measure actual chunking
                // work, not Redis hits).
                "quarkus.redis.hosts", "redis://unused:6379"
        );
    }
}
