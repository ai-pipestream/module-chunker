package ai.pipestream.module.chunker.cache;

import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.module.chunker.directive.DirectiveKeyComputer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ChunkCacheService} using Mockito to fake
 * {@link ReactiveRedisDataSource} and {@link ReactiveValueCommands}.
 *
 * <p>No Quarkus container is started. The service is constructed directly and its
 * injected fields are set via reflection — this is appropriate for testing a thin
 * adapter class whose logic is entirely in how it delegates to the Redis API.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>put + get round-trip (same chunks returned)</li>
 *   <li>get on unknown key returns empty list</li>
 *   <li>put with {@code isRtbfSuppressed=true} is a no-op (§21.7)</li>
 *   <li>10-chunk list round-trips with byte-identical content</li>
 *   <li>Redis error recovery on both GET and PUT paths (§9.3)</li>
 *   <li>Corrupt / malformed bytes in cache are caught and treated as miss (§9.4)</li>
 *   <li>Cache key format verification</li>
 * </ul>
 *
 * <p>A real {@link SimpleMeterRegistry} is wired for the {@code chunker.cache.errors}
 * counter so the service's {@code @PostConstruct} counter registration succeeds
 * without starting a full Quarkus container.
 */
@SuppressWarnings("unchecked")
class ChunkCacheServiceTest {

    private static final long TTL = 2_592_000L;
    private static final String CHUNKER_CONFIG_ID = "sentence_v1";

    private ReactiveRedisDataSource redisMock;
    private ReactiveValueCommands<String, byte[]> commandsMock;
    private MeterRegistry meterRegistry;
    private ChunkCacheService service;

    // In-memory store to simulate Redis SET/GET behaviour
    private final java.util.Map<String, byte[]> store = new java.util.HashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        store.clear();

        redisMock = Mockito.mock(ReactiveRedisDataSource.class);
        commandsMock = Mockito.mock(ReactiveValueCommands.class);

        // Wire redis.value(byte[].class) → commandsMock
        when(redisMock.value(byte[].class)).thenReturn(commandsMock);

        // setex stores bytes in the in-memory map
        when(commandsMock.setex(anyString(), anyLong(), any(byte[].class)))
                .thenAnswer(invocation -> {
                    String key = invocation.getArgument(0);
                    byte[] bytes = invocation.getArgument(2);
                    store.put(key, bytes);
                    return Uni.createFrom().voidItem();
                });

        // get reads from the in-memory map (returns null on miss)
        when(commandsMock.get(anyString()))
                .thenAnswer(invocation -> {
                    String key = invocation.getArgument(0);
                    byte[] bytes = store.get(key);
                    return Uni.createFrom().item(bytes);
                });

        // A real in-memory MeterRegistry for the chunker.cache.errors counter.
        meterRegistry = new SimpleMeterRegistry();

        // Construct and initialise the service without CDI
        service = new ChunkCacheService();
        injectField(service, "redis", redisMock);
        injectField(service, "meterRegistry", meterRegistry);
        service.init(); // @PostConstruct
    }

    // =========================================================================
    // put then get round-trips
    // =========================================================================

    @Test
    void putThenGet_returnsSameChunkList() {
        String text = "The quick brown fox jumps over the lazy dog.";
        List<SemanticChunk> original = buildChunks(3, "test-chunk");

        service.put(text, CHUNKER_CONFIG_ID, original, TTL, false).await().indefinitely();
        List<SemanticChunk> retrieved = service.get(text, CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(retrieved)
                .as("get() after put() must return the same chunk list (same size)")
                .hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(retrieved.get(i).getChunkId())
                    .as("chunk[%d] chunk_id must survive the Redis round-trip", i)
                    .isEqualTo(original.get(i).getChunkId());
        }
    }

    @Test
    void putThenGet_tenChunks_allRoundTrip_byteIdentical() {
        String text = "Some source text for a 10-chunk round-trip test.";
        List<SemanticChunk> original = buildChunks(10, "para-chunk");

        service.put(text, CHUNKER_CONFIG_ID, original, TTL, false).await().indefinitely();
        List<SemanticChunk> retrieved = service.get(text, CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(retrieved)
                .as("10-chunk list must round-trip with all 10 chunks preserved")
                .hasSize(10);
        for (int i = 0; i < 10; i++) {
            final int idx = i;
            assertThat(retrieved.get(idx).getChunkId())
                    .as("chunk[%d] chunk_id must be byte-identical after round-trip", idx)
                    .isEqualTo(original.get(idx).getChunkId());
            assertThat(retrieved.get(idx).getEmbeddingInfo().getTextContent())
                    .as("chunk[%d] text_content must be byte-identical after round-trip", idx)
                    .isEqualTo(original.get(idx).getEmbeddingInfo().getTextContent());
        }
    }

    // =========================================================================
    // Cache miss
    // =========================================================================

    @Test
    void get_unknownKey_returnsEmptyList() {
        String text = "text that was never put into cache";

        List<SemanticChunk> result = service.get(text, CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(result)
                .as("get() on a key that was never stored must return an empty list (cache miss)")
                .isEmpty();
    }

    // =========================================================================
    // RTBF suppression §21.7
    // =========================================================================

    @Test
    void put_withRtbfSuppressed_isNoOp_getReturnsEmpty() {
        String text = "RTBF-marked document text that must not be cached.";
        List<SemanticChunk> chunks = buildChunks(2, "rtbf-chunk");

        // Write with RTBF suppressed
        service.put(text, CHUNKER_CONFIG_ID, chunks, TTL, true).await().indefinitely();

        // Subsequent get must return empty (nothing was stored)
        List<SemanticChunk> result = service.get(text, CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(result)
                .as("get() after RTBF-suppressed put() must return empty list — RTBF writes are no-ops per §21.7")
                .isEmpty();
    }

    @Test
    void put_withRtbfSuppressed_neverCallsSetex() {
        String text = "RTBF text — setex must never be called.";
        List<SemanticChunk> chunks = buildChunks(1, "rtbf-chunk");

        service.put(text, CHUNKER_CONFIG_ID, chunks, TTL, true).await().indefinitely();

        verify(commandsMock, never())
                .setex(anyString(), anyLong(), any(byte[].class));
    }

    // =========================================================================
    // Key format verification
    // =========================================================================

    @Test
    void buildKey_hasCorrectFormat() {
        String text = "sample text";
        String expectedHash = DirectiveKeyComputer.sha256b64url(text);
        String expectedKey = "chunk:" + expectedHash + ":" + CHUNKER_CONFIG_ID;

        String key = ChunkCacheService.buildKey(text, CHUNKER_CONFIG_ID);

        assertThat(key)
                .as("cache key must follow format 'chunk:{sha256b64url(text)}:{chunker_config_id}'")
                .isEqualTo(expectedKey);
    }

    @Test
    void buildKey_startsWithChunkPrefix() {
        String key = ChunkCacheService.buildKey("any text", CHUNKER_CONFIG_ID);

        assertThat(key)
                .as("cache key must always start with 'chunk:' prefix")
                .startsWith("chunk:");
    }

    @Test
    void buildKey_differentTexts_differentKeys() {
        String key1 = ChunkCacheService.buildKey("text A", CHUNKER_CONFIG_ID);
        String key2 = ChunkCacheService.buildKey("text B", CHUNKER_CONFIG_ID);

        assertThat(key1)
                .as("different texts must produce different cache keys")
                .isNotEqualTo(key2);
    }

    @Test
    void buildKey_differentConfigs_differentKeys() {
        String key1 = ChunkCacheService.buildKey("same text", "config_a");
        String key2 = ChunkCacheService.buildKey("same text", "config_b");

        assertThat(key1)
                .as("same text with different chunker config IDs must produce different cache keys")
                .isNotEqualTo(key2);
    }

    // =========================================================================
    // Redis error recovery
    // =========================================================================

    @Test
    void get_redisError_returnsEmptyList_doesNotThrow() {
        when(commandsMock.get(anyString()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Redis connection refused")));

        List<SemanticChunk> result = service.get("some text", CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(result)
                .as("Redis GET error must be recovered as empty list (compute-through per §9.3) — never throw")
                .isEmpty();
    }

    @Test
    void put_redisError_completesSuccessfully_doesNotThrow() {
        when(commandsMock.setex(anyString(), anyLong(), any(byte[].class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Redis write error")));

        List<SemanticChunk> chunks = buildChunks(1, "error-chunk");

        // The assertion IS the "no throw" — Mutiny's .await().indefinitely() propagates
        // any unhandled failure, so reaching the end of the lambda without an exception
        // means the recovery path in put() swallowed it per §9.3.
        assertThatCode(() ->
                service.put("some text", CHUNKER_CONFIG_ID, chunks, TTL, false).await().indefinitely())
                .as("put() must complete without throwing even when Redis returns an error (§9.3 compute-through)")
                .doesNotThrowAnyException();
    }

    @Test
    void get_corruptCachedBytes_returnsEmptyList_doesNotThrow() {
        // §9.4 cache invariant: a cache hit must produce byte-identical output.
        // If the bytes in the store are not a valid SemanticProcessingResult, the
        // service catches InvalidProtocolBufferException and treats the hit as a miss.
        String text = "some text";
        String key = ChunkCacheService.buildKey(text, CHUNKER_CONFIG_ID);
        store.put(key, new byte[]{1, 2, 3}); // deliberately not valid protobuf

        List<SemanticChunk> result = service.get(text, CHUNKER_CONFIG_ID).await().indefinitely();

        assertThat(result)
                .as("a corrupt cache entry must be caught as InvalidProtocolBufferException and degrade to empty list (cache miss)")
                .isEmpty();
    }

    @Test
    void redisErrors_incrementErrorCounter() {
        // §9.3: each Redis failure increments chunker.cache.errors. Run one GET failure
        // and one PUT failure and assert the counter moved by exactly 2.
        when(commandsMock.get(anyString()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("GET fail")));
        when(commandsMock.setex(anyString(), anyLong(), any(byte[].class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("PUT fail")));

        double before = meterRegistry.counter("chunker.cache.errors").count();

        service.get("some text", CHUNKER_CONFIG_ID).await().indefinitely();
        service.put("some text", CHUNKER_CONFIG_ID, buildChunks(1, "err"), TTL, false).await().indefinitely();

        double after = meterRegistry.counter("chunker.cache.errors").count();

        assertThat(after - before)
                .as("each Redis GET/PUT failure must increment the chunker.cache.errors counter exactly once")
                .isEqualTo(2.0);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Builds a list of {@code count} {@link SemanticChunk} instances with
     * distinct chunk_ids and text_content values.
     */
    private static List<SemanticChunk> buildChunks(int count, String prefix) {
        List<SemanticChunk> chunks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            chunks.add(SemanticChunk.newBuilder()
                    .setChunkId(prefix + "-" + i)
                    .setChunkNumber(i)
                    .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                            .setTextContent("chunk text " + i + " for " + prefix)
                            .build())
                    .build());
        }
        return chunks;
    }

    /**
     * Injects a value into a private field via reflection — used to wire mocks
     * into the CDI bean without starting a Quarkus container.
     */
    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            // Walk up the hierarchy if needed
            field = clazz.getSuperclass().getDeclaredField(fieldName);
        }
        field.setAccessible(true);
        field.set(target, value);
    }
}
