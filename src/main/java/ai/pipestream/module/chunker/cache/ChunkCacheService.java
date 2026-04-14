package ai.pipestream.module.chunker.cache;

import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.module.chunker.directive.DirectiveKeyComputer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quarkus CDI bean that wraps a Redis cache for {@code List<SemanticChunk>} results
 * produced by the chunker step, per DESIGN.md §9.1.
 *
 * <h2>Implementation strategy</h2>
 * <p>Uses {@link ReactiveRedisDataSource} (Strategy A from the sub-task spec) rather
 * than {@code @CacheResult} annotations. This choice allows:
 * <ul>
 *   <li>Explicit RTBF (Right-To-Be-Forgotten) gate on writes per §21.7 — the
 *       {@code isRtbfSuppressed} parameter on {@link #put} is a placeholder for
 *       {@code RtbfPolicy.isSuppressed(doc)}, which a future PR will wire.</li>
 *   <li>Explicit error recovery: Redis failures are caught with Mutiny's
 *       {@code onFailure().recoverWith...} and treated as cache misses (compute-through),
 *       per §9.3. Errors are logged at WARN; they never fail the doc.</li>
 *   <li>Future batch ({@code MGET}/{@code MSET}) upgrade without API change.</li>
 * </ul>
 *
 * <h2>Key format</h2>
 * <pre>
 * chunk:{sha256b64url(text)}:{chunker_config_id}
 * </pre>
 * where {@code sha256b64url} is SHA-256 hashed, base64url-encoded, no {@code =} padding.
 *
 * <h2>Value format</h2>
 * <p>The list is serialized by wrapping it in a {@link SemanticProcessingResult} proto
 * (using only the {@code chunks} field) and calling {@code toByteArray()}. On read,
 * {@code SemanticProcessingResult.parseFrom(bytes).getChunksList()} extracts the list.
 * This reuses an existing proto shape without inventing a new message type.
 *
 * <h2>TTL</h2>
 * <p>Governed by {@code ChunkerStepOptions.effectiveCacheTtlSeconds()} — default 30 days.
 * The TTL value is passed in on each {@link #put} call by the caller.
 *
 * <h2>RTBF gate</h2>
 * <p>Per §21.7, cache <em>writes</em> are suppressed for RTBF-marked traffic.
 * Cache <em>reads</em> are always allowed (cached values contain no identifying
 * payload). The {@code isRtbfSuppressed} boolean parameter is a placeholder —
 * today callers pass {@code false}; a future PR wires {@code RtbfPolicy.isSuppressed(doc)}.
 *
 * <h2>Outage behavior</h2>
 * <p>Redis errors during get/put are caught, logged at WARN, and treated as misses
 * (compute-through) per §9.3. They never propagate to the caller as failures.
 */
@ApplicationScoped
public class ChunkCacheService {

    private static final Logger LOG = Logger.getLogger(ChunkCacheService.class);
    private static final String KEY_PREFIX = "chunk:";

    /** Minimum interval between duplicate WARN logs on the same error path (per DESIGN.md §9.3). */
    static final long WARN_INTERVAL_MS = 60_000L;

    @Inject
    ReactiveRedisDataSource redis;

    @Inject
    MeterRegistry meterRegistry;

    private ReactiveValueCommands<String, byte[]> commands;

    /** Counter incremented on every Redis failure (read or write) per DESIGN.md §9.3. */
    private Counter errorCounter;

    /** Timestamp of the last GET-error WARN log, used to rate-limit to 1/min per §9.3. */
    private final AtomicLong lastGetWarnMs = new AtomicLong(0L);

    /** Timestamp of the last PUT-error WARN log, used to rate-limit to 1/min per §9.3. */
    private final AtomicLong lastPutWarnMs = new AtomicLong(0L);

    @PostConstruct
    void init() {
        commands = redis.value(byte[].class);
        errorCounter = Counter.builder("chunker.cache.errors")
                .description("Number of Redis chunk cache GET/PUT failures recovered as compute-through")
                .register(meterRegistry);
        LOG.debug("ChunkCacheService initialized with ReactiveRedisDataSource");
    }

    /**
     * Returns true if the WARN log for this error path should fire now, false if it
     * should be rate-limited. Uses a CAS on the last-log timestamp so two concurrent
     * failures cannot both slip through.
     */
    private static boolean shouldWarn(AtomicLong lastWarnMs) {
        long now = System.currentTimeMillis();
        long last = lastWarnMs.get();
        if (now - last < WARN_INTERVAL_MS) {
            return false;
        }
        return lastWarnMs.compareAndSet(last, now);
    }

    /**
     * Looks up a cached chunk list for the given text and chunker config.
     *
     * @param text            the source text that was chunked
     * @param chunkerConfigId the config_id of the chunker configuration used
     * @return a {@link Uni} emitting the cached chunk list, or an empty list on
     *         cache miss or Redis error (compute-through per §9.3)
     */
    public Uni<List<SemanticChunk>> get(String text, String chunkerConfigId) {
        String key = buildKey(text, chunkerConfigId);
        return commands.get(key)
                .map(bytes -> {
                    if (bytes == null) {
                        LOG.debugf("Chunk cache MISS key=%s", key);
                        return Collections.<SemanticChunk>emptyList();
                    }
                    try {
                        List<SemanticChunk> chunks = SemanticProcessingResult.parseFrom(bytes).getChunksList();
                        LOG.debugf("Chunk cache HIT key=%s chunks=%d", key, chunks.size());
                        return chunks;
                    } catch (InvalidProtocolBufferException e) {
                        LOG.warnf("Chunk cache deserialization error for key=%s, treating as miss: %s",
                                key, e.getMessage());
                        return Collections.<SemanticChunk>emptyList();
                    }
                })
                .onFailure().recoverWithItem(failure -> {
                    errorCounter.increment();
                    if (shouldWarn(lastGetWarnMs)) {
                        LOG.warnf("Chunk cache GET error (rate-limited, 1/min) key=%s type=%s: %s",
                                key, failure.getClass().getSimpleName(), failure.getMessage());
                    }
                    return Collections.emptyList();
                });
    }

    /**
     * Stores a chunk list in the cache with the given TTL.
     *
     * <p>Per §21.7, writes are skipped when {@code isRtbfSuppressed} is {@code true}.
     * The write is silently treated as a no-op so the caller need not branch.
     *
     * @param text              the source text that was chunked (used to build the cache key)
     * @param chunkerConfigId   the config_id of the chunker configuration used
     * @param chunks            the chunk list to cache; empty lists are still stored
     * @param ttlSeconds        the TTL for this cache entry in seconds
     * @param isRtbfSuppressed  placeholder for {@code RtbfPolicy.isSuppressed(doc)} per §21.7;
     *                          pass {@code false} today — a future PR wires the real predicate
     * @return a {@link Uni} completing when the write is acknowledged, or immediately on
     *         RTBF suppression or Redis error (errors are logged WARN and swallowed)
     */
    public Uni<Void> put(String text, String chunkerConfigId,
                         List<SemanticChunk> chunks, long ttlSeconds,
                         boolean isRtbfSuppressed) {
        if (isRtbfSuppressed) {
            // §21.7: RTBF-marked docs skip cache writes; reads are still allowed
            LOG.debugf("Chunk cache PUT suppressed (RTBF) for chunkerConfigId=%s", chunkerConfigId);
            return Uni.createFrom().voidItem();
        }

        String key = buildKey(text, chunkerConfigId);
        byte[] bytes = SemanticProcessingResult.newBuilder()
                .addAllChunks(chunks)
                .build()
                .toByteArray();

        return commands.setex(key, ttlSeconds, bytes)
                .invoke(() -> LOG.debugf("Chunk cache PUT key=%s chunks=%d ttl=%ds",
                        key, chunks.size(), ttlSeconds))
                .replaceWithVoid()
                .onFailure().recoverWithUni(failure -> {
                    errorCounter.increment();
                    if (shouldWarn(lastPutWarnMs)) {
                        LOG.warnf("Chunk cache PUT error (rate-limited, 1/min) key=%s type=%s: %s",
                                key, failure.getClass().getSimpleName(), failure.getMessage());
                    }
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Builds the Redis key for the given text and chunker config.
     *
     * <p>Key format: {@code chunk:{sha256b64url(text)}:{chunker_config_id}}
     */
    static String buildKey(String text, String chunkerConfigId) {
        return KEY_PREFIX + DirectiveKeyComputer.sha256b64url(text) + ":" + chunkerConfigId;
    }
}
