package ai.pipestream.module.chunker.config;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Typed parse of the chunker step's {@code ProcessConfiguration.json_config}
 * (a {@code google.protobuf.Struct}), per DESIGN.md §6.1.
 *
 * <p>Fields are optional in the JSON; callers receive {@code null} for absent
 * values and should use the {@code effective*()} accessors, which apply the
 * canonical defaults:
 * <ul>
 *   <li>{@code cache_enabled} — default {@code true}</li>
 *   <li>{@code cache_ttl_seconds} — default {@code 2_592_000} (30 days)</li>
 *   <li>{@code always_emit_sentences} — default {@code true}</li>
 * </ul>
 *
 * <p>Per DESIGN.md §21.1, there is no {@code legacy_fallback}: if the JSON
 * cannot be parsed into this record, the caller MUST fail the step with
 * {@code INVALID_ARGUMENT}. This class does not perform the parse itself —
 * the caller uses Jackson's {@code ObjectMapper} to hydrate it from the
 * Struct's JSON printout via {@code JsonFormat.printer().print(struct)}.
 *
 * <p>Usage example:
 * <pre>{@code
 * String json = JsonFormat.printer().print(processConfig.getJsonConfig());
 * ChunkerStepOptions opts = mapper.readValue(json, ChunkerStepOptions.class);
 * boolean cacheEnabled = opts.effectiveCacheEnabled();
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChunkerStepOptions(
        @JsonProperty("cache_enabled") @JsonAlias("cacheEnabled") Boolean cacheEnabled,
        @JsonProperty("cache_ttl_seconds") @JsonAlias("cacheTtlSeconds") Long cacheTtlSeconds,
        @JsonProperty("always_emit_sentences") @JsonAlias("alwaysEmitSentences") Boolean alwaysEmitSentences
) {

    /** Default TTL for chunk cache entries: 30 days in seconds. */
    public static final long DEFAULT_CACHE_TTL_SECONDS = 2_592_000L;

    /**
     * Returns {@code true} if the chunk cache is enabled.
     * Defaults to {@code true} when the field is absent or null.
     */
    public boolean effectiveCacheEnabled() {
        return cacheEnabled == null || cacheEnabled;
    }

    /**
     * Returns the cache TTL in seconds.
     * Defaults to {@value #DEFAULT_CACHE_TTL_SECONDS} (30 days) when the field
     * is absent, null, or non-positive.
     */
    public long effectiveCacheTtlSeconds() {
        return cacheTtlSeconds != null && cacheTtlSeconds > 0 ? cacheTtlSeconds : DEFAULT_CACHE_TTL_SECONDS;
    }

    /**
     * Returns {@code true} if sentence chunks should always be emitted even when
     * they duplicate another chunk type's output.
     * Defaults to {@code true} when the field is absent or null.
     */
    public boolean effectiveAlwaysEmitSentences() {
        return alwaysEmitSentences == null || alwaysEmitSentences;
    }

    /**
     * Returns the canonical defaults instance — every field is {@code null},
     * so all {@code effective*()} accessors return their default values.
     */
    public static ChunkerStepOptions defaults() {
        return new ChunkerStepOptions(null, null, null);
    }
}
