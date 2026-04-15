package ai.pipestream.module.chunker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ChunkerStepOptions} — pure record, no Quarkus context needed.
 */
class ChunkerStepOptionsTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // =========================================================================
    // defaults() factory
    // =========================================================================

    @Test
    void defaults_allFieldsNull() {
        ChunkerStepOptions opts = ChunkerStepOptions.defaults();

        assertThat(opts.cacheEnabled())
                .as("defaults() cacheEnabled field should be null")
                .isNull();
        assertThat(opts.cacheTtlSeconds())
                .as("defaults() cacheTtlSeconds field should be null")
                .isNull();
        assertThat(opts.alwaysEmitSentences())
                .as("defaults() alwaysEmitSentences field should be null")
                .isNull();
    }

    @Test
    void defaults_effectiveAccessors_returnCanonicalDefaults() {
        ChunkerStepOptions opts = ChunkerStepOptions.defaults();

        assertThat(opts.effectiveCacheEnabled())
                .as("defaults(): effectiveCacheEnabled should return true")
                .isTrue();
        assertThat(opts.effectiveCacheTtlSeconds())
                .as("defaults(): effectiveCacheTtlSeconds should return 30-day default (2592000)")
                .isEqualTo(2_592_000L);
        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("defaults(): effectiveAlwaysEmitSentences should return true")
                .isTrue();
    }

    // =========================================================================
    // effectiveCacheEnabled
    // =========================================================================

    @Test
    void effectiveCacheEnabled_nullField_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, null, null);

        assertThat(opts.effectiveCacheEnabled())
                .as("null cacheEnabled should default to true")
                .isTrue();
    }

    @Test
    void effectiveCacheEnabled_explicitTrue_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(true, null, null);

        assertThat(opts.effectiveCacheEnabled())
                .as("explicit cacheEnabled=true should return true")
                .isTrue();
    }

    @Test
    void effectiveCacheEnabled_explicitFalse_returnsFalse() {
        ChunkerStepOptions opts = new ChunkerStepOptions(false, null, null);

        assertThat(opts.effectiveCacheEnabled())
                .as("explicit cacheEnabled=false should return false")
                .isFalse();
    }

    // =========================================================================
    // effectiveCacheTtlSeconds
    // =========================================================================

    @Test
    void effectiveCacheTtlSeconds_nullField_returns30DayDefault() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, null, null);

        assertThat(opts.effectiveCacheTtlSeconds())
                .as("null cacheTtlSeconds should default to 2592000 (30 days)")
                .isEqualTo(2_592_000L);
    }

    @Test
    void effectiveCacheTtlSeconds_zeroValue_returns30DayDefault() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, 0L, null);

        assertThat(opts.effectiveCacheTtlSeconds())
                .as("cacheTtlSeconds=0 should fall back to 30-day default")
                .isEqualTo(2_592_000L);
    }

    @Test
    void effectiveCacheTtlSeconds_negativeValue_returns30DayDefault() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, -1L, null);

        assertThat(opts.effectiveCacheTtlSeconds())
                .as("negative cacheTtlSeconds should fall back to 30-day default")
                .isEqualTo(2_592_000L);
    }

    @Test
    void effectiveCacheTtlSeconds_customPositiveValue_returnsThatValue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, 86400L, null);

        assertThat(opts.effectiveCacheTtlSeconds())
                .as("custom cacheTtlSeconds=86400 (1 day) should be returned as-is")
                .isEqualTo(86400L);
    }

    // =========================================================================
    // effectiveAlwaysEmitSentences
    // =========================================================================

    @Test
    void effectiveAlwaysEmitSentences_nullField_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, null, null);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("null alwaysEmitSentences should default to true")
                .isTrue();
    }

    @Test
    void effectiveAlwaysEmitSentences_explicitTrue_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, null, true);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("explicit alwaysEmitSentences=true should return true")
                .isTrue();
    }

    @Test
    void effectiveAlwaysEmitSentences_explicitFalse_returnsFalse() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null, null, false);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("explicit alwaysEmitSentences=false should return false")
                .isFalse();
    }

    // =========================================================================
    // Jackson parsing
    // =========================================================================

    @Test
    void parseEmptyJson_allFieldsNull() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("{}", ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled())
                .as("empty JSON: cacheEnabled should be null")
                .isNull();
        assertThat(opts.cacheTtlSeconds())
                .as("empty JSON: cacheTtlSeconds should be null")
                .isNull();
        assertThat(opts.alwaysEmitSentences())
                .as("empty JSON: alwaysEmitSentences should be null")
                .isNull();
    }

    @Test
    void parseAllFields_setsCorrectly() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("""
                {
                  "cache_enabled": false,
                  "cache_ttl_seconds": 3600,
                  "always_emit_sentences": false
                }
                """, ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled())
                .as("parsed cache_enabled should be false")
                .isFalse();
        assertThat(opts.cacheTtlSeconds())
                .as("parsed cache_ttl_seconds should be 3600")
                .isEqualTo(3600L);
        assertThat(opts.alwaysEmitSentences())
                .as("parsed always_emit_sentences should be false")
                .isFalse();
    }

    @Test
    void parseUnknownFields_ignoredGracefully() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("""
                {
                  "cache_enabled": true,
                  "unknown_future_field": "should_be_ignored",
                  "another_unknown": 42
                }
                """, ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled())
                .as("known field cache_enabled should still parse correctly despite unknown fields")
                .isTrue();
        assertThat(opts.cacheTtlSeconds())
                .as("absent cacheTtlSeconds should be null despite unknown fields present")
                .isNull();
    }

    @Test
    void parsePartialFields_absentFieldsAreNull() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("""
                { "cache_ttl_seconds": 7776000 }
                """, ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled())
                .as("absent cache_enabled should be null")
                .isNull();
        assertThat(opts.cacheTtlSeconds())
                .as("present cache_ttl_seconds should be 7776000")
                .isEqualTo(7_776_000L);
        assertThat(opts.alwaysEmitSentences())
                .as("absent always_emit_sentences should be null")
                .isNull();
        assertThat(opts.effectiveCacheEnabled())
                .as("absent cache_enabled: effective should be true (default)")
                .isTrue();
    }

    // =========================================================================
    // snake_case ↔ camelCase naming-convention coverage
    //
    // ProcessConfiguration.json_config is a google.protobuf.Struct — the
    // field names inside are raw map keys, so whatever convention the
    // caller used (snake_case from a Python/protobuf-native client,
    // camelCase from a TypeScript admin form) lands in Jackson unchanged.
    // Every field must round-trip under BOTH conventions or the "wrong"
    // one silently drops to null and the effective() accessor returns the
    // canonical default — which LOOKS like the caller's config was
    // honoured but isn't. Prior audit found all three fields were missing
    // their camelCase alias; these tests pin the fix so it can't regress.
    // =========================================================================

    @Test
    void parseCamelCase_allFieldsRoundTripCorrectly() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("""
                {
                  "cacheEnabled": false,
                  "cacheTtlSeconds": 3600,
                  "alwaysEmitSentences": false
                }
                """, ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled())
                .as("camelCase cacheEnabled → cacheEnabled (regression: this used "
                        + "to silently default to null because @JsonAlias was missing, "
                        + "and effectiveCacheEnabled() then incorrectly returned true)")
                .isFalse();
        assertThat(opts.cacheTtlSeconds())
                .as("camelCase cacheTtlSeconds → cacheTtlSeconds (regression: used "
                        + "to silently default to null)")
                .isEqualTo(3600L);
        assertThat(opts.alwaysEmitSentences())
                .as("camelCase alwaysEmitSentences → alwaysEmitSentences (regression: "
                        + "used to silently default to null)")
                .isFalse();
    }

    @Test
    void parseMixedCaseConventions_allFieldsRoundTripCorrectly() throws Exception {
        // A half-converted form — some fields camelCase, some snake_case.
        // Every combination must hydrate to the declared value.
        ChunkerStepOptions opts = mapper.readValue("""
                {
                  "cache_enabled": false,
                  "cacheTtlSeconds": 86400,
                  "always_emit_sentences": false
                }
                """, ChunkerStepOptions.class);

        assertThat(opts.cacheEnabled()).as("mixed: cache_enabled").isFalse();
        assertThat(opts.cacheTtlSeconds()).as("mixed: cacheTtlSeconds").isEqualTo(86400L);
        assertThat(opts.alwaysEmitSentences()).as("mixed: always_emit_sentences").isFalse();
    }

    // =========================================================================
    // DEFAULT_CACHE_TTL_SECONDS constant
    // =========================================================================

    @Test
    void defaultCacheTtlSeconds_constant_is30Days() {
        assertThat(ChunkerStepOptions.DEFAULT_CACHE_TTL_SECONDS)
                .as("DEFAULT_CACHE_TTL_SECONDS should be 30 days = 2592000 seconds")
                .isEqualTo(2_592_000L);
    }
}
