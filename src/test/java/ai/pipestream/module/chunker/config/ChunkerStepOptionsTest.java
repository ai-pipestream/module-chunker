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

        assertThat(opts.alwaysEmitSentences())
                .as("defaults() alwaysEmitSentences field should be null")
                .isNull();
    }

    @Test
    void defaults_effectiveAccessorsReturnDefaults() {
        ChunkerStepOptions opts = ChunkerStepOptions.defaults();

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("defaults(): effectiveAlwaysEmitSentences should return true")
                .isTrue();
    }

    // =========================================================================
    // effectiveAlwaysEmitSentences
    // =========================================================================

    @Test
    void effectiveAlwaysEmitSentences_nullField_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(null);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("null alwaysEmitSentences should default to true")
                .isTrue();
    }

    @Test
    void effectiveAlwaysEmitSentences_explicitTrue_returnsTrue() {
        ChunkerStepOptions opts = new ChunkerStepOptions(Boolean.TRUE);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("explicit alwaysEmitSentences=true should return true")
                .isTrue();
    }

    @Test
    void effectiveAlwaysEmitSentences_explicitFalse_returnsFalse() {
        ChunkerStepOptions opts = new ChunkerStepOptions(Boolean.FALSE);

        assertThat(opts.effectiveAlwaysEmitSentences())
                .as("explicit alwaysEmitSentences=false should return false")
                .isFalse();
    }

    // =========================================================================
    // JSON parse — sanity checks for snake_case + camelCase aliases
    // =========================================================================

    @Test
    void parse_emptyJson_allFieldsNull() throws Exception {
        ChunkerStepOptions opts = mapper.readValue("{}", ChunkerStepOptions.class);

        assertThat(opts.alwaysEmitSentences())
                .as("empty JSON: alwaysEmitSentences should be null")
                .isNull();
    }

    @Test
    void parse_snakeCase_alwaysEmitSentences() throws Exception {
        String json = """
                {
                  "always_emit_sentences": false
                }
                """;
        ChunkerStepOptions opts = mapper.readValue(json, ChunkerStepOptions.class);

        assertThat(opts.alwaysEmitSentences())
                .as("snake_case always_emit_sentences should parse to alwaysEmitSentences")
                .isFalse();
    }

    @Test
    void parse_camelCase_alwaysEmitSentences() throws Exception {
        String json = """
                {
                  "alwaysEmitSentences": false
                }
                """;
        ChunkerStepOptions opts = mapper.readValue(json, ChunkerStepOptions.class);

        assertThat(opts.alwaysEmitSentences())
                .as("camelCase alwaysEmitSentences should parse via @JsonAlias")
                .isFalse();
    }

    @Test
    void parse_unknownFields_ignored() throws Exception {
        // Unknown fields (e.g. legacy cache_enabled / cache_ttl_seconds from
        // older fixtures) must not fail the parse — @JsonIgnoreProperties
        // is set on the record.
        String json = """
                {
                  "cache_enabled": true,
                  "cache_ttl_seconds": 3600,
                  "always_emit_sentences": true,
                  "unknown_field": "whatever"
                }
                """;
        ChunkerStepOptions opts = mapper.readValue(json, ChunkerStepOptions.class);

        assertThat(opts.alwaysEmitSentences())
                .as("known field alwaysEmitSentences should still parse correctly despite unknown fields")
                .isTrue();
    }
}
