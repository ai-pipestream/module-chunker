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
 * boolean alwaysEmit = opts.effectiveAlwaysEmitSentences();
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChunkerStepOptions(
        @JsonProperty("always_emit_sentences") @JsonAlias("alwaysEmitSentences") Boolean alwaysEmitSentences
) {

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
        return new ChunkerStepOptions(null);
    }
}
