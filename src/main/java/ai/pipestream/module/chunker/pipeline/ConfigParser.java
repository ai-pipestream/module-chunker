package ai.pipestream.module.chunker.pipeline;

import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Parses a {@link NamedChunkerConfig}'s embedded {@link Struct} into a typed
 * {@link ChunkerConfig}. The parser never substitutes defaults: an empty
 * struct, a missing field, or a malformed value all throw
 * {@link IllegalArgumentException}, which the gRPC layer converts into
 * {@code PROCESSING_OUTCOME_FAILURE} / {@code INVALID_ARGUMENT}.
 *
 * <p>Configuration is the caller's responsibility. There is no
 * {@code ChunkerStepOptions}, no {@code cache_enabled}, no defaults catalog
 * anywhere in this module.
 */
@ApplicationScoped
public class ConfigParser {

    private final ObjectMapper objectMapper;

    @Inject
    public ConfigParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ChunkerConfig parseNamedConfig(NamedChunkerConfig named, String sourceLabel) {
        Struct struct = named.getConfig();
        if (struct == null || struct.getFieldsCount() == 0) {
            throw new IllegalArgumentException(
                    "NamedChunkerConfig '" + named.getConfigId()
                            + "' for source_label='" + sourceLabel
                            + "' has no config struct — every field is required, no defaults");
        }

        ChunkerConfig parsed;
        try {
            String json = JsonFormat.printer().print(struct);
            parsed = objectMapper.readValue(json, ChunkerConfig.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid NamedChunkerConfig '" + named.getConfigId()
                            + "' for source_label='" + sourceLabel + "': " + e.getMessage(), e);
        }

        // Inject the directive's source_label so the chunker reads the right field.
        ChunkerConfig resolved = new ChunkerConfig(
                parsed.algorithm(),
                sourceLabel,
                parsed.chunkSize(),
                parsed.chunkOverlap(),
                parsed.preserveUrls(),
                parsed.cleanText());

        String validationError = resolved.validate();
        if (validationError != null) {
            throw new IllegalArgumentException(
                    "Invalid NamedChunkerConfig '" + named.getConfigId()
                            + "' for source_label='" + sourceLabel + "': " + validationError);
        }
        return resolved;
    }
}
