package ai.pipestream.module.chunker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChunkerOptions(
        @JsonProperty("source_field") String sourceField,
        @JsonProperty("chunk_size") int chunkSize,
        @JsonProperty("chunk_overlap") int chunkOverlap,
        @JsonProperty("chunk_id_template") String chunkIdTemplate,
        @JsonProperty("chunk_config_id") String chunkConfigId,
        @JsonProperty("result_set_name_template") String resultSetNameTemplate,
        @JsonProperty("log_prefix") String logPrefix,
        @JsonProperty("preserve_urls") Boolean preserveUrls
) {
    public static final String DEFAULT_SOURCE_FIELD = "body";
    public static final int DEFAULT_CHUNK_SIZE = 500;
    public static final int DEFAULT_CHUNK_OVERLAP = 50;
    public static final String DEFAULT_CHUNK_ID_TEMPLATE = "%s_%s_chunk_%d";
    public static final String DEFAULT_CHUNK_CONFIG_ID = "default_overlap_char_500_50";
    public static final String DEFAULT_RESULT_SET_NAME_TEMPLATE = "{step_name}_chunks";

    /** Recognized placeholders for result_set_name_template. */
    private static final java.util.Set<String> VALID_PLACEHOLDERS = java.util.Set.of(
            "{step_name}", "{config_id}", "{source_field}");

    /**
     * Resolves a result set name template by replacing recognized placeholders.
     */
    public static String resolveResultSetName(String template, String stepName, String configId, String sourceField) {
        if (template == null || template.isEmpty()) {
            template = DEFAULT_RESULT_SET_NAME_TEMPLATE;
        }
        String resolved = template
                .replace("{step_name}", stepName != null ? stepName : "chunker")
                .replace("{config_id}", configId != null ? configId : "default")
                .replace("{source_field}", sourceField != null ? sourceField : "body");
        return resolved.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    }

    /**
     * Validates a template string. Returns null if valid, or an error message describing the problem.
     */
    public static String validateResultSetNameTemplate(String template) {
        if (template == null || template.isEmpty()) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\{([^}]+)\\}").matcher(template);
        java.util.List<String> unknown = new java.util.ArrayList<>();
        while (m.find()) {
            String token = "{" + m.group(1) + "}";
            if (!VALID_PLACEHOLDERS.contains(token)) {
                unknown.add(token);
            }
        }
        if (!unknown.isEmpty()) {
            return "Unrecognized placeholder(s) in result_set_name_template: " + unknown
                    + ". Valid placeholders: " + VALID_PLACEHOLDERS;
        }
        return null;
    }
    public static final String DEFAULT_LOG_PREFIX = "";
    public static final boolean DEFAULT_PRESERVE_URLS = false;

    public ChunkerOptions(
            String sourceField,
            int chunkSize,
            int chunkOverlap,
            String chunkIdTemplate,
            String chunkConfigId,
            String resultSetNameTemplate,
            String logPrefix,
            Boolean preserveUrls
    ) {
        this.sourceField = sourceField != null ? sourceField : DEFAULT_SOURCE_FIELD;
        this.chunkSize = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
        this.chunkOverlap = chunkOverlap >= 0 ? chunkOverlap : DEFAULT_CHUNK_OVERLAP;
        this.chunkIdTemplate = chunkIdTemplate != null ? chunkIdTemplate : DEFAULT_CHUNK_ID_TEMPLATE;
        this.chunkConfigId = chunkConfigId != null ? chunkConfigId : DEFAULT_CHUNK_CONFIG_ID;
        this.resultSetNameTemplate = resultSetNameTemplate != null ? resultSetNameTemplate : DEFAULT_RESULT_SET_NAME_TEMPLATE;
        this.logPrefix = logPrefix != null ? logPrefix : DEFAULT_LOG_PREFIX;
        this.preserveUrls = preserveUrls != null ? preserveUrls : DEFAULT_PRESERVE_URLS;
    }

    public ChunkerOptions() {
        this(DEFAULT_SOURCE_FIELD,
                DEFAULT_CHUNK_SIZE,
                DEFAULT_CHUNK_OVERLAP,
                DEFAULT_CHUNK_ID_TEMPLATE,
                DEFAULT_CHUNK_CONFIG_ID,
                DEFAULT_RESULT_SET_NAME_TEMPLATE,
                DEFAULT_LOG_PREFIX,
                DEFAULT_PRESERVE_URLS);
    }

    public static String getJsonV7Schema() {
        return """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "title": "ChunkerOptions",
                  "description": "Configuration options for chunking text. Chunk size and overlap are typically character-based unless a specific tokenizer-based strategy is implied by the implementation.",
                  "type": "object",
                  "properties": {
                    "source_field": {
                      "description": "The field in the source data (e.g., PipeDoc) that contains the text to be chunked. Example: 'body' or 'extracted_text'.",
                      "type": "string",
                      "default": "%s"
                    },
                    "chunk_size": {
                      "description": "The target size of each chunk (e.g., in characters or tokens, depending on implementation). Must be a positive integer.",
                      "type": "integer",
                      "default": %d,
                      "minimum": 1
                    },
                    "chunk_overlap": {
                      "description": "The number of characters/tokens to overlap between consecutive chunks. Must be non-negative and ideally less than chunk_size.",
                      "type": "integer",
                      "default": %d,
                      "minimum": 0
                    },
                    "chunk_id_template": {
                      "description": "A template string for generating unique IDs for each chunk. E.g., %%s_%%s_chunk_%%d (placeholders for stream_id, document_id, chunk_index).",
                      "type": "string",
                      "default": "%s"
                    },
                    "chunk_config_id": {
                      "description": "An identifier for this specific chunking configuration.",
                      "type": "string",
                      "default": "%s"
                    },
                    "result_set_name_template": {
                      "description": "Template for naming the result set. Placeholders: {step_name}, {config_id}, {source_field}.",
                      "type": "string",
                      "default": "%s"
                    },
                    "log_prefix": {
                      "description": "A prefix to add to log messages. Can be used for custom logging.",
                      "type": "string",
                      "default": "%s"
                    },
                    "preserve_urls": {
                      "description": "Whether to attempt to preserve URLs as whole units during chunking. If true, URLs might be treated as atomic tokens or replaced with placeholders before chunking and restored after.",
                      "type": "boolean",
                      "default": %b
                    }
                  },
                  "required": [
                    "source_field",
                    "chunk_size",
                    "chunk_overlap",
                    "chunk_id_template",
                    "chunk_config_id",
                    "result_set_name_template",
                    "log_prefix"
                  ]
                }
                """.formatted(
                DEFAULT_SOURCE_FIELD,
                DEFAULT_CHUNK_SIZE,
                DEFAULT_CHUNK_OVERLAP,
                DEFAULT_CHUNK_ID_TEMPLATE.replace("%", "%%"),
                DEFAULT_CHUNK_CONFIG_ID,
                DEFAULT_RESULT_SET_NAME_TEMPLATE.replace("%", "%%"),
                DEFAULT_LOG_PREFIX,
                DEFAULT_PRESERVE_URLS
        );
    }

    public Struct toStruct() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(this);
            Struct.Builder builder = Struct.newBuilder();
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert ChunkerOptions to Struct", e);
        }
    }
}