package ai.pipestream.module.chunker.config;

import ai.pipestream.module.chunker.model.ChunkerOptions;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fast unit tests for ChunkerConfig and ChunkerOptions JSON parsing, defaults,
 * template resolution, and validation. No Quarkus context needed.
 */
class ChunkerConfigParsingTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // =========================================================================
    // ChunkerConfig: JSON parsing
    // =========================================================================

    @Test
    void parseMinimalConfig_usesDefaults() throws Exception {
        ChunkerConfig config = mapper.readValue("""
                { "algorithm": "token", "chunkSize": 500 }
                """, ChunkerConfig.class);

        assertThat(config.algorithm()).as("algorithm").isEqualTo(ChunkingAlgorithm.TOKEN);
        assertThat(config.chunkSize()).as("chunkSize").isEqualTo(500);
        assertThat(config.chunkOverlap()).as("chunkOverlap default").isEqualTo(ChunkerConfig.DEFAULT_CHUNK_OVERLAP);
        assertThat(config.sourceField()).as("sourceField default").isEqualTo(ChunkerConfig.DEFAULT_SOURCE_FIELD);
        assertThat(config.preserveUrls()).as("preserveUrls default").isEqualTo(ChunkerConfig.DEFAULT_PRESERVE_URLS);
        assertThat(config.cleanText()).as("cleanText default").isEqualTo(ChunkerConfig.DEFAULT_CLEAN_TEXT);
    }

    @Test
    void parseFullConfig_allFieldsSet() throws Exception {
        ChunkerConfig config = mapper.readValue("""
                {
                  "algorithm": "sentence",
                  "sourceField": "title",
                  "chunkSize": 1000,
                  "chunkOverlap": 200,
                  "preserveUrls": false,
                  "cleanText": false
                }
                """, ChunkerConfig.class);

        assertThat(config.algorithm()).as("algorithm").isEqualTo(ChunkingAlgorithm.SENTENCE);
        assertThat(config.sourceField()).as("sourceField").isEqualTo("title");
        assertThat(config.chunkSize()).as("chunkSize").isEqualTo(1000);
        assertThat(config.chunkOverlap()).as("chunkOverlap").isEqualTo(200);
        assertThat(config.preserveUrls()).as("preserveUrls").isFalse();
        assertThat(config.cleanText()).as("cleanText").isFalse();
    }

    @Test
    void parseEmptyObject_usesDefaults() throws Exception {
        ChunkerConfig config = mapper.readValue("{}", ChunkerConfig.class);

        assertThat(config.algorithm()).as("default algorithm").isEqualTo(ChunkerConfig.DEFAULT_ALGORITHM);
        assertThat(config.sourceField()).as("default sourceField").isEqualTo(ChunkerConfig.DEFAULT_SOURCE_FIELD);
        assertThat(config.chunkSize()).as("default chunkSize").isEqualTo(ChunkerConfig.DEFAULT_CHUNK_SIZE);
    }

    @ParameterizedTest
    @CsvSource({
            "token, TOKEN",
            "character, CHARACTER",
            "sentence, SENTENCE"
    })
    void parseAlgorithm_caseInsensitive(String jsonValue, String expected) throws Exception {
        String json = String.format("{\"algorithm\": \"%s\", \"chunkSize\": 100}", jsonValue);
        ChunkerConfig config = mapper.readValue(json, ChunkerConfig.class);
        assertThat(config.algorithm()).as("algorithm from '%s'", jsonValue)
                .isEqualTo(ChunkingAlgorithm.valueOf(expected));
    }

    // =========================================================================
    // ChunkerConfig: snake_case ↔ camelCase naming-convention coverage
    //
    // Callers build their config Struct with whatever convention they prefer —
    // a TypeScript admin form may emit camelCase; a Python client using
    // `preservingProtoFieldNames()` JSON printer may emit snake_case. Every
    // field must round-trip correctly under BOTH conventions or Jackson
    // silently drops the unknown key and the field reverts to its default.
    // Prior audit found that preserveUrls + cleanText were missing their
    // snake_case aliases; these tests pin both conventions on every field
    // so the regression can't come back.
    // =========================================================================

    @Test
    void parseSnakeCase_allFieldsRoundTripCorrectly() throws Exception {
        ChunkerConfig config = mapper.readValue("""
                {
                  "algorithm": "sentence",
                  "source_field": "title",
                  "chunk_size": 750,
                  "chunk_overlap": 125,
                  "preserve_urls": false,
                  "clean_text": false
                }
                """, ChunkerConfig.class);

        assertThat(config.algorithm()).as("snake_case algorithm").isEqualTo(ChunkingAlgorithm.SENTENCE);
        assertThat(config.sourceField()).as("snake_case source_field → sourceField").isEqualTo("title");
        assertThat(config.chunkSize()).as("snake_case chunk_size → chunkSize").isEqualTo(750);
        assertThat(config.chunkOverlap()).as("snake_case chunk_overlap → chunkOverlap").isEqualTo(125);
        assertThat(config.preserveUrls())
                .as("snake_case preserve_urls → preserveUrls (regression: this used "
                        + "to silently default to true because @JsonAlias was missing)")
                .isFalse();
        assertThat(config.cleanText())
                .as("snake_case clean_text → cleanText (regression: this used to "
                        + "silently default to true because @JsonAlias was missing)")
                .isFalse();
    }

    @Test
    void parseCamelCase_allFieldsRoundTripCorrectly() throws Exception {
        ChunkerConfig config = mapper.readValue("""
                {
                  "algorithm": "sentence",
                  "sourceField": "title",
                  "chunkSize": 750,
                  "chunkOverlap": 125,
                  "preserveUrls": false,
                  "cleanText": false
                }
                """, ChunkerConfig.class);

        assertThat(config.algorithm()).as("camelCase algorithm").isEqualTo(ChunkingAlgorithm.SENTENCE);
        assertThat(config.sourceField()).as("camelCase sourceField").isEqualTo("title");
        assertThat(config.chunkSize()).as("camelCase chunkSize").isEqualTo(750);
        assertThat(config.chunkOverlap()).as("camelCase chunkOverlap").isEqualTo(125);
        assertThat(config.preserveUrls()).as("camelCase preserveUrls").isFalse();
        assertThat(config.cleanText()).as("camelCase cleanText").isFalse();
    }

    @Test
    void parseMixedCaseConventions_allFieldsRoundTripCorrectly() throws Exception {
        // A half-converted form — some fields camelCase, some snake_case.
        // Both must still hydrate cleanly.
        ChunkerConfig config = mapper.readValue("""
                {
                  "algorithm": "token",
                  "source_field": "body",
                  "chunkSize": 400,
                  "chunk_overlap": 40,
                  "preserveUrls": false,
                  "clean_text": false
                }
                """, ChunkerConfig.class);

        assertThat(config.sourceField()).as("mixed: source_field").isEqualTo("body");
        assertThat(config.chunkSize()).as("mixed: chunkSize").isEqualTo(400);
        assertThat(config.chunkOverlap()).as("mixed: chunk_overlap").isEqualTo(40);
        assertThat(config.preserveUrls()).as("mixed: preserveUrls").isFalse();
        assertThat(config.cleanText()).as("mixed: clean_text").isFalse();
    }

    // =========================================================================
    // ChunkerConfig: validation
    // =========================================================================

    @Test
    void validate_validConfig_returnsNull() {
        ChunkerConfig config = ChunkerConfig.createDefault();
        assertThat(config.validate()).as("default config validation").isNull();
        assertThat(config.isValid()).as("default config isValid").isTrue();
    }

    @Test
    void validate_chunkSizeTooSmall_returnsError() {
        ChunkerConfig config = new ChunkerConfig(ChunkingAlgorithm.TOKEN, "body", 0, 5, false);
        assertThat(config.validate()).as("chunkSize=0 should fail validation")
                .isNotNull()
                .contains("chunkSize");
    }

    @Test
    void validate_chunkSizeTooLarge_returnsError() {
        ChunkerConfig config = new ChunkerConfig(ChunkingAlgorithm.TOKEN, "body", 20000, 5, false);
        assertThat(config.validate()).as("chunkSize=20000 should fail validation")
                .isNotNull()
                .contains("chunkSize");
    }

    @Test
    void validate_overlapGteChunkSize_returnsError() {
        ChunkerConfig config = new ChunkerConfig(ChunkingAlgorithm.TOKEN, "body", 500, 500, false);
        assertThat(config.validate()).as("chunkOverlap >= chunkSize should fail")
                .isNotNull()
                .contains("chunkOverlap");
    }

    @Test
    void validate_semanticAlgorithm_returnsError() {
        ChunkerConfig config = new ChunkerConfig(ChunkingAlgorithm.SEMANTIC, "body", 500, 50, false);
        assertThat(config.validate()).as("SEMANTIC algorithm should fail validation")
                .isNotNull()
                .contains("not yet implemented");
    }

    // =========================================================================
    // ChunkerOptions: result_set_name_template resolution
    // =========================================================================

    @Test
    void resolveResultSetName_defaultTemplate() {
        String result = ChunkerOptions.resolveResultSetName(null, "my-step", "cfg1", "body");
        assertThat(result).as("default template resolved").isEqualTo("my-step_chunks");
    }

    @Test
    void resolveResultSetName_customTemplate_allPlaceholders() {
        String result = ChunkerOptions.resolveResultSetName(
                "{source_field}_{step_name}_{config_id}", "step1", "cfg1", "body");
        assertThat(result).as("custom template with all placeholders").isEqualTo("body_step1_cfg1");
    }

    @Test
    void resolveResultSetName_sanitizesSpecialChars() {
        String result = ChunkerOptions.resolveResultSetName(
                "{step_name}_chunks", "my.step/name", "cfg", "body");
        assertThat(result).as("dots and slashes should be sanitized to underscores")
                .isEqualTo("my_step_name_chunks");
    }

    @Test
    void resolveResultSetName_allNulls_usesDefaults() {
        String result = ChunkerOptions.resolveResultSetName(null, null, null, null);
        assertThat(result).as("all-null args should use defaults").isEqualTo("chunker_chunks");
    }

    // =========================================================================
    // ChunkerOptions: template validation
    // =========================================================================

    @Test
    void validateTemplate_validPlaceholders_returnsNull() {
        assertThat(ChunkerOptions.validateResultSetNameTemplate("{step_name}_chunks"))
                .as("valid template").isNull();
        assertThat(ChunkerOptions.validateResultSetNameTemplate("{step_name}_{config_id}_{source_field}"))
                .as("all valid placeholders").isNull();
        assertThat(ChunkerOptions.validateResultSetNameTemplate("my_literal_name"))
                .as("literal name with no placeholders").isNull();
        assertThat(ChunkerOptions.validateResultSetNameTemplate(null))
                .as("null template").isNull();
        assertThat(ChunkerOptions.validateResultSetNameTemplate(""))
                .as("empty template").isNull();
    }

    @Test
    void validateTemplate_unknownPlaceholder_returnsDescriptiveError() {
        String error = ChunkerOptions.validateResultSetNameTemplate("{unknown}_chunks");
        assertThat(error).as("unknown placeholder should produce error")
                .isNotNull()
                .contains("{unknown}")
                .startsWith("Unrecognized");
    }

    @Test
    void validateTemplate_mixedValidAndInvalid_reportsOnlyInvalid() {
        String error = ChunkerOptions.validateResultSetNameTemplate("{step_name}_{bogus}");
        assertThat(error).as("should report {bogus} but not {step_name}")
                .isNotNull()
                .contains("{bogus}")
                .startsWith("Unrecognized");
    }

    // =========================================================================
    // ChunkerOptions: defaults and round-trip
    // =========================================================================

    @Test
    void chunkerOptions_defaultConstructor_allDefaultsSet() {
        ChunkerOptions opts = new ChunkerOptions();
        assertThat(opts.sourceField()).as("default sourceField").isEqualTo(ChunkerOptions.DEFAULT_SOURCE_FIELD);
        assertThat(opts.chunkSize()).as("default chunkSize").isEqualTo(ChunkerOptions.DEFAULT_CHUNK_SIZE);
        assertThat(opts.chunkOverlap()).as("default chunkOverlap").isEqualTo(ChunkerOptions.DEFAULT_CHUNK_OVERLAP);
        assertThat(opts.resultSetNameTemplate()).as("default resultSetNameTemplate")
                .isEqualTo(ChunkerOptions.DEFAULT_RESULT_SET_NAME_TEMPLATE);
    }

    @Test
    void chunkerOptions_toStruct_roundTrip() throws Exception {
        ChunkerOptions original = new ChunkerOptions("body", 300, 30, null,
                "my_chunker", "{step_name}_custom", "[TEST] ", true);
        var struct = original.toStruct();
        String json = com.google.protobuf.util.JsonFormat.printer().print(struct);
        ChunkerOptions parsed = mapper.readValue(json, ChunkerOptions.class);

        assertThat(parsed.sourceField()).as("round-trip sourceField").isEqualTo(original.sourceField());
        assertThat(parsed.chunkSize()).as("round-trip chunkSize").isEqualTo(original.chunkSize());
        assertThat(parsed.chunkOverlap()).as("round-trip chunkOverlap").isEqualTo(original.chunkOverlap());
        assertThat(parsed.resultSetNameTemplate()).as("round-trip resultSetNameTemplate")
                .isEqualTo(original.resultSetNameTemplate());
    }

    // =========================================================================
    // JSON Schema
    // =========================================================================

    @Test
    void jsonSchema_isValidJson_andDocumentsTemplateField() throws Exception {
        String schema = ChunkerOptions.getJsonV7Schema();
        var tree = mapper.readTree(schema);
        assertThat(tree.get("type").asText()).as("schema type").isEqualTo("object");
        assertThat(tree.has("properties")).as("has properties").isTrue();
        assertThat(tree.get("properties").has("result_set_name_template"))
                .as("schema has result_set_name_template property").isTrue();
    }
}
