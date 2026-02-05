package ai.pipestream.module.chunker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import ai.pipestream.module.chunker.schema.SchemaExtractorService;
import io.quarkus.arc.Arc;
import io.quarkus.smallrye.openapi.runtime.OpenApiConstants;
import io.quarkus.smallrye.openapi.runtime.OpenApiDocumentService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import io.smallrye.openapi.runtime.io.Format;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Test to prove that we can extract ChunkerConfig schema from OpenAPI dynamically
 * and validate it against both OpenAPI 3.1 and JSON Schema v7 standards.
 */
@QuarkusTest
public class OpenApiSchemaExtractionTest {

    private static final Logger LOG = Logger.getLogger(OpenApiSchemaExtractionTest.class);

    @Inject
    SchemaExtractorService schemaExtractorService;

    /**
     * TEST 1: Extract ChunkerConfig schema from the dynamically generated OpenAPI document
     */
    @Test
    public void test1_extractChunkerConfigSchemaFromOpenApi() {
        LOG.info("=== TEST 1: Extract ChunkerConfig schema from OpenAPI ===");

        // Get OpenApiDocumentService via CDI container (like the reference shows)
        OpenApiDocumentService documentService = Arc.container()
                .instance(OpenApiDocumentService.class).get();

        assertThat("OpenApiDocumentService should be available via CDI", documentService, is(notNullValue()));

        // Get the full OpenAPI document as JSON
        byte[] jsonBytes = documentService.getDocument(OpenApiConstants.DEFAULT_DOCUMENT_NAME, Format.JSON);
        assertThat("OpenAPI document should be generated", jsonBytes, is(notNullValue()));
        assertThat("OpenAPI document should not be empty", jsonBytes.length, is(greaterThan(0)));

        String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
        LOG.infof("OpenAPI document size: %d characters", jsonString.length());

        // Parse the JSON and extract ChunkerConfig schema
        try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
            JsonObject openApiDoc = reader.readObject();

            // Verify OpenAPI document structure
            assertThat("OpenAPI document should have 'openapi' field", openApiDoc.containsKey("openapi"), is(true));
            assertThat("OpenAPI document should have 'components' field", openApiDoc.containsKey("components"), is(true));

            JsonObject components = openApiDoc.getJsonObject("components");
            assertThat("Components should not be null", components, is(notNullValue()));
            assertThat("Components should have 'schemas' field", components.containsKey("schemas"), is(true));

            JsonObject schemas = components.getJsonObject("schemas");
            assertThat("Schemas should not be null", schemas, is(notNullValue()));
            assertThat("Schemas should contain 'ChunkerConfig'", schemas.containsKey("ChunkerConfig"), is(true));

            JsonObject chunkerConfigSchema = schemas.getJsonObject("ChunkerConfig");
            assertThat("ChunkerConfig schema should not be null", chunkerConfigSchema, is(notNullValue()));

            // Verify schema has expected structure
            assertThat("Schema should have 'type' field", chunkerConfigSchema.containsKey("type"), is(true));
            assertThat("Schema type should be 'object'", chunkerConfigSchema.getString("type"), is(equalTo("object")));
            assertThat("Schema should have 'properties' field", chunkerConfigSchema.containsKey("properties"), is(true));

            JsonObject properties = chunkerConfigSchema.getJsonObject("properties");
            assertThat("Properties should not be null", properties, is(notNullValue()));

            // Verify key properties exist
            assertThat("Should have 'algorithm' property", properties.containsKey("algorithm"), is(true));
            assertThat("Should have 'chunkSize' property", properties.containsKey("chunkSize"), is(true));
            assertThat("Should have 'chunkOverlap' property", properties.containsKey("chunkOverlap"), is(true));
            assertThat("Should have 'sourceField' property", properties.containsKey("sourceField"), is(true));
            assertThat("Should have 'preserveUrls' property", properties.containsKey("preserveUrls"), is(true));
            assertThat("Should have 'cleanText' property", properties.containsKey("cleanText"), is(true));

            String extractedSchema = chunkerConfigSchema.toString();
            LOG.infof("Extracted ChunkerConfig schema (%d chars): %s", extractedSchema.length(), extractedSchema);

            assertThat("Extracted schema should be substantial", extractedSchema.length(), is(greaterThan(100)));

            LOG.info("TEST 1 PASSED: Successfully extracted ChunkerConfig schema from OpenAPI");
        }
    }

    /**
     * TEST 2: Validate that the extracted schema is valid OpenAPI 3.1 format
     */
    @Test
    public void test2_validateSchemaIsValidOpenApi31() {
        LOG.info("=== TEST 2: Validate schema is valid OpenAPI 3.1 format ===");

        // Extract the schema first
        String chunkerConfigSchema = extractChunkerConfigSchema();

        try {
            // Parse as JSON to verify it's valid JSON
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode schemaNode = objectMapper.readTree(chunkerConfigSchema);

            // Verify it has OpenAPI schema characteristics
            assertThat("Schema should have 'type' field", schemaNode.has("type"), is(true));
            assertThat("Schema type should be 'object'", schemaNode.get("type").asText(), is(equalTo("object")));
            assertThat("Schema should have 'properties' field", schemaNode.has("properties"), is(true));

            JsonNode properties = schemaNode.get("properties");
            assertThat("Properties should be an object", properties.isObject(), is(true));

            // Verify specific ChunkerConfig properties
            assertThat("Should have algorithm property", properties.has("algorithm"), is(true));
            assertThat("Should have chunkSize property", properties.has("chunkSize"), is(true));

            // Check if algorithm has enum values (OpenAPI style)
            JsonNode algorithmProp = properties.get("algorithm");
            if (algorithmProp.has("enum")) {
                JsonNode enumValues = algorithmProp.get("enum");
                assertThat("Algorithm enum should have values", enumValues.size(), is(greaterThan(0)));
                LOG.infof("Algorithm enum values: %s", enumValues);
            }

            // Check for OpenAPI-style constraints on chunkSize
            JsonNode chunkSizeProp = properties.get("chunkSize");
            if (chunkSizeProp.has("minimum")) {
                assertThat("ChunkSize should have minimum constraint", chunkSizeProp.get("minimum").asInt(), is(equalTo(50)));
            }
            if (chunkSizeProp.has("maximum")) {
                assertThat("ChunkSize should have maximum constraint", chunkSizeProp.get("maximum").asInt(), is(equalTo(10000)));
            }

            LOG.info("TEST 2 PASSED: Schema is valid OpenAPI 3.1 format");

        } catch (Exception e) {
            throw new AssertionError("Schema should be valid JSON and OpenAPI format: " + e.getMessage(), e);
        }
    }

    /**
     * TEST 3: Validate that the extracted schema passes JSON Schema v7 validation (like ConsulModuleRegistryService uses)
     */
    @Test
    public void test3_validateSchemaPassesJsonSchemaV7Validation() {
        LOG.info("=== TEST 3: Validate schema passes JSON Schema v7 validation ===");

        // Extract the schema cleaned for JSON Schema v7 validation
        String chunkerConfigSchema = schemaExtractorService.extractChunkerConfigSchemaForValidation()
                .orElseThrow(() -> new AssertionError("Failed to extract ChunkerConfig schema for validation"));

        try {
            // Use the same JSON Schema v7 validation that ConsulModuleRegistryService uses
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode schemaNode = objectMapper.readTree(chunkerConfigSchema);

            // Create a JsonSchema validator from the extracted schema
            JsonSchema validator = factory.getSchema(schemaNode);
            assertThat("JsonSchema validator should be created successfully", validator, is(notNullValue()));

            // Test the schema against a valid ChunkerConfig example
            String validExample = """
                {
                    "algorithm": "token",
                    "sourceField": "body",
                    "chunkSize": 500,
                    "chunkOverlap": 50,
                    "preserveUrls": true,
                    "cleanText": true
                }
                """;

            JsonNode exampleNode = objectMapper.readTree(validExample);
            Set<ValidationMessage> validationErrors = validator.validate(exampleNode);

            LOG.infof("Validation errors for valid example: %s", validationErrors);
            assertThat("Valid example should pass validation", validationErrors, is(empty()));

            // Test with an invalid example (missing required fields)
            String invalidExample = """
                {
                    "sourceField": "body"
                }
                """;

            JsonNode invalidNode = objectMapper.readTree(invalidExample);
            Set<ValidationMessage> invalidErrors = validator.validate(invalidNode);

            LOG.infof("Validation errors for invalid example: %s", invalidErrors);
            // We expect validation errors for the invalid example (missing required 'algorithm' and 'chunkSize')
            assertThat("Invalid example should have validation errors", invalidErrors.size(), is(greaterThan(0)));

            // Test with invalid values (chunkSize out of range)
            String invalidRangeExample = """
                {
                    "algorithm": "token",
                    "chunkSize": 25,
                    "sourceField": "body"
                }
                """;

            JsonNode invalidRangeNode = objectMapper.readTree(invalidRangeExample);
            Set<ValidationMessage> rangeErrors = validator.validate(invalidRangeNode);

            LOG.infof("Validation errors for out-of-range example: %s", rangeErrors);
            // We expect validation errors for chunkSize < 50
            assertThat("Out-of-range example should have validation errors", rangeErrors.size(), is(greaterThan(0)));

            LOG.info("TEST 3 PASSED: Schema passes JSON Schema v7 validation and correctly validates examples");

        } catch (Exception e) {
            throw new AssertionError("Schema should be valid for JSON Schema v7 validation: " + e.getMessage(), e);
        }
    }

    /**
     * TEST 4: Verify all validation constraints are properly represented in the schema
     */
    @Test
    public void test4_verifyValidationConstraintsInSchema() {
        LOG.info("=== TEST 4: Verify validation constraints in schema ===");

        String chunkerConfigSchema = extractChunkerConfigSchema();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode schemaNode = objectMapper.readTree(chunkerConfigSchema);
            JsonNode properties = schemaNode.get("properties");

            // Verify chunkSize constraints
            JsonNode chunkSizeProp = properties.get("chunkSize");
            assertThat("ChunkSize should have minimum constraint", chunkSizeProp.has("minimum"), is(true));
            assertThat("ChunkSize minimum should be 50", chunkSizeProp.get("minimum").asInt(), is(equalTo(50)));
            assertThat("ChunkSize should have maximum constraint", chunkSizeProp.has("maximum"), is(true));
            assertThat("ChunkSize maximum should be 10000", chunkSizeProp.get("maximum").asInt(), is(equalTo(10000)));

            // Verify chunkOverlap constraints
            JsonNode chunkOverlapProp = properties.get("chunkOverlap");
            assertThat("ChunkOverlap should have minimum constraint", chunkOverlapProp.has("minimum"), is(true));
            assertThat("ChunkOverlap minimum should be 0", chunkOverlapProp.get("minimum").asInt(), is(equalTo(0)));
            assertThat("ChunkOverlap should have maximum constraint", chunkOverlapProp.has("maximum"), is(true));
            assertThat("ChunkOverlap maximum should be 5000", chunkOverlapProp.get("maximum").asInt(), is(equalTo(5000)));

            // Verify algorithm enum values
            JsonNode algorithmProp = properties.get("algorithm");
            assertThat("Algorithm should have enum values", algorithmProp.has("enum"), is(true));
            JsonNode enumValues = algorithmProp.get("enum");
            assertThat("Algorithm should have 4 enum values", enumValues.size(), is(equalTo(4)));

            // Verify required fields
            if (schemaNode.has("required")) {
                JsonNode requiredFields = schemaNode.get("required");
                LOG.infof("Required fields: %s", requiredFields);
                // algorithm and chunkSize should be required due to @NotNull
                boolean hasAlgorithm = false;
                boolean hasChunkSize = false;
                for (JsonNode field : requiredFields) {
                    String fieldName = field.asText();
                    if ("algorithm".equals(fieldName)) hasAlgorithm = true;
                    if ("chunkSize".equals(fieldName)) hasChunkSize = true;
                }
                assertThat("Algorithm should be required", hasAlgorithm, is(true));
                assertThat("ChunkSize should be required", hasChunkSize, is(true));
            }

            LOG.info("TEST 4 PASSED: All validation constraints are properly represented in schema");

        } catch (Exception e) {
            throw new AssertionError("Failed to verify validation constraints: " + e.getMessage(), e);
        }
    }

    /**
     * TEST 5: Verify config_id is not in schema (node_id is the identifier; opensearch-manager derives field names).
     */
    @Test
    public void test5_configIdNotInSchema() {
        LOG.info("=== TEST 5: Verify config_id is not in ChunkerConfig schema ===");

        String chunkerConfigSchema = extractChunkerConfigSchema();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode schemaNode = objectMapper.readTree(chunkerConfigSchema);
            JsonNode properties = schemaNode.get("properties");

            assertThat("config_id should not be in schema (identifier is node_id)", properties.has("config_id"), is(false));
            LOG.info("TEST 5 PASSED: config_id not present in schema");
        } catch (Exception e) {
            throw new AssertionError("Failed to verify config_id absence: " + e.getMessage(), e);
        }
    }

    /**
     * TEST 6: Test Bean Validation integration with actual ChunkerConfig instances
     */
    @Test
    public void test6_testBeanValidationIntegration() {
        LOG.info("=== TEST 6: Test Bean Validation integration ===");

        // This test verifies that our validation annotations work at runtime
        // We'll inject the Validator and test ChunkerConfig instances

        try {
            // Test valid config
            var validConfig = ai.pipestream.module.chunker.config.ChunkerConfig.createDefault();
            String validationResult = validConfig.validate();
            assertThat("Default config should be valid", validationResult, is(nullValue()));

            // Test invalid chunkSize (too small)
            var invalidConfig1 = ai.pipestream.module.chunker.config.ChunkerConfig.create(
                ai.pipestream.module.chunker.model.ChunkingAlgorithm.TOKEN,
                "body",
                25, // Invalid: below minimum of 50
                20,
                true
            );
            String validation1 = invalidConfig1.validate();
            assertThat("Config with chunkSize < 50 should be invalid", validation1, is(notNullValue()));
            assertThat("Validation message should mention chunkSize range", validation1, containsString("chunkSize must be between 50 and 10000"));

            // Test invalid chunkOverlap (too large)
            var invalidConfig2 = ai.pipestream.module.chunker.config.ChunkerConfig.create(
                ai.pipestream.module.chunker.model.ChunkingAlgorithm.TOKEN,
                "body",
                500,
                6000, // Invalid: above maximum of 5000
                true
            );
            String validation2 = invalidConfig2.validate();
            assertThat("Config with chunkOverlap > 5000 should be invalid", validation2, is(notNullValue()));
            assertThat("Validation message should mention chunkOverlap range", validation2, containsString("chunkOverlap must be between 0 and 5000"));

            // Test cross-field validation (overlap >= chunkSize)
            var invalidConfig3 = ai.pipestream.module.chunker.config.ChunkerConfig.create(
                ai.pipestream.module.chunker.model.ChunkingAlgorithm.TOKEN,
                "body",
                100,
                100, // Invalid: overlap should be < chunkSize
                true
            );
            String validation3 = invalidConfig3.validate();
            assertThat("Config with chunkOverlap >= chunkSize should be invalid", validation3, is(notNullValue()));
            assertThat("Validation message should mention overlap vs size relationship", validation3, containsString("chunkOverlap must be less than chunkSize"));

            LOG.info("TEST 6 PASSED: Bean Validation integration works correctly");

        } catch (Exception e) {
            throw new AssertionError("Bean Validation integration test failed: " + e.getMessage(), e);
        }
    }

    /**
     * Helper method to extract ChunkerConfig schema as JSON string
     */
    private String extractChunkerConfigSchema() {
        OpenApiDocumentService documentService = Arc.container()
                .instance(OpenApiDocumentService.class).get();

        byte[] jsonBytes = documentService.getDocument(OpenApiConstants.DEFAULT_DOCUMENT_NAME, Format.JSON);
        String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);

        try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
            JsonObject openApiDoc = reader.readObject();
            JsonObject components = openApiDoc.getJsonObject("components");
            JsonObject schemas = components.getJsonObject("schemas");
            JsonObject chunkerConfigSchema = schemas.getJsonObject("ChunkerConfig");

            return chunkerConfigSchema.toString();
        }
    }
}
