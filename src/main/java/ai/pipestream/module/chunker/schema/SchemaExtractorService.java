package ai.pipestream.module.chunker.schema;

import io.quarkus.smallrye.openapi.runtime.OpenApiConstants;
import io.quarkus.smallrye.openapi.runtime.OpenApiDocumentService;
import io.smallrye.openapi.runtime.io.Format;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.*;
import org.jboss.logging.Logger;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Service for extracting OpenAPI schema components from the dynamically generated OpenAPI document.
 * This service provides a centralized way to access schema definitions for validation and registration.
 */
@ApplicationScoped
public class SchemaExtractorService {

    private static final Logger LOG = Logger.getLogger(SchemaExtractorService.class);

    private final Instance<OpenApiDocumentService> openApiDocumentService;

    @Inject
    public SchemaExtractorService(Instance<OpenApiDocumentService> openApiDocumentService) {
        this.openApiDocumentService = openApiDocumentService;
    }

    /**
     * Extracts a specific schema component from the OpenAPI document by name.
     *
     * @param schemaName The name of the schema to extract (e.g., "ChunkerConfig")
     * @return Optional containing the schema as JSON string if found, empty if not found
     */
    public Optional<String> extractSchemaByName(String schemaName) {
        LOG.debugf("Extracting schema for: %s", schemaName);

        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;

            if (documentService == null) {
                LOG.warnf("OpenApiDocumentService not available - schema extraction failed for: %s", schemaName);
                return Optional.empty();
            }

            byte[] jsonBytes = documentService.getDocument(OpenApiConstants.DEFAULT_DOCUMENT_NAME, Format.JSON);
            if (jsonBytes == null || jsonBytes.length == 0) {
                LOG.warnf("OpenAPI document is empty - schema extraction failed for: %s", schemaName);
                return Optional.empty();
            }

            String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
            LOG.debugf("OpenAPI document size: %d characters", jsonString.length());

            try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
                JsonObject openApiDoc = reader.readObject();

                JsonObject components = openApiDoc.getJsonObject("components");
                if (components == null) {
                    LOG.debugf("No components section found in OpenAPI document for schema: %s", schemaName);
                    return Optional.empty();
                }

                JsonObject schemas = components.getJsonObject("schemas");
                if (schemas == null) {
                    LOG.debugf("No schemas section found in OpenAPI components for schema: %s", schemaName);
                    return Optional.empty();
                }

                JsonObject targetSchema = schemas.getJsonObject(schemaName);
                if (targetSchema == null) {
                    LOG.debugf("Schema '%s' not found in OpenAPI schemas section", schemaName);
                    return Optional.empty();
                }

                String extractedSchema = targetSchema.toString();
                LOG.debugf("Successfully extracted schema '%s' (%d characters)", schemaName, extractedSchema.length());

                return Optional.of(extractedSchema);
            }

        } catch (Exception e) {
            LOG.errorf(e, "Error extracting schema '%s' from OpenAPI document", schemaName);
            return Optional.empty();
        }
    }

    /**
     * Convenience method to extract the ChunkerConfig schema specifically.
     *
     * @return Optional containing ChunkerConfig schema as JSON string if found
     */
    public Optional<String> extractChunkerConfigSchema() {
        return extractSchemaByName("ChunkerConfig");
    }

    /**
     * Extracts ChunkerConfig schema and cleans it for JSON Schema v7 validation.
     *
     * @return Optional containing JSON Schema v7 compatible schema
     */
    public Optional<String> extractChunkerConfigSchemaForValidation() {
        return extractSchemaByName("ChunkerConfig").map(this::cleanSchemaForJsonSchemaV7);
    }

    /**
     * Cleans an OpenAPI schema to make it compatible with JSON Schema v7 validation.
     */
    private String cleanSchemaForJsonSchemaV7(String openApiSchema) {
        try (JsonReader reader = Json.createReader(new StringReader(openApiSchema))) {
            JsonObject schema = reader.readObject();
            return cleanJsonObjectForValidation(schema).toString();
        } catch (Exception e) {
            LOG.warnf(e, "Failed to clean schema for JSON Schema v7 validation, returning original");
            return openApiSchema;
        }
    }

    /**
     * Recursively cleans a JsonObject by removing OpenAPI-specific elements.
     */
    private JsonObject cleanJsonObjectForValidation(JsonObject obj) {
        var builder = Json.createObjectBuilder();

        obj.forEach((key, value) -> {
            if (key.startsWith("x-")) {
                return;
            }
            if ("$ref".equals(key)) {
                return;
            }
            if (value instanceof JsonObject) {
                builder.add(key, cleanJsonObjectForValidation((JsonObject) value));
            } else if (value instanceof JsonArray) {
                JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                ((JsonArray) value).forEach(arrayValue -> {
                    if (arrayValue instanceof JsonObject) {
                        arrayBuilder.add(cleanJsonObjectForValidation((JsonObject) arrayValue));
                    } else {
                        arrayBuilder.add(arrayValue);
                    }
                });
                builder.add(key, arrayBuilder.build());
            } else {
                builder.add(key, value);
            }
        });

        return builder.build();
    }

    /**
     * Extracts a schema and resolves OpenAPI $ref references against components.schemas,
     * returning a JSON Schema object suitable for JSONForms rendering.
     */
    public Optional<String> extractSchemaResolvedForJsonForms(String schemaName) {
        LOG.debugf("Extracting JSONForms-ready schema for: %s", schemaName);
        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;
            if (documentService == null) {
                LOG.warn("OpenApiDocumentService not available - cannot resolve refs");
                return Optional.empty();
            }

            byte[] jsonBytes = documentService.getDocument(OpenApiConstants.DEFAULT_DOCUMENT_NAME, Format.JSON);
            if (jsonBytes == null || jsonBytes.length == 0) {
                LOG.warn("OpenAPI document is empty - cannot resolve refs");
                return Optional.empty();
            }

            String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
            try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
                JsonObject openApiDoc = reader.readObject();
                JsonObject components = openApiDoc.getJsonObject("components");
                if (components == null) return Optional.empty();
                JsonObject schemas = components.getJsonObject("schemas");
                if (schemas == null) return Optional.empty();
                JsonObject target = schemas.getJsonObject(schemaName);
                if (target == null) return Optional.empty();

                JsonObject resolved = resolveRefsAndClean(target, schemas);
                JsonObject withMeta = addDraft7MetaIfMissing(resolved);
                String resolvedString = withMeta.toString();
                LOG.debugf("Resolved schema '%s' for JSONForms (%d chars)", schemaName, resolvedString.length());
                return Optional.of(resolvedString);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Error resolving schema '%s' for JSONForms", schemaName);
            return Optional.empty();
        }
    }

    /**
     * Convenience method for ChunkerConfig specifically.
     */
    public Optional<String> extractChunkerConfigSchemaResolvedForJsonForms() {
        return extractSchemaResolvedForJsonForms("ChunkerConfig");
    }

    private JsonObject resolveRefsAndClean(JsonObject node, JsonObject componentsSchemas) {
        if (node.containsKey("$ref")) {
            String ref = node.getString("$ref", null);
            JsonObject base = resolveRef(ref, componentsSchemas).orElse(Json.createObjectBuilder().build());
            JsonObject resolvedBase = resolveRefsAndClean(base, componentsSchemas);

            var builder = Json.createObjectBuilder();
            resolvedBase.forEach(builder::add);

            for (String key : node.keySet()) {
                if ("$ref".equals(key) || key.startsWith("x-")) continue;
                var value = node.get(key);
                if (value instanceof JsonObject) {
                    builder.add(key, resolveRefsAndClean((JsonObject) value, componentsSchemas));
                } else if (value instanceof JsonArray) {
                    builder.add(key, resolveArray((JsonArray) value, componentsSchemas));
                } else {
                    builder.add(key, value);
                }
            }
            return builder.build();
        }

        var builder = Json.createObjectBuilder();
        for (String key : node.keySet()) {
            if (key.startsWith("x-")) continue;
            var value = node.get(key);
            if (value instanceof JsonObject) {
                builder.add(key, resolveRefsAndClean((JsonObject) value, componentsSchemas));
            } else if (value instanceof JsonArray) {
                builder.add(key, resolveArray((JsonArray) value, componentsSchemas));
            } else {
                builder.add(key, value);
            }
        }
        return builder.build();
    }

    private JsonArray resolveArray(JsonArray array, JsonObject componentsSchemas) {
        JsonArrayBuilder ab = Json.createArrayBuilder();
        for (var v : array) {
            if (v instanceof JsonObject) {
                ab.add(resolveRefsAndClean((JsonObject) v, componentsSchemas));
            } else {
                ab.add(v);
            }
        }
        return ab.build();
    }

    private Optional<JsonObject> resolveRef(String ref, JsonObject componentsSchemas) {
        if (ref == null) return Optional.empty();
        final String prefix = "#/components/schemas/";
        if (ref.startsWith(prefix)) {
            String name = ref.substring(prefix.length());
            JsonObject target = componentsSchemas.getJsonObject(name);
            if (target != null) return Optional.of(target);
        }
        LOG.debugf("Unsupported or unresolved $ref: %s", ref);
        return Optional.empty();
    }

    private JsonObject addDraft7MetaIfMissing(JsonObject obj) {
        if (obj.containsKey("$schema")) return obj;
        var builder = Json.createObjectBuilder();
        builder.add("$schema", "http://json-schema.org/draft-07/schema#");
        obj.forEach(builder::add);
        return builder.build();
    }
}
