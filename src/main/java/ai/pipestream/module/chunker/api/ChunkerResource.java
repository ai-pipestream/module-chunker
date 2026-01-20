package ai.pipestream.module.chunker.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;

@Path("/api/chunker")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Chunker Service", description = "Test endpoint for the Chunker gRPC module")
@ApplicationScoped
public class ChunkerResource {

    private static final Logger LOG = Logger.getLogger(ChunkerResource.class);

    @Inject
    @GrpcService
    PipeStepProcessorService chunkerService;

    @Inject
    ObjectMapper objectMapper;


    @POST
    @Path("/process")
    @Operation(summary = "Process a document through the Chunker service",
               description = "Sends a document to the Chunker gRPC service which will split it into chunks")
    @APIResponses({
        @APIResponse(responseCode = "200",
                     description = "Document processed successfully",
                     content = @Content(mediaType = MediaType.APPLICATION_JSON)),
        @APIResponse(responseCode = "400",
                     description = "Bad request - invalid input",
                     content = @Content(mediaType = MediaType.APPLICATION_JSON)),
        @APIResponse(responseCode = "500",
                     description = "Internal server error",
                     content = @Content(mediaType = MediaType.APPLICATION_JSON))
    })
    public Uni<Response> processDocument(
            @RequestBody(
                description = "The document to be processed by the chunker.",
                required = true,
                content = @Content(
                    schema = @Schema(implementation = ChunkerRequest.class),
                    examples = @ExampleObject(
                        name = "chunker-request-example",
                        summary = "Example Chunker Request",
                        description = "A complete example of a request to the chunker service, including metadata and custom configuration.",
                        value = "{\n" +
                                "  \"id\": \"doc-12345\",\n" +
                                "  \"type\": \"text/plain\",\n" +
                                "  \"content\": \"This is a long piece of text that will be broken down into smaller chunks by the service based on the provided configuration.\",\n" +
                                "  \"metadata\": {\n" +
                                "    \"source\": \"/path/to/document.txt\",\n" +
                                "    \"author\": \"Jane Doe\"\n" +
                                "  },\n" +
                                "  \"customConfig\": {\n" +
                                "    \"chunkSize\": 256,\n" +
                                "    \"overlap\": 32\n" +
                                "  }\n" +
                                "}"
                    )
                )
            )
            Map<String, Object> input) {
        LOG.debugf("REST endpoint received: %s", input);

        try {
            // Build SearchMetadata from input
            ai.pipestream.data.v1.SearchMetadata.Builder searchMetadataBuilder = ai.pipestream.data.v1.SearchMetadata.newBuilder()
                    .setDocumentType(input.getOrDefault("type", "text/plain").toString());

            // Add content if provided
            if (input.containsKey("content")) {
                searchMetadataBuilder.setBody(input.get("content").toString());
            }

            // Add metadata if provided
            if (input.containsKey("metadata") && input.get("metadata") instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, String> metadata = (Map<String, String>) input.get("metadata");
                searchMetadataBuilder.putAllMetadata(metadata);
            }

            // Build PipeDoc with nested structure
            PipeDoc.Builder docBuilder = PipeDoc.newBuilder()
                    .setDocId(input.getOrDefault("id", "test-doc-" + System.currentTimeMillis()).toString())
                    .setSearchMetadata(searchMetadataBuilder.build());

            // Build configuration
            ProcessConfiguration.Builder configBuilder = ProcessConfiguration.newBuilder();

            // Add custom JSON config if provided
            if (input.containsKey("customConfig") && input.get("customConfig") instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> customConfig = (Map<String, Object>) input.get("customConfig");

                // Convert to Protobuf Struct
                String jsonString = objectMapper.writeValueAsString(customConfig);
                Struct.Builder structBuilder = Struct.newBuilder();
                JsonFormat.parser().merge(jsonString, structBuilder);

                configBuilder.setJsonConfig(structBuilder.build());
            }

            // Build the request
            ProcessDataRequest request = ProcessDataRequest.newBuilder()
                    .setDocument(docBuilder.build())
                    .setConfig(configBuilder.build())
                    .setMetadata(ServiceMetadata.newBuilder()
                            .setPipelineName("web-test")
                            .setPipeStepName("chunker")
                            .setStreamId("web-" + System.currentTimeMillis())
                            .setCurrentHopNumber(1)
                            .build())
                    .build();

            // Call the gRPC service
            return chunkerService.processData(request)
                    .map(response -> {
                        try {
                            // Convert response to JSON
                            String jsonResponse = JsonFormat.printer()
                                    .includingDefaultValueFields()
                                    .print(response);

                            return Response.ok(jsonResponse).build();
                        } catch (Exception e) {
                            LOG.error("Failed to convert response to JSON", e);
                            return Response.serverError()
                                    .entity(Map.of("error", "Failed to format response"))
                                    .build();
                        }
                    })
                    .onFailure().recoverWithItem(throwable -> {
                        LOG.error("Failed to process document", throwable);
                        return Response.serverError()
                                .entity(Map.of("error", throwable.getMessage()))
                                .build();
                    });

        } catch (Exception e) {
            LOG.error("Failed to build request", e);
            return Uni.createFrom().item(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity(Map.of("error", e.getMessage()))
                            .build()
            );
        }
    }

    @GET
    @Path("/info")
    @Operation(summary = "Get Chunker service information",
               description = "Returns basic information about the Chunker module including configuration schema")
    @APIResponse(responseCode = "200",
                 description = "Service information retrieved successfully",
                 content = @Content(mediaType = MediaType.APPLICATION_JSON))
    public Uni<Response> getServiceInfo() {
        return chunkerService.getServiceRegistration(GetServiceRegistrationRequest.newBuilder().build())
                .map(registration -> {
                    Map<String, Object> info = new HashMap<>();
                    info.put("moduleName", registration.getModuleName());
                    info.put("hasSchema", !registration.getJsonConfigSchema().isEmpty());

                    // Parse and include the schema if available
                    if (!registration.getJsonConfigSchema().isEmpty()) {
                        try {
                            Map<String, Object> schema = objectMapper.readValue(
                                registration.getJsonConfigSchema(),
                                new TypeReference<Map<String, Object>>() {}
                            );
                            info.put("schema", schema);
                        } catch (Exception e) {
                            LOG.warn("Failed to parse schema", e);
                            info.put("schemaRaw", registration.getJsonConfigSchema());
                        }
                    }

                    return Response.ok(info).build();
                })
                .onFailure().recoverWithItem(throwable ->
                    Response.serverError()
                            .entity(Map.of("error", throwable.getMessage()))
                            .build()
                );
    }


    @GET
    @Path("/health")
    @Operation(summary = "Health check",
               description = "Simple health check endpoint")
    @APIResponse(responseCode = "200",
                 description = "Service is healthy",
                 content = @Content(mediaType = MediaType.APPLICATION_JSON))
    public Response health() {
        return Response.ok(Map.of("status", "UP")).build();
    }

    // Schema classes for OpenAPI documentation
    @Schema(name = "ChunkerRequest", description = "Request payload for chunker processing")
    public static class ChunkerRequest {
        @Schema(description = "A unique identifier for the document.",
                required = true)
        public String id;

        @Schema(description = "The MIME type of the document.",
                required = true)
        public String type;

        @Schema(description = "The full text content of the document to be chunked.",
                required = true)
        public String content;

        @Schema(description = "A map of key-value pairs for document metadata.")
        public Map<String, String> metadata;

        @Schema(description = "A map of custom configuration options for the chunking process.")
        public Map<String, Object> customConfig;
    }
}
