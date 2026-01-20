package ai.pipestream.module.chunker.api;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.chunker.api.dto.AdvancedChunkRequest;
import ai.pipestream.module.chunker.api.dto.ChunkResponse;
import ai.pipestream.module.chunker.api.dto.SimpleChunkRequest;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.demo.DemoDocumentService;
import ai.pipestream.module.chunker.demo.DocumentMetadata;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.service.OverlapChunker;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * REST API endpoints for chunker service.
 * Provides developer-friendly HTTP endpoints for testing and integration.
 */
@Path("/api/chunker/service")
@Tag(name = "Chunker Service", description = "Text chunking operations for document processing")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ChunkerServiceEndpoint {

    private static final Logger LOG = Logger.getLogger(ChunkerServiceEndpoint.class);

    @Inject
    OverlapChunker overlapChunker;

    @Inject
    Tokenizer tokenizer;

    @Inject
    SentenceDetector sentenceDetector;
    
    @Inject
    DemoDocumentService demoDocumentService;
    
    @Inject
    ai.pipestream.module.chunker.schema.SchemaExtractorService schemaExtractorService;
    
    @Inject
    ObjectMapper objectMapper;

    @POST
    @Path("/simple")
    @Operation(summary = "Simple text chunking", description = "Chunk plain text with basic options")
    @APIResponse(responseCode = "200", description = "Chunking successful", 
                 content = @Content(schema = @Schema(implementation = ChunkResponse.class)))
    @APIResponse(responseCode = "400", description = "Invalid request")
    @APIResponse(responseCode = "500", description = "Internal server error")
    public Uni<Response> simpleChunk(SimpleChunkRequest request) {
        LOG.debugf("Simple chunking request received - text length: %d, chunkSize: %d", 
                 request.getText() != null ? request.getText().length() : 0, request.getChunkSize());
        
        long startTime = System.currentTimeMillis();
        
        // Validate input
        if (request.getText() == null || request.getText().trim().isEmpty()) {
            return Uni.createFrom().item(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ChunkResponse(false, "Text cannot be null or empty"))
                    .build()
            );
        }

        return Uni.createFrom().item(() -> {
            // Create a simple PipeDoc
            String docId = "simple-doc-" + UUID.randomUUID().toString();
            PipeDoc document = PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(ai.pipestream.data.v1.SearchMetadata.newBuilder()
                    .setTitle("Simple Text Document")
                    .setBody(request.getText())
                    .build())
                .build();

            // Create ChunkerConfig for better ID generation
            ChunkerConfig config = ChunkerConfig.create(
                request.getAlgorithm() != null ? request.getAlgorithm() : ChunkerConfig.DEFAULT_ALGORITHM,
                request.getSourceField() != null ? request.getSourceField() : ChunkerConfig.DEFAULT_SOURCE_FIELD,
                request.getChunkSize(),
                request.getChunkOverlap(),
                request.getPreserveUrls()
            );

            // Perform chunking with ChunkerConfig
            String streamId = "simple-stream-" + UUID.randomUUID().toString();
            ChunkingResult result = overlapChunker.createChunks(document, config, streamId, "simple-chunker");
            return new Object[] { result, config };
        })
        .map(resultArray -> {
            // Extract result and config from array
            Object[] dataArray = (Object[]) resultArray;
            ChunkingResult result = (ChunkingResult) dataArray[0];
            ChunkerConfig configUsed = (ChunkerConfig) dataArray[1];
            
            // Convert to response format
            List<ChunkResponse.ChunkDto> chunkDtos = result.chunks().stream()
                .map(chunk -> new ChunkResponse.ChunkDto(
                    chunk.id(),
                    chunk.text(),
                    chunk.originalIndexStart(),
                    chunk.originalIndexEnd()
                ))
                .collect(Collectors.toList());

            long processingTime = System.currentTimeMillis() - startTime;
            ChunkResponse.ChunkingMetadata metadata = new ChunkResponse.ChunkingMetadata(
                chunkDtos.size(),
                processingTime,
                request.getText().length(),
                getTokenizerType(),
                getSentenceDetectorType(),
                configUsed
            );

            ChunkResponse response = new ChunkResponse(true, chunkDtos, metadata);
            LOG.debugf("Simple chunking completed - generated %d chunks in %dms", chunkDtos.size(), processingTime);
            
            return Response.ok(response).build();
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.error("Error during simple chunking", throwable);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new ChunkResponse(false, "Chunking failed: " + throwable.getMessage()))
                .build();
        });
    }

    @POST
    @Path("/advanced")
    @Operation(summary = "Advanced document chunking", description = "Chunk PipeDoc with full configuration options")
    @APIResponse(responseCode = "200", description = "Chunking successful",
                 content = @Content(schema = @Schema(implementation = ChunkResponse.class)))
    @APIResponse(responseCode = "400", description = "Invalid request")
    @APIResponse(responseCode = "500", description = "Internal server error")
    public Uni<Response> advancedChunk(AdvancedChunkRequest request) {
        LOG.debugf("Advanced chunking request received - docId: %s, sourceField: %s", 
                 request.getDocument() != null ? request.getDocument().getId() : "null",
                 request.getOptions() != null ? request.getOptions().getSourceField() : "null");
        
        long startTime = System.currentTimeMillis();
        
        // Validate input
        if (request.getDocument() == null) {
            return Uni.createFrom().item(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ChunkResponse(false, "Document cannot be null"))
                    .build()
            );
        }

        if (request.getOptions() == null) {
            return Uni.createFrom().item(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ChunkResponse(false, "Options cannot be null"))
                    .build()
            );
        }

        return Uni.createFrom().item(() -> {
            // Convert DTO to PipeDoc
            AdvancedChunkRequest.DocumentDto docDto = request.getDocument();
            
            ai.pipestream.data.v1.SearchMetadata.Builder searchMetadataBuilder = ai.pipestream.data.v1.SearchMetadata.newBuilder();
            if (docDto.getTitle() != null) {
                searchMetadataBuilder.setTitle(docDto.getTitle());
            }
            if (docDto.getBody() != null) {
                searchMetadataBuilder.setBody(docDto.getBody());
            }
            if (docDto.getMetadata() != null) {
                searchMetadataBuilder.putAllMetadata(docDto.getMetadata());
            }
            
            PipeDoc document = PipeDoc.newBuilder()
                .setDocId(docDto.getId() != null ? docDto.getId() : "advanced-doc-" + UUID.randomUUID().toString())
                .setSearchMetadata(searchMetadataBuilder.build())
                .build();

            // Convert DTO to ChunkerConfig for better ID generation
            AdvancedChunkRequest.ChunkerOptionsDto optionsDto = request.getOptions();
            ChunkerConfig config = ChunkerConfig.create(
                ChunkerConfig.DEFAULT_ALGORITHM, // Use default algorithm for advanced requests
                optionsDto.getSourceField(),
                optionsDto.getChunkSize(),
                optionsDto.getChunkOverlap(),
                optionsDto.getPreserveUrls()
            );

            // Get metadata
            String streamId = "advanced-stream-" + UUID.randomUUID().toString();
            String pipeStepName = "advanced-chunker";
            if (request.getMetadata() != null) {
                if (request.getMetadata().getStreamId() != null) {
                    streamId = request.getMetadata().getStreamId();
                }
                if (request.getMetadata().getPipeStepName() != null) {
                    pipeStepName = request.getMetadata().getPipeStepName();
                }
            }

            // Perform chunking with ChunkerConfig
            ChunkingResult result = overlapChunker.createChunks(document, config, streamId, pipeStepName);
            
            return new Object[] { result, document, optionsDto };
        })
        .map(data -> {
            ChunkingResult result = (ChunkingResult) ((Object[]) data)[0];
            PipeDoc document = (PipeDoc) ((Object[]) data)[1];
            AdvancedChunkRequest.ChunkerOptionsDto optionsDto = (AdvancedChunkRequest.ChunkerOptionsDto) ((Object[]) data)[2];
            
            // Convert to response format
            List<ChunkResponse.ChunkDto> chunkDtos = result.chunks().stream()
                .map(chunk -> new ChunkResponse.ChunkDto(
                    chunk.id(),
                    chunk.text(),
                    chunk.originalIndexStart(),
                    chunk.originalIndexEnd()
                ))
                .collect(Collectors.toList());

            long processingTime = System.currentTimeMillis() - startTime;
            
            // Calculate original text length based on source field
            int originalTextLength = getTextLengthFromDocument(document, optionsDto.getSourceField());
            
            ChunkResponse.ChunkingMetadata metadata = new ChunkResponse.ChunkingMetadata(
                chunkDtos.size(),
                processingTime,
                originalTextLength,
                getTokenizerType(),
                getSentenceDetectorType()
            );

            ChunkResponse response = new ChunkResponse(true, chunkDtos, metadata);
            LOG.debugf("Advanced chunking completed - generated %d chunks in %dms", chunkDtos.size(), processingTime);
            
            return Response.ok(response).build();
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.error("Error during advanced chunking", throwable);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new ChunkResponse(false, "Chunking failed: " + throwable.getMessage()))
                .build();
        });
    }

    @POST
    @Path("/test")
    @Operation(summary = "Quick text test", description = "Test chunker with plain text using default settings")
    @Consumes(MediaType.TEXT_PLAIN)
    @APIResponse(responseCode = "200", description = "Test successful")
    public Uni<Response> testChunk(String text) {
        LOG.debugf("Test chunking request received - text length: %d", text != null ? text.length() : 0);
        
        if (text == null || text.trim().isEmpty()) {
            return Uni.createFrom().item(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ChunkResponse(false, "Text cannot be null or empty"))
                    .build()
            );
        }

        // Use default settings for test
        SimpleChunkRequest testRequest = new SimpleChunkRequest(text, 300, 30, true);
        return simpleChunk(testRequest);
    }

    @GET
    @Path("/config")
    @Operation(summary = "Get chunker configuration schema", description = "Retrieve the OpenAPI 3.1 JSON Schema for ChunkerConfig")
    @APIResponse(
        responseCode = "200", 
        description = "OpenAPI 3.1 JSON Schema for ChunkerConfig",
        content = @Content(
            mediaType = MediaType.APPLICATION_JSON,
            schema = @Schema(implementation = ChunkerConfig.class)
        )
    )
    public Uni<Response> getConfig() {
        LOG.debug("Configuration schema request received");
        
        return Uni.createFrom().item(() -> {
            // Extract the cleaned schema for frontend validation (removes $ref and x-* extensions)
            Optional<String> schemaOptional = schemaExtractorService.extractChunkerConfigSchemaForValidation();
            
            if (schemaOptional.isPresent()) {
                String schemaJson = schemaOptional.get();
                LOG.debugf("Successfully returning ChunkerConfig schema (%d characters)", schemaJson.length());
                
                // Return the JSON string directly - Quarkus JAX-RS handles this perfectly
                return Response.ok(schemaJson)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
            } else {
                LOG.warn("Could not extract ChunkerConfig schema from OpenAPI document");
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(Map.of("error", "Schema not available - check OpenAPI document generation"))
                    .build();
            }
        });
    }

    @GET
    @Path("/health")
    @Operation(summary = "Health check", description = "Check chunker service health and model status")
    @APIResponse(responseCode = "200", description = "Health check successful")
    public Uni<Response> healthCheck() {
        LOG.debug("Health check request received");
        
        return Uni.createFrom().item(() -> {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "healthy");
            health.put("version", "1.0.0");
            
            Map<String, String> models = new HashMap<>();
            models.put("sentence", getSentenceDetectorType());
            models.put("tokenizer", getTokenizerType());
            health.put("models", models);
            
            return health;
        })
        .map(health -> Response.ok(health).build());
    }

    // ================================
    // FORM-BASED ENDPOINTS (for Swagger UI)
    // ================================

    @POST
    @Path("/simple-form")
    @Operation(summary = "Simple text chunking (Form)", description = "Chunk plain text using form inputs - perfect for Swagger UI testing")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @APIResponse(responseCode = "200", description = "Chunking successful", 
                 content = @Content(schema = @Schema(implementation = ChunkResponse.class)))
    @APIResponse(responseCode = "400", description = "Invalid input")
    @APIResponse(responseCode = "500", description = "Internal server error")
    public Uni<Response> simpleChunkForm(
            @FormParam("text") 
            @Schema(description = "Text content to chunk (can be large blocks of text)", example = "This is a large block of text that will be chunked into smaller pieces...")
            String text,
            
            @FormParam("algorithm")
            @DefaultValue("token")
            @Schema(description = "Chunking algorithm to use", enumeration = {"character", "token", "sentence", "semantic"}, defaultValue = "token")
            String algorithmStr,
            
            @FormParam("chunkSize") 
            @DefaultValue("500")
            @Schema(description = "Target character size for each chunk", example = "500", minimum = "50", maximum = "10000")
            Integer chunkSize,
            
            @FormParam("chunkOverlap") 
            @DefaultValue("50")
            @Schema(description = "Character overlap between consecutive chunks", example = "50", minimum = "0", maximum = "5000")
            Integer chunkOverlap,
            
            @FormParam("preserveUrls") 
            @DefaultValue("true")
            @Schema(description = "Whether to preserve URLs during chunking", example = "true")
            Boolean preserveUrls) {
        
        LOG.debugf("Form-based simple chunking request - text length: %d, algorithm: %s, chunkSize: %d", 
                 text != null ? text.length() : 0, algorithmStr, chunkSize);

        // Convert form data to request object
        ChunkingAlgorithm algorithm;
        try {
            algorithm = ChunkingAlgorithm.fromValue(algorithmStr);
        } catch (IllegalArgumentException e) {
            LOG.errorf("Invalid algorithm specified: %s. Valid algorithms: character, token, sentence, semantic", algorithmStr);
            return Uni.createFrom().item(
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ChunkResponse(false, "Invalid algorithm: " + algorithmStr + ". Valid algorithms: character, token, sentence, semantic"))
                    .build()
            );
        }
        
        SimpleChunkRequest request = new SimpleChunkRequest(text, algorithm, "body", chunkSize, chunkOverlap, preserveUrls);
        return simpleChunk(request);
    }

    @POST
    @Path("/advanced-form")
    @Operation(summary = "Advanced document chunking (Form)", description = "Advanced chunking with full configuration using form inputs")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @APIResponse(responseCode = "200", description = "Chunking successful",
                 content = @Content(schema = @Schema(implementation = ChunkResponse.class)))
    @APIResponse(responseCode = "400", description = "Invalid input")
    @APIResponse(responseCode = "500", description = "Internal server error")
    public Uni<Response> advancedChunkForm(
            @FormParam("documentId")
            @DefaultValue("form-doc")
            @Schema(description = "Document ID", example = "my-test-document")
            String documentId,
            
            @FormParam("title")
            @Schema(description = "Document title (optional)", example = "My Test Document")
            String title,
            
            @FormParam("body")
            @Schema(description = "Document body content", example = "This is the main content of the document that will be chunked...")
            String body,
            
            @FormParam("sourceField")
            @DefaultValue("body")
            @Schema(description = "Field to extract text from (body, title, or id)", example = "body")
            String sourceField,
            
            @FormParam("chunkSize")
            @DefaultValue("500")
            @Schema(description = "Target character size for each chunk", example = "500", minimum = "50", maximum = "10000")
            Integer chunkSize,
            
            @FormParam("chunkOverlap")
            @DefaultValue("50")
            @Schema(description = "Character overlap between consecutive chunks", example = "50", minimum = "0", maximum = "5000")
            Integer chunkOverlap,
            
            @FormParam("preserveUrls")
            @DefaultValue("true")
            @Schema(description = "Whether to preserve URLs during chunking", example = "true")
            Boolean preserveUrls,
            
            @FormParam("chunkIdTemplate")
            @DefaultValue("form-%s-%s-%d")
            @Schema(description = "Template for chunk IDs", example = "form-%s-%s-%d")
            String chunkIdTemplate,
            
            @FormParam("streamId")
            @DefaultValue("form-stream")
            @Schema(description = "Stream ID for processing", example = "my-test-stream")
            String streamId,
            
            @FormParam("pipeStepName")
            @DefaultValue("form-chunker")
            @Schema(description = "Pipeline step name", example = "my-chunker-step")
            String pipeStepName) {
        
        LOG.debugf("Form-based advanced chunking request - docId: %s, sourceField: %s", documentId, sourceField);

        // Build advanced request from form data
        AdvancedChunkRequest.DocumentDto documentDto = new AdvancedChunkRequest.DocumentDto(
            documentId, title, body, null // metadata can be null for form input
        );
        
        AdvancedChunkRequest.ChunkerOptionsDto optionsDto = new AdvancedChunkRequest.ChunkerOptionsDto();
        optionsDto.setSourceField(sourceField);
        optionsDto.setChunkSize(chunkSize);
        optionsDto.setChunkOverlap(chunkOverlap);
        optionsDto.setPreserveUrls(preserveUrls);
        optionsDto.setChunkIdTemplate(chunkIdTemplate);
        
        AdvancedChunkRequest.RequestMetadata metadata = new AdvancedChunkRequest.RequestMetadata(streamId, pipeStepName);
        
        AdvancedChunkRequest request = new AdvancedChunkRequest(documentDto, optionsDto, metadata);
        return advancedChunk(request);
    }

    @POST
    @Path("/quick-test-form")
    @Operation(summary = "Quick text test (Form)", description = "Fastest way to test chunking - just paste your text!")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @APIResponse(responseCode = "200", description = "Test successful")
    @APIResponse(responseCode = "400", description = "Invalid input")
    public Uni<Response> quickTestForm(
            @FormParam("text")
            @Schema(description = "Text to chunk (paste any amount of text here)", 
                   example = "Paste your large block of text here. It can have multiple paragraphs, quotes, and any content you want to test with the chunker.")
            String text) {
        
        LOG.debugf("Quick test form request - text length: %d", text != null ? text.length() : 0);
        
        return testChunk(text);
    }

    // ================================
    // DEMO DOCUMENTS ENDPOINTS
    // ================================

    @GET
    @Path("/demo/documents")
    @Operation(summary = "List demo documents", description = "Get all available demo documents with metadata")
    @APIResponse(responseCode = "200", description = "Demo documents retrieved successfully")
    public Uni<Response> getDemoDocuments() {
        LOG.debug("Demo documents list request received");
        
        return Uni.createFrom().item(() -> {
            List<DocumentMetadata> documents = demoDocumentService.getAllDocuments();
            
            Map<String, Object> response = new HashMap<>();
            response.put("documents", documents);
            response.put("count", documents.size());
            response.put("description", "Demo documents available for chunking tests");
            
            return response;
        })
        .map(response -> Response.ok(response).build());
    }

    @GET
    @Path("/demo/documents/{filename}")
    @Operation(summary = "Get demo document content", description = "Retrieve content of a specific demo document")
    @APIResponse(responseCode = "200", description = "Document content retrieved successfully")
    @APIResponse(responseCode = "404", description = "Document not found")
    public Uni<Response> getDemoDocument(@PathParam("filename") String filename) {
        LOG.debugf("Demo document content request for: %s", filename);
        
        return Uni.createFrom().item(() -> {
            Optional<String> contentOpt = demoDocumentService.getDocumentContent(filename);
            Optional<DocumentMetadata> metadataOpt = demoDocumentService.getDocumentByFilename(filename);
            
            if (contentOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Document not found: " + filename))
                    .build();
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("filename", filename);
            response.put("content", contentOpt.get());
            response.put("contentLength", contentOpt.get().length());
            
            if (metadataOpt.isPresent()) {
                response.put("metadata", metadataOpt.get());
            }
            
            return Response.ok(response).build();
        });
    }

    @POST
    @Path("/demo/chunk/{filename}")
    @Operation(summary = "Chunk demo document", description = "Chunk a demo document using its recommended settings or custom parameters")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @APIResponse(responseCode = "200", description = "Document chunked successfully")
    @APIResponse(responseCode = "404", description = "Document not found")
    public Uni<Response> chunkDemoDocument(
            @PathParam("filename") String filename,
            
            @FormParam("algorithm")
            @Schema(description = "Override the recommended algorithm", enumeration = {"character", "token", "sentence", "semantic"})
            String algorithmOverride,
            
            @FormParam("chunkSize")
            @Schema(description = "Override the recommended chunk size", minimum = "50", maximum = "10000")
            Integer chunkSizeOverride,
            
            @FormParam("chunkOverlap")
            @Schema(description = "Override the recommended chunk overlap", minimum = "0", maximum = "5000")
            Integer chunkOverlapOverride,
            
            @FormParam("preserveUrls")
            @DefaultValue("true")
            @Schema(description = "Whether to preserve URLs during chunking")
            Boolean preserveUrls,
            
            @FormParam("useRecommended")
            @DefaultValue("true")
            @Schema(description = "Use recommended settings from metadata")
            Boolean useRecommended) {
        
        LOG.debugf("Demo document chunking request for: %s", filename);
        
        return Uni.createFrom().item(() -> {
            // Get document content and metadata
            Optional<String> contentOpt = demoDocumentService.getDocumentContent(filename);
            Optional<DocumentMetadata> metadataOpt = demoDocumentService.getDocumentByFilename(filename);
            
            if (contentOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ChunkResponse(false, "Document not found: " + filename))
                    .build();
            }
            
            String content = contentOpt.get();
            DocumentMetadata metadata = metadataOpt.orElse(null);
            
            // Determine chunking parameters
            ChunkingAlgorithm algorithm;
            Integer chunkSize;
            Integer chunkOverlap;
            
            if (useRecommended && metadata != null) {
                // Use recommended settings from metadata
                try {
                    algorithm = ChunkingAlgorithm.fromValue(metadata.recommendedAlgorithm());
                } catch (IllegalArgumentException e) {
                    algorithm = ChunkingAlgorithm.TOKEN; // fallback
                }
                chunkSize = metadata.recommendedChunkSize() != null ? metadata.recommendedChunkSize() : 500;
                chunkOverlap = chunkSize / 10; // Default to 10% overlap
            } else {
                // Use provided overrides or defaults
                algorithm = algorithmOverride != null ? 
                    ChunkingAlgorithm.fromValue(algorithmOverride) : ChunkingAlgorithm.TOKEN;
                chunkSize = chunkSizeOverride != null ? chunkSizeOverride : 500;
                chunkOverlap = chunkOverlapOverride != null ? chunkOverlapOverride : 50;
            }
            
            // Create ChunkerConfig
            ChunkerConfig config = ChunkerConfig.create(
                algorithm,
                "body",
                chunkSize,
                chunkOverlap,
                preserveUrls
            );
            
            // Create PipeDoc with clean ID (replace underscores and special chars)
            String cleanName = filename.replaceAll("\\.[^.]+$", "") // Remove extension
                                      .replaceAll("[_\\s]+", "-") // Replace underscores and spaces with hyphens
                                      .replaceAll("[^a-zA-Z0-9\\-]", "") // Remove non-alphanumeric except hyphens
                                      .toLowerCase();
            String docId = "demo-" + cleanName;
            ai.pipestream.data.v1.SearchMetadata.Builder searchMetadataBuilder = ai.pipestream.data.v1.SearchMetadata.newBuilder()
                .setTitle(metadata != null ? metadata.title() : filename)
                .setBody(content);
            
            if (metadata != null && metadata.author() != null) {
                searchMetadataBuilder
                    .putMetadata("author", metadata.author())
                    .putMetadata("category", metadata.category())
                    .putMetadata("description", metadata.description());
            }
            
            PipeDoc document = PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(searchMetadataBuilder.build())
                .build();
            
            // Perform chunking
            String streamId = "demo-stream-" + UUID.randomUUID().toString();
            ChunkingResult result = overlapChunker.createChunks(document, config, streamId, "demo-chunker");
            
            // Return result with config for metadata inclusion
            return new Object[] { result, config };
        })
        .map(resultArray -> {
            // Extract result and config from array
            Object[] dataArray = (Object[]) resultArray;
            ChunkingResult chunkingResult = (ChunkingResult) dataArray[0];
            ChunkerConfig configUsed = (ChunkerConfig) dataArray[1];
            
            // Convert to response format
            List<ChunkResponse.ChunkDto> chunkDtos = chunkingResult.chunks().stream()
                .map(chunk -> new ChunkResponse.ChunkDto(
                    chunk.id(),
                    chunk.text(),
                    chunk.originalIndexStart(),
                    chunk.originalIndexEnd()
                ))
                .collect(Collectors.toList());

            long processingTime = 0L; // Processing time calculation would be added properly
            int originalTextLength = chunkingResult.chunks().isEmpty() ? 0 : 
                chunkingResult.chunks().stream().mapToInt(chunk -> chunk.text().length()).sum();
            
            ChunkResponse.ChunkingMetadata metadata = new ChunkResponse.ChunkingMetadata(
                chunkDtos.size(),
                processingTime,
                originalTextLength,
                getTokenizerType(),
                getSentenceDetectorType(),
                configUsed // Include the ChunkerConfig in the response
            );

            ChunkResponse response = new ChunkResponse(true, chunkDtos, metadata);
            LOG.debugf("Demo document chunking completed - generated %d chunks for %s", chunkDtos.size(), filename);
            
            return Response.ok(response).build();
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.error("Error during demo document chunking", throwable);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new ChunkResponse(false, "Chunking failed: " + throwable.getMessage()))
                .build();
        });
    }

    private String getTokenizerType() {
        return tokenizer.getClass().getSimpleName();
    }

    private String getSentenceDetectorType() {
        return sentenceDetector.getClass().getSimpleName();
    }

    private int getTextLengthFromDocument(PipeDoc document, String sourceField) {
        switch (sourceField.toLowerCase()) {
            case "body":
                return document.hasSearchMetadata() && document.getSearchMetadata().hasBody() ? 
                    document.getSearchMetadata().getBody().length() : 0;
            case "title":
                return document.hasSearchMetadata() && document.getSearchMetadata().hasTitle() ? 
                    document.getSearchMetadata().getTitle().length() : 0;
            case "id":
            case "doc_id":
                return document.getDocId().length();
            default:
                return 0;
        }
    }

    @POST
    @Path("/process-json")
    @Operation(summary = "Process with JSON config", description = "Chunk text using complete ChunkerConfig JSON - perfect for JSONForms and Config Card")
    @Consumes(MediaType.APPLICATION_JSON)
    @APIResponse(responseCode = "200", description = "Chunking successful", 
                 content = @Content(schema = @Schema(implementation = ChunkResponse.class)))
    @APIResponse(responseCode = "400", description = "Invalid input")
    @APIResponse(responseCode = "500", description = "Internal server error")
    public Uni<Response> processWithJsonConfig(
            @Schema(description = "Complete chunking request with ChunkerConfig and text content")
            Map<String, Object> request) {
        
        LOG.debugf("JSON-based chunking request received");
        
        return Uni.createFrom().item(() -> {
            try {
                // Extract text from request
                String text = (String) request.get("text");
                if (text == null || text.trim().isEmpty()) {
                    return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ChunkResponse(false, "Text field is required"))
                        .build();
                }
                
                // Extract config from request - if not provided, use defaults
                ChunkerConfig config;
                if (request.containsKey("config")) {
                    // Convert config map to ChunkerConfig using Jackson ObjectMapper
                    config = objectMapper.convertValue(request.get("config"), ChunkerConfig.class);
                } else {
                    // Use default config
                    config = ChunkerConfig.createDefault();
                }
                
                LOG.debugf("Chunking with config ID: %s, text length: %d", config.configId(), text.length());
                
                // Create PipeDoc for chunking
                PipeDoc document = PipeDoc.newBuilder()
                    .setDocId(UUID.randomUUID().toString())
                    .setSearchMetadata(ai.pipestream.data.v1.SearchMetadata.newBuilder()
                        .setBody(text)
                        .build())
                    .build();

                // Perform chunking using ChunkerConfig directly (same as createChunks method)
                long startTime = System.currentTimeMillis();
                String streamId = "json-stream-" + UUID.randomUUID().toString();
                ChunkingResult chunkingResult = overlapChunker.createChunks(document, config, streamId, "json-chunker");
                long processingTime = System.currentTimeMillis() - startTime;

                // Convert to DTOs (same as other endpoints)
                List<ChunkResponse.ChunkDto> chunkDtos = chunkingResult.chunks().stream()
                    .map(chunk -> new ChunkResponse.ChunkDto(
                        chunk.id(),
                        chunk.text(),
                        chunk.originalIndexStart(),
                        chunk.originalIndexEnd()
                    ))
                    .collect(Collectors.toList());
                
                // Calculate original text length
                int originalTextLength = getTextLengthFromDocument(document, config.sourceField());
                
                ChunkResponse.ChunkingMetadata metadata = new ChunkResponse.ChunkingMetadata(
                    chunkDtos.size(),
                    processingTime,
                    originalTextLength,
                    getTokenizerType(),
                    getSentenceDetectorType()
                );

                ChunkResponse response = new ChunkResponse(true, chunkDtos, metadata);
                LOG.debugf("JSON-based chunking completed - generated %d chunks in %dms", chunkDtos.size(), processingTime);
                
                return Response.ok(response).build();
                
            } catch (Exception e) {
                LOG.error("Error during JSON-based chunking", e);
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ChunkResponse(false, "JSON chunking failed: " + e.getMessage()))
                    .build();
            }
        });
    }
}