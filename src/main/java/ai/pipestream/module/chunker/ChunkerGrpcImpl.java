package ai.pipestream.module.chunker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import ai.pipestream.module.chunker.schema.SchemaExtractorService;
import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.LogEntrySource;
import ai.pipestream.data.v1.LogLevel;
import ai.pipestream.data.v1.ModuleLogOrigin;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingResult;
import ai.pipestream.module.chunker.service.*;
import ai.pipestream.server.meta.BuildInfoProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Chunker gRPC service implementation using Quarkus reactive patterns with Mutiny.
 * This service receives documents through gRPC and processes them by breaking them
 * into smaller, overlapping chunks for further processing.
 */
@Singleton
@GrpcService
public class ChunkerGrpcImpl implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(ChunkerGrpcImpl.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    OverlapChunker overlapChunker;

    @Inject
    ChunkMetadataExtractor metadataExtractor;

    @Inject
    SchemaExtractorService schemaExtractorService;

    @Inject
    BuildInfoProvider buildInfoProvider;

    @Override
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        if (request == null) {
            LOG.error("Received null request");
            return Uni.createFrom().item(createErrorResponse("Request cannot be null", null));
        }

        boolean isTest = request.getIsTest();
        String logPrefix = isTest ? "[TEST] " : "";

        return Uni.createFrom().item(() -> {
            long startTime = System.currentTimeMillis();
            try {
                ProcessDataResponse.Builder responseBuilder = ProcessDataResponse.newBuilder();

                if (!request.hasDocument()) {
                    LOG.info(logPrefix + "No document provided in request");
                    return ProcessDataResponse.newBuilder()
                            .setSuccess(true)
                            .addLogEntries(moduleLog("Chunker service: no document to process", LogLevel.LOG_LEVEL_INFO))
                            .build();
                }

                PipeDoc inputDoc = request.getDocument();
                ProcessConfiguration config = request.getConfig();
                ServiceMetadata metadata = request.getMetadata();
                String streamId = metadata.getStreamId();
                String pipeStepName = metadata.getPipeStepName();

                LOG.infof("%sProcessing document ID: %s for step: %s in stream: %s",
                    logPrefix,
                    inputDoc.getDocId() != null ? inputDoc.getDocId() : "unknown",
                    pipeStepName, streamId);

                PipeDoc.Builder outputDocBuilder = inputDoc.toBuilder();

                // Parse chunker config - only support ChunkerConfig format
                ChunkerConfig chunkerConfig;
                Struct customJsonConfig = config.getJsonConfig();
                if (customJsonConfig != null && customJsonConfig.getFieldsCount() > 0) {
                    String jsonStr = JsonFormat.printer().print(customJsonConfig);
                    LOG.debugf("Parsing JSON config: %s", jsonStr);
                    try {
                        chunkerConfig = objectMapper.readValue(jsonStr, ChunkerConfig.class);
                        LOG.debugf("Successfully parsed as ChunkerConfig: algorithm=%s", chunkerConfig.algorithm());
                    } catch (Exception e) {
                        LOG.errorf("Failed to parse JSON config as ChunkerConfig: %s", e.getMessage());
                        throw new RuntimeException("Invalid configuration format. Expected ChunkerConfig structure.", e);
                    }
                } else {
                    chunkerConfig = ChunkerConfig.createDefault();
                    LOG.debugf("Using default ChunkerConfig: algorithm=%s", chunkerConfig.algorithm());
                }

                LOG.debugf("Final ChunkerConfig: algorithm=%s, chunkSize=%s", chunkerConfig.algorithm(), chunkerConfig.chunkSize());

                if (chunkerConfig.sourceField() == null || chunkerConfig.sourceField().isEmpty()) {
                    return createErrorResponse("Missing 'sourceField' in ChunkerConfig", null);
                }

                responseBuilder.addLogEntries(moduleLog(String.format(
                        "Chunking strategy: %s with chunk size %s and overlap %s",
                        chunkerConfig.algorithm().getValue(),
                        chunkerConfig.chunkSize() != null ? chunkerConfig.chunkSize() : "default",
                        chunkerConfig.chunkOverlap() != null ? chunkerConfig.chunkOverlap() : "default"), LogLevel.LOG_LEVEL_INFO));

                // Log source field text length for audit
                String sourceText = extractSourceText(inputDoc, chunkerConfig.sourceField());
                int charCount = sourceText != null ? sourceText.length() : 0;
                responseBuilder.addLogEntries(moduleLog(String.format(
                        "Chunking document %s: source field '%s', text length: %d characters",
                        inputDoc.getDocId(), chunkerConfig.sourceField(), charCount), LogLevel.LOG_LEVEL_INFO));

                // Create chunks using ChunkerConfig for better ID generation
                ChunkingResult chunkingResult = overlapChunker.createChunks(inputDoc, chunkerConfig, streamId, pipeStepName);
                List<Chunk> chunkRecords = chunkingResult.chunks();

                if (!chunkRecords.isEmpty()) {
                    Map<String, String> placeholderToUrlMap = chunkingResult.placeholderToUrlMap();
                    // Node/step (pipeStepName) is the identifier; opensearch-manager derives field names.
                    SemanticProcessingResult.Builder newSemanticResultBuilder = SemanticProcessingResult.newBuilder()
                            .setResultId(UUID.randomUUID().toString())
                            .setSourceFieldName(chunkerConfig.sourceField())
                            .setChunkConfigId(pipeStepName);

                    String resultSetName = ai.pipestream.module.chunker.model.ChunkerOptions.resolveResultSetName(
                            null, pipeStepName, pipeStepName, chunkerConfig.sourceField());
                    newSemanticResultBuilder.setResultSetName(resultSetName);

                    int currentChunkNumber = 0;
                    for (Chunk chunkRecord : chunkRecords) {
                        // Sanitize the chunk text to ensure valid UTF-8
                        String sanitizedText = UnicodeSanitizer.sanitizeInvalidUnicode(chunkRecord.text());

                        ChunkEmbedding.Builder chunkEmbeddingBuilder = ChunkEmbedding.newBuilder()
                                .setTextContent(sanitizedText)
                                .setChunkId(chunkRecord.id())
                                .setOriginalCharStartOffset(chunkRecord.originalIndexStart())
                                .setOriginalCharEndOffset(chunkRecord.originalIndexEnd())
                                .setChunkConfigId(pipeStepName);

                        boolean containsUrlPlaceholder = (chunkerConfig.preserveUrls() != null && chunkerConfig.preserveUrls()) &&
                                !placeholderToUrlMap.isEmpty() &&
                                placeholderToUrlMap.keySet().stream().anyMatch(ph -> chunkRecord.text().contains(ph));

                        Map<String, com.google.protobuf.Value> extractedMetadata = metadataExtractor.extractAllMetadata(
                                sanitizedText,
                                currentChunkNumber,
                                chunkRecords.size(),
                                containsUrlPlaceholder
                        );

                        SemanticChunk.Builder semanticChunkBuilder = SemanticChunk.newBuilder()
                                .setChunkId(chunkRecord.id())
                                .setChunkNumber(currentChunkNumber)
                                .setEmbeddingInfo(chunkEmbeddingBuilder.build())
                                .putAllMetadata(extractedMetadata);

                        newSemanticResultBuilder.addChunks(semanticChunkBuilder.build());
                        currentChunkNumber++;
                    }
                    // Add semantic results to search metadata
                    ai.pipestream.data.v1.SearchMetadata.Builder searchMetadataBuilder =
                        outputDocBuilder.hasSearchMetadata() ?
                            outputDocBuilder.getSearchMetadata().toBuilder() :
                            ai.pipestream.data.v1.SearchMetadata.newBuilder();

                    searchMetadataBuilder.addSemanticResults(newSemanticResultBuilder.build());
                    outputDocBuilder.setSearchMetadata(searchMetadataBuilder.build());

                    String successMessage = isTest ?
                        String.format("%sSuccessfully created and added metadata to %d chunks for testing using %s algorithm. Chunker service validated successfully.",
                            logPrefix, chunkRecords.size(), chunkerConfig.algorithm().getValue()) :
                        String.format("%sSuccessfully created and added metadata to %d chunks from source field '%s' into result set '%s' using %s algorithm. Chunker service successfully processed document.",
                            logPrefix, chunkRecords.size(), chunkerConfig.sourceField(), resultSetName, chunkerConfig.algorithm().getValue());

                    int avgChunkSize = chunkRecords.stream()
                            .mapToInt(c -> c.text().length()).sum() / chunkRecords.size();
                    responseBuilder.addLogEntries(moduleLog(String.format(
                            "Produced %d chunks from %d characters (avg chunk size: %d chars)",
                            chunkRecords.size(), charCount, avgChunkSize), LogLevel.LOG_LEVEL_INFO));
                    responseBuilder.addLogEntries(moduleLog(successMessage, LogLevel.LOG_LEVEL_INFO));
                } else {
                    responseBuilder.addLogEntries(moduleLog(String.format(
                            "No text found in source field '%s' — no chunks produced for document %s",
                            chunkerConfig.sourceField(), inputDoc.getDocId()), LogLevel.LOG_LEVEL_INFO));
                }

                long duration = System.currentTimeMillis() - startTime;
                responseBuilder.addLogEntries(moduleLog(String.format("Chunking completed in %dms", duration), LogLevel.LOG_LEVEL_INFO));

                responseBuilder.setSuccess(true);
                PipeDoc outputDoc = outputDocBuilder.build();
                responseBuilder.setOutputDoc(outputDoc);

                return responseBuilder.build();

            } catch (Exception e) {
                String errorMessage = String.format("Error in ChunkerService: %s", e.getMessage());
                LOG.error(errorMessage, e);
                return createErrorResponse(errorMessage, e);
            }
        });
    }

    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.debug("Chunker service registration requested");

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("chunker")
                .setVersion(buildInfoProvider.getVersion())
                .putAllMetadata(buildInfoProvider.registrationMetadata())
                .setCapabilities(Capabilities.newBuilder().addTypes(CapabilityType.CAPABILITY_TYPE_UNSPECIFIED).build());

        // Use SchemaExtractorService to get a JSONForms-ready ChunkerConfig schema (refs resolved)
        Optional<String> schemaOptional = schemaExtractorService.extractChunkerConfigSchemaResolvedForJsonForms();

        if (schemaOptional.isPresent()) {
            String jsonSchema = schemaOptional.get();
            responseBuilder.setJsonConfigSchema(jsonSchema);
            LOG.debugf("Successfully extracted JSONForms-ready schema (%d characters)", jsonSchema.length());
            LOG.info("Returning JSON schema for chunker module (refs resolved).");
        } else {
            responseBuilder.setHealthCheckPassed(false);
            responseBuilder.setHealthCheckMessage("Failed to resolve ChunkerConfig schema for JSONForms");
            LOG.error("SchemaExtractorService could not resolve ChunkerConfig schema for JSONForms");
            return Uni.createFrom().item(responseBuilder.build());
        }

        // If test request is provided, perform health check
        if (request.hasTestRequest()) {
            LOG.debug("Performing health check with test request");
            return processData(request.getTestRequest())
                .map(processResponse -> {
                    if (processResponse.getSuccess()) {
                        responseBuilder
                            .setHealthCheckPassed(true)
                            .setHealthCheckMessage("Chunker module is healthy - successfully processed test document");
                    } else {
                        responseBuilder
                            .setHealthCheckPassed(false)
                            .setHealthCheckMessage("Chunker module health check failed: " +
                                processResponse.getLogEntriesList().stream()
                                    .map(LogEntry::getMessage)
                                    .reduce((a, b) -> a + "; " + b)
                                    .orElse("unknown error"));
                    }
                    return responseBuilder.build();
                })
                .onFailure().recoverWithItem(error -> {
                    LOG.error("Health check failed with exception", error);
                    return responseBuilder
                        .setHealthCheckPassed(false)
                        .setHealthCheckMessage("Health check failed with exception: " + error.getMessage())
                        .build();
                });
        } else {
            // No test request provided, assume healthy
            responseBuilder.setHealthCheckPassed(true);
            responseBuilder.setHealthCheckMessage("Chunker module is ready");
            return Uni.createFrom().item(responseBuilder.build());
        }
    }

    private String extractSourceText(PipeDoc doc, String sourceField) {
        if (doc == null || !doc.hasSearchMetadata()) return null;
        return switch (sourceField.toLowerCase()) {
            case "body" -> doc.getSearchMetadata().hasBody() ? doc.getSearchMetadata().getBody() : null;
            case "title" -> doc.getSearchMetadata().hasTitle() ? doc.getSearchMetadata().getTitle() : null;
            case "doc_id" -> doc.getDocId();
            default -> null;
        };
    }

    private static LogEntry moduleLog(String message, LogLevel level) {
        return LogEntry.newBuilder()
            .setSource(LogEntrySource.LOG_ENTRY_SOURCE_MODULE)
            .setLevel(level)
            .setMessage(message)
            .setTimestampEpochMs(System.currentTimeMillis())
            .setModule(ModuleLogOrigin.newBuilder().setModuleName("chunker").build())
            .build();
    }

    private ProcessDataResponse createErrorResponse(String errorMessage, Exception e) {
        ProcessDataResponse.Builder responseBuilder = ProcessDataResponse.newBuilder();
        responseBuilder.setSuccess(false);
        responseBuilder.addLogEntries(moduleLog(errorMessage, LogLevel.LOG_LEVEL_ERROR));

        Struct.Builder errorDetailsBuilder = Struct.newBuilder();
        errorDetailsBuilder.putFields("error_message", com.google.protobuf.Value.newBuilder().setStringValue(errorMessage).build());
        if (e != null) {
            errorDetailsBuilder.putFields("error_type", com.google.protobuf.Value.newBuilder().setStringValue(e.getClass().getName()).build());
            if (e.getCause() != null) {
                errorDetailsBuilder.putFields("error_cause", com.google.protobuf.Value.newBuilder().setStringValue(e.getCause().getMessage()).build());
            }
        }
        responseBuilder.setErrorDetails(errorDetailsBuilder.build());
        return responseBuilder.build();
    }

}
