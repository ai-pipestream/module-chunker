package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.LogEntrySource;
import ai.pipestream.data.v1.LogLevel;
import ai.pipestream.data.v1.ModuleLogOrigin;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.chunker.pipeline.ChunkerPipeline;
import ai.pipestream.module.chunker.schema.SchemaExtractorService;
import ai.pipestream.data.module.v1.Capabilities;
import ai.pipestream.data.module.v1.CapabilityType;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.ServiceMetadata;
import ai.pipestream.server.meta.BuildInfoProvider;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.Optional;

/**
 * Thin gRPC entry point for the chunker. All behaviour lives in
 * {@link ChunkerPipeline}. Each request runs end-to-end on its own
 * virtual thread via {@link RunOnVirtualThread} — no shared worker pool,
 * no queueing above a fixed size. OS-level parallelism, bounded only by
 * inbound gRPC concurrency.
 *
 * <p>No caches, no shared mutable state, no silent fallbacks. Every
 * malformed request → {@code PROCESSING_OUTCOME_FAILURE} with the
 * cause attached; the pipeline itself throws
 * {@link IllegalArgumentException} for every missing-field case.
 */
@Singleton
@GrpcService
public class ChunkerGrpcImpl implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(ChunkerGrpcImpl.class);

    /** Chunk config id used for the always-emitted sentence SPR (§21.9). */
    public static final String SENTENCES_INTERNAL_CONFIG_ID =
            ai.pipestream.module.chunker.pipeline.SprAssembler.SENTENCES_INTERNAL_CONFIG_ID;

    private final ChunkerPipeline pipeline;
    private final SchemaExtractorService schemaExtractorService;
    private final BuildInfoProvider buildInfoProvider;

    @Inject
    public ChunkerGrpcImpl(ChunkerPipeline pipeline,
                           SchemaExtractorService schemaExtractorService,
                           BuildInfoProvider buildInfoProvider) {
        this.pipeline = pipeline;
        this.schemaExtractorService = schemaExtractorService;
        this.buildInfoProvider = buildInfoProvider;
    }

    @Override
    @RunOnVirtualThread
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        if (request == null) {
            return Uni.createFrom().item(error("Request cannot be null"));
        }
        if (!request.hasDocument()) {
            return Uni.createFrom().item(error(
                    "ProcessDataRequest has no document — chunker requires a PipeDoc"));
        }

        final String logPrefix = request.getIsTest() ? "[TEST] " : "";
        final long startMs = System.currentTimeMillis();
        final PipeDoc inputDoc = request.getDocument();
        final ServiceMetadata meta = request.getMetadata();

        LOG.infof("%sProcessing docId=%s stream=%s step=%s",
                logPrefix, inputDoc.getDocId(), meta.getStreamId(), meta.getPipeStepName());

        try {
            ChunkerPipeline.Outcome outcome = pipeline.process(inputDoc);
            long duration = System.currentTimeMillis() - startMs;
            return Uni.createFrom().item(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                    .setOutputDoc(outcome.outputDoc())
                    .addLogEntries(log(logPrefix + "Chunking completed in " + duration
                                    + "ms: produced " + outcome.newSprCount() + " new SPR(s), "
                                    + outcome.totalSprCount() + " total for doc " + inputDoc.getDocId(),
                            LogLevel.LOG_LEVEL_INFO))
                    .build());
        } catch (IllegalArgumentException iae) {
            LOG.warnf("INVALID_ARGUMENT for doc %s: %s", inputDoc.getDocId(), iae.getMessage());
            return Uni.createFrom().item(error(iae.getMessage()));
        } catch (Exception e) {
            LOG.errorf(e, "Chunker pipeline failed for doc %s", inputDoc.getDocId());
            return Uni.createFrom().item(error(e.getMessage() != null ? e.getMessage() : e.toString()));
        }
    }

    @Override
    @RunOnVirtualThread
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        GetServiceRegistrationResponse.Builder b = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("chunker")
                .setVersion(buildInfoProvider.getVersion())
                .putAllMetadata(buildInfoProvider.registrationMetadata())
                .setCapabilities(Capabilities.newBuilder()
                        .addTypes(CapabilityType.CAPABILITY_TYPE_UNSPECIFIED).build());

        Optional<String> schema = schemaExtractorService.extractChunkerConfigSchemaResolvedForJsonForms();
        if (schema.isEmpty()) {
            return Uni.createFrom().item(b
                    .setHealthCheckPassed(false)
                    .setHealthCheckMessage("Failed to resolve ChunkerConfig schema")
                    .build());
        }
        b.setJsonConfigSchema(schema.get());

        if (request.hasTestRequest()) {
            return processData(request.getTestRequest())
                    .map(resp -> {
                        boolean ok = resp.getOutcome() == ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS;
                        b.setHealthCheckPassed(ok);
                        b.setHealthCheckMessage(ok
                                ? "Chunker module healthy — test document processed"
                                : "Chunker health check failed");
                        return b.build();
                    })
                    .onFailure().recoverWithItem(e -> b
                            .setHealthCheckPassed(false)
                            .setHealthCheckMessage("Health check failed: " + e.getMessage())
                            .build());
        }
        return Uni.createFrom().item(b
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("Chunker module ready")
                .build());
    }

    private static ProcessDataResponse error(String message) {
        Struct details = Struct.newBuilder()
                .putFields("error_message", Value.newBuilder().setStringValue(message).build())
                .build();
        return ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                .addLogEntries(log(message, LogLevel.LOG_LEVEL_ERROR))
                .setErrorDetails(details)
                .build();
    }

    private static LogEntry log(String message, LogLevel level) {
        return LogEntry.newBuilder()
                .setSource(LogEntrySource.LOG_ENTRY_SOURCE_MODULE)
                .setLevel(level)
                .setMessage(message)
                .setTimestampEpochMs(System.currentTimeMillis())
                .setModule(ModuleLogOrigin.newBuilder().setModuleName("chunker").build())
                .build();
    }
}
