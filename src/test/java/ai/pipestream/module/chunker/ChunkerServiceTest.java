package ai.pipestream.module.chunker;

import ai.pipestream.data.module.v1.PipeStepProcessorService;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;


@QuarkusTest
class ChunkerServiceTest extends ChunkerServiceTestBase {

    @GrpcClient("chunker")
    PipeStepProcessorService chunkerService;

    @Override
    protected PipeStepProcessorService getChunkerService() {
        return chunkerService;
    }
}
