package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.chunker.service.NlpPreprocessor;
import ai.pipestream.module.chunker.service.NlpPreprocessor.NlpResult;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for NlpPreprocessor using real parsed PipeDoc protobuf files.
 * <p>
 * These documents come from the parser module and contain messy text extracted
 * from PDFs, HTML, and other formats. OpenNLP's tokenizer and sentence detector
 * can produce inconsistent/null/out-of-bounds spans with such text, causing
 * ArrayIndexOutOfBoundsException and NullPointerException in the preprocessor.
 * <p>
 * This test loads all 102 parsed documents and feeds their body text through
 * the NlpPreprocessor to ensure it handles real-world text without crashing.
 */
@QuarkusTest
class NlpPreprocessorParsedDocsTest {

    private static final Logger LOG = Logger.getLogger(NlpPreprocessorParsedDocsTest.class);

    @Inject
    NlpPreprocessor preprocessor;

    @TestFactory
    Collection<DynamicTest> allParsedDocsShouldPreprocessWithoutCrashing() {
        List<DynamicTest> tests = new ArrayList<>();

        for (int i = 1; i <= 102; i++) {
            String resourceName = String.format("/parser_pipedoc_parsed/parsed_document_%03d.pb", i);
            final int docNum = i;

            tests.add(DynamicTest.dynamicTest(
                    "parsed_document_" + String.format("%03d", i),
                    () -> {
                        try (InputStream is = getClass().getResourceAsStream(resourceName)) {
                            if (is == null) {
                                LOG.warnf("Resource not found: %s", resourceName);
                                return;
                            }

                            PipeDoc doc = PipeDoc.parseFrom(is.readAllBytes());
                            String body = "";
                            if (doc.hasSearchMetadata() && doc.getSearchMetadata().hasBody()) {
                                body = doc.getSearchMetadata().getBody();
                            }

                            if (body.isBlank()) {
                                LOG.infof("Document %03d has no body text, skipping", docNum);
                                return;
                            }

                            LOG.infof("Document %03d: %d chars body text", docNum, body.length());

                            NlpResult result = preprocessor.preprocess(body);

                            assertThat(result)
                                    .as("NlpResult should not be null for document %03d", docNum)
                                    .isNotNull();

                            assertThat(result.tokens())
                                    .as("Document %03d should produce tokens", docNum)
                                    .isNotNull();

                            assertThat(result.tokenSpans())
                                    .as("Document %03d tokenSpans length should match tokens length", docNum)
                                    .hasSize(result.tokens().length);

                            assertThat(result.posTags())
                                    .as("Document %03d posTags length should match tokens length", docNum)
                                    .hasSize(result.tokens().length);

                            assertThat(result.lemmas())
                                    .as("Document %03d lemmas length should match tokens length", docNum)
                                    .hasSize(result.tokens().length);

                            assertThat(result.sentences())
                                    .as("Document %03d should detect sentences", docNum)
                                    .isNotNull();

                            assertThat(result.sentenceSpans())
                                    .as("Document %03d sentenceSpans length should match sentences length", docNum)
                                    .hasSize(result.sentences().length);

                            LOG.infof("Document %03d: %d tokens, %d sentences — OK",
                                    docNum, result.tokens().length, result.sentences().length);
                        }
                    }));
        }

        return tests;
    }
}
