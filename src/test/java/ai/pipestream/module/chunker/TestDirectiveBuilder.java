package ai.pipestream.module.chunker;

import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

/**
 * Test helper that builds {@link VectorSetDirectives} for pack-2 unit tests.
 *
 * <p>The new {@code ChunkerGrpcImpl.processData} requires directives on the doc;
 * legacy per-step chunker config in {@code json_config} is no longer supported.
 * Use these factory methods to construct directive sets that satisfy the
 * directive-driven path.
 *
 * <p>Every directive includes at least one dummy {@link NamedEmbedderConfig} so
 * {@link ai.pipestream.module.chunker.directive.DirectiveKeyComputer#compute} produces
 * a stable key.
 */
public final class TestDirectiveBuilder {

    private TestDirectiveBuilder() {}

    /**
     * Builds a {@link VectorSetDirectives} with one directive, one chunker config
     * (token algorithm), and one dummy embedder config.
     *
     * @param sourceLabel label for this directive (e.g. "body")
     * @param chunkSize   token chunk size
     * @param overlap     token overlap
     * @return ready-to-use {@link VectorSetDirectives}
     */
    public static VectorSetDirectives withSingleTokenDirective(
            String sourceLabel, int chunkSize, int overlap) {
        return withSingleDirective(sourceLabel, "token", chunkSize, overlap);
    }

    /**
     * Builds a {@link VectorSetDirectives} with one directive, one chunker config
     * (sentence algorithm), and one dummy embedder config.
     *
     * @param sourceLabel label for this directive
     * @param chunkSize   sentences per chunk
     * @param overlap     sentence overlap
     * @return ready-to-use {@link VectorSetDirectives}
     */
    public static VectorSetDirectives withSingleSentenceDirective(
            String sourceLabel, int chunkSize, int overlap) {
        return withSingleDirective(sourceLabel, "sentence", chunkSize, overlap);
    }

    /**
     * Builds a {@link VectorSetDirectives} with one directive that carries
     * two chunker configs (token + sentence) and one dummy embedder config.
     * Useful for testing multi-config fan-out.
     *
     * @param sourceLabel label for this directive
     * @return ready-to-use {@link VectorSetDirectives}
     */
    public static VectorSetDirectives withTwoConfigDirective(String sourceLabel) {
        NamedChunkerConfig tokenConfig = namedChunkerConfig("token_500_50", "token", 500, 50);
        NamedChunkerConfig sentenceConfig = namedChunkerConfig("sentence_3_1", "sentence", 3, 1);
        NamedEmbedderConfig embedderConfig = dummyEmbedderConfig("minilm");

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel(sourceLabel)
                .setCelSelector("document.search_metadata." + sourceLabel)
                .addChunkerConfigs(tokenConfig)
                .addChunkerConfigs(sentenceConfig)
                .addEmbedderConfigs(embedderConfig)
                .build();

        return VectorSetDirectives.newBuilder().addDirectives(directive).build();
    }

    /**
     * Builds a {@link VectorSetDirectives} with two directives (body + title),
     * each with one token chunker config and one dummy embedder config.
     */
    public static VectorSetDirectives withTwoSourceDirectives() {
        VectorDirective bodyDirective = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(namedChunkerConfig("token_500_50", "token", 500, 50))
                .addEmbedderConfigs(dummyEmbedderConfig("minilm"))
                .build();

        VectorDirective titleDirective = VectorDirective.newBuilder()
                .setSourceLabel("title")
                .setCelSelector("document.search_metadata.title")
                .addChunkerConfigs(namedChunkerConfig("token_100_10", "token", 100, 10))
                .addEmbedderConfigs(dummyEmbedderConfig("minilm"))
                .build();

        return VectorSetDirectives.newBuilder()
                .addDirectives(bodyDirective)
                .addDirectives(titleDirective)
                .build();
    }

    // -------------------------------------------------------------------------
    // Private builders
    // -------------------------------------------------------------------------

    private static VectorSetDirectives withSingleDirective(
            String sourceLabel, String algorithm, int chunkSize, int overlap) {
        String configId = algorithm + "_" + chunkSize + "_" + overlap;
        NamedChunkerConfig namedChunker = namedChunkerConfig(configId, algorithm, chunkSize, overlap);
        NamedEmbedderConfig embedder = dummyEmbedderConfig("minilm");

        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel(sourceLabel)
                .setCelSelector("document.search_metadata." + sourceLabel)
                .addChunkerConfigs(namedChunker)
                .addEmbedderConfigs(embedder)
                .build();

        return VectorSetDirectives.newBuilder().addDirectives(directive).build();
    }

    /** Builds a {@link NamedChunkerConfig} from primitive parameters. */
    public static NamedChunkerConfig namedChunkerConfig(
            String configId, String algorithm, int chunkSize, int overlap) {
        Struct configStruct = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue(algorithm).build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(chunkSize).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(overlap).build())
                .build();
        return NamedChunkerConfig.newBuilder()
                .setConfigId(configId)
                .setConfig(configStruct)
                .build();
    }

    /** Builds a dummy {@link NamedEmbedderConfig} with an empty config struct. */
    public static NamedEmbedderConfig dummyEmbedderConfig(String configId) {
        return NamedEmbedderConfig.newBuilder()
                .setConfigId(configId)
                .build();
    }
}
