package ai.pipestream.module.chunker.config;

import ai.pipestream.module.chunker.examples.SampleDocuments;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Typed parse of a {@code NamedChunkerConfig}'s embedded config {@link com.google.protobuf.Struct}.
 *
 * <p><b>Every caller-visible field is required.</b> There are no defaults, no
 * null-substitution, no {@code createDefault()} factory. If a caller sends a
 * config struct without one of these fields set, the constructor throws
 * {@link IllegalArgumentException} — which {@code ConfigParser} converts into
 * an {@code INVALID_ARGUMENT} outcome for the request. Defaults are the
 * caller's responsibility, not this module's.
 */
@RegisterForReflection
@Schema(
    name = "ChunkerConfig",
    description = "Configuration for text chunking operations; every field is required.",
    examples = {
        SampleDocuments.US_CONSTITUTION_PREAMBLE,
        SampleDocuments.TECHNICAL_DOCUMENTATION,
        SampleDocuments.LITERARY_EXCERPT
    }
)
public record ChunkerConfig(

    @JsonProperty("algorithm")
    @Schema(
        description = "Chunking algorithm to use for splitting text",
        enumeration = {"character", "token", "sentence", "semantic"},
        required = true
    )
    @NotNull
    ChunkingAlgorithm algorithm,

    @JsonProperty("sourceField")
    @JsonAlias("source_field")
    @Schema(
        description = "Document field to extract text from for chunking",
        required = true
    )
    @NotNull
    String sourceField,

    @JsonProperty("chunkSize")
    @JsonAlias("chunk_size")
    @Schema(
        description = "Target size for each chunk (characters for 'character', tokens for 'token', sentences for 'sentence')",
        minimum = "1",
        maximum = "10000",
        required = true
    )
    @NotNull @Min(1) @Max(10000)
    Integer chunkSize,

    @JsonProperty("chunkOverlap")
    @JsonAlias("chunk_overlap")
    @Schema(
        description = "Amount of overlap between consecutive chunks (same units as chunkSize)",
        minimum = "0",
        maximum = "5000",
        required = true
    )
    @NotNull @Min(0) @Max(5000)
    Integer chunkOverlap,

    @JsonProperty("preserveUrls")
    @JsonAlias("preserve_urls")
    @Schema(description = "Whether to preserve URLs as atomic units during chunking", required = true)
    @NotNull
    Boolean preserveUrls,

    @JsonProperty("cleanText")
    @JsonAlias("clean_text")
    @Schema(description = "Whether to normalise whitespace and line endings before chunking", required = true)
    @NotNull
    Boolean cleanText
) {

    public ChunkerConfig {
        if (algorithm == null)    throw new IllegalArgumentException("ChunkerConfig.algorithm is required");
        if (sourceField == null)  throw new IllegalArgumentException("ChunkerConfig.sourceField is required");
        if (chunkSize == null)    throw new IllegalArgumentException("ChunkerConfig.chunkSize is required");
        if (chunkOverlap == null) throw new IllegalArgumentException("ChunkerConfig.chunkOverlap is required");
        if (preserveUrls == null) throw new IllegalArgumentException("ChunkerConfig.preserveUrls is required");
        if (cleanText == null)    throw new IllegalArgumentException("ChunkerConfig.cleanText is required");
    }

    /**
     * Validates structural invariants. Returns {@code null} if valid, an error
     * string otherwise. Null / missing fields are already rejected by the
     * canonical constructor, so this only checks cross-field rules.
     */
    public String validate() {
        if (chunkSize < 1 || chunkSize > 10000) {
            return "chunkSize must be between 1 and 10000";
        }
        if (chunkOverlap < 0 || chunkOverlap > 5000) {
            return "chunkOverlap must be between 0 and 5000";
        }
        if (chunkOverlap >= chunkSize) {
            return "chunkOverlap must be less than chunkSize";
        }
        if (algorithm == ChunkingAlgorithm.SEMANTIC) {
            return "Semantic chunking is not implemented";
        }
        return null;
    }

    public boolean isValid() {
        return validate() == null;
    }
}
