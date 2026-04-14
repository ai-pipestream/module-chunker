package ai.pipestream.module.chunker.directive;

import ai.pipestream.data.v1.NamedChunkerConfig;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.VectorDirective;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DirectiveKeyComputer} — pure functions, no Quarkus context needed.
 *
 * <p>Pre-computed expected values are derived by the Python reference implementation:
 * {@code sha256b64url(s) = base64url(sha256(s.encode('utf-8'))).rstrip('=')}
 *
 * <p>Reference for the canonical expected key:
 * Input: {@code "body|document.search_metadata.body|sentence_v1,token_256|minilm,mpnet"}
 * Expected: {@code "teE6qwC8PJ43x06cWROsqu9uII0CqUbcrxBBcFEhrzI"}
 */
class DirectiveKeyComputerTest {

    // Pre-computed expected values (verified against Python hashlib reference)
    private static final String EXPECTED_KEY_SORTED =
            "teE6qwC8PJ43x06cWROsqu9uII0CqUbcrxBBcFEhrzI";
    private static final String EXPECTED_KEY_DIFFERENT_CEL =
            "pOZSG5IcIBCyOwt8iFioQ_6csXk9Uu80UqUwhCXzHjI";
    private static final String EXPECTED_KEY_EXTRA_CHUNKER =
            "feO0UR2D0LKB6iP8eI6OrczI_OcjF-l6ciXm1qMezsA";

    /** Build a standard VectorDirective with configs in lexicographic order. */
    private static VectorDirective buildStandardDirective() {
        return VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("sentence_v1").build())
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("token_256").build())
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("mpnet").build())
                .build();
    }

    /** Build the same directive but with configs in reversed (non-sorted) insertion order. */
    private static VectorDirective buildReversedDirective() {
        return VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                // Reversed order vs buildStandardDirective()
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("token_256").build())
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("sentence_v1").build())
                // Reversed order vs buildStandardDirective()
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("mpnet").build())
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .build();
    }

    // =========================================================================
    // Pre-computed expected value assertion
    // =========================================================================

    @Test
    void compute_matchesPrecomputedExpectedValue() {
        VectorDirective directive = buildStandardDirective();

        String key = DirectiveKeyComputer.compute(directive);

        assertThat(key)
                .as("directive_key for known input 'body|document.search_metadata.body|sentence_v1,token_256|minilm,mpnet' should match pre-computed SHA-256 base64url value")
                .isEqualTo(EXPECTED_KEY_SORTED);
    }

    // =========================================================================
    // Determinism
    // =========================================================================

    @Test
    void compute_isDeterministic_twoCallsSameInput_sameOutput() {
        VectorDirective directive = buildStandardDirective();

        String key1 = DirectiveKeyComputer.compute(directive);
        String key2 = DirectiveKeyComputer.compute(directive);

        assertThat(key1)
                .as("two compute() calls with identical input must produce identical keys (determinism)")
                .isEqualTo(key2);
    }

    // =========================================================================
    // Order independence
    // =========================================================================

    @Test
    void compute_isOrderIndependent_reversedConfigOrder_sameKey() {
        VectorDirective sorted = buildStandardDirective();
        VectorDirective reversed = buildReversedDirective();

        String keySorted = DirectiveKeyComputer.compute(sorted);
        String keyReversed = DirectiveKeyComputer.compute(reversed);

        assertThat(keySorted)
                .as("directive_key must be order-independent: reversing chunker and embedder config insertion order should not change the key because IDs are sorted before hashing")
                .isEqualTo(keyReversed);
    }

    @Test
    void compute_isOrderIndependent_matchesPrecomputedValue() {
        VectorDirective reversed = buildReversedDirective();

        String key = DirectiveKeyComputer.compute(reversed);

        assertThat(key)
                .as("reversed-order directive should still produce the same pre-computed key as sorted-order directive")
                .isEqualTo(EXPECTED_KEY_SORTED);
    }

    // =========================================================================
    // Sensitivity to input changes
    // =========================================================================

    @Test
    void compute_differsWhenCelSelectorChanges() {
        VectorDirective original = buildStandardDirective();
        VectorDirective modified = original.toBuilder()
                .setCelSelector("document.search_metadata.title")
                .build();

        String keyOriginal = DirectiveKeyComputer.compute(original);
        String keyModified = DirectiveKeyComputer.compute(modified);

        assertThat(keyOriginal)
                .as("changing cel_selector must produce a different directive_key")
                .isNotEqualTo(keyModified);
        assertThat(keyModified)
                .as("directive_key with modified cel_selector should match the pre-computed value for that input")
                .isEqualTo(EXPECTED_KEY_DIFFERENT_CEL);
    }

    @Test
    void compute_differsWhenSourceLabelChanges() {
        VectorDirective original = buildStandardDirective();
        VectorDirective modified = original.toBuilder()
                .setSourceLabel("title")
                .build();

        String keyOriginal = DirectiveKeyComputer.compute(original);
        String keyModified = DirectiveKeyComputer.compute(modified);

        assertThat(keyOriginal)
                .as("changing source_label must produce a different directive_key")
                .isNotEqualTo(keyModified);
    }

    @Test
    void compute_differsWhenExtraChunkerConfigAdded() {
        VectorDirective original = buildStandardDirective();
        VectorDirective modified = original.toBuilder()
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("char_1000").build())
                .build();

        String keyOriginal = DirectiveKeyComputer.compute(original);
        String keyModified = DirectiveKeyComputer.compute(modified);

        assertThat(keyOriginal)
                .as("adding an extra chunker config must produce a different directive_key")
                .isNotEqualTo(keyModified);
        assertThat(keyModified)
                .as("directive_key with extra chunker config 'char_1000' should match the pre-computed value for that input (configs sorted: char_1000, sentence_v1, token_256)")
                .isEqualTo(EXPECTED_KEY_EXTRA_CHUNKER);
    }

    @Test
    void compute_differsWhenEmbedderConfigAdded() {
        VectorDirective original = buildStandardDirective();
        VectorDirective modified = original.toBuilder()
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("ada_002").build())
                .build();

        String keyOriginal = DirectiveKeyComputer.compute(original);
        String keyModified = DirectiveKeyComputer.compute(modified);

        assertThat(keyOriginal)
                .as("adding an extra embedder config must produce a different directive_key")
                .isNotEqualTo(keyModified);
    }

    @Test
    void compute_differsWhenEmbedderConfigRemoved() {
        VectorDirective original = buildStandardDirective();
        // Remove one embedder config — use only minilm
        VectorDirective modified = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("sentence_v1").build())
                .addChunkerConfigs(NamedChunkerConfig.newBuilder().setConfigId("token_256").build())
                .addEmbedderConfigs(NamedEmbedderConfig.newBuilder().setConfigId("minilm").build())
                .build();

        String keyOriginal = DirectiveKeyComputer.compute(original);
        String keyModified = DirectiveKeyComputer.compute(modified);

        assertThat(keyOriginal)
                .as("removing an embedder config must produce a different directive_key")
                .isNotEqualTo(keyModified);
    }

    // =========================================================================
    // sha256b64url helper
    // =========================================================================

    @Test
    void sha256b64url_knownInput_producesExpectedHash() {
        // "The quick brown fox jumps over the lazy dog"
        // SHA-256: d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592 (hex)
        // base64url without padding: 16j7swfXgJRpypq8sAguT41WUeRtPNt2LQLQvzfJ5ZI
        String result = DirectiveKeyComputer.sha256b64url("The quick brown fox jumps over the lazy dog");

        assertThat(result)
                .as("sha256b64url of 'The quick brown fox jumps over the lazy dog' should match the pre-computed reference value")
                .isEqualTo("16j7swfXgJRpypq8sAguT41WUeRtPNt2LQLQvzfJ5ZI");
    }

    @Test
    void sha256b64url_emptyString_producesConsistentHash() {
        // SHA-256("") is always 47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU in base64url
        String result = DirectiveKeyComputer.sha256b64url("");

        assertThat(result)
                .as("sha256b64url of empty string should match the well-known SHA-256 of empty input")
                .isEqualTo("47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU");
    }

    @Test
    void sha256b64url_noPaddingChars() {
        String result = DirectiveKeyComputer.sha256b64url("any input string");

        assertThat(result)
                .as("sha256b64url output must never contain '=' padding characters")
                .doesNotContain("=");
    }

    @Test
    void sha256b64url_urlSafeAlphabet_noStandardBase64Chars() {
        // Standard base64 uses '+' and '/'; url-safe uses '-' and '_'
        // Run a few strings and verify neither '+' nor '/' appears
        String result1 = DirectiveKeyComputer.sha256b64url("body|document.search_metadata.body|sentence_v1,token_256|minilm,mpnet");
        String result2 = DirectiveKeyComputer.sha256b64url("test string with various chars: !@#$%");

        assertThat(result1)
                .as("sha256b64url must use URL-safe alphabet: no '+' characters in result 1")
                .doesNotContain("+");
        assertThat(result1)
                .as("sha256b64url must use URL-safe alphabet: no '/' characters in result 1")
                .doesNotContain("/");
        assertThat(result2)
                .as("sha256b64url must use URL-safe alphabet: no '+' characters in result 2")
                .doesNotContain("+");
        assertThat(result2)
                .as("sha256b64url must use URL-safe alphabet: no '/' characters in result 2")
                .doesNotContain("/");
    }

    @Test
    void sha256b64url_alwaysProduces43CharOutput() {
        // SHA-256 produces 32 bytes; base64url of 32 bytes without padding is always 43 chars
        String result = DirectiveKeyComputer.sha256b64url("some input");

        assertThat(result)
                .as("sha256b64url must always produce exactly 43 characters (SHA-256 = 32 bytes, base64url without padding = ceil(32*4/3) = 43)")
                .hasSize(43);
    }

    // =========================================================================
    // Empty config lists
    // =========================================================================

    @Test
    void compute_emptyConfigLists_doesNotThrow() {
        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                // no chunker or embedder configs
                .build();

        String key = DirectiveKeyComputer.compute(directive);

        assertThat(key)
                .as("compute() with empty config lists should return a non-null 43-char hash without throwing")
                .isNotNull()
                .hasSize(43);
    }

    @Test
    void compute_emptyConfigLists_isDeterministic() {
        VectorDirective directive = VectorDirective.newBuilder()
                .setSourceLabel("body")
                .setCelSelector("document.search_metadata.body")
                .build();

        String key1 = DirectiveKeyComputer.compute(directive);
        String key2 = DirectiveKeyComputer.compute(directive);

        assertThat(key1)
                .as("compute() with empty config lists must be deterministic across calls")
                .isEqualTo(key2);
    }
}
