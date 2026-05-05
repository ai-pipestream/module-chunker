package ai.pipestream.module.chunker.directive;

import ai.pipestream.data.v1.VectorDirective;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Pure-function utility for computing a deterministic {@code directive_key} from
 * a {@link VectorDirective}, per DESIGN.md §21.2.
 *
 * <p>Formula:
 * <pre>
 * directive_key = sha256b64url(
 *     source_label + "|" +
 *     cel_selector + "|" +
 *     join(",", sorted(chunker_config_ids)) + "|" +
 *     join(",", sorted(embedder_config_ids))
 * )
 * </pre>
 *
 * <p>The config-ID lists are lexicographically sorted before joining so that
 * insertion order has no effect on the key — two directives with the same
 * configs in different order produce the same key.
 *
 * <p><b>Naming constraint:</b> {@code source_label}, {@code cel_selector},
 * and every config id must not contain the reserved delimiter characters
 * {@code |} or {@code ,}. The spec does not define escaping — the key
 * reduces to a concatenation with deterministic delimiters, so two distinct
 * directives with unusual characters could theoretically collide (for example,
 * a {@code source_label} of {@code "a|b"} is indistinguishable from a
 * {@code source_label} of {@code "a"} with a {@code cel_selector} starting
 * with {@code "b|"}). The project's naming conventions for these fields —
 * alphanumeric, underscore, hyphen, dot — satisfy the non-collision invariant.
 * If future usage relaxes those conventions, this helper needs a proper
 * escaping pass.
 *
 * <p>{@code sha256b64url} means: SHA-256 hash of the UTF-8 bytes of the
 * concatenated string, then base64url-encoded (URL-safe alphabet, no {@code =}
 * padding). This produces a 43-character string.
 *
 * <p>This class has no mutable state and is safe for concurrent use.
 */
public final class DirectiveKeyComputer {

    private DirectiveKeyComputer() {
        // utility class — no instances
    }

    /**
     * Computes the deterministic {@code directive_key} for the given directive.
     *
     * @param directive the {@link VectorDirective} to hash; must not be {@code null}
     * @return a 43-character base64url-encoded SHA-256 hash string, no padding
     */
    public static String compute(VectorDirective directive) {
        List<String> chunkerIds = directive.getChunkerConfigsList()
                .stream()
                .map(c -> c.getConfigId())
                .sorted()
                .collect(Collectors.toList());

        List<String> embedderIds = directive.getEmbedderConfigsList()
                .stream()
                .map(e -> e.getConfigId())
                .sorted()
                .collect(Collectors.toList());

        String combined = directive.getSourceLabel()
                + "|" + directive.getCelSelector()
                + "|" + String.join(",", chunkerIds)
                + "|" + String.join(",", embedderIds);

        return sha256b64url(combined);
    }

    /**
     * Computes the SHA-256 hash of {@code input} (UTF-8 encoded) and returns it
     * as a URL-safe base64 string without {@code =} padding.
     *
     * <p>This is the shared hash primitive used for directive keys.
     *
     * @param input the string to hash; must not be {@code null}
     * @return a 43-character base64url string (no padding)
     */
    public static String sha256b64url(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by the Java spec — this cannot happen
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
