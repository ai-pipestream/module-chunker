package ai.pipestream.module.chunker.pipeline;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.VectorDirective;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Resolves the source text for one {@link VectorDirective}. Only the
 * {@code body}, {@code title}, and {@code doc_id} fields are resolvable today.
 * Any other name throws {@link IllegalArgumentException} — no silent skip,
 * no fallback to an empty string. A real CEL evaluator lives outside this
 * module's scope; when callers need additional fields they must extend this
 * class explicitly.
 */
@ApplicationScoped
public class SourceTextResolver {

    public String resolve(PipeDoc doc, VectorDirective directive) {
        if (doc == null) {
            throw new IllegalArgumentException("PipeDoc is required");
        }
        if (!doc.hasSearchMetadata()) {
            throw new IllegalArgumentException(
                    "PipeDoc '" + doc.getDocId() + "' has no search_metadata");
        }

        String celSelector = directive.getCelSelector();
        String sourceLabel = directive.getSourceLabel();
        String fieldName;
        if (celSelector != null && !celSelector.isBlank()) {
            fieldName = normalizeSelector(celSelector);
        } else {
            fieldName = sourceLabel;
        }
        return resolveByName(doc, fieldName, sourceLabel);
    }

    private static String normalizeSelector(String selector) {
        String lower = selector.toLowerCase().trim();
        if (lower.contains("body"))  return "body";
        if (lower.contains("title")) return "title";
        if (lower.contains("doc_id") || lower.contains("docid")) return "doc_id";
        return lower;
    }

    private static String resolveByName(PipeDoc doc, String fieldName, String sourceLabel) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException(
                    "Directive source_label='" + sourceLabel + "' has no resolvable field name");
        }
        return switch (fieldName.toLowerCase()) {
            case "body"   -> doc.getSearchMetadata().hasBody()  ? doc.getSearchMetadata().getBody()  : "";
            case "title"  -> doc.getSearchMetadata().hasTitle() ? doc.getSearchMetadata().getTitle() : "";
            case "doc_id" -> doc.getDocId();
            default       -> throw new IllegalArgumentException(
                    "Unsupported source field '" + fieldName
                            + "' for directive source_label='" + sourceLabel
                            + "' — only body, title, and doc_id are implemented");
        };
    }
}
