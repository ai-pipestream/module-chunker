package ai.pipestream.module.chunker.text;

/**
 * Whitespace / line-ending normalization. Pure function, no state, no DI.
 * Collapses runs of whitespace within a paragraph, preserves paragraph breaks
 * (runs of two or more newlines).
 */
public final class TextCleaner {

    private TextCleaner() {}

    public static String clean(String text) {
        if (text == null || text.isEmpty()) return text;

        String normalized = text.replace("\r\n", "\n").replace('\r', '\n');
        String[] paragraphs = normalized.split("\n{2,}");
        StringBuilder out = new StringBuilder(normalized.length());
        for (String p : paragraphs) {
            String collapsed = p.replaceAll("\\s+", " ").trim();
            if (collapsed.isEmpty()) continue;
            if (out.length() > 0) out.append("\n\n");
            out.append(collapsed);
        }
        return out.toString();
    }
}
