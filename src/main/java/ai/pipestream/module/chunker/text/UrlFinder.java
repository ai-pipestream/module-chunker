package ai.pipestream.module.chunker.text;

import opennlp.tools.util.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Locates URL spans in source text. Returns character offsets into the original
 * text — no substitution, no placeholders. Downstream chunkers use these spans
 * to keep URLs atomic by snapping chunk boundaries out of URL interiors.
 */
public final class UrlFinder {

    private UrlFinder() {}

    private static final Pattern URL = Pattern.compile(
            "\\b(?:https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]",
            Pattern.CASE_INSENSITIVE);

    public static List<Span> find(String text) {
        if (text == null || text.isEmpty()) return List.of();
        List<Span> spans = new ArrayList<>();
        Matcher m = URL.matcher(text);
        while (m.find()) {
            spans.add(new Span(m.start(), m.end(), "URL"));
        }
        return spans;
    }
}
