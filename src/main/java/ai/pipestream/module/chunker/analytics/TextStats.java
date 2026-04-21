package ai.pipestream.module.chunker.analytics;

import java.util.HashMap;
import java.util.Map;

/**
 * Lightweight character-class histogram computed in a single pass over a
 * string. Used by both document- and chunk-level analytics builders.
 */
final class TextStats {

    private TextStats() {}

    static Result compute(String text) {
        if (text == null || text.isEmpty()) return Result.EMPTY;
        int chars = text.length();
        int whitespace = 0, alnum = 0, digit = 0, upper = 0;
        Map<String, Integer> punct = new HashMap<>();
        for (int i = 0; i < chars; i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c))        whitespace++;
            if (Character.isLetterOrDigit(c))     alnum++;
            if (Character.isDigit(c))             digit++;
            if (Character.isUpperCase(c))         upper++;
            int type = Character.getType(c);
            if (isPunctuation(type)) {
                String key = String.valueOf(c);
                punct.merge(key, 1, Integer::sum);
            }
        }
        return new Result(chars, whitespace, alnum, digit, upper, punct);
    }

    private static boolean isPunctuation(int type) {
        return type == Character.CONNECTOR_PUNCTUATION
                || type == Character.DASH_PUNCTUATION
                || type == Character.END_PUNCTUATION
                || type == Character.FINAL_QUOTE_PUNCTUATION
                || type == Character.INITIAL_QUOTE_PUNCTUATION
                || type == Character.OTHER_PUNCTUATION
                || type == Character.START_PUNCTUATION;
    }

    record Result(int chars, int whitespace, int alnum, int digit, int upper, Map<String, Integer> punctuation) {
        static final Result EMPTY = new Result(0, 0, 0, 0, 0, Map.of());

        float whitespacePct()   { return chars == 0 ? 0f : (float) whitespace / chars; }
        float alphanumericPct() { return chars == 0 ? 0f : (float) alnum / chars; }
        float digitPct()        { return chars == 0 ? 0f : (float) digit / chars; }
        float uppercasePct()    { return chars == 0 ? 0f : (float) upper / chars; }
    }
}
