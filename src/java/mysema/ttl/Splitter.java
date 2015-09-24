package mysema.ttl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Character.isWhitespace;

/**
 * Splits a string by whitespace apart if the whitspace is between
 * quotes. Quoted strings can contain escaped quotes. Triple quoted strings
 * can contain unescaped quotes.
 * <p/>
 * Removes ',' from the end of resulting words.
 * <p/>
 * Created by lim on 24/08/15.
 */
public class Splitter {

    private static int quotedString(final String s, int from, List<String> words, StringBuilder builder) {

        boolean tripleQuote = false;
        boolean removedEscape = false;
        char input = 0;
        char prev1 = 0;
        char prev2 = 0;
        char prev3 = 0;
        final int len = s.length();

        int start = from;
        int i = from + 1;

        // Consume triplequotes at start away
        if (len - i > 1) {
            if (s.charAt(i) == '"' && s.charAt(i + 1) == '"') {
                tripleQuote = true;
                start = i + 1;
                i = i + 2;
            }
        }

        for (; i < len; i++) {
            input = s.charAt(i);

            if (!tripleQuote && input == '"' && prev1 != '\\') break;
            if (tripleQuote && input != '"' && prev1 == '"' && prev2 == '"' && prev3 == '"') break;

            // Clean out escaped quotes
            if (input == '"' && prev1 == '\\') {
                builder.append(s.substring(start, i - 1));
                start = i;
                removedEscape = true;
            }

            prev3 = prev2;
            prev2 = prev1;
            prev1 = input;
        }

        if (i == len || tripleQuote) i--;

        int end = tripleQuote ? i - 1 : i + 1;

        // Don't use builder at all if escape removal did not happen
        if (removedEscape) {
            if (end - start > 0) {
                builder.append(s.substring(start, end));
            }
            if (builder.length() > 0) {
                words.add(builder.toString());
            }
            builder.setLength(0);
        } else if (end - start > 0) {
            words.add(s.substring(start, end));
        }

        return i;
    }

    public static List<String> splitToWords(final String s) {
        if (s == null || s.length() == 0) return Collections.emptyList();

        List<String> words = new ArrayList<String>();
        StringBuilder builder = new StringBuilder();

        int start = 0;
        int end;
        char input = 0;
        char prevInput;
        final int len = s.length();

        for (int i = 0; i < len; i++) {
            prevInput = input;
            input = s.charAt(i);

            if (isWhitespace(input)) {
                if (i - start > 0) {
                    end = i;
                    if (prevInput == ',') end--;
                    if (end - start > 0) words.add(s.substring(start, end));
                }
                start = i + 1;
            } else if (input == '"' && isWhitespace(prevInput)) {
                i = quotedString(s, i, words, builder);
                start = i + 1;
                input = s.charAt(i);
            } else if (i == len - 1) {
                end = len;
                if (input == ',') end--;
                if (end - start > 0) words.add(s.substring(start, end));
            }
        }
        return words;
    }

}
