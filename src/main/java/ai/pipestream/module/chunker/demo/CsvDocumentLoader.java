package ai.pipestream.module.chunker.demo;

import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple CSV document loader for reading demo document metadata.
 * This is a local implementation replacing the old pipestream-data-util CsvDocumentLoader.
 */
@Singleton
public class CsvDocumentLoader {

    private static final Logger LOG = Logger.getLogger(CsvDocumentLoader.class);

    /**
     * Loads CSV data from a resource path.
     *
     * @param resourcePath the path to the CSV resource
     * @return list of string arrays representing CSV rows (excluding header)
     */
    public List<String[]> loadCsvData(String resourcePath) {
        List<String[]> records = new ArrayList<>();

        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                LOG.warnf("CSV resource not found: %s", resourcePath);
                return records;
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

                // Skip header line
                String line = reader.readLine();
                if (line == null) {
                    LOG.warn("CSV file is empty");
                    return records;
                }

                // Read data rows
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        String[] fields = parseCsvLine(line);
                        records.add(fields);
                    }
                }

                LOG.debugf("Loaded %d records from CSV: %s", records.size(), resourcePath);
            }
        } catch (IOException e) {
            LOG.errorf("Error reading CSV from %s: %s", resourcePath, e.getMessage());
        }

        return records;
    }

    /**
     * Parses a single CSV line, handling quoted fields.
     *
     * @param line the CSV line to parse
     * @return array of field values
     */
    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Escaped quote
                    currentField.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }

        // Add the last field
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }

    /**
     * Cleans a CSV field by trimming whitespace and removing surrounding quotes.
     *
     * @param field the field to clean
     * @return the cleaned field value, or empty string if null
     */
    public String cleanField(String field) {
        if (field == null) {
            return "";
        }
        String cleaned = field.trim();
        // Remove surrounding quotes if present
        if (cleaned.length() >= 2 && cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }
        return cleaned;
    }

    /**
     * Parses a field as an integer.
     *
     * @param field the field to parse
     * @return the parsed integer, or null if the field is empty or invalid
     */
    public Integer parseInteger(String field) {
        String cleaned = cleanField(field);
        if (cleaned.isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(cleaned);
        } catch (NumberFormatException e) {
            LOG.debugf("Could not parse integer from field: %s", field);
            return null;
        }
    }
}
