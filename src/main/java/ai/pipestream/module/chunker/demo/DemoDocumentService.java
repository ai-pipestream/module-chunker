package ai.pipestream.module.chunker.demo;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing demo documents and their metadata.
 * Reads CSV metadata and provides access to demo text files.
 */
@Singleton
public class DemoDocumentService {
    
    private static final Logger LOG = Logger.getLogger(DemoDocumentService.class);
    private static final String CSV_RESOURCE_PATH = "/demo-documents/documents.csv";
    private static final String TEXTS_RESOURCE_PATH = "/demo-documents/texts/";
    
    @Inject
    CsvDocumentLoader csvLoader;
    
    /**
     * Loads all demo document metadata from the CSV file.
     * 
     * @return List of DocumentMetadata objects with file size and preview info
     */
    public List<DocumentMetadata> getAllDocuments() {
        List<DocumentMetadata> documents = new ArrayList<>();
        
        List<String[]> records = csvLoader.loadCsvData(CSV_RESOURCE_PATH);
        
        for (int i = 0; i < records.size(); i++) {
            String[] record = records.get(i);
            if (record.length >= 8) {
                try {
                    DocumentMetadata metadata = parseCSVRecord(record);
                    if (metadata.isValid()) {
                        // Enhance with file info
                        DocumentMetadata enhancedMetadata = addFileInfo(metadata);
                        documents.add(enhancedMetadata);
                    } else {
                        LOG.warnf("Invalid document metadata in CSV row %d", i + 2); // +2 because we skipped header
                    }
                } catch (Exception e) {
                    LOG.errorf("Error parsing CSV row %d: %s", i + 2, e.getMessage());
                }
            } else {
                LOG.warnf("CSV row %d has insufficient columns (expected 8, got %d)", i + 2, record.length);
            }
        }
        
        LOG.debugf("Loaded %d demo documents from CSV", documents.size());
        return documents;
    }
    
    /**
     * Gets a specific document's metadata by filename.
     * 
     * @param filename The filename to search for
     * @return Optional DocumentMetadata if found
     */
    public Optional<DocumentMetadata> getDocumentByFilename(String filename) {
        return getAllDocuments().stream()
                .filter(doc -> doc.filename().equals(filename))
                .findFirst();
    }
    
    /**
     * Reads the content of a demo document.
     * 
     * @param filename The filename of the document to read
     * @return Optional containing the file content, or empty if not found
     */
    public Optional<String> getDocumentContent(String filename) {
        String resourcePath = TEXTS_RESOURCE_PATH + filename;
        
        try (InputStream textStream = getClass().getResourceAsStream(resourcePath)) {
            if (textStream == null) {
                LOG.warnf("Demo document not found: %s", resourcePath);
                return Optional.empty();
            }
            
            String content = new String(textStream.readAllBytes(), StandardCharsets.UTF_8);
            LOG.debugf("Read demo document %s (%d characters)", filename, content.length());
            return Optional.of(content);
            
        } catch (IOException e) {
            LOG.errorf("Error reading demo document %s: %s", filename, e.getMessage());
            return Optional.empty();
        }
    }
    
    /**
     * Parses a CSV record into DocumentMetadata.
     */
    private DocumentMetadata parseCSVRecord(String[] record) {
        String filename = csvLoader.cleanField(record[0]);
        String title = csvLoader.cleanField(record[1]);
        String author = csvLoader.cleanField(record[2]); 
        String category = csvLoader.cleanField(record[3]);
        String description = csvLoader.cleanField(record[4]);
        Integer estimatedChunks = csvLoader.parseInteger(record[5]);
        Integer recommendedChunkSize = csvLoader.parseInteger(record[6]);
        String recommendedAlgorithm = csvLoader.cleanField(record[7]);
        
        return DocumentMetadata.fromCsv(
            filename, title, author, category, description,
            estimatedChunks, recommendedChunkSize, recommendedAlgorithm
        );
    }
    
    /**
     * Adds file size and preview information to document metadata.
     */
    private DocumentMetadata addFileInfo(DocumentMetadata metadata) {
        Optional<String> contentOpt = getDocumentContent(metadata.filename());
        
        if (contentOpt.isPresent()) {
            String content = contentOpt.get();
            long fileSize = content.getBytes(StandardCharsets.UTF_8).length;
            String preview = content.length() > 200 ? content.substring(0, 200) : content;
            
            return metadata.withFileInfo(fileSize, preview);
        } else {
            return metadata.withFileInfo(0L, "File not found");
        }
    }
    
}