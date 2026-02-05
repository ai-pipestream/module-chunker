package ai.pipestream.module.chunker.service;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.chunker.config.ChunkerConfig;
import ai.pipestream.module.chunker.model.Chunk;
import ai.pipestream.module.chunker.model.ChunkingAlgorithm;
import ai.pipestream.module.chunker.model.ChunkerOptions;
import ai.pipestream.module.chunker.model.ChunkingResult;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;

/**
 * Core chunking implementation that breaks text into overlapping chunks.
 * Uses token-based chunking with configurable size and overlap parameters.
 * Supports URL preservation during chunking.
 */
@Singleton
public class OverlapChunker {

    private static final Logger LOG = Logger.getLogger(OverlapChunker.class);
    private static final long MAX_TEXT_BYTES = 40 * 1024 * 1024; // 40MB limit
    private static final int MAX_CHUNKS_PER_DOCUMENT = 1000; // Limit chunks to prevent gRPC message size issues
    private final Tokenizer tokenizer;
    private final SentenceDetector sentenceDetector;

    private static final Pattern URL_PATTERN = Pattern.compile(
            "\\b(?:https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]",
            Pattern.CASE_INSENSITIVE);
    private static final String URL_PLACEHOLDER_PREFIX = "__URL_PLACEHOLDER_";
    private static final String URL_PLACEHOLDER_SUFFIX = "__";

    @Inject
    public OverlapChunker(Tokenizer tokenizer, SentenceDetector sentenceDetector) {
        this.tokenizer = tokenizer;
        this.sentenceDetector = sentenceDetector;
    }

    /**
     * Helper method to squish a list of strings into a single string.
     * 
     * @param list List of strings to squish
     * @return A list containing a single concatenated string, or empty list if input is empty
     */
    public List<String> squish(List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        StringBuilder currentString = new StringBuilder();
        for (String s : list) {
            if (s != null && !s.isEmpty()) {
                if (currentString.length() > 0) {
                    currentString.append(" ");
                }
                currentString.append(s.trim());
            }
        }
        if (currentString.length() > 0) {
            result.add(currentString.toString());
        }
        return result;
    }

    /**
     * Cleans text by normalizing whitespace and line endings.
     * Based on the existing squish logic but adapted for single strings.
     * 
     * @param text Text to clean
     * @return Cleaned text with normalized whitespace and line endings
     */
    private String cleanText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        
        // Normalize line endings (Windows \r\n and Mac \r to Unix \n)
        String cleaned = text.replaceAll("\\r\\n", "\n").replaceAll("\\r", "\n");
        
        // Remove excessive whitespace while preserving paragraph breaks
        // Split by double newlines to preserve paragraphs
        String[] paragraphs = cleaned.split("\n\n+");
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < paragraphs.length; i++) {
            String paragraph = paragraphs[i];
            
            // For each paragraph, normalize internal whitespace
            // Replace multiple spaces/tabs with single space, trim lines
            String[] lines = paragraph.split("\n");
            StringBuilder paragraphBuilder = new StringBuilder();
            
            for (String line : lines) {
                String trimmedLine = line.trim();
                if (!trimmedLine.isEmpty()) {
                    if (paragraphBuilder.length() > 0) {
                        paragraphBuilder.append(" ");
                    }
                    // Replace multiple whitespace with single space
                    paragraphBuilder.append(trimmedLine.replaceAll("\\s+", " "));
                }
            }
            
            if (paragraphBuilder.length() > 0) {
                if (result.length() > 0) {
                    result.append("\n\n");
                }
                result.append(paragraphBuilder.toString());
            }
        }
        
        return result.toString();
    }

    /**
     * Transforms URLs in text to placeholders to preserve them during chunking.
     * 
     * @param text Text to process
     * @param placeholderToUrlMap Map to store placeholder-to-URL mappings
     * @param urlSpans List to store original URL spans
     * @return Text with URLs replaced by placeholders
     */
    private String transformURLsToPlaceholders(String text, Map<String, String> placeholderToUrlMap, List<Span> urlSpans) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        Matcher matcher = URL_PATTERN.matcher(text);
        StringBuilder sb = new StringBuilder();
        int placeholderIndex = 0;
        while (matcher.find()) {
            String placeholder = URL_PLACEHOLDER_PREFIX + placeholderIndex + URL_PLACEHOLDER_SUFFIX;
            String url = matcher.group(0);
            placeholderToUrlMap.put(placeholder, url);
            urlSpans.add(new Span(matcher.start(), matcher.end(), "URL")); // Store original URL span
            matcher.appendReplacement(sb, Matcher.quoteReplacement(placeholder));
            placeholderIndex++;
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Restores URL placeholders in a chunk back to their original URLs.
     * 
     * @param chunkText Chunk text with placeholders
     * @param placeholderToUrlMap Map of placeholder-to-URL mappings
     * @return Chunk text with original URLs restored
     */
    private String restorePlaceholdersInChunk(String chunkText, Map<String, String> placeholderToUrlMap) {
        if (chunkText == null || chunkText.isEmpty() || placeholderToUrlMap.isEmpty()) {
            return chunkText;
        }
        String restoredText = chunkText;
        for (Map.Entry<String, String> entry : placeholderToUrlMap.entrySet()) {
            restoredText = restoredText.replaceAll(Pattern.quote(entry.getKey()), Matcher.quoteReplacement(entry.getValue()));
        }
        return restoredText;
    }

    /**
     * Extracts text from a specific field in a PipeDoc.
     * 
     * @param document The PipeDoc to extract from
     * @param fieldPath Path to the field (e.g., "body", "title")
     * @return Optional containing the extracted text, or empty if not found
     */
    private Optional<String> extractTextFromPipeDoc(PipeDoc document, String fieldPath) {
        if (document == null || fieldPath == null || fieldPath.isEmpty()) {
            return Optional.empty();
        }

        try {
            // For now, only support the commonly used fields directly
            switch (fieldPath.toLowerCase()) {
                case "body":
                    return document.hasSearchMetadata() && document.getSearchMetadata().hasBody() ? 
                        Optional.of(document.getSearchMetadata().getBody()) : Optional.empty();
                case "title":
                    return document.hasSearchMetadata() && document.getSearchMetadata().hasTitle() ? 
                        Optional.of(document.getSearchMetadata().getTitle()) : Optional.empty();
                case "id":
                case "doc_id":
                    return Optional.of(document.getDocId());
                default:
                    LOG.warnf("Field '%s' is not supported. Only 'body', 'title', and 'doc_id' are currently supported.", fieldPath);
                    return Optional.empty();
            }

        } catch (Exception e) {
            LOG.errorf("Error extracting field '%s': %s", fieldPath, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Create chunks using ChunkerConfig for better ID generation.
     * This is the preferred method that generates clean, semantic chunk IDs.
     * 
     * @param document The PipeDoc to chunk
     * @param config Chunker configuration (node/step identifier is pipeStepName; opensearch-manager derives field names)
     * @param streamId Stream ID for logging
     * @param pipeStepName Pipeline step name for logging
     * @return ChunkingResult containing the created chunks and URL placeholder mappings
     */
    public ChunkingResult createChunks(PipeDoc document, ChunkerConfig config, String streamId, String pipeStepName) {
        // Convert ChunkerConfig to ChunkerOptions for internal processing (node/step = pipeStepName is the identifier)
        ChunkerOptions options = new ChunkerOptions(
            config.sourceField(),
            config.chunkSize(),
            config.chunkOverlap(),
            null, // IDs generated using pipeStepName
            pipeStepName,
            "%s_chunks_%s", // resultSetNameTemplate
            "chunker", // logPrefix
            config.preserveUrls()
        );
        
        return createChunksInternal(document, options, config, streamId, pipeStepName);
    }

    /**
     * Main method to create chunks from a document using legacy ChunkerOptions.
     * 
     * @param document The PipeDoc to chunk
     * @param options Chunking configuration options
     * @param streamId Stream ID for logging and chunk ID generation
     * @param pipeStepName Pipeline step name for logging
     * @return ChunkingResult containing the created chunks and URL placeholder mappings
     */
    public ChunkingResult createChunks(PipeDoc document, ChunkerOptions options, String streamId, String pipeStepName) {
        return createChunksInternal(document, options, null, streamId, pipeStepName);
    }

    /**
     * Internal method that handles the actual chunking logic.
     * 
     * @param document The PipeDoc to chunk
     * @param options Chunking configuration options
     * @param config Optional ChunkerConfig for better ID generation (can be null)
     * @param streamId Stream ID for logging and chunk ID generation
     * @param pipeStepName Pipeline step name for logging
     * @return ChunkingResult containing the created chunks and URL placeholder mappings
     */
    private ChunkingResult createChunksInternal(PipeDoc document, ChunkerOptions options, ChunkerConfig config, String streamId, String pipeStepName) {
        if (document == null) {
            LOG.warnf("Input document is null. Cannot create chunks. streamId: %s, pipeStepName: %s", streamId, pipeStepName);
            return new ChunkingResult(Collections.emptyList(), Collections.emptyMap()); // Return empty result
        }
        String documentId = document.getDocId();
        String textFieldPath = options.sourceField();

        Optional<String> textOptional = extractTextFromPipeDoc(document, textFieldPath);
        if (textOptional.isEmpty() || textOptional.get().trim().isEmpty()) {
            LOG.warnf("No text found or text is empty at path '%s'. No chunks will be created. streamId: %s, pipeStepName: %s", 
                    textFieldPath, streamId, pipeStepName);
            return new ChunkingResult(Collections.emptyList(), Collections.emptyMap()); // Return empty result
        }
        String originalText = textOptional.get();
        
        // Sanitize the text to ensure valid UTF-8 encoding before processing
        originalText = UnicodeSanitizer.sanitizeInvalidUnicode(originalText);
        
        // Clean text if enabled (normalize whitespace and line endings)
        if (config != null && config.cleanText() != null && config.cleanText()) {
            originalText = cleanText(originalText);
        }

        // Handle MAX_TEXT_BYTES before URL processing to avoid issues with placeholder lengths
        byte[] originalTextBytes = originalText.getBytes(StandardCharsets.UTF_8);
        if (originalTextBytes.length > MAX_TEXT_BYTES) {
            LOG.warnf("Original text from field '%s' exceeds MAX_TEXT_BYTES (%d bytes). Truncating. streamId: %s, pipeStepName: %s",
                    textFieldPath, MAX_TEXT_BYTES, streamId, pipeStepName);
            originalText = new String(originalTextBytes, 0, (int) MAX_TEXT_BYTES, StandardCharsets.UTF_8);
        }

        Map<String, String> placeholderToUrlMap = new HashMap<>();
        List<Span> originalUrlSpans = new ArrayList<>(); // To store original URL positions
        String textToProcess = originalText;

        if (options.preserveUrls() != null && options.preserveUrls()) {
            textToProcess = transformURLsToPlaceholders(originalText, placeholderToUrlMap, originalUrlSpans);
            LOG.debugf("Text after URL placeholder replacement: %d characters, %d URLs replaced", 
                      textToProcess.length(), placeholderToUrlMap.size());
        }

        // Determine chunking algorithm from config
        ChunkingAlgorithm algorithm = ChunkingAlgorithm.TOKEN; // Default to token
        if (config != null) {
            algorithm = config.algorithm();
        }

        LOG.debugf("Using chunking algorithm: %s for document ID: %s", algorithm, documentId);

        // Branch based on algorithm
        if (algorithm == ChunkingAlgorithm.SENTENCE) {
            return createSentenceBasedChunks(textToProcess, placeholderToUrlMap, options, config, streamId, pipeStepName, documentId);
        } else if (algorithm == ChunkingAlgorithm.CHARACTER) {
            return createCharacterBasedChunks(textToProcess, placeholderToUrlMap, options, config, streamId, pipeStepName, documentId, textFieldPath);
        } else {
            // Default to token-based chunking (TOKEN algorithm, SEMANTIC not implemented yet)
            return createTokenBasedChunks(textToProcess, placeholderToUrlMap, options, config, streamId, pipeStepName, documentId, textFieldPath);
        }
    }

    /**
     * Extracts a short, clean document ID for use in chunk IDs.
     * Removes common prefixes and UUID suffixes to create readable IDs.
     * 
     * @param documentId The full document ID
     * @return A shortened version suitable for chunk IDs
     */
    private String extractShortDocumentId(String documentId) {
        if (documentId == null || documentId.isEmpty()) {
            return "doc";
        }
        
        String shortId = documentId;
        
        // Remove common prefixes
        if (shortId.startsWith("simple-doc-")) {
            shortId = shortId.substring("simple-doc-".length());
        } else if (shortId.startsWith("advanced-doc-")) {
            shortId = shortId.substring("advanced-doc-".length());
        } else if (shortId.startsWith("form-doc-")) {
            shortId = shortId.substring("form-doc-".length());
        } else if (shortId.contains("-doc-")) {
            // Extract just the part after "-doc-"
            int docIndex = shortId.indexOf("-doc-");
            shortId = shortId.substring(docIndex + "-doc-".length());
        }
        
        // If it's a UUID (36 chars with dashes), take first 8 characters
        if (shortId.length() == 36 && shortId.charAt(8) == '-' && shortId.charAt(13) == '-') {
            shortId = shortId.substring(0, 8);
        } else if (shortId.length() > 12) {
            // For other long IDs, take first 12 characters
            shortId = shortId.substring(0, 12);
        }
        
        // Clean up any remaining dashes at start/end
        shortId = shortId.replaceAll("^-+|-+$", "");
        
        // If empty after cleanup, use default
        if (shortId.isEmpty()) {
            shortId = "doc";
        }
        
        return shortId;
    }

    /**
     * Creates chunks using token-based algorithm with proper token counting for size and overlap.
     * 
     * @param textToProcess Text that has been preprocessed (URL placeholders if enabled)
     * @param placeholderToUrlMap Map of URL placeholders to original URLs
     * @param options Chunking configuration options (chunkSize = token count, chunkOverlap = token count)
     * @param config ChunkerConfig for ID generation (can be null)
     * @param streamId Stream ID for logging and chunk IDs
     * @param pipeStepName Pipeline step name for logging
     * @param documentId Document ID for chunk ID generation
     * @param textFieldPath Field path being chunked (for logging)
     * @return ChunkingResult containing chunks and URL placeholder mappings
     */
    private ChunkingResult createTokenBasedChunks(String textToProcess, Map<String, String> placeholderToUrlMap,
                                                  ChunkerOptions options, ChunkerConfig config, String streamId,
                                                  String pipeStepName, String documentId, String textFieldPath) {
        
        LOG.debugf("Creating chunks with target token size: %d, token overlap: %d, for document ID: %s, streamId: %s, pipeStepName: %s",
                options.chunkSize(), options.chunkOverlap(), documentId, streamId, pipeStepName);

        // Tokenize the text to get both tokens and their positions
        String[] tokens = tokenizer.tokenize(textToProcess);
        opennlp.tools.util.Span[] tokenSpans = tokenizer.tokenizePos(textToProcess);
        
        if (tokens.length == 0) {
            LOG.debugf("No tokens found in text. Returning empty chunks. streamId: %s, pipeStepName: %s", streamId, pipeStepName);
            return new ChunkingResult(Collections.emptyList(), placeholderToUrlMap);
        }

        List<Chunk> chunks = new ArrayList<>();
        int chunkIndex = 0;
        int currentTokenStartIndex = 0;
        int targetTokensPerChunk = options.chunkSize(); // This is now token count, not characters
        int tokenOverlap = options.chunkOverlap(); // This is now token count, not characters

        while (currentTokenStartIndex < tokens.length) {
            StringBuilder currentChunkTextBuilder = new StringBuilder();
            
            // Determine how many tokens to include in this chunk
            int tokensInThisChunk = Math.min(targetTokensPerChunk, tokens.length - currentTokenStartIndex);
            int currentTokenEndIndex = currentTokenStartIndex + tokensInThisChunk;

            // Build the chunk text from tokens
            for (int i = currentTokenStartIndex; i < currentTokenEndIndex; i++) {
                String tokenText = tokens[i];
                
                // Add spacing between tokens (smart spacing logic)
                if (i > currentTokenStartIndex) {
                    // Don't add a space if the current token is punctuation that doesn't need preceding space
                    if (!(tokenText.length() == 1 && ".?!,:;)]}".contains(tokenText))) {
                        // Also, don't add a space if the previous token ended with something like an opening quote or bracket
                        if (currentChunkTextBuilder.length() > 0) {
                            char lastChar = currentChunkTextBuilder.charAt(currentChunkTextBuilder.length() - 1);
                            if (!"([{\"'".contains(String.valueOf(lastChar))) {
                                currentChunkTextBuilder.append(" ");
                            }
                        }
                    }
                }
                currentChunkTextBuilder.append(tokenText);
            }

            String chunkTextWithPlaceholders = currentChunkTextBuilder.toString().trim();
            if (chunkTextWithPlaceholders.isEmpty()) {
                break; // No more content to process
            }

            // Restore URLs from placeholders if needed
            String finalChunkText = chunkTextWithPlaceholders;
            if (options.preserveUrls() != null && options.preserveUrls()) {
                finalChunkText = restorePlaceholdersInChunk(chunkTextWithPlaceholders, placeholderToUrlMap);
            }

            // Calculate character offsets for this chunk
            int chunkStartCharOffset = tokenSpans[currentTokenStartIndex].getStart();
            int chunkEndCharOffset = tokenSpans[currentTokenEndIndex - 1].getEnd();
            
            int originalStartOffset = chunkStartCharOffset;
            int originalEndOffset = chunkEndCharOffset - 1; // Span.getEnd() is exclusive

            if (options.preserveUrls() != null && options.preserveUrls() && !placeholderToUrlMap.isEmpty()) {
                LOG.debugf("URL preservation is active, original character offsets for chunks might be approximate " +
                         "due to placeholder substitutions. StreamID: %s, DocID: %s", streamId, documentId);
            }

            // Generate clean, semantic chunk ID (node/step = options.chunkConfigId() is the identifier)
            String chunkId;
            String shortDocId = extractShortDocumentId(documentId);
            chunkId = String.format("%s-%s-%04d", options.chunkConfigId(), shortDocId, chunkIndex++);
            
            chunks.add(new Chunk(chunkId, finalChunkText, originalStartOffset, originalEndOffset));

            // Calculate next starting position with token-based overlap
            if (currentTokenEndIndex >= tokens.length) {
                break; // No more tokens to process
            }

            // Move forward by (tokensInThisChunk - tokenOverlap) to create proper token overlap
            int advancement = Math.max(1, tokensInThisChunk - tokenOverlap);
            currentTokenStartIndex += advancement;
            
            // Ensure we don't go beyond available tokens
            if (currentTokenStartIndex >= tokens.length) {
                break;
            }
        }

        LOG.debugf("Created %d token-based chunks from %d total tokens (target: %d tokens per chunk, overlap: %d tokens) for document part from field '%s'. streamId: %s, pipeStepName: %s",
                chunks.size(), tokens.length, targetTokensPerChunk, tokenOverlap, textFieldPath, streamId, pipeStepName);
        
        return new ChunkingResult(chunks, placeholderToUrlMap);
    }

    /**
     * Creates chunks using character-based algorithm with proper character counting for size and overlap.
     * 
     * @param textToProcess Text that has been preprocessed (URL placeholders if enabled)
     * @param placeholderToUrlMap Map of URL placeholders to original URLs
     * @param options Chunking configuration options (chunkSize = character count, chunkOverlap = character count)
     * @param config ChunkerConfig for ID generation (can be null)
     * @param streamId Stream ID for logging and chunk IDs
     * @param pipeStepName Pipeline step name for logging
     * @param documentId Document ID for chunk ID generation
     * @param textFieldPath Field path being chunked (for logging)
     * @return ChunkingResult containing chunks and URL placeholder mappings
     */
    private ChunkingResult createCharacterBasedChunks(String textToProcess, Map<String, String> placeholderToUrlMap,
                                                     ChunkerOptions options, ChunkerConfig config, String streamId,
                                                     String pipeStepName, String documentId, String textFieldPath) {
        
        LOG.debugf("Creating chunks with target character size: %d, character overlap: %d, for document ID: %s, streamId: %s, pipeStepName: %s",
                options.chunkSize(), options.chunkOverlap(), documentId, streamId, pipeStepName);

        if (textToProcess.isEmpty()) {
            LOG.debugf("No text to process. Returning empty chunks. streamId: %s, pipeStepName: %s", streamId, pipeStepName);
            return new ChunkingResult(Collections.emptyList(), placeholderToUrlMap);
        }

        List<Chunk> chunks = new ArrayList<>();
        int chunkIndex = 0;
        int currentCharStartIndex = 0;
        int targetCharsPerChunk = options.chunkSize(); // This is character count
        int charOverlap = options.chunkOverlap(); // This is character count
        int textLength = textToProcess.length();

        while (currentCharStartIndex < textLength) {
            // Determine how many characters to include in this chunk
            int charsInThisChunk = Math.min(targetCharsPerChunk, textLength - currentCharStartIndex);
            int currentCharEndIndex = currentCharStartIndex + charsInThisChunk;

            // Extract the chunk text
            String chunkTextWithPlaceholders = textToProcess.substring(currentCharStartIndex, currentCharEndIndex);
            
            if (chunkTextWithPlaceholders.isEmpty()) {
                break; // No more content to process
            }

            // Restore URLs from placeholders if needed
            String finalChunkText = chunkTextWithPlaceholders;
            if (options.preserveUrls() != null && options.preserveUrls()) {
                finalChunkText = restorePlaceholdersInChunk(chunkTextWithPlaceholders, placeholderToUrlMap);
            }

            // Character offsets are straightforward for character chunking
            int originalStartOffset = currentCharStartIndex;
            int originalEndOffset = currentCharEndIndex - 1;

            if (options.preserveUrls() != null && options.preserveUrls() && !placeholderToUrlMap.isEmpty()) {
                LOG.debugf("URL preservation is active, original character offsets for chunks might be approximate " +
                         "due to placeholder substitutions. StreamID: %s, DocID: %s", streamId, documentId);
            }

            // Generate clean, semantic chunk ID (node/step = options.chunkConfigId())
            String chunkId;
            String shortDocId = extractShortDocumentId(documentId);
            chunkId = String.format("%s-%s-%04d", options.chunkConfigId(), shortDocId, chunkIndex++);
            
            chunks.add(new Chunk(chunkId, finalChunkText, originalStartOffset, originalEndOffset));

            // Calculate next starting position with character-based overlap
            if (currentCharEndIndex >= textLength) {
                break; // No more characters to process
            }

            // Move forward by (charsInThisChunk - charOverlap) to create proper character overlap
            int advancement = Math.max(1, charsInThisChunk - charOverlap);
            currentCharStartIndex += advancement;
            
            // Ensure we don't go beyond available text
            if (currentCharStartIndex >= textLength) {
                break;
            }
        }

        LOG.debugf("Created %d character-based chunks from %d total characters (target: %d characters per chunk, overlap: %d characters) for document part from field '%s'. streamId: %s, pipeStepName: %s",
                chunks.size(), textLength, targetCharsPerChunk, charOverlap, textFieldPath, streamId, pipeStepName);
        
        return new ChunkingResult(chunks, placeholderToUrlMap);
    }

    /**
     * Creates chunks using sentence-based algorithm that respects sentence boundaries.
     * For sentence chunking, chunkSize represents the number of sentences per chunk.
     * 
     * @param textToProcess Text that has been preprocessed (URL placeholders if enabled)
     * @param placeholderToUrlMap Map of URL placeholders to original URLs
     * @param options Chunking configuration options (chunkSize = number of sentences, chunkOverlap = number of sentences)
     * @param config ChunkerConfig for ID generation (can be null)
     * @param streamId Stream ID for logging and chunk IDs
     * @param pipeStepName Pipeline step name for logging
     * @param documentId Document ID for chunk ID generation
     * @return ChunkingResult containing chunks and URL placeholder mappings
     */
    private ChunkingResult createSentenceBasedChunks(String textToProcess, Map<String, String> placeholderToUrlMap,
                                                     ChunkerOptions options, ChunkerConfig config, String streamId,
                                                     String pipeStepName, String documentId) {
        
        LOG.debugf("Creating sentence-based chunks with target sentences per chunk: %d, sentence overlap: %d, for document ID: %s, streamId: %s, pipeStepName: %s",
                options.chunkSize(), options.chunkOverlap(), documentId, streamId, pipeStepName);

        // Detect sentences and their positions
        String[] sentences = sentenceDetector.sentDetect(textToProcess);
        opennlp.tools.util.Span[] sentenceSpans = sentenceDetector.sentPosDetect(textToProcess);
        
        if (sentences.length == 0) {
            LOG.debugf("No sentences found in text. Returning empty chunks. streamId: %s, pipeStepName: %s", streamId, pipeStepName);
            return new ChunkingResult(Collections.emptyList(), placeholderToUrlMap);
        }

        List<Chunk> chunks = new ArrayList<>();
        int chunkIndex = 0;
        int currentSentenceIndex = 0;
        int sentencesPerChunk = options.chunkSize(); // This is the number of sentences per chunk
        int sentenceOverlap = options.chunkOverlap(); // This is the number of sentences to overlap

        while (currentSentenceIndex < sentences.length) {
            StringBuilder currentChunkBuilder = new StringBuilder();
            int chunkStartCharOffset = sentenceSpans[currentSentenceIndex].getStart();
            int chunkEndCharOffset = chunkStartCharOffset;
            int sentencesInThisChunk = 0;

            // Add up to sentencesPerChunk sentences to this chunk
            while (currentSentenceIndex + sentencesInThisChunk < sentences.length && sentencesInThisChunk < sentencesPerChunk) {
                String sentence = sentences[currentSentenceIndex + sentencesInThisChunk];
                
                // Add sentence to chunk
                if (currentChunkBuilder.length() > 0) {
                    currentChunkBuilder.append(" ");
                }
                currentChunkBuilder.append(sentence);
                chunkEndCharOffset = sentenceSpans[currentSentenceIndex + sentencesInThisChunk].getEnd();
                sentencesInThisChunk++;
            }

            // Create chunk if we have content
            if (currentChunkBuilder.length() > 0) {
                String chunkTextWithPlaceholders = currentChunkBuilder.toString().trim();
                
                String finalChunkText = chunkTextWithPlaceholders;
                if (options.preserveUrls() != null && options.preserveUrls()) {
                    finalChunkText = restorePlaceholdersInChunk(chunkTextWithPlaceholders, placeholderToUrlMap);
                }

                // Calculate offsets
                int originalStartOffset = chunkStartCharOffset;
                int originalEndOffset = chunkEndCharOffset - 1; // Span.getEnd() is exclusive

                // Generate chunk ID (node/step = options.chunkConfigId())
                String shortDocId = extractShortDocumentId(documentId);
                String chunkId = String.format("%s-%s-%04d", options.chunkConfigId(), shortDocId, chunkIndex++);
                
                chunks.add(new Chunk(chunkId, finalChunkText, originalStartOffset, originalEndOffset));
            }

            // Calculate next starting position with sentence-based overlap
            if (currentSentenceIndex + sentencesInThisChunk >= sentences.length) {
                break; // No more sentences to process
            }

            // Move forward by (sentencesInThisChunk - sentenceOverlap) to create overlap
            int advancement = Math.max(1, sentencesInThisChunk - sentenceOverlap);
            currentSentenceIndex += advancement;
        }

        LOG.debugf("Created %d sentence-based chunks from %d sentences (target: %d sentences per chunk, overlap: %d sentences). streamId: %s, pipeStepName: %s",
                chunks.size(), sentences.length, sentencesPerChunk, sentenceOverlap, streamId, pipeStepName);
        
        return new ChunkingResult(chunks, placeholderToUrlMap);
    }
}