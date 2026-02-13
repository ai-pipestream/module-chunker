# module-chunker

Text chunking module for the [Pipestream](https://github.com/ai-pipestream) document processing platform. Splits document text into smaller, overlapping chunks suitable for embedding and semantic search.

## Chunking Algorithms

| Algorithm | Description |
|-----------|-------------|
| `token` (default) | Token-based chunking using OpenNLP tokenization |
| `sentence` | Sentence-boundary chunking using NLP sentence detection |
| `character` | Character-based chunking with sliding window |
| `semantic` | Semantic chunking (planned) |

All algorithms support configurable chunk size and overlap. URL preservation keeps links intact across chunk boundaries.

## APIs

### gRPC

Implements `PipeStepProcessorService` with two RPCs:

- **`ProcessData`** — accepts a `PipeDoc` with a `ChunkerConfig` JSON payload, returns the doc enriched with chunked `SemanticProcessingResult`
- **`GetServiceRegistration`** — returns module metadata and the ChunkerConfig JSON Schema

### REST

Base path: `/modules/chunker/api/chunker/service`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/simple` | Chunk text with basic options |
| POST | `/advanced` | Chunk a full PipeDoc |
| POST | `/test` | Quick test with plain text (`text/plain`) |
| POST | `/process-json` | Process with a complete ChunkerConfig JSON |
| GET | `/config` | Get ChunkerConfig JSON Schema |
| GET | `/health` | Health check with NLP model status |
| GET | `/demo/documents` | List available demo documents |
| POST | `/demo/chunk/{filename}` | Chunk a demo document with recommended settings |

Swagger UI is available at `/modules/chunker/q/swagger-ui`.

## Configuration

### ChunkerConfig

Passed as JSON via gRPC or REST:

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `algorithm` | string | `token` | character, token, sentence, semantic | Chunking algorithm |
| `sourceField` | string | `body` | | Document field to extract text from |
| `chunkSize` | int | `500` | 50–10000 | Target size per chunk (units depend on algorithm) |
| `chunkOverlap` | int | `50` | 0–5000 | Overlap between consecutive chunks |
| `preserveUrls` | boolean | `true` | | Keep URLs as atomic units during chunking |
| `cleanText` | boolean | `true` | | Normalize whitespace and line endings before chunking |

### Application Properties

| Property | Default | Description |
|----------|---------|-------------|
| `quarkus.http.port` | `39002` | HTTP/gRPC port |
| `quarkus.http.root-path` | `/modules/chunker` | Context path |
| `module.name` | `chunker` | Module name for platform registration |
| `pipestream.registration.enabled` | `true` | Auto-register with platform |

Service discovery uses Consul via SmallRye Stork. Set `CONSUL_HOST` and `CONSUL_PORT` environment variables to configure.

## Chunk Metadata

Each chunk is enriched with metadata including:

- **Text stats** — word count, character count, sentence count, vocabulary density
- **Position** — is_first_chunk, is_last_chunk, relative_position
- **Content analysis** — punctuation counts, heading score, list item detection

## Building

```bash
./gradlew clean build
```

## Testing

```bash
./gradlew test
```

## Docker

```bash
# Build
./gradlew clean build
docker build -f src/main/docker/Dockerfile.jvm -t module-chunker .

# Run
docker run --rm -p 39002:39002 \
  -e CONSUL_HOST=consul \
  -e CONSUL_PORT=8500 \
  module-chunker
```

Docker profile uses port `39102` with gRPC reflection enabled.

## Requirements

- JDK 21+
- Gradle 9.x
