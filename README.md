# module-chunker

Text chunking module for the Pipestream document processing platform.

## Overview

The chunker module splits document text into smaller, overlapping chunks suitable for embedding and semantic search. It supports multiple chunking algorithms and exposes both gRPC and REST APIs.

## Building

```bash
./gradlew clean build
```

## Testing

```bash
./gradlew test
```

## Configuration

Key configuration properties (via `application.properties` or environment variables):

| Property | Description | Default |
|---|---|---|
| `module.name` | Module name for registration | `chunker` |
| `quarkus.http.port` | HTTP port | `39002` |
| `mp.openapi.version` | OpenAPI spec version | `3.1.0` |
