# Venturi

Venturi is a local, encrypted memory system for AI applications. It stores
original content as sealed chunks, indexes summaries and graph metadata in
SQLite, and rehydrates the original bytes on retrieval.

## Run locally

Venturi requires a local Ollama-compatible embedding service. By default it
uses `http://localhost:11434`, stores state in `~/venturi-data`, and listens
only on `127.0.0.1:9271`.

```bash
cargo run --release
```

Optional environment variables:

- `VENTURI_DATA` — directory for local memory, keys, and SQLite databases.
- `VENTURI_PORT` — localhost HTTP port (default `9271`).
- `VENTURI_OLLAMA` — local embedding service URL.
- `VENTURI_EMBEDDING_MODEL` and `VENTURI_EMBEDDING_DIM` — embedding settings.

## Security boundary

Venturi encrypts stored content and keeps its HTTP server bound to localhost.
Its API does not authenticate callers: any local process that can reach the
server can access the memory it is permitted to request. Keep the data
directory private, do not commit it, and put authentication and network access
control in front of Venturi before exposing it beyond the local machine.

## Verify

```bash
cargo test
```

## License

MIT. See [LICENSE](LICENSE).
