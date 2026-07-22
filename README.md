# Venturi

Venturi is a local, encrypted memory system for AI applications. It stores
original content as sealed chunks, indexes summaries and graph metadata in
SQLite, and rehydrates the original bytes on retrieval.

## Run locally

Venturi requires a local Ollama-compatible embedding service and an
administrator API key. By default it
uses `http://localhost:11434`, stores state in `~/venturi-data`, and listens
only on `127.0.0.1:9271`.

```bash
export VENTURI_ADMIN_KEY="replace-with-a-long-random-secret"
cargo run --release
```

Optional environment variables:

- `VENTURI_DATA` — directory for local memory, keys, and SQLite databases.
- `VENTURI_PORT` — localhost HTTP port (default `9271`).
- `VENTURI_OLLAMA` — local embedding service URL.
- `VENTURI_EMBEDDING_MODEL` and `VENTURI_EMBEDDING_DIM` — embedding settings.
- `VENTURI_ADMIN_KEY` — required administrator key. Venturi refuses to start
  without it.

## Security boundary

Venturi encrypts stored content, keeps its HTTP server bound to localhost, and
requires a Bearer API key for every memory operation. An administrator key is
required at startup; it can issue scoped agent keys for read, write, or full
administrative access. Keep keys and the data directory private. If you expose
Venturi beyond the local machine, put it behind TLS and a network boundary.

## Operator dashboard

`ui/` is a standalone Elixir/Phoenix app that gives an operator a read/light-write
console over the API: health, retrieval audit lookup, chain references, and
legal hold. It talks to Venturi over HTTP and adds no new backend endpoints.

```bash
cd ui
mix deps.get
VENTURI_API_URL="http://127.0.0.1:9271" VENTURI_API_KEY="$VENTURI_ADMIN_KEY" mix phx.server
```

## Verify

```bash
cargo test
```

## License

MIT. See [LICENSE](LICENSE).
