# Venturi Operator Dashboard

A read/light-write console over the [Venturi](../README.md) API: system
health and capability status, retrieval audit proof lookup, chain reference
viewing/linking, and legal hold placement/release. Every page is a thin view
over routes that already exist on the Venturi API — this app adds no new
backend surface.

## Run locally

Venturi itself must be running first (see the root README). Then:

```bash
mix deps.get
VENTURI_API_URL="http://127.0.0.1:9271" VENTURI_API_KEY="$VENTURI_ADMIN_KEY" mix phx.server
```

Visit [`localhost:4000`](http://localhost:4000).

Environment variables:

- `VENTURI_API_URL` — base URL of the Venturi API (default `http://localhost:8080`).
- `VENTURI_API_KEY` — Bearer key sent on every request. Needs admin scope to
  use the legal hold and chain link pages; a read-scoped key is enough for
  health and audit lookup.

## Verify

```bash
mix test
```
