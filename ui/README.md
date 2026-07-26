# Venturi Operator Dashboard

A read/light-write console over the [Venturi](../README.md) API: system
health and capability status, retrieval audit proof lookup, chain reference
viewing/linking, and legal hold placement/release. Every page is a thin view
over routes that already exist on the Venturi API — this app adds no new
backend surface.

Requires Elixir 1.15 or later.

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

## Production

The dashboard binds to `127.0.0.1` in production and uses OIDC Authorization
Code + PKCE authentication. Set `SECRET_KEY_BASE`,
`VENTURI_UI_OIDC_ISSUER`, `VENTURI_UI_OIDC_CLIENT_ID`, and the configured
operator/auditor group mappings, then place a TLS-terminating reverse proxy
in front of it. Operators may make changes; auditors have read-only routes.
Do not expose the dashboard port directly. See the root
[deployment guide](../DEPLOYMENT.md) for the full configuration.
