# Deployment Guide

## Supported operation

Venturi is developed and tested on Linux with Rust's stable toolchain. The
operator dashboard requires Elixir 1.15 or newer. A local Ollama-compatible
service enables semantic retrieval, graph extraction, and HyPE processing;
keyword and metadata retrieval continue to work when it is unavailable.

## Service account and storage

Run Venturi under a dedicated operating-system account. Give that account sole
access to `VENTURI_DATA`; it contains encrypted orbs, SQLite catalogs, audit
records, and the raw-key keystore. Keep it on durable storage and back up the
entire directory atomically while the service is stopped or from a
filesystem-consistent snapshot.

Set a long random `VENTURI_ADMIN_KEY`, bind the API only to localhost, and put
any remote access behind a TLS-terminating reverse proxy and network policy.
Do not expose the API or Phoenix dashboard ports directly.

## API service

```bash
export VENTURI_DATA=/var/lib/venturi
export VENTURI_ADMIN_KEY="$(openssl rand -base64 32)"
export VENTURI_PORT=9271
export VENTURI_OLLAMA=http://127.0.0.1:11434
cargo run --release
```

Configure scoped agent keys only where needed. `VENTURI_AGENT_KEYS` accepts
comma-separated `name:key:scope:namespaces` entries, where `scope` is `read`,
`write`, or `admin` and namespaces are `|` separated. The key name is the
authenticated service identity. The legacy three-part form grants all
namespaces and must not be used with the HIPAA profile. There is no automated
key-rotation or re-encryption tool; plan a maintenance window and tested
backup/restore process before changing credentials or storage paths. Before
enabling the HIPAA profile on existing catalog data,
stop the service, back up `VENTURI_DATA`, and run
`venturi migrate-namespace default` as the service account. This idempotently
assigns legacy blank namespace rows and records the administrative migration.

## Operator dashboard

Build assets and run the dashboard with production secrets:

```bash
cd ui
mix deps.get
MIX_ENV=prod mix assets.deploy
export PHX_SERVER=true
export SECRET_KEY_BASE="$(mix phx.gen.secret)"
export VENTURI_UI_OIDC_ISSUER="https://idp.example.org"
export VENTURI_UI_OIDC_CLIENT_ID="venturi-dashboard"
export VENTURI_UI_OIDC_OPERATOR_GROUPS="venturi-operator"
export VENTURI_UI_OIDC_AUDITOR_GROUPS="venturi-auditor"
export VENTURI_API_URL=http://127.0.0.1:9271
export VENTURI_API_KEY="$VENTURI_ADMIN_KEY"
MIX_ENV=prod mix phx.server
```

The dashboard uses OIDC Authorization Code + PKCE through its OIDC library and
maps a configured group claim to operator or auditor. Operators can make
changes and auditors have read-only routes. Before production use, test the
chosen IdP's issuer/discovery, token audience/signature/expiry validation,
nonce handling, callback URL, and group claims in the deployed environment.
The production endpoint listens on `127.0.0.1`. Configure the reverse proxy
to forward the original HTTPS scheme so Phoenix's `force_ssl` protection
operates correctly.

## HIPAA-ready profile

See [HIPAA_READINESS.md](HIPAA_READINESS.md). `VENTURI_DEPLOYMENT_PROFILE=hipaa`
fails startup without explicit retention, audit-signing, TLS-proxy, OIDC, and
named-key settings. Use the reference [nginx configuration](deploy/nginx-venturi.conf)
and [systemd unit](deploy/venturi.service) as deployment starting points.
Run `venturi export-audit > audit.jsonl` with the configured signing key to
produce an integrity-checked JSONL export; retain the command's stderr
verification metadata alongside the protected export.

## Recovery

On startup, Venturi removes incomplete ingestions and reconciles committed
chains that were not fully catalogued. A failed or corrupt orb is reported as
a retrieval warning rather than silently returning altered content. Restore
the full data directory from the same snapshot if SQLite catalog, shelf, and
keystore state diverge.
