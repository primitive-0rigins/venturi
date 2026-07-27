# Venturi

Venturi is a local, encrypted memory system for AI applications. It stores
original content as sealed chunks, indexes summaries and graph metadata in
SQLite, and rehydrates the original bytes on retrieval.

> Portfolio and deployment disclaimer: Venturi is an engineering project, not
> a HIPAA certification, compliance determination, hosted healthcare service,
> BAA, or legal advice. Its HIPAA-ready profile is a self-hosted technical
> safeguard baseline; the deploying organization remains responsible for risk
> analysis, policies, workforce controls, physical safeguards, contracts, and
> validating its production environment.

## Run locally

Venturi requires an administrator API key. A local Ollama-compatible embedding
service enables semantic retrieval and graph extraction; keyword and metadata
retrieval remain available when it is unavailable. By default Venturi
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
- `VENTURI_AGENT_KEYS` — optional comma-separated `name:key:scope:namespaces`
  entries; namespaces are `|`-separated. The three-field legacy form grants
  all namespaces and is rejected by the HIPAA profile.
- `VENTURI_RETENTION_DAYS` — a positive duration in days or `indefinite`.

## Security boundary

Venturi encrypts stored content, keeps its HTTP server bound to localhost, and
requires a Bearer API key for every memory operation. An administrator key is
required at startup; optional scoped agent keys are configured with
`VENTURI_AGENT_KEYS` for read, write, or full administrative access. Keep keys
and the data directory private. If you expose Venturi beyond the local machine,
put it behind TLS and a network boundary.

## Operator dashboard

`ui/` is a standalone Elixir/Phoenix app that gives an operator a read/light-write
console over the API: health, retrieval audit lookup, chain references, and
legal hold. It talks to Venturi over HTTP and adds no new backend endpoints.
In production it binds only to localhost, requires OIDC Authorization Code +
PKCE authentication, and expects a TLS-terminating reverse proxy.

Configure issuer, client ID, PKCE redirect URI, and IdP groups before enabling
the HIPAA-ready profile; see [HIPAA_READINESS.md](HIPAA_READINESS.md).

```bash
cd ui
mix deps.get
VENTURI_API_URL="http://127.0.0.1:9271" VENTURI_API_KEY="$VENTURI_ADMIN_KEY" mix phx.server
```

For a production dashboard, set `SECRET_KEY_BASE`, the OIDC issuer/client
configuration, and the operator/auditor group mappings; expose it only through
a TLS reverse proxy. Basic-auth environment variables are not used.

## Verify

```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo deny check advisories licenses
```

See [DEPLOYMENT.md](DEPLOYMENT.md), [BACKUP_RESTORE.md](BACKUP_RESTORE.md),
and [RELEASE.md](RELEASE.md) for operations, recovery, and releases.
[SECURITY.md](SECURITY.md) covers vulnerability reporting; supported versions
and security boundaries are in [SUPPORT.md](SUPPORT.md) and
[THREAT_MODEL.md](THREAT_MODEL.md).

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) and follow the
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

[FSL-1.1-ALv2](LICENSE) — the Functional Source License with an Apache-2.0
future license.

- Running Venturi for your own internal use and access is a Permitted Purpose,
  as are non-commercial education, non-commercial research, and professional
  services you provide to a licensee.
- The one restriction is Competing Use: you may not make Venturi available to
  others in a commercial product or service that substitutes for it or offers
  substantially similar functionality.
- Each released version additionally becomes available under the Apache License,
  Version 2.0 on the second anniversary of that version's publication. The
  conversion is per release, not one date for the project.
- Versions published on or before the relicense commit remain available under
  the MIT License they were released under.

This summary is for orientation only; [LICENSE](LICENSE) is the controlling
text.
