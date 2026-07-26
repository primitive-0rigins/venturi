# Support Policy

## Versioning

Venturi follows semantic versioning once `v1.0.0` is released. Before then,
the public API, on-disk schema, and operational procedures may change between
minor releases.

## Supported environments

The automated checks cover Rust stable on Linux and the Phoenix dashboard on
Elixir 1.18 / OTP 27. The dashboard declares compatibility with Elixir 1.15
or later, but only the CI version is continuously verified.

Venturi requires local writable storage for SQLite databases and the orb shelf.
An Ollama-compatible service is optional: semantic retrieval, graph extraction,
and HyPE indexing degrade when it is unavailable.

## Security support

Security fixes target the latest tagged release; before the first tag, they
target `master`. Report vulnerabilities as described in
[SECURITY.md](SECURITY.md).
