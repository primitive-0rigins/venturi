# Contributing to Venturi

## Before opening a pull request

Keep changes focused, add regression coverage for bug fixes, and avoid adding
dependencies unless they are necessary. Never commit runtime databases, keys,
audit records, or `.env` files.

Run the required checks:

```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings

cd ui
mix format --check-formatted
mix test
```

## Pull requests

Explain the behavior change, tests run, and any operational or security impact.
For security-sensitive issues, follow [SECURITY.md](SECURITY.md) instead of
filing a public pull request or issue.

By contributing, you agree that your contribution is licensed under the MIT
License in this repository.
