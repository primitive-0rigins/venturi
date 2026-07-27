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

## Sign-off and licensing

Every commit needs a `Signed-off-by` line, which you get from `git commit -s`.
It certifies the [Developer Certificate of Origin](https://developercertificate.org)
— in plain terms, that you wrote the change or otherwise have the right to
submit it. By signing off you also agree that your contribution is licensed to
this project under its current license, and that the maintainer may release it
under a different license in future versions of Venturi.
