# Release Procedure

## Preflight

Run from a clean checkout:

```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo audit
cargo deny check advisories licenses

cd ui
mix format --check-formatted
mix test
MIX_ENV=prod mix assets.deploy
```

Build the release binary with `cargo build --release`, then follow the isolated
restore verification in [BACKUP_RESTORE.md](BACKUP_RESTORE.md).

## Publish

1. Ensure CI is green and branch protection requires it.
2. Update `CHANGELOG.md` with the release version and date.
3. Create an annotated `vX.Y.Z` tag from the reviewed commit.
4. Attach the release binary and a SHA-256 checksum to the hosted release.
5. Enable private vulnerability reporting on the repository host before
   announcing the release.

This checkout has no configured Git remote, so pushing, configuring repository
settings, creating a hosted release, and publishing a tag require the release
owner to perform them.
# Regulated deployment release procedure

For a HIPAA-ready deployment, release owners must record the source commit,
Rust/Phoenix test results, dependency/license/vulnerability review, and a
signed release archive. Generate a CycloneDX SBOM with:

```bash
./scripts/generate-sbom.sh
```

Sign the SBOM and artifacts with the organization's approved signing key and
publish verification instructions and checksums. A release without those
records is not approved for a regulated deployment.
