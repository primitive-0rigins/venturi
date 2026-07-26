#!/usr/bin/env bash
set -euo pipefail

# Generate a CycloneDX SBOM when cargo-cyclonedx is installed. The release
# process signs both this file and the release archive with customer-approved
# signing infrastructure.
command -v cargo-cyclonedx >/dev/null || {
  echo "cargo-cyclonedx is required: cargo install cargo-cyclonedx" >&2
  exit 1
}
cargo cyclonedx --format json
