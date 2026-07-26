# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository
host's private vulnerability-reporting mechanism and include a minimal
reproduction, affected version or commit, impact, and any suggested mitigation.

Maintainers will acknowledge reports within seven days and coordinate a fix and
disclosure timeline privately. If private reporting is not enabled on the
repository host, contact the project maintainer through the account that owns
the release before publishing details.

## Supported versions

Only the latest release on the default branch receives security fixes before a
formal support policy is published.

## Deployment note

Venturi encrypts stored content and defaults to loopback-only HTTP, but it is
not a compliance certification. Operators remain responsible for TLS, network
segmentation, access control, backups, retention, and key handling.
