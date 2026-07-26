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

The latest tagged release receives security fixes. Until the first release is
tagged, security fixes target the current `main` branch.

## Deployment note

Venturi encrypts stored content and defaults to loopback-only HTTP, but it is
not a compliance certification. Operators remain responsible for TLS, network
segmentation, access control, backups, retention, and key handling.
