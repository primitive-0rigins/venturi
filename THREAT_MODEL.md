# Threat Model

## Security properties provided

- Content is compressed then encrypted with ChaCha20-Poly1305 before it is
  written to the orb shelf.
- Raw chain keys are stored separately from catalog metadata in the keystore.
- The API binds to loopback by default and requires a Bearer key for memory
  operations. Keys have endpoint scopes: `read`, `write`, and `admin`.
- The production dashboard binds to loopback and uses OIDC Authorization Code
  + PKCE with operator and auditor roles; it expects a TLS-terminating reverse
  proxy.
- Stored orbs verify their format, parent binding, and content-derived ID on
  read. Corrupt content is surfaced as a warning or typed error.

## Explicitly out of scope

- Per-user record permissions or a multi-organization tenancy model. Namespace
  grants isolate supported HIPAA-profile operations; unscoped retrieval modes
  are disabled in that profile.
- TLS termination, network segmentation, host hardening, secret distribution,
  and physical storage security.
- Key rotation, key revocation, and re-encryption of existing chains.
- Protection from a host-level attacker able to read the keystore and process
  memory, or from an operator who holds an admin key.
- Compliance certification or a complete audit/retention program.

## Operator requirements

Run under a dedicated OS account, keep the data directory and environment
secrets private, terminate remote traffic with TLS, restrict network access,
and test backup/restore before handling sensitive data. Treat the keystore and
the data directory as one recovery unit.
