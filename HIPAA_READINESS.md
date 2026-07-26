# HIPAA-ready self-hosted deployment profile

Venturi's `hipaa` deployment profile is a technical safeguard baseline for a
customer-operated, single-organization deployment that may handle ePHI. It is
not a HIPAA certification, compliance determination, BAA, or a substitute for
the customer's risk analysis, policies, workforce training, physical
safeguards, and contractual obligations.

## Enable the profile

Set `VENTURI_DEPLOYMENT_PROFILE=hipaa`. Startup fails closed unless all of the
following are configured:

- `VENTURI_RETENTION_DAYS` is a positive number of days or `indefinite`.
- `VENTURI_AUDIT_SIGNING_KEY` is a 32-byte hexadecimal Ed25519 seed held in a
  customer-managed secret.
- `VENTURI_TLS_PROXY=enabled` declares that the localhost API is behind the
  customer's TLS proxy and network policy.
- `VENTURI_UI_OIDC_ISSUER` and `VENTURI_UI_OIDC_CLIENT_ID` identify the
  dashboard's OIDC deployment. Configure and validate the issuer, discovery
  document, authorization-code PKCE callback, audience, signature, expiry,
  and group claim in the proxy or IdP-aware dashboard integration before use.
- `VENTURI_AGENT_KEYS` contains named, namespace-granted service keys. The
  required format is `name:key:read|write|admin:namespace1|namespace2`.

The name is the service principal used by the Rust API; request-body
`agent_id` is compatibility metadata and is never the authenticated actor.
Legacy three-part keys remain only outside this profile and imply `*`; migrate
them before enabling it.

## Safeguard mapping and responsibilities

| Area | Venturi implements | Customer / operators must implement |
| --- | --- | --- |
| Access control | Scoped named keys, localhost binding, protected API routes | Unique workforce accounts, OIDC group mapping, periodic access review, emergency-access policy |
| Audit controls | Append-only hash-chained events, retrieval proofs, retention decisions | Protect and review exports, retain evidence, investigate alerts |
| Integrity | Encrypted orbs, checksummed object parsing, write-ahead recovery | Backup integrity checks, OS patching, change approval |
| Authentication | Named service credentials | OIDC issuer/JWKS validation, MFA, session timeout and revocation |
| Transmission security | Localhost API deployment model | TLS termination, certificate lifecycle, firewall and proxy policy |
| Administrative / physical | N/A | Risk analysis, training, incident response, facilities and media controls, BAAs/vendor assessments |

## Evidence templates

Maintain these customer-owned records for each deployment: risk analysis;
system inventory and data-flow diagram; access-review log; incident response
and breach-escalation runbook; backup/restore test evidence; change-management
record; workforce-training acknowledgement; and vendor/BAA assessment. Link
each record to the deployment version, data location, responsible owner, date,
approver, and evidence location.

Reusable tables are in [docs/hipaa-templates.md](docs/hipaa-templates.md).

## Retention and audit

Retention is a customer policy. `indefinite` disables expiry; otherwise the
daily sweep removes expired chains unless a legal hold is active. Each delete
or hold-preservation decision is recorded without content. New HIPAA-profile
retrieval audit events omit raw queries and proof filters. The event database
uses a SHA-256 hash chain; include the final hash and a customer-managed
signature when exporting JSONL for independent verification.

## Namespace migration and boundary

Namespaces are the in-instance isolation boundary. In HIPAA mode, ingestion,
context, structured retrieval, metadata retrieval, retrieval-proof lookup,
holds, links, references, and verdicts require a granted namespace. Retrieval
operations that have not yet been made namespace-aware (document, graph,
consensus, temporal, and foresights) are rejected rather than falling back to
global results.

Before enabling HIPAA mode on existing data, stop the service, make a tested
backup, and run the explicit, idempotent migration as the Venturi OS account:

```bash
VENTURI_DATA=/var/lib/venturi venturi migrate-namespace default
```

Record the command, result, backup identifier, and approver in change-control
evidence. The migration assigns only legacy blank namespace rows and writes a
content-free audit event.

Export an integrity-checked, signed audit trail with stdout redirected to a
protected destination. The command writes detached verification metadata to
stderr; retain it with the JSONL export.

```bash
VENTURI_DATA=/var/lib/venturi venturi export-audit > audit.jsonl
```
