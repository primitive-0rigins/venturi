# VENTURI — Engineering Roadmap

**Status:** Living document — updated as engineering review passes complete.
**Philosophy:** Accuracy over speed. Regulated industries (healthcare, legal, finance) need correct
retrieval more than fast retrieval. Speed is optimized after correctness is proven.

> Portfolio note: this is a planning document. Treat only the Shipped section
> as describing the current codebase, and corroborate it against the code and
> tests. It is not a compliance attestation, product commitment, or substitute
> for the deployment limitations in [HIPAA_READINESS.md](HIPAA_READINESS.md).

---

## Shipped

Inventory of implemented work. Item IDs are the historical roadmap labels that
source comments and other documents reference. Design rationale lives with the
code.

### Retrieval

- **N1 — Overlay consensus retrieval** — `POST /retrieve/consensus` runs two or more
  retrieval modes over one query and splits results into a high-confidence core
  (orbs every mode agreed on) and supplementary single-mode hits, so callers get
  a confidence tier they can act on.
- **B10 — sqlite-vec backend + BM25 RRF fusion** — embeddings live on disk rather than
  in a resident `HashMap`. FTS5 keyword search and vector KNN run together and
  merge by Reciprocal Rank Fusion, so exact terms (drug names, ICD codes, case
  numbers) and semantic matches both land.
- **B11 — HyPE indexing** — at ingest time the embedding worker generates hypothetical
  questions each orb answers and embeds them alongside it, so a real query
  matches against questions instead of raw document prose. Controlled by
  `VENTURI_HYPE_ENABLED`.
- **R1 — Spectral community detection** — the knowledge graph clusters concepts over
  the normalized Laplacian of its edges and hyperedges, and traversal expands by
  community membership as well as BFS, so related content surfaces regardless of
  hop distance. Runs as a periodic background sweep.
- **N2 — Hyperedge-aware graph ingestion** — concept groups extracted from a summary
  are stored as a single hyperedge rather than dissolved into pairwise edges,
  preserving the joint co-occurrence signal.
- **B12 — Token budget enforcement** — retrieval accepts an optional `max_tokens` and
  stops assembling once the budget is spent, returning `token_budget_applied`.
- **B2 — Streaming document retrieval** — `GET /retrieve/document/:parent_id/stream`
  delivers a chain as newline-delimited JSON, one orb per line, always
  terminated by exactly one `done` or `error` line.
- **Metadata-only retrieval** — `POST /retrieve/metadata` returns catalog
  metadata without rehydrating content.
- **B8 — Retrieval stability checks** — an optional `check_stability` flag replays
  candidate selection and reports Jaccard agreement, flagging unstable results.
- **B9 — Foresight memory** — time-bounded predictive facts retrievable by when they
  become actionable, via `GET /retrieve/foresights?on=YYYY-MM-DD`.
- **B13 — Table-aware indexing** — `content_type` and `table_summary` on ingest let
  tabular content be indexed by its natural-language interpretation while the
  raw bytes are preserved verbatim.

### Governance and audit

- **R4 — Retrieval proofs** — every retrieval returns a `retrieval_audit_id`;
  `GET /audit/:retrieval_audit_id` returns the proof. Proofs never expose key IDs.
- **R5 — Expiry, tombstones, and legal hold** — expiry deletes shelf bytes and keys
  while preserving a tombstone; legal hold blocks expiry until explicitly
  released, with the release recorded.
- **B4 — Data classification per orb** — required at ingest, filterable at retrieval;
  `secret` content is kept out of embedding and graph indexing.
- **B3 / B7 — Summary trust metadata and verified fact atoms** — summaries carry author,
  model, and verification state; individual facts can be marked verified, and
  metadata retrieval can return only those.
- **B6 — Cross-chain reference edges** — chains can declare that they supersede,
  support, contradict, or cite another, enabling blast-radius queries.
- **R3 — Actor ID on all retrieval** and stable failure categories across API,
  audit, and test paths.
- **B1 — Per-agent rate limits** keyed to the authenticated API key.

### Runtime and durability

- **R2 — Single-owner worker thread** — one thread owns all SQLite state; handlers
  send commands over a bounded channel and await a reply. A full channel returns
  a typed overload error rather than queuing without limit.
- **Durable embedding queue** — SQLite-backed, survives restarts.
- **B15 — Lifecycle tiering** — per-actor hot/warm/cold tiering over embeddings in
  working memory, with pinned-orb protection and cache-tier visibility on
  responses. Orbs always remain on disk; tiering governs RAM only.
- **B5 — Capability degradation** — `/health` reports per-subsystem readiness, so
  ingest and retrieval stay available while embedding is degraded.
- **Partial rehydration with corruption markers**, hard storage size limits, and
  `MemoryNotFound` as a first-class typed result.
- **Catalog reconciliation** — a failed catalog write no longer leaves an orb
  durable but unfindable; registration is replayed at startup from journal state.
- **B17 — Orb format validation** — the disk parser rejects unsupported versions,
  mismatched parent bindings, invalid chain positions, length overflow, and
  trailing bytes.

### Operability

- **O1 — Operator dashboard** — `ui/` is a standalone Elixir/Phoenix app over the
  existing HTTP API: health and capability status, retrieval audit lookup, chain
  reference viewing and linking, and legal hold placement and release. It adds
  no new backend endpoints.

### Test and hardening passes

- **S1 — Benchmark suite** — deterministic local coverage of ten retrieval failure
  modes (basic, constrained, not-found, completeness, conflicting,
  intra-document, stale, semantic, recall, high-level synthesis) with no LLM
  required.
- **S2 — Corruption drills** — modified orbs, missing orbs, wrong keys, catalog
  holes, partial chain rehydration, interrupted journals, and invalid input all
  assert fail-closed behavior.
- **B16 — Fault-tolerance audit** — mutex poison recovery, catalog reconciliation, and
  explicit keystore file permissions.

---

## In progress

Nothing currently in flight.

---

## Near-term horizon

No item is selected yet. The next one is chosen only after the shipped surface
above is reviewed against real operator needs rather than added to speculatively.

---

## Source Index

Items in this roadmap were derived from:

| Source | Date | License | What we took |
|---|---|---|---|
| EnterpriseRAG-Bench | 2026-05-29 | MIT | Question taxonomy → benchmark suite; answer_facts → summary atoms; document_recall → orb recall metric; info_not_found → MemoryNotFound |
| HypergraphPartitioning | 2026-05-29 | BSD 3-Clause | Overlay concept → consensus retrieval; hyperedge model → graph ingestion; spectral Laplacian → community detection |
| hollow-agentOS | 2026-05-29 | MIT | Cross-chain lineage → reference edges; L5 fact-check → claim verification; checkpoint/replay → consistency scoring |
| EverOS | 2026-05-29 | Apache 2.0 | Foresight memory type; BM25/RRF concept |
| CocoIndex | 2026-05-29 | Apache 2.0 | sqlite-vec disk-backed vector index (replaces in-memory HashMap, combined with BM25+RRF) |
| RAG_Techniques | 2026-05-29 | MIT | HyPE (Hypothetical Prompt Embeddings); query-to-query matching at indexing time |
| LightRAG | 2026-05-29 | MIT | Token budget enforcement on retrieval responses |
| agentic-rag (Fareed Khan) | 2026-05-29 | MIT | Content type + table summary on ingest; embedding sidecar concept |
| MemOS-main | 2026-05-29 | Apache 2.0 | DreamMemoryLifecycle metadata → context lifecycle manager |
