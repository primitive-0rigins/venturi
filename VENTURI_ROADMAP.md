# VENTURI — Engineering Roadmap

**Status:** Living document — updated as engineering review passes complete.
**Philosophy:** Accuracy over speed. Regulated industries (healthcare, legal, finance) need correct
retrieval more than fast retrieval. Speed is optimized after correctness is proven.

---

## How to read this

Each item has a **phase**, **effort**, **value**, and **source** (where the idea came from).
Items are ordered within each phase by value / effort ratio.

**Phases:**
- `NOW` — current sprint, actively being built
- `COMPLETED` — implemented and covered by tests in the current codebase
- `NEXT` — next sprint, fully specified, ready to pick up
- `SOON` — specified enough to start, needs a design pass first
- `ROADMAP` — confirmed direction, implementation design not started
- `BACKLOG` — good idea, not yet scheduled

---

## Phase: NOW (active)

Nothing currently in flight. Current codebase has completed the main accuracy,
governance, and operator-hardening pass:
- Durable embedding queue (SQLite-backed, survives restarts)
- Hard size limits (`StorageLimits` struct)
- Partial rehydration with corruption markers (3× retry, gap marker)
- Metadata-only retrieval mode (`POST /retrieve/metadata`)
- Actor ID on all retrieval (Scribe audit)
- Summary atoms (`answer_facts: Vec<String>`, independently embedded)
- Orb recall in Scribe EXIT events (`recall: Option<f32>`)
- `MemoryNotFound` as first-class typed result (`NotFoundReason` enum)
- Overlay consensus retrieval (`POST /retrieve/consensus`)
- Hyperedge-aware graph ingestion and traversal
- Deterministic benchmark and corruption drill suites
- Stable failure categories in API/Scribe/test paths
- Retrieval proofs with `GET /audit/:retrieval_audit_id`
- Expiry tombstones, legal hold, and hold-release API
- Per-agent rate limits for ingest and retrieval
- Summary trust metadata, classification, verified fact atoms, chain references,
  foresights, table-aware indexing, token budgets, retrieval stability checks,
  sqlite-vec/FTS5 retrieval fusion, HyPE indexing, and lifecycle tiering

---

## Phase: NEXT

No new NEXT item is selected. Pick the next item only after the completed
surface above is reviewed against operator needs.

---

## Phase: COMPLETED

### N1 — Overlay Consensus Retrieval
**Status:** Implemented — `Venturi::consensus()` and
`POST /retrieve/consensus` return core vs. supplementary chunks with retrieval
proof IDs.

**Effort:** Medium (2–3 days) | **Value:** High
**Source:** HypergraphPartitioning — overlay.jl + cut_distillation.jl

New retrieval mode: `POST /retrieve/consensus`

Run two or more retrieval modes against the same query, find orbs that appear in
multiple mode results — those are the **high-confidence core**. Orbs that appeared
in only one mode are **supplementary**.

```
consensus retrieval:
  run context()    → orb set A
  run graph_query() → orb set B
  core         = intersection(A, B)   -- every mode agrees
  supplementary = union(A, B) - core  -- single-mode hits
```

Response shape:
```json
{
  "core_chunks":         [...],
  "supplementary_chunks": [...],
  "core_count":          3,
  "supplementary_count": 2,
  "modes_run":           ["context", "graph"],
  "warnings":            []
}
```

**Why:** Agents currently have no confidence signal at the retrieval level — every
orb looks equally valid. The core/supplementary split gives them a tier they can
act on: cite core with confidence, surface supplementary for review.

**Where it goes:**
- New `Venturi::consensus()` method in `src/api.rs`
- New `retrieve_consensus` handler + route in `src/server.rs`
- No new DB tables — reuses existing retrieval methods
- Modes list is caller-specified (default: context + graph)

---

### N2 — Hyperedge-Aware Graph Ingestion
**Status:** Implemented — `graph.db` now stores hyperedges and traversal expands
through hyperedge co-members.

**Effort:** Medium (1–2 days) | **Value:** Medium-High
**Source:** HypergraphPartitioning — graphification.jl + hypergraph.jl

Currently `ingest_summary` extracts pairwise concept edges. When a summary mentions
"patient, hospital, chest pain, Dr. Smith," we create 6 pairwise edges and lose
the signal that **all four co-occurred simultaneously**.

Add a `hyperedges` table to graph.db:
```sql
CREATE TABLE IF NOT EXISTS hyperedges (
    edge_id   TEXT PRIMARY KEY,  -- UUID
    parent_id TEXT NOT NULL,     -- which orb chain produced this
    members   TEXT NOT NULL,     -- JSON array of concept node IDs
    weight    REAL NOT NULL DEFAULT 1.0
);
CREATE INDEX IF NOT EXISTS idx_he_parent ON hyperedges(parent_id);
```

`ingest_summary` writes one hyperedge record per extracted concept group (in
addition to, or instead of, pairwise edges).

BFS in `traverse()` expands through both `edges` and `hyperedges`. A concept
reachable via a hyperedge (even with no direct pairwise edge) is included.

**Why:** Pairwise edges dilute co-occurrence signal. A hospital and a diagnosis
appearing together in one document is stronger evidence than two separate pairwise
edges would suggest. Hyperedges preserve that joint signal.

**Where it goes:** `src/intelligence/graph.rs` — schema, ingest_summary, traverse

### S1 — Venturi Benchmark Suite
**Status:** Implemented — deterministic local benchmark suite covers the 10
EnterpriseRAG-Bench-inspired retrieval cases without requiring an LLM.

**Effort:** Medium (2–3 days) | **Value:** High
**Source:** EnterpriseRAG-Bench — question taxonomy

Build a deterministic local benchmark covering all 10 retrieval failure modes
identified by EnterpriseRAG-Bench. No LLM required — all assertions are
content-equality checks or MemoryNotFound checks.

Categories to cover (in order of implementation priority):

| # | Category | What it tests | Test approach |
|---|---|---|---|
| 1 | Basic | Exact single-orb match | Already covered |
| 2 | Constrained | Filter eliminates all but one matching orb | Ingest 3 similar orbs, filter by date/domain |
| 3 | Info Not Found | Query for non-existent content | Expect `MemoryNotFound::NoSimilarContent` |
| 4 | Completeness | Query must return ALL N related orbs | Assert orb recall = 1.0 |
| 5 | Conflicting Info | Two orbs contradict each other | Both returned, neither suppressed |
| 6 | Intra-Document | Combines distant sections of a long chain | Long multi-orb chain, verify full assembly |
| 7 | Stale/Superseded | v2 ingested after v1, same topic | Both retrievable, ordered by date |
| 8 | Semantic | No keyword overlap, meaning must match | Embedding-dependent (soft skip if Ollama down) |
| 9 | Completeness Recall | `expected_orb_ids` recall = 1.0 check | Use new `record_verdict` recall field |
| 10 | High Level | Synthesis across multiple chains | graph_query or consensus mode |

**Where it goes:** `tests/benchmark.rs` — new test file, ~300 lines

---

### S2 — Corruption Drill Tests
**Status:** Implemented — disk corruption, missing orbs, wrong keys, catalog
holes, partial chain rehydration, startup recovery, and invalid input are covered.

**Effort:** Low–Medium (1 day) | **Value:** High
**Source:** Design notes — Corruption Drills section

Intentional corruption tests proving fail-closed behavior:

- Orb file modified on disk → decryption fails → `OrbCorrupted`
- Orb file missing → `OrbNotFound`
- Wrong key for orb → `WrongKey`
- Catalog row references missing orb → graceful skip
- Chain with one corrupted orb → partial rehydration with corruption marker
- Journal stuck IN_PROGRESS → recovered on startup

Each test should assert: no silent success, no unauthenticated content returned,
explicit error category, Scribe records failure.

**Where it goes:** `tests/corruption.rs` — new test file

---

## Phase: ROADMAP

### R1 — Spectral Community Detection on Knowledge Graph
**Effort:** High (1–2 weeks) | **Value:** Very High
**Source:** HypergraphPartitioning — SpectralPartitioning.jl, K_SpecPart algorithm

**The core idea:** The current `graph_query()` does BFS — it's local and greedy.
It only finds concepts within N hops of the query's anchor nodes. Two concepts can
be semantically in the same community but separated by many hops — BFS misses
them. Spectral analysis sees the **whole graph at once** and surfaces natural
concept communities regardless of hop distance.

**Algorithm (Laplacian eigenvectors):**
1. Build adjacency matrix A from `concepts` + `edges` in graph.db
2. Compute degree matrix D (diagonal, D[i,i] = sum of edge weights for concept i)
3. Form the normalized Laplacian: L = I - D^(-1/2) A D^(-1/2)
4. Compute the K smallest non-zero eigenvectors of L (K = 5–10)
5. Use eigenvector coordinates as concept embeddings
6. Run k-means on those coordinates → assign `community_id` to each concept
7. Store `community_id TEXT` on each concept node in graph.db

**How retrieval improves:**
- `graph_query()` currently: BFS from anchor concepts, 2 hops
- After R1: `graph_query()` uses community membership. Query "security incidents"
  → find anchor concepts → look up their community_id → return all orbs whose
  parent chains contributed concepts in that community
- Result: surfaces related content even when hop distance is large

**Performance note:** Eigenvalue computation is O(n²) to O(n³) for dense graphs.
For Venturi's typical graph sizes (hundreds to low thousands of concepts), this is
fast on modern hardware. Schedule as a background sweep — runs after every N new
ingestions or on a time interval, same pattern as `sweep_tiers`.

**Implementation:**
- Add `nalgebra` crate for matrix/eigenvector computation
- New `CommunityDetector` in `src/intelligence/` or `src/pipeline/`
- New background sweep in `main.rs` alongside existing three sweeps
- New `community_id` column on `concepts` table in graph.db
- `traverse()` updated to use community filter as a secondary pass

**Why this matters for regulated industries:** A hospital deploying Venturi needs
"find everything related to this patient" to actually find *everything*, not just
what's within 2 BFS hops. Spectral methods guarantee global coverage. The accuracy
improvement is worth the compute cost — accuracy is not negotiable in clinical
contexts.

---

### R2 — Single Owner Worker (Channel-Based SQLite)
**Effort:** High (1 week) | **Value:** High
**Source:** Design notes — "Use a Single Owner Worker for SQLite First"

Current architecture: `Mutex<Venturi>` — one lock for all operations. Reads
block writes and each other. This is the correct solution for rusqlite's `!Sync`
constraint but it serializes everything.

**Target architecture:**
```
Axum request
  → command enum (Ingest | Retrieve | Sweep | ...)
  → tokio::sync::mpsc channel
  → Venturi worker thread (owns all SQLite connections)
  → result channel back
  → Axum response
```

Benefits:
- Ingest ordering preserved
- No lock contention in async handlers
- Opens the door to Phase 2: dedicated retrieval workers with read-only WAL connections
- Makes backpressure explicit: channel has bounded capacity → `queue_full` errors
  instead of unbounded queuing

**Implementation order:**
1. Define `VenturiCommand` enum covering all current API operations
2. Spawn worker thread, move `Venturi` into it
3. Replace `Arc<Mutex<Venturi>>` in server with `Arc<CommandSender>`
4. Each handler sends command, awaits response on a oneshot channel
5. Add channel capacity limit → `ServiceUnavailable` when full

---

### R3 — Failure Taxonomy
**Status:** Implemented — `TunnelError::category()` and
`NotFoundReason::category()` provide stable machine-readable categories used by
API responses and tests.

**Effort:** Low (0.5 days) | **Value:** Medium-High
**Source:** Design notes — Failure Taxonomy section

Extend `TunnelError` + `NotFoundReason` into a stable, documented failure taxonomy
that maps 1:1 across API responses, Scribe events, test assertions, and operator
messages.

Stable categories (additions to what exists):
```
metadata_invalid      — bad topic/domain/date at ingest
summary_invalid       — summary over 100 words, or missing
journal_incomplete    — crash recovery found IN_PROGRESS
journal_corrupt       — journal rows inconsistent
shelf_unavailable     — orb directory inaccessible
chain_incomplete      — fetch_chain returned fewer orbs than chain_length
catalog_inconsistent  — orb in journal but not in librarian
embedding_unavailable — already exists as NotFoundReason
graph_unavailable     — KnowledgeGraph failed to open
legal_hold            — (future) deletion blocked by hold
overloaded            — channel full, retry_after_ms in response
```

**Where it goes:** `src/types/error.rs` additions + `src/server.rs` mapping

---

### R4 — Retrieval Proofs
**Status:** Implemented — retrieval responses carry `retrieval_audit_id`, Scribe
records proof payloads, and `GET /audit/:retrieval_audit_id` returns the proof
without exposing key IDs.

**Effort:** Medium (2 days) | **Value:** High (regulated industry requirement)
**Source:** Design notes — Retrieval Proofs section

Every retrieval result should be able to explain itself. Return alongside content:

```json
{
  "retrieval_audit_id": "...",
  "actor_id": "example-agent",
  "mode": "consensus",
  "query": "patient chest pain admission",
  "filters_applied": {"domain": "medical"},
  "candidate_count": 12,
  "selected_orb_ids": ["abc", "def"],
  "selected_parent_ids": ["xyz"],
  "key_ids_used": [],
  "chain_complete": true,
  "retrieval_timestamp": "1748534400Z"
}
```

Key rule: `key_ids_used` is empty in the proof — the Librarian knows key_ids but
the proof must not expose them. The proof proves *access happened* without proving
*which key was used*.

**Why:** In regulated environments, a retrieval proof is a compliance artifact.
"Show me the audit trail for every time this patient's record was accessed" is a
real legal requirement.

**Where it goes:** Scribe records full proof as a new `RETRIEVAL_PROOF` event type.
API returns `retrieval_audit_id` in every retrieval response. Separate endpoint
`GET /audit/:retrieval_audit_id` returns the full proof.

---

### R5 — Deletion, Expiry, and Legal Hold Semantics
**Status:** Implemented — expiry deletes shelf bytes and keys while preserving
tombstones; legal hold blocks expiry until explicitly released.

**Effort:** Medium (2–3 days) | **Value:** High (regulated industry blocker)
**Source:** Design notes — Deletion/Expiry/Legal Hold section

Currently expiry deletes orb bytes and key. Define the full semantics:

```
Normal expiry:
  delete orb bytes from shelf
  delete key from keystore
  keep tombstone row in librarian (orb_id, parent_id, expired_at, reason)
  keep Scribe audit entry

Legal hold:
  block key/orb deletion
  set legal_hold_reason TEXT on orb catalog row
  require explicit operator release (API call with reason)
  record who/what released the hold in Scribe
```

New fields on librarian `orbs` table:
- `legal_hold BOOLEAN NOT NULL DEFAULT 0`
- `legal_hold_reason TEXT`
- `expired_at TEXT` (NULL = not expired)

**Where it goes:** `src/intelligence/librarian.rs` + `src/pipeline/sweep.rs` +
new API endpoint `POST /hold` and `DELETE /hold/:parent_id`

---

## Phase: COMPLETED BACKLOG ITEMS

### B1 — Per-Agent Rate Limits and Backpressure
**Status:** Implemented phase 1 — per-agent rolling-window limits protect ingest
and retrieval endpoints. Full channel backpressure remains part of R2.

**Effort:** Medium | **Value:** Medium
**Source:** Design notes — Backpressure section

Track ingest and retrieval counts per `agent_id` within a rolling window.
Return `retry_after_ms` when an agent exceeds its quota. Prevents a runaway
agent from starving others through the single Mutex.

---

## Phase: BACKLOG

### B2 — Streaming Document Retrieval
**Effort:** High | **Value:** Medium
**Source:** Design notes — Concurrency Roadmap Phase 3

For very large document chains (hundreds of orbs), return content as a chunked
stream rather than waiting for full reassembly. Requires axum streaming response.
Not needed until document sizes warrant it.

---

## Phase: COMPLETED BACKLOG ITEMS CONTINUED

### B3 — Summary Trust Metadata
**Status:** Implemented — ingest accepts summary author/model/verification fields
and metadata retrieval surfaces verified facts without exposing key pointers.

**Effort:** Low | **Value:** Medium
**Source:** EnterpriseRAG-Bench (summary_author concept) + Design notes

Add to `IngestionRequest` and librarian catalog:
- `summary_author: String` — who wrote the summary (agent_id or "human")
- `summary_model: Option<String>` — which model generated it, if any
- `summary_verified: bool` — human has confirmed accuracy
- `summary_verified_at: Option<String>` — when verification happened

Lets retrieval distinguish verified human summaries from model-generated ones.
High value in regulated contexts (clinical notes vs. auto-summaries).

### B4 — Data Classification per Orb
**Status:** Implemented — classification is required at ingest, stored in the
catalog, filterable in structured/metadata queries, and `secret` content is kept
out of embedding and graph indexing.

**Effort:** Low–Medium | **Value:** Medium-High
**Source:** Design notes — Data Classification section

Add `classification TEXT NOT NULL DEFAULT 'internal'` to orbs table.
Classes: `public | internal | sensitive | regulated | secret`

Each class gates:
- Whether summary can be embedded (public yes, secret no)
- Whether graph extraction is allowed (regulated: extract entities but not content)
- Retrieval restrictions (secret: require explicit parent_id, no similarity search)

### B5 — Lazy Startup with Capability Degradation
**Status:** Implemented phase 1 — `/health` reports subsystem capability states;
embedding can be degraded while ingest and retrieval remain available.

**Effort:** Medium | **Value:** Low-Medium
**Source:** Design notes — Lazy Startup section

On startup, report capabilities immediately:
```json
{"embedding": "degraded", "graph": "degraded", "retrieval": "ready", "ingest": "ready"}
```

Background tasks bring capabilities online as indexes rebuild. Currently startup
blocks until everything is ready, which is slow when the librarian DB is large.

---

### B6 — Cross-Chain Reference Edges
**Status:** Implemented — chain references are stored in librarian.db and exposed
through `POST /chain/link` and `GET /chain/references/:parent_id`.

**Effort:** Low | **Value:** Medium-High
**Source:** hollow-agentOS — lineage DAG concept

Add a `chain_references` table to librarian.db:
```sql
CREATE TABLE IF NOT EXISTS chain_references (
    from_parent_id  TEXT NOT NULL,
    to_parent_id    TEXT NOT NULL,
    reference_type  TEXT NOT NULL,  -- "supersedes" | "supports" | "contradicts" | "cites"
    created_at      TEXT NOT NULL,
    PRIMARY KEY (from_parent_id, to_parent_id, reference_type)
);
```

Callers can declare that a chain supersedes an older one, or that evidence
in chain A supports a claim in chain B. Enables blast-radius queries:
"if chain X is retracted, which chains cited it?"

Critical for regulated contexts: clinical retraction cascades, legal exhibit
cross-references, security incident → remediation linkage.

**Where it goes:** `src/intelligence/librarian.rs` schema + new `link_chains` method;
new `POST /chain/link` and `GET /chain/references/:parent_id` endpoints in server.rs

---

### B7 — Ingest-Side Claim Verification Tags
**Status:** Implemented — legacy string facts are converted to unverified atoms;
callers can submit verified fact atoms and metadata retrieval returns only
verified facts.

**Effort:** Low | **Value:** Medium
**Source:** hollow-agentOS — L5 fact-check validation layer

Extend `answer_facts` atoms with a verification flag. Current atom: plain String.
New atom: `{ "fact": "...", "verified": false, "verified_by": null }`.

When an agent ingests a summary, it can mark individual facts as verified
(human-checked or tool-checked) vs. model-generated assertions. Retrieval
metadata can then surface only verified facts to downstream agents that
require high-confidence claims.

Maps to B3 (Summary Trust Metadata) — implement together.

**Where it goes:** `src/intelligence/gatekeeper.rs` IngestionRequest;
`src/intelligence/librarian.rs` fact storage; MetaRow response

---

### B9 — Foresight Memory Type
**Status:** Implemented — foresights are stored per parent chain and exposed via
`GET /retrieve/foresights?on=YYYY-MM-DD`.

**Effort:** Low–Medium | **Value:** High (regulated industry requirement)
**Source:** EverOS — Foresight memory type

A time-bounded predictive memory: a fact that becomes relevant at a future date.
Fields: `foresight_text`, `relevant_from`, `relevant_until`, `duration_days`.

Examples in regulated industries:
- "Patient medication runs out 2026-06-15 — alert window: 2026-06-08 to 2026-06-15"
- "Contract auto-renews 2026-09-01 — review window: 30 days prior"
- "Annual audit due 2026-12-01"

Retrieved via temporal mode with a `date_relevant_on` parameter — "give me all
active foresights for today." Currently Venturi retrieves by document date (when
something was recorded). Foresight retrieves by *when a fact becomes actionable*.

**Where it goes:**
- New `foresights` table in librarian.db
- New `foresight` field on `IngestionRequest` (optional `Vec<Foresight>`)
- `temporal()` gains optional `date_relevant_on` filter
- New `GET /retrieve/foresights?on=2026-06-15` endpoint in server.rs

---

### B10 — sqlite-vec Vector Backend + BM25 RRF Fusion
**Status:** Implemented — resident embedding `HashMap` removed; FTS5 BM25 + RRF
fusion path implemented; optional sqlite-vec extension loading is gated by
`VENTURI_SQLITE_VEC_EXTENSION`; embeddings are mirrored into a lazy Vec0 table
when sqlite-vec is available, with BLOB scan fallback when it is not.

**Effort:** Medium | **Value:** Very High (RAM bloat blocker)
**Source:** CocoIndex analysis (2026-05-29) + EverOS BM25/RRF concept

**Current problem:** Venturi loads ALL embeddings into a `HashMap<String, Vec<f32>>`
at startup via `load_embeddings()`. Every orb embedding and every fact atom
embedding lives in RAM permanently. This directly contradicts the local-first,
resource-constrained deployment target.

**Solution: two-part replacement**

**Part 1 — sqlite-vec vector backend (replaces HashMap)**

Add sqlite-vec as a rusqlite extension. Move embeddings from BLOB columns into a
Vec0 virtual table with HNSW indexing. Vectors stay on disk; KNN queries hit the
index, not RAM. Nothing loaded at startup.

```sql
-- replaces in-memory HashMap
CREATE VIRTUAL TABLE IF NOT EXISTS embeddings_vec USING vec0(
    orb_id TEXT PRIMARY KEY,
    embedding FLOAT[768]   -- dimension matches Ollama model
);
```

`similarity_search()` becomes a SQL KNN query:
```sql
SELECT orb_id, distance
FROM embeddings_vec
WHERE embedding MATCH ?  -- query vector
ORDER BY distance
LIMIT ?
```

**Part 2 — FTS5 keyword index + RRF fusion (runs alongside vec)**

Add SQLite FTS5 virtual table over summaries and answer_facts. For each
`context()` query, run FTS5 BM25 search in parallel with sqlite-vec KNN,
fuse results via Reciprocal Rank Fusion:

```
RRF score = Σ 1/(k + rank)   where k = 60 (standard constant)
```

Keyword path catches exact terms (drug names, ICD codes, case numbers, legal
citations) that semantic similarity misses. Both paths are pure SQLite — no
new processes, no new services.

**Why together:** sqlite-vec alone solves RAM. FTS5+RRF alone still requires
loading candidate embeddings for final scoring. Together: FTS5 pre-filters
candidate set → sqlite-vec scores semantically → RRF merges → top-k returned.
Full retrieval with near-zero RAM footprint.

**Dependency:** sqlite-vec loadable extension (C, single file, MIT license).
Load via `rusqlite::Connection::load_extension()`.

**Where it goes:**
- `src/intelligence/librarian.rs` — swap `embeddings: HashMap` field for
  sqlite-vec Vec0 table; add FTS5 virtual table; rewrite `similarity_search()`
  and `load_embeddings()` to use SQL; add RRF merge step in `context()`
- `Cargo.toml` — no new Rust crates; sqlite-vec loaded as extension at runtime

---

### B11 — HyPE (Hypothetical Prompt Embeddings)
**Status:** Implemented — embedding worker can generate HyPE questions after a
summary embedding succeeds, queue them with `source='hype'`, and embed them
through the existing fact embedding path. Controlled locally with
`VENTURI_HYPE_ENABLED` (default enabled).

**Effort:** Low–Medium | **Value:** High (precision + recall without query-time LLM)
**Source:** RAG_Techniques — HyPE pattern

**The problem:** When an agent queries Venturi for "patient medication interaction," the
query embedding must match *document* embeddings. Documents describe facts; queries
describe intentions. The vocabulary gap means high-signal orbs can miss on pure
cosine similarity.

**The idea:** At *indexing time*, generate N hypothetical questions that each orb's
content would answer. Embed those questions and store them alongside the orb.
At query time, the user's real query matches against *other queries*, not raw
document text — vocabulary mismatch nearly disappears.

**How it fits the existing pipeline:**

Venturi already has `answer_facts: Vec<String>` atoms that are independently embedded
and stored in `fact_embeddings`. HyPE is the same pipeline, one step earlier:

```
ingest → process_embedding_queue → generate_hype_questions() → push to fact_queue
                                                                      ↓
                                                            fact_embeddings table
                                                            (tagged source="hype")
```

`generate_hype_questions()` calls Ollama with the orb summary:
```
"Generate 5 questions that this text is the answer to:
{summary}"
```

Returns e.g.:
- "What medication was the patient taking on admission?"
- "What is the contraindication between drug A and drug B?"
- "What allergies were documented at intake?"

Each question is pushed through the existing `process_fact_queue` path with a new
source tag (`source = "hype"` vs. current `source = "fact"`). The `fact_embeddings`
table already has the right schema — no migrations needed.

`similarity_search()` already searches `fact_embeddings`. Orbs whose hypothetical
questions match the real query surface naturally — no query-time LLM call, no new
retrieval path.

**Failure modes handled:**
- Ollama down at ingest time: skip question generation, log warning, orb still
  ingested normally. HyPE questions can be generated later via a re-queue pass.
- Config flag `hype_enabled: bool` controls the step — can disable per deployment.
- Bad LLM output (non-questions, empty response): drop silently, do not block ingest.

**Claimed improvement (from RAG_Techniques):** +42pp precision, +45pp recall over
raw embedding retrieval. Conservative estimate for regulated-domain vocabulary:
assume 20–30pp precision gain. Even conservative gains matter when an agent is
searching clinical records or legal exhibits.

**Where it goes:**
- `src/intelligence/librarian.rs` — new `generate_hype_questions(orb_id, summary)`
  function called inside `process_embedding_queue`; questions pushed to `fact_queue`
  with `source = "hype"`
- `src/config.rs` — `hype_enabled: bool` field (default: true)
- No new DB tables, no new crates, no new API surface

---

### B12 — Token Budget Enforcement on Retrieval Responses
**Status:** Implemented — content retrieval accepts optional `max_tokens`,
applies a whitespace-based token approximation before adding each chunk/orb, and
returns `token_budget_applied` plus a warning when truncation occurs.

**Effort:** Low (0.5 day) | **Value:** High (immediate value for LLM callers)
**Source:** LightRAG — max_entity_tokens / max_relation_tokens / max_total_tokens pattern

**The problem:** Venturi returns all retrieved orbs regardless of size. A `context()`
call that matches 40 orbs returns all 40 — potentially 80k+ tokens of text. Any
agent building an LLM prompt on top of that has no way to tell Venturi to stop
early; it must receive everything and truncate itself, wasting decrypt+decompress
work on orbs that never reach the prompt.

**Solution:** Add `max_tokens: Option<u32>` to retrieval request types. After
assembling the candidate orb list (similarity search + decrypt + decompress),
walk the list in score order, accumulate token counts, stop when the budget is
exhausted. Return only what fits.

Token counting: count whitespace-split words as a conservative proxy
(`words * 1.3 ≈ tokens`). No tokenizer dependency — the approximation is
sufficient for context window budgeting; callers who need exact counts do their
own final check.

```rust
// ContextRequest gains one optional field
pub struct ContextRequest {
    pub query:      String,
    pub actor_id:   String,
    pub top_k:      Option<usize>,
    pub max_tokens: Option<u32>,   // ← new
    pub filters:    Option<FilterParams>,
}
```

Assembly loop in `context()`:
```rust
let mut token_budget = request.max_tokens;
let mut chunks = Vec::new();

for orb in candidates {
    let content = decrypt_and_decompress(orb)?;
    let approx_tokens = (content.split_whitespace().count() as f32 * 1.3) as u32;
    if let Some(budget) = token_budget {
        if approx_tokens > budget { break; }
        token_budget = Some(budget - approx_tokens);
    }
    chunks.push(content);
}
```

Response gains a `token_budget_applied: bool` flag so callers know truncation
occurred and can decide to re-query with different parameters.

**Where it goes:**
- `src/types/request.rs` — `max_tokens: Option<u32>` on `ContextRequest`,
  `DocumentRequest`, `StructuredRequest`
- `src/intelligence/librarian.rs` — budget accumulation loop in `context()`,
  `document()`, `structured()`
- `src/server.rs` — parse new query param `?max_tokens=4096`; pass to retrieval
- `src/types/response.rs` — `token_budget_applied: bool` on retrieval responses

---

### B8 — Retrieval Consistency Scoring
**Status:** Implemented — `/retrieve/context` accepts `check_stability`; context
retrieval can replay candidate selection, compute Jaccard similarity over orb
IDs, and return a `stability` response field with an unstable warning below
0.8.

**Effort:** Low | **Value:** Medium
**Source:** hollow-agentOS — checkpoint + replay with Jaccard consistency

Run the same query twice (second pass with a tiny score perturbation),
compare orb_id sets. If Jaccard similarity < 0.8, annotate retrieval
result with `"stability": "unstable"` warning.

Unstable retrievals indicate the embedding space is not converged (e.g.,
background worker is mid-batch). Useful signal for agents to decide
whether to retry after a short wait.

Best implemented as an optional flag `?check_stability=true` on the
`/retrieve/context` endpoint to avoid doubling latency by default.

**Where it goes:** `src/api.rs` context() — optional second pass;
`src/server.rs` — query param + ContextResponse stability field

---

### B13 — Content Type Awareness + Table Summary on Ingest
**Status:** Implemented — ingest accepts `content_type` and `table_summary`;
table content preserves raw bytes while indexing and graph ingestion use the
natural-language table summary. Librarian stores `content_type` in metadata.

**Effort:** Low | **Value:** Medium-High (regulated industry data is heavily tabular)
**Source:** agentic-rag (Fareed Khan) — chunk_by_title table-atomic-unit pattern

Currently Venturi treats all orbs as plain text. A caller ingesting a clinical lab
results table, a financial schedule, or a legal exhibit table gets no special
handling — the table is embedded as whatever text the caller happened to provide.
Structure is lost. A query for "creatinine values above 1.5" can't match a table
whose content string is raw HTML or CSV.

**Add `content_type` to `IngestionRequest`:**
```rust
pub enum ContentType {
    Text,       // default — narrative prose
    Table,      // structured rows/columns
    TimeSeries, // temporal numeric data
    Code,       // source code or SQL
}

pub struct IngestionRequest {
    // ... existing fields ...
    pub content_type:  Option<ContentType>,  // ← new, defaults to Text
    pub table_summary: Option<String>,       // ← new, natural language interpretation
}
```

When `content_type = Table`, the caller provides both the raw table (as `content`)
and a `table_summary` — a natural language interpretation of the table's key
insights: "Lab results show creatinine 2.1 mg/dL on 2026-05-15, above threshold.
HbA1c 8.4%, uncontrolled." The `table_summary` is what gets embedded and entered
into `fact_embeddings`. The raw table structure is preserved in the orb bytes and
returned on retrieval.

This means a semantic query ("patient with elevated creatinine") matches against
the interpretation, not the raw numbers — exactly how a clinician would search.

**Where it goes:**
- `src/types/request.rs` — `ContentType` enum + `table_summary` field on `IngestionRequest`
- `src/intelligence/gatekeeper.rs` — route table ingest through `table_summary` path
- `src/intelligence/librarian.rs` — store `content_type` on orb catalog row;
  use `table_summary` as the embedding target when present
- `src/intelligence/librarian.db` schema — `content_type TEXT DEFAULT 'text'` on orbs table

---

### B14 — Evolving Embedding Sidecar
**Status:** Implemented phase 1 infrastructure — embedding model name and
dimension are config/env driven, Librarian records the active model version, and
retrieval proofs include `embedding_model_version` for future training data
lineage.

**Effort:** High (long-term, phased) | **Value:** Very High (self-improving retrieval)
**Source:** Architecture decision — agentic-rag embedding sidecar pattern

Venturi's librarian and gatekeeper both run a permanent embedding sidecar —
currently nomic-embed-text, a general-purpose model. This sidecar is not static.
It is the most natural application of a self-improving retrieval loop, where
usage data continuously trains a better embedding model than the general-purpose
default.

**The evolution path:**

```
Phase 1 (now):       nomic-embed-text (general, 768-dim)
                     permanent sidecar process, called by process_embedding_queue

Phase 2 (data acc.): fine-tune nomic-embed-text on Scribe data
                     what queries retrieved what orbs
                     which retrievals were followed by ingest (signal: useful)
                     which retrievals returned MemoryNotFound (signal: gap)
                     actor_id + domain + query + retrieved_orb_ids → training pairs

Phase 3 (volume):    7B domain embedding model trained from Scribe corpus
                     embedding space reflects YOUR data, YOUR actors, YOUR vocabulary
                     "creatinine" in a clinical Venturi is not the same semantic
                     neighborhood as "creatinine" in a general corpus

Phase 4 (maturity):  per-tenant embedding models
                     a legal Venturi and a clinical Venturi have separate embedding
                     spaces trained on separate Scribe histories
```

**What Venturi needs to support this today:**

The embedding function is already abstracted — `process_embedding_queue` calls
`embed_text()` which hits the Ollama sidecar. The sidecar model name is config-driven.
Swapping nomic-embed-text for a fine-tuned model is a config change, not a code
change — IF the embedding dimension matches (or the vec table is rebuilt).

The only infrastructure needed now: **log the full retrieval context to Scribe** in
a format usable as training data. The RETRIEVAL_PROOF event (R4) is the right home
for this. When R4 lands, every retrieval event contains: actor_id, query, mode,
filters, selected_orb_ids, embedding_model_version. That IS the training signal.

**What this means architecturally:**
- The embedding model is not a dependency. It is a slot. It gets upgraded.
- Venturi's retrieval quality compounds over time without changing the retrieval code.
- The Scribe log IS the dataset foundry input for embedding model evolution.
- The embedding dimension must be fixed per Vec0 table — model upgrades require
  a migration step (rebuild embeddings_vec with new model). Plan for this.

**Where it goes:**
- `src/config.rs` — `embedding_model: String`, `embedding_dim: usize` (already need these for sqlite-vec B10)
- `src/intelligence/librarian.rs` — log `embedding_model` version in RETRIEVAL_PROOF event
- No code changes needed for model swap — sidecar is already abstracted

---

### B15 — Context Lifecycle Manager (Hot/Warm/Cold Tiering)
**Status:** Implemented phase 1 + hardening (2026-05-29): per-agent hot caps,
pinned protection, cache tier visibility, and Scribe daemon health events.

**Effort:** Medium (2–3 days) | **Value:** High (RAM protection at scale)
**Source:** MemOS-main — DreamMemoryLifecycle concept + Skeptic review 2026-05-29

**The problem:** Venturi is on track to eliminate the startup `HashMap` (B10), but retrieval
still loads embeddings into working memory per query. With 25 simultaneous agents across 6
nodes, each with their own corpus, active working sets compete for RAM. No mechanism exists
to evict unused orb context from memory — everything that's been touched stays touched.

**The design:** Three memory tiers, driven by wall clock time, scoped per actor_id.

```
HOT  → embedding loaded, fast path retrieval
  ↓ idle for T_warm (default: 5 min)
WARM → still loaded, flagged for eviction
  ↓ idle for T_cold (default: 10 min total)         ↑ 2 accesses in window → HOT
COLD → embedding dropped from working memory
       orb stays on disk, reloaded on demand (slower)
```

**Why wall clock, not turns:** Request frequency varies widely across callers.
"Turns" have no universal definition. Wall clock time is caller-agnostic and always
available.

**Why 2 accesses for WARM→HOT:** Single-access promotion causes thrashing — periodic
workflows access an orb once every 11 minutes, promoting it to HOT, which then sits idle
and gets evicted again. Two accesses within one eviction window prove genuine active use.

**Schema additions to orbs table:**
```sql
ALTER TABLE orbs ADD COLUMN last_accessed_at  TEXT;       -- ISO8601, updated on every retrieval hit
ALTER TABLE orbs ADD COLUMN access_count      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE orbs ADD COLUMN usefulness_score  REAL    NOT NULL DEFAULT 1.0;  -- future: feedback decay
ALTER TABLE orbs ADD COLUMN pinned            BOOLEAN NOT NULL DEFAULT 0;    -- never evict
```

---

### B16 — Fault-Tolerance Audit Pass
**Status:** Implemented (2026-07-13): four hardening fixes found and closed in
one pass — a security-adjacent-hardening review of the whole codebase, not a
response to a live incident.

**Effort:** Medium (~1 day) | **Value:** High (single-point-of-failure removal)
**Source:** Codebase scan for bugs/stubs/hardening gaps at owner's request

**1. Mutex poisoning cascade.** Every HTTP handler locked `SharedVenturi` and
the rate limiter via plain `std::sync::Mutex::lock().unwrap()` (20+ call
sites). A single panic anywhere under either lock poisoned it permanently —
every request to every endpoint from then on panicked too, with no recovery
short of a process restart. Fixed with `server::lock_mutex()`, a generic
helper that recovers via `PoisonError::into_inner()` instead of unwrapping:
the guarded data is still valid after a panic (the panic means one request
didn't finish, not that the state is corrupt), so recovering and continuing
is correct. All call sites in `server.rs` and `main.rs` now go through it.

**2. Catalog registration could silently vanish with no way back.**
Ingestion's steps 1–3 (seal, shelf write, journal) are atomic with crash
recovery (`recover_incomplete()`). Steps 4–6 (Librarian catalog, graph index,
Scribe audit) were fire-and-forget — the doc comment claimed "catalog can be
rebuilt," but no rebuild mechanism existed anywhere. A failed catalog write
left an orb durable on disk and undeletable, but permanently unfindable
through search. Fixed: `journal.db`'s `ingestions` table gained
`request_json` (the ingestion metadata minus raw chunks, captured at
`open_ingestion` time) and `catalog_registered` (set only once Librarian
registration actually succeeds). `Gatekeeper::reconcile_catalog()` finds
`COMPLETE` rows still at `catalog_registered = 0` and replays registration
using the stored metadata plus the chain key from the keystore. Runs once at
startup alongside `recover_incomplete()`. See `tests/catalog_reconcile.rs`.

**3. Keystore file permissions didn't match the documented invariant.** The
doc comment on `Keystore::open()` said the exit-gate keystore file should be
chmod 600 — the code only set the *parent directory* to 0700, never the file
itself. Directory permissions already blocked other users in practice, but
this is the one file holding every raw encryption key in the system, so it
now gets explicit 0600 permissions (plus its `-wal`/`-shm` sidecars) as
defense in depth, matching what the comment already claimed.

**Per-actor RAM budget cap:**
Each `actor_id` has a configurable `max_hot_orbs` limit (default: 500 orbs = ~1.5MB embeddings).
Eviction is scoped per actor — one agent's large corpus cannot crowd out others.
Total worst-case RAM across 25 agents: 25 × 1.5MB = 37.5MB. Trivially safe.

```rust
// config.rs additions
pub struct LifecycleConfig {
    pub enabled:        bool,
    pub t_warm_secs:    u64,    // default: 300  (5 min)
    pub t_cold_secs:    u64,    // default: 600  (10 min)
    pub max_hot_orbs:   usize,  // default: 500 per actor_id
    pub sweep_interval: u64,    // default: 60 sec
}
```

**Eviction daemon (supervised async task):**
```rust
// spawned in main.rs alongside existing sweeps
tokio::spawn(async move {
    loop {
        tokio::time::sleep(sweep_interval).await;
        venturi.lifecycle_sweep().await;
        // heartbeat: log sweep completion timestamp
        // if 3 consecutive sweeps missed → warn, fall back to no-eviction mode
    }
});
```

`lifecycle_sweep()` per actor_id:
1. Load `(orb_id, last_accessed_at, access_count, pinned)` from orbs table
2. Skip pinned orbs
3. Compute idle duration = now - last_accessed_at
4. If idle >= T_cold AND access_count_in_window < 2: mark COLD, drop from working set
5. If idle >= T_warm: mark WARM

**Pinned orbs:** System-level facts, agent identity records, active task context.
Pinned orbs never enter WARM or COLD regardless of idle time.
```rust
// example: pin via ingest flag
pub struct IngestionRequest {
    // ... existing ...
    pub pinned: Option<bool>,   // ← new, defaults false
}
```

**Retrieval response gains tier visibility:**
```rust
pub struct ContextResponse {
    // ... existing ...
    pub cache_tier: String,   // "hot" | "warm" | "cold"
}
```
Cold retrievals are slower (disk reload). Callers can log this. Over time, chronically-cold
but frequently-queried orbs become training signal for the embedding sidecar (B14).

**Failure safety:** If eviction daemon panics or misses 3 cycles, Venturi falls back to
no-eviction mode — RAM grows but nothing breaks. No silent data loss. Scribe records
daemon health events.

**Where it goes:**
- `src/config.rs` — `LifecycleConfig` struct
- `src/intelligence/librarian.rs` — `lifecycle_sweep()`, schema migration, `last_accessed_at` + `access_count` updates on every retrieval hit
- `src/types/request.rs` — `pinned: Option<bool>` on `IngestionRequest`
- `src/types/response.rs` — `cache_tier: String` on retrieval responses
- `src/main.rs` — supervised eviction daemon task alongside existing sweeps

---

### B17 — Orb Format Validation Debug Pass
**Status:** Implemented (2026-07-13): the disk parser now rejects unsupported
format versions, mismatched parent bindings, invalid chain positions, content
length overflow, and trailing bytes. Retrieval now supplies the catalogued
parent ID to the shelf parser instead of an empty placeholder, making the
serialized parent hash an enforced integrity boundary. Three parser regression
tests cover the newly enforced invariants; the full 74-test suite passes.

**Effort:** Small | **Value:** High (detect malformed or misbound stored orbs)
**Source:** CPU-only debug pass at owner's request

---

## Source Index

Items in this roadmap were derived from:

| Source | Date | License | What we took |
|---|---|---|---|
| EnterpriseRAG-Bench | 2026-05-29 | MIT | Question taxonomy → benchmark suite; answer_facts → summary atoms; document_recall → orb recall metric; info_not_found → MemoryNotFound |
| HypergraphPartitioning | 2026-05-29 | BSD 3-Clause | Overlay concept → consensus retrieval; hyperedge model → graph ingestion; spectral Laplacian → community detection |
| Design notes | 2026-05-29 | Private design notes | Owner worker, failure taxonomy, retrieval proofs, legal hold, backpressure |
| hollow-agentOS | 2026-05-29 | MIT | Cross-chain lineage → B6 reference edges; L5 fact-check → B7 claim verification; checkpoint/replay → B8 consistency scoring |
| EverOS | 2026-05-29 | Apache 2.0 | Foresight memory type → B9; BM25/RRF concept → part of B10 |
| CocoIndex | 2026-05-29 | Apache 2.0 | sqlite-vec disk-backed vector index → B10 (replaces in-memory HashMap, combined with BM25+RRF) |
| RAG_Techniques | 2026-05-29 | MIT | HyPE (Hypothetical Prompt Embeddings) → B11; query-to-query matching at indexing time |
| LightRAG | 2026-05-29 | MIT | Token budget enforcement on retrieval responses → B12 |
| agentic-rag (Fareed Khan) | 2026-05-29 | MIT | Content type + table summary on ingest → B13; embedding sidecar evolution concept → B14 |
| MemOS-main | 2026-05-29 | Apache 2.0 | DreamMemoryLifecycle metadata → context lifecycle manager → B15 |
