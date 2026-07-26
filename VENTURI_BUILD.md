# VENTURI — Build Specification
**Version:** 1.0  
**Status:** Implementation reference
**Classification:** Public

---

## What Venturi Is

Venturi is encrypted, governed, lossless memory infrastructure for autonomous agent systems operating in sensitive and regulated environments.

Standard RAG was built for developers who want fast retrieval. It makes the right tradeoffs for that market: chunk the content, embed the chunks, search by similarity, inject into a prompt. Those tradeoffs — lossy chunking, plaintext storage, no audit trail, no ingestion governance — disqualify standard RAG from hospitals, security firms, and government agencies. Those organizations will deploy AI agents. They will need memory infrastructure. Their requirements are not optional:

- Regulated deployments need controls beyond encryption, including access
  management, operational procedures, and independent compliance assessment
- **Compliance audits and incident response** require a full end-to-end audit trail of who put what in, when, and whether it was retrieved correctly
- **Legal and medical documents** cannot be approximated — the dropped chunk may be the critical sentence
- **Classified and patient data** cannot leave the building — cloud RAG is disqualified by law
- **Regulated ingestion** requires chain of custody — who ingested what, when, under what identity

Venturi is designed for deployments that need these controls. It is not, by
itself, a HIPAA, FedRAMP, or SOC 2 certification.

**What Venturi is not:**
- Not a general-purpose developer RAG tool
- Not a vector database
- Not a document store with search bolted on
- Not a wrapper around an existing RAG framework
- Not dependent on any cloud API

**Scale target:** 4TB external hard drive as the orb shelf. SQLite as the catalog.

---

## Core Concepts

### The Orb
The atomic unit of Venturi. An orb is a sealed, encrypted, compressed chunk of original content.

- Addressed by **OrbId**: SHA256 of the encrypted blob post-Gate 3, including the nonce. Computed after encryption, not before. This means two identical documents ingested separately get different OrbIds (different keys, different nonces) — no collisions, no deduplication across ingestions.
- Stores **full original content** — not a summary, not a fragment of a summary
- The 100-word summary attached at ingestion is the **retrieval anchor only** — it is indexed, not stored as the content
- Orbs are immutable once written. They are removed only by the configured
  retention process (unless retention is `indefinite`) or replaced by a new
  ingestion; legal holds prevent retention deletion.
- A single document that exceeds one orb in size is split into a **chain**: multiple orbs sharing a `parent_id`, each with a `sequence` number

### The Wormhole
The transit-processing pipeline. Data is never at rest in a partial state. Everything — compression, separation, encryption, tethering — happens **in motion** through the wormhole. Not stop-process-send. In transit.

Entry direction: data enters → gates transform it → sealed orbs exit to shelf  
Exit direction: encrypted orbs enter → gates reverse-transform them → original content exits

### The Key Model
One key per chain (one key per `parent_id`). Single-orb ingestions are their own chain.  
The raw key lives **only** in the exit gate keystore. The Librarian holds a Key ID pointer. Nothing else in the system ever sees the raw key.

---

## System Components

### 1. Gatekeeper (Entry)
The ingestion validator. Lives before Gate 1. Does not process data — it validates, counts, and opens the journal.

**Responsibilities:**
- Validates required metadata from the ingesting agent: `agent_id`, `topic`, `domain`, `date`
- Validates the **100-word summary** — the ingesting agent provides this. The gatekeeper checks it exists and is ≤100 words. Rejects if missing. Does not generate it. The agent knows the content best.
- Counts how many orbs will be needed based on content size and target orb size. Sets Gate 1 parameters: `target_chunk_size`, `expected_N`
- Opens an ingestion journal entry: `open_ingestion(parent_id, expected_N, agent_id, timestamp)`
- Sends ingestion event to Scribe: `agent_id | parent_id | summary | gatekeeper_result | timestamp`
- Runs entity extraction on the 100-word summary (local qwen3 3B) → extracted entities and relationships queued for Knowledge Graph write on commit

**The gatekeeper does not store anything.** It is a checkpoint, not a processor. Graph writes happen at commit time alongside Librarian writes — not before.

---

### 2. Wormhole — Ingestion Gates (Gates 1–4)

All gates process data in transit. Each gate adds a wrapper. Wrappers constrict — they cannot expand without the correct credentials. The basketball-through-bedsheet model: as data passes each gate, the gate closes around it.

**Gate 1 — Section**  
Receives parameters from Gatekeeper: `target_chunk_size`, `expected_N`.  
Sections the incoming data stream into orb-sized chunks in transit.  
Output: N independent data chunks flowing forward.

**Gate 2 — Compress**  
Algorithm: zstd  
Hard compression applied to each chunk independently, in transit.  
Output: N compressed chunks.

**Gate 3 — Encrypt**  
Algorithm: ChaCha20-Poly1305  
Generates one chain key for this `parent_id`.  
Generates a **Key ID** = UUID v4.  
Immediately sends raw key + Key ID to the **Exit Gate Keystore** (separate path, separate permissions).  
Sends **Key ID only** to the Librarian catalog — never the raw key.  
Wraps each compressed chunk in the encryption envelope.  
This is the straight-jacket gate: data cannot be expanded without the key.  
Output: N encrypted orbs.

**Gate 4 — Tether**  
Assigns each orb its `parent_id` and `sequence` number (1 of N, 2 of N, etc.).  
Single-orb ingestions still get a `parent_id` (self-referencing) and `sequence` = 1 of 1.  
Output: N fully sealed orbs ready for shelf.

**Atomic commit:**  
After Gate 4 completes all N orbs: `commit_ingestion(parent_id)` — journal marked complete, Librarian rows written, Scribe shelf event recorded.  
If anything fails before commit: `rollback_ingestion(parent_id, reason)` — journal records failure reason, startup sweep finds uncommitted ingestions, deletes orphaned orbs from disk, **and deletes the key from the exit gate keystore** (key_id lookup by parent_id).

---

### 3. OrbShelf (Disk)
The 4TB external hard drive. Orbs are written as files addressed by OrbId.

- Write is atomic per orb (temp file → rename)
- All N orbs in an ingestion must be written before journal is committed
- Shelf does not index or search — that is the Librarian's job
- Scribe records a shelf event per orb on write: `orb_id | tier=hot | timestamp`

---

### 4. Librarian (Catalog)
SQLite database. The index of all orbs. Does not hold content. Does not hold raw keys.

**Schema:**

| Column | Type | Description |
|---|---|---|
| key_id | TEXT | Pointer to key in exit gate keystore |
| orb_id | TEXT | SHA256 content address |
| topic | TEXT | From ingesting agent metadata |
| domain | TEXT | From ingesting agent metadata |
| date | TEXT | Ingestion timestamp |
| parent_id | TEXT | Chain identifier (self-ref if single orb) |
| sequence | INTEGER | Position in chain (1 of N) |
| chain_length | INTEGER | Total orbs in chain |
| tier | TEXT | hot / warm / cold |
| last_accessed | TEXT | Timestamp used by the configured retention policy |
| embedding | BLOB | Nomic embed of 100-word summary (hot/warm only) |
| format | TEXT | Output format declared at ingestion: "md" / "json" / "text" |

**Nomic Embed Sidecar:**  
Always-on embedded representation of the 100-word summary. Used for semantic query routing.  
Hot and warm tiers: embedding cached in Librarian row.  
Cold tier: row exists, embedding column is null. Re-embedded on demand at retrieval time (slower, acceptable).  
The Librarian embedding is the query surface — no other component handles search.

**Search index:**
The Librarian stores embeddings in SQLite and combines optional `sqlite-vec`
search with FTS5 keyword search. If the vector extension is not configured,
keyword retrieval remains available while semantic retrieval degrades.

**The Librarian never holds raw keys.** Only `key_id` pointer.

---

### 5. Knowledge Graph (Concept Map)

The Librarian handles similarity search — "find orbs about similar things." The Knowledge Graph handles relationship search — "what connects X to Y across all documents?" These are different questions and require different structures.

The graph is built incrementally as orbs are ingested. It lives in SQLite alongside the Librarian. Every node is a concept or entity. Every edge is a named relationship. Both point back to the orb chains that contain them.

**Graph construction (at Gatekeeper):**  
When the Gatekeeper validates a 100-word summary, it also runs entity extraction on that summary using a local small model (qwen3 3B). Extracted entities become nodes. Relationships between entities in the same summary become edges. Both are linked to the ingestion's `parent_id`.  
The graph is built from summaries, not raw content — keeping extraction fast and local.

**Graph nodes:**  
Each node = one concept or named entity. If the same entity appears in multiple ingestions, its node accumulates references to multiple `parent_id` chains. One node can point to many documents.

**Graph edges:**  
Each edge = a named relationship between two nodes (e.g., "treats", "regulates", "references", "contradicts"). Edges carry the `parent_id` of the document where the relationship was observed.

**Graph retrieval mode:**  
A third retrieval mode alongside chunk and document. Given a query, the graph finds concepts and traverses relationships — returning the orb chains connected to those concepts. Useful for multi-hop questions: "what documents discuss both X and its relationship to Y?"

**Graph tiers:**  
When a non-held chain reaches the configured retention period, its graph
references are removed. A node with no remaining references is removed.

---

### 6. Exit Gate Keystore
The "little box" outside the exit gate. Separate file, separate directory, separate file permissions from everything else. Only the exit gate process has read access.

**Contents:** `key_id → raw ChaCha20-Poly1305 key`  
**One entry per chain** (one key per `parent_id`)  
**Lifecycle:** Key written by Gate 3 at ingestion. Key removed when the chain
is deleted by the configured retention process; a legal hold prevents that
deletion.

This is the only place in the system where raw keys exist. The Librarian cannot decrypt. The OrbShelf cannot decrypt. Only the exit gate, reading from this box, can decrypt.

---

### 6. Retrieval API

Single entry point. Two modes.

**Parameters:**
```
query:  string                      — the retrieval question or context
mode:   context | document | graph | temporal | structured
                             — explicit intent declaration, no default
```

**Temporal Mode Flow:**
1. Query parameters: `subject` (what to look up), `from` (ISO8601 date), `to` (ISO8601 date), optional `agent_id` filter
2. Librarian: pure date-range query against `date` and `last_accessed` columns — no embedding, no similarity search
3. Also queries Scribe event log for all events touching matching `parent_id` chains within the date range
4. Exit gates: retrieve and reassemble each matching chain in chronological order
5. Return: ordered sequence of content + event history — a timeline, not a relevance ranking
6. Scribe records retrieval event (mode=temporal)
7. Exit verdict fires per chain returned

**Structured Mode Flow:**
1. Query parameters: any combination of Librarian columns — `topic`, `domain`, `agent_id`, `tier`, `date` range, `parent_id`, `format`
2. Librarian: pure SQL WHERE clause against metadata columns — no embedding, no similarity search, no graph traversal
3. Caller gets exactly what they filtered for, nothing more
4. Exit gates: retrieve and reassemble each matching chain
5. Return: filtered result set
6. Scribe records retrieval event (mode=structured)
7. Exit verdict fires per chain returned

**Context Mode Flow:**
1. Query → nomic embed → similarity search in Librarian
2. Match one or more orbs
3. Exit gates: unlock → decompress → unwrap each orb individually
4. Content returned for prompt injection
5. Scribe records retrieval event
6. Scribe prompts retrieving agent: "is this what you wanted?" → 1 or 0
7. Verdict folds into the orb's usefulness posterior on the next lifecycle sweep
   (protects it from cold demotion if proven useful — does not force a
   promotion; see Tier Update Sweep in VENTURI_COMPONENTS.md)

**Graph Mode Flow:**
1. Query → entity extraction (qwen3 3B) → find matching nodes in Knowledge Graph
2. Traverse edges from matched nodes → collect all connected `parent_id` chains
3. Exit gates: retrieve and reassemble each connected chain
4. Return: set of related documents and the relationship path that connected them
5. Scribe records retrieval event (mode=graph)
6. Exit verdict fires per chain: 1 or 0

**Document Mode Flow:**
1. Query → nomic embed → similarity search in Librarian → highest-ranked matching orb. Its `parent_id` determines the document returned. Other matches from the search are discarded.
2. Pull `parent_id` → fetch all sibling rows from Librarian (full chain)
3. Exit gates: unlock each chain member using the chain key
4. Decompress all orbs
5. Unwrap all orbs
6. Assemble in `sequence` order
7. Output: assembled file — MD, JSON, plain text, or whatever the ingesting agent declared
8. Scribe records retrieval event, fires exit verdict once on `parent_id`, propagates 1/0 to all siblings
9. Tier updated for all siblings

---

### 7. Wormhole — Exit Gates (Reassembly)

Mirror of ingestion gates, but in reverse. Also in transit — not stop-decompress-send.

**Exit Gate 1 — Parallel Unlock**  
For document mode: all chain members arrive simultaneously.  
Key ID → keystore lookup → raw key → decrypt all orbs in parallel.  
For context mode: single orb (or N best matches), single decrypt per orb.

**Exit Gate 2 — Decompress**  
Reverse zstd on each decrypted orb.

**Exit Gate 3 — Unwrap**  
Strip gate metadata, remove wrappers, return to raw content chunks.

**Exit Gate 4 — Assemble**  
Context mode: return individual content blob.  
Document mode: sort by `sequence`, concatenate into full document.  
Output format: declared at ingestion (MD, JSON, plain text, etc.).

All reassembly is in transit. The document is never fully materialized until the final gate output.

---

### 8. Scribe (End-to-End Event Recorder)

Scribe is Venturi's append-only event log for ingestion, retrieval, lifecycle,
and administrative events. In the HIPAA-ready profile it records minimized
metadata rather than raw retrieval queries or content. Customers control audit
retention, exports, review, and any authorized secondary use of audit data.

**Events Recorded:**

```
INGESTION
  agent_id | orb_id | parent_id | chain_length | summary | 
  gatekeeper_result (pass/fail + reason) | timestamp

SHELF
  orb_id | parent_id | tier | timestamp

RETRIEVAL
  query | mode (chunk/document) | orbs_matched (list of orb_ids) | timestamp

EXIT
  orb_id(s) | parent_id | verdict (1 or 0) | timestamp
```

**The Exit Verdict:**  
After retrieval completes, Scribe prompts the retrieving agent: "Is this what you wanted?"  
Agent returns 1 (yes) or 0 (no).  
In document mode: fires once, verdict applies to all siblings via `parent_id`.  
This is the only interaction Scribe has with an agent. Everything else is passive recording.

**Dataset Flywheel (optional, outside the HIPAA-ready profile):**
Venturi does not automatically create a training dataset from expired content.
Any secondary use of sensitive data requires a customer-approved policy,
separate controls, and appropriate legal authorization. HIPAA-profile audit
events deliberately omit raw queries and content.
Tier retention also weighs Scribe verdicts: an orb with enough accumulated
verdict evidence and a high enough usefulness posterior is exempted from
cold demotion, even when recency-stale (see section 10). Verdicts do not
force promotion or a warm/cold demotion outright — recency still drives
those.

---

### 9. Ingestion Journal (Write-Ahead Log)

Guarantees atomic ingestion. Separate from the Librarian. Lightweight — just enough to know what was in flight.

**Operations:**
```
open_ingestion(parent_id, expected_N, agent_id, timestamp)
  → creates journal entry, status=IN_PROGRESS

record_orb(parent_id, orb_id, sequence)
  → appends each completed orb to the journal entry

commit_ingestion(parent_id)
  → status=COMPLETE, Librarian rows written, Scribe shelf events fired

rollback_ingestion(parent_id, reason)
  → status=FAILED, reason logged, cleanup flagged
```

**Startup Sweep:**  
On Venturi startup, sweep journal for any `IN_PROGRESS` entries.  
These are failed mid-ingestion states.  
For each: delete orphaned orbs from disk, log failure to Scribe as a failed ingestion event, mark journal entry FAILED.  
This is how partial chains are detected and cleaned.

---

### 10. Shelf Lifecycle (Retention + Tier System)

**Tier System:**  
Driven by recency (`last_accessed`): hot within `TIER_HOT_SECS`, warm within
`TIER_WARM_SECS`, cold otherwise. New ingestions start at hot.
Scribe exit verdicts feed a Beta-Bernoulli `usefulness_score` per orb (see
Dataset Flywheel above); an orb with enough verdict evidence and a high
enough score is exempted from the cold demotion step even when
recency-stale — everything else about tiering is recency-only. See
`spec/math-application-proposal-usefulness-score-tiering.md`.

**Sibling Refresh:**  
Accessing any orb in a chain refreshes `last_accessed` for ALL siblings.  
This is handled by a background sweep — not synchronous on the retrieval path.  
On retrieval: mark `parent_id` as accessed.  
Background sweep: find all orbs with that `parent_id`, update `last_accessed`.

**Retention:**
The customer chooses `VENTURI_RETENTION_DAYS=<positive integer>` or
`indefinite`. The daily sweep removes expired non-held chains from the shelf,
catalog, keystore, and graph, and records a content-free retention decision.
`indefinite` disables expiry. Venturi does not automatically copy ejected
content into a training dataset; customers must separately authorize and
govern any secondary use of sensitive data.

---

## Data Flow Summary

### Ingestion (Single Document)
```
Agent provides: content + metadata + 100-word summary
        ↓
Gatekeeper: validate → count orbs → open journal
        ↓
Gate 1: section into N chunks (in transit)
        ↓
Gate 2: compress each chunk (in transit)
        ↓
Gate 3: encrypt each chunk → key → keystore, key_id → Librarian (in transit)
        ↓
Gate 4: tether (parent_id + sequence) (in transit)
        ↓
OrbShelf: N orbs written to 4TB disk
        ↓
commit_ingestion → Librarian rows written → Scribe shelf events
```

### Retrieval — Temporal Mode
```
Agent: subject + from_date + to_date (+ optional agent_id filter)
        ↓
Librarian: date-range SQL query (no embedding)
Scribe: event log query for same parent_ids in date range
        ↓
Exit gates: retrieve + reassemble each chain
        ↓
Output: chronological sequence of content + event history
        ↓
Scribe: record retrieval → prompt agent 1/0 per chain → update tier
```

### Retrieval — Structured Mode
```
Agent: metadata filters (any Librarian columns)
        ↓
Librarian: SQL WHERE clause — exact match, no semantic search
        ↓
Exit gates: retrieve + reassemble each matching chain
        ↓
Output: filtered result set
        ↓
Scribe: record retrieval → prompt agent 1/0 per chain → update tier
```

### Retrieval — Context Mode
```
Agent: query + mode=context
        ↓
Librarian: nomic embed similarity search → matching orb(s)
        ↓
Exit Gate 1: key_id → keystore → decrypt
        ↓
Exit Gate 2: decompress
        ↓
Exit Gate 3: unwrap
        ↓
Content → prompt injection
        ↓
Scribe: record retrieval → prompt agent 1/0 → update tier
```

### Retrieval — Document Mode
```
Agent: query + mode=document
        ↓
Librarian: nomic embed similarity search → one matching orb → pull all siblings by parent_id
        ↓
Exit Gate 1: unlock chain members using the chain key
        ↓
Exit Gate 2: decompress all (parallel)
        ↓
Exit Gate 3: unwrap all (parallel)
        ↓
Exit Gate 4: sort by sequence → assemble → output as file (MD/JSON/plain text)
        ↓
Scribe: record retrieval → prompt agent 1/0 on parent_id → propagate to all siblings → update tier
```

---

## Implementation Notes

**Language:** Rust (async, tokio for gate pipeline, rayon for parallel exit decryption)  
**Compression:** zstd crate  
**Encryption:** chacha20poly1305 crate  
**Embedding:** nomic-embed-text (local, sidecar process)  
**Catalog:** SQLite via rusqlite  
**Orb storage:** flat files on 4TB external, named by OrbId  
**Keystore:** separate SQLite file, different directory, chmod 600, separate process  
**Scribe log:** append-only SQLite table or flat line-delimited JSON  
**Ingestion journal:** SQLite table in separate DB file  
**Knowledge graph:** SQLite (graph.db) — nodes, edges, refs tables  
**Entity extraction:** qwen3 3B via Ollama (local, no cloud) — runs at Gatekeeper on the 100-word summary  

**No cloud dependencies. No external APIs. Fully local.**

---

## Authorization Boundary

Venturi provides **audit and coarse-grained authorization**, not per-orb access control.

- **Audit** — Scribe records every ingestion, retrieval, and exit verdict with `agent_id` and full timestamps. The full history of who put what in and who retrieved what is always available.
- **Authorization** — every request requires a Bearer API key, scoped `read`, `write`, or `admin` (see `src/auth.rs`). A key's scope gates which *endpoints* it may call — `/ingest` and `/verdict` need write, `/retrieve/*` and `/audit/*` need read, everything else (`/hold`, `/chain/link`, unlisted paths) needs admin, fail-closed by default. It does not gate access to individual orbs — any key with sufficient scope can query any orb; there is no per-agent or per-classification ACL.

**When Venturi runs behind another service or is called directly:**
Any caller holding a validly-scoped key can query any orb within that scope. Finer-grained access control — which agent may see which orb — is the responsibility of whatever sits in front of Venturi: a proxy, an orchestrator, a network boundary.

This is a deliberate separation of concerns. Venturi enforces the coarse boundary (can this caller reach this class of endpoint at all) and audits everything; per-orb access control is the orchestration layer's job.

---

## What Venturi Is Not Trying to Solve

- Real-time streaming ingestion (batch ingestion is fine)
- Multi-user concurrent write (single-agent write at a time is acceptable for v1)
- Sub-millisecond retrieval (correctness over speed in v1)
- Hot standby / replication (single node, 4TB drive is the target)
