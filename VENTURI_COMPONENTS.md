# VENTURI — Component Boundaries and Contracts

---

## Component Map

```
┌─────────────────────────────────────────────────────────────┐
│                        VENTURI                              │
│                                                             │
│  [Agent / Caller]                                           │
│       │                                                     │
│       ▼                                                     │
│  [Gatekeeper]──────────────────────────────[Scribe]        │
│       │                                         ▲           │
│       ▼                                         │           │
│  [Wormhole — Ingestion Gates 1-4]               │           │
│       Gate1: Section                            │           │
│       Gate2: Compress (zstd)                    │           │
│       Gate3: Encrypt ──────► [Exit Keystore]   │           │
│       Gate4: Tether                             │           │
│       │                                         │           │
│       ▼                                         │           │
│  [OrbShelf — 4TB Disk]                          │           │
│       │                                         │           │
│       ▼                                         │           │
│  [Librarian — librarian.db]                     │           │
│       │  (nomic embed sidecar)                  │           │
│       │                                         │           │
│  [Retrieval API] ◄──── Agent Query              │           │
│       │                                         │           │
│       ▼                                         │           │
│  [Wormhole — Exit Gates]                        │           │
│       ExitGate1: Parallel Unlock ◄─[Keystore]  │           │
│       ExitGate2: Decompress                     │           │
│       ExitGate3: Unwrap                         │           │
│       ExitGate4: Assemble                       │           │
│       │                                         │           │
│       ▼                                         │           │
│  [Output: content or file] ──────────────────►[Scribe]     │
│                                                             │
│  ┌──────────────┐  ┌─────────────┐                         │
│  │ journal.db   │  │ keystore.db │  (separate paths)       │
│  └──────────────┘  └─────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Contracts

### Gatekeeper Input Contract
The ingesting agent MUST provide:
```
content:   bytes          — the full content to ingest
summary:   string         — 100 words maximum, describes the content
agent_id:  string         — identifier for the ingesting agent
topic:     string         — coarse topic label
domain:    string         — domain classification
date:      ISO8601        — date of content creation (not ingestion date)
format:    string         — output format hint for document mode: "md" | "json" | "text"
```
Gatekeeper REJECTS if: summary missing, summary >100 words, any required field absent.

### Retrieval API Input Contract
```
query:   string           — retrieval query
mode:    "context" | "document" | "graph" | "temporal" | "structured"
         — required, no default assumed

-- context mode: no additional params required
-- document mode: no additional params required
-- graph mode: no additional params required
-- temporal mode requires:
     subject:   string        — what to look up (entity, topic, agent_id)
     from:      ISO8601       — start of date range
     to:        ISO8601       — end of date range
     agent_id:  string        — optional filter by ingesting agent
-- structured mode: any combination of Librarian columns as filters
     topic, domain, agent_id, tier, date, parent_id, format
```

### Scribe — No Input Contract
Scribe is passive. It observes events. It does not accept calls from agents.  
The only agent interaction is the exit verdict: after retrieval, the exit gate prompts the agent with the output and waits for 1 or 0. This is a fire-and-continue — Venturi does not block on the verdict.

---

## Startup Sequence

1. Check journal.db for any `IN_PROGRESS` entries → rollback all → delete orphaned disk orbs → delete orphaned keys from keystore (by parent_id) → log to Scribe
2. Check keystore.db is accessible and readable by exit gate process
3. Verify nomic embed sidecar is running and reachable
4. Start background sweep scheduler:
   - Sibling refresh sweep: every 5 minutes, process any `parent_id` access marks
   - Tier update sweep: every 15 minutes, recency-based tier changes, plus a
     verdict-fed exemption from cold demotion (see below)
   - Retention sweep: once daily, applies the customer-selected retention
     policy; `indefinite` disables expiry and legal holds preserve chains
   - Lifecycle manager sweep: every 60 seconds, fast in-memory hot/warm/cold
     RAM eviction scoped per actor (see below)
   - Spectral community detection sweep: every 30 minutes, clusters the
     knowledge graph and writes `community_id` on `kg_nodes`
   - Embedding queue sweep: every 30 seconds (no initial skip — drains any
     backlog left from a prior restart immediately), processes pending
     `embedding_queue` and `fact_queue`/HyPE entries against the Ollama sidecar
5. Ready

---

## Background Sweeps

### Sibling Refresh Sweep
```
find all rows in access_marks table (parent_id, accessed_at)
for each parent_id:
    UPDATE orbs SET last_accessed = accessed_at WHERE parent_id = ?
    delete from access_marks where parent_id = ?
```

### Retention Sweep
```
if retention is indefinite: no expiry action
otherwise find chains WHERE last_accessed < (now - configured retention days)
for each chain:
    preserve it if a legal hold is active and record that decision
    otherwise delete catalog rows, sealed-orb files, the chain key, and graph references
    record the content-free retention decision in Scribe
```

### Tier Update Sweep
```
read new EXIT events from Scribe since the last checkpoint
for each event, for each orb_id in it:
    Beta-Bernoulli update: verdict=1 -> alpha += 1, else beta += 1
    usefulness_score = alpha / (alpha + beta)
advance the checkpoint to the last event's timestamp

then, purely recency-driven as before:
    hot  = accessed within TIER_HOT_SECS
    warm = accessed within TIER_WARM_SECS but not hot
    cold = everything else, EXCEPT an orb with enough verdict evidence
           (alpha + beta >= USEFULNESS_MIN_EVIDENCE) and a high enough
           usefulness_score (>= USEFULNESS_COLD_FLOOR) is exempted from
           cold demotion — embedding is left intact for it
    cold demotion otherwise sets embedding = NULL
```
Verdicts never force a hot/warm promotion or a warm/cold demotion outright —
only recency does. The one thing verdict evidence changes is whether an
otherwise-stale orb gets demoted to cold. See
`spec/math-application-proposal-usefulness-score-tiering.md` for why a
blunt promote-on-1/demote-on-0 rule (the original sketch here) was rejected
in favor of this narrower floor.

### Lifecycle Manager Sweep
A second, much faster hot/warm/cold pass than the Tier Update Sweep above —
deliberately different in purpose, though it writes the same `orbs.tier`
column. Where the Tier Update Sweep is long-term retention tiering
(day-scale), this is an in-memory RAM-eviction cache scoped per
`owner_agent_id`, driven by `LifecycleConfig` (defaults: 5 min to warm, 10
min to cold, capped at 500 hot orbs per actor). See
`VENTURI_ROADMAP.md` (B15 — Lifecycle tiering) for the roadmap entry.

```
cold demotion:  idle >= t_cold_secs AND access_count < 2
                AND not pinned AND not verdict-exempt (same floor as above)
                -> tier = 'cold', embedding = NULL
warm demotion:  idle >= t_warm_secs, < t_cold_secs, not pinned
                -> tier = 'warm'
promotion:      pinned OR idle < t_warm_secs
                -> tier = 'hot'
                -> if embedding was NULL, re-queue into embedding_queue
                   (async re-embed, picked up by the Embedding Queue Sweep —
                   this is what makes cold demotion a reversible cache
                   eviction rather than a one-way loss of searchability)
cap:            per actor, keep only the `max_hot_orbs` most-recently-accessed
                hot orbs; excess demoted to warm
```

### Spectral Community Detection Sweep
Rebuilds `kg_nodes.community_id` from the current `kg_edges` +
`hyperedges` graph via normalized-Laplacian eigendecomposition + k-means
(see `VENTURI_ROADMAP.md`, R1). `traverse()` uses community membership as a
secondary pass alongside BFS, so a concept can surface as related even when
it's many hops away.

### Embedding Queue Sweep
Drains `embedding_queue` (orb summaries) and `fact_queue` (answer-fact
atoms and, when enabled, HyPE hypothetical-question atoms) against the
Ollama sidecar, up to 10 `embedding_queue` rows and 5 `fact_queue` rows per
pass. A row that fails is retried up to 3 attempts before being left in
place (visible via `embedding_queue_depth`) rather than silently dropped.
This is the same queue ingestion enqueues into and the Lifecycle Manager
Sweep re-enqueues into on promotion.

---

## Failure Modes and Recovery

| Failure | Detection | Recovery |
|---|---|---|
| Process dies mid-ingestion | Journal has IN_PROGRESS on startup | Rollback: delete orphaned orbs, delete orphaned key from keystore, log FAILED |
| Gate 3 fails (encryption) | Exception caught in gate pipeline | rollback_ingestion with reason, no orbs written |
| Disk full on OrbShelf | Write error on orb file | rollback_ingestion with reason "disk_full" |
| Keystore inaccessible at retrieval | Permission error on keystore read | Return error to agent, log to Scribe as retrieval failure |
| Nomic embed sidecar down | Connection refused | Retrieval returns error, ingestion queues summary for embedding on recovery |
| Partial document reassembly (one orb corrupt) | Poly1305 auth tag fails on decrypt | Return error to agent, log to Scribe as exit failure, do NOT fire exit verdict |
| Agent does not return verdict | Timeout (30s default) | Scribe logs verdict=null, no tier change |
