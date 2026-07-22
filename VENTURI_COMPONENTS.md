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
   - 90-day expiry sweep: once daily, find expired orbs, eject to dataset, remove from Librarian + keystore
   - Tier update sweep: every 15 minutes, recency-based tier changes, plus a
     verdict-fed exemption from cold demotion (see below)
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

### 90-Day Expiry Sweep
```
find all orbs WHERE last_accessed < (now - 90 days)
for each orb:
    read orb bytes from OrbShelf
    write orb + Scribe history to dataset collection
    DELETE from orbs WHERE orb_id = ?
    DELETE from keystore WHERE parent_id = ? (only if last orb in chain)
    delete orb file from disk
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
