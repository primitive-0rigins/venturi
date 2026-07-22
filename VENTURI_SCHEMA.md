# VENTURI — Schema Reference

---

## Librarian (librarian.db)

```sql
CREATE TABLE orbs (
    orb_id        TEXT PRIMARY KEY,   -- SHA256 content hash
    key_id        TEXT NOT NULL,       -- pointer to keystore entry, never raw key
    topic         TEXT NOT NULL,
    domain        TEXT NOT NULL,
    date          TEXT NOT NULL,       -- ISO8601 ingestion timestamp
    parent_id     TEXT NOT NULL,       -- self-ref if single orb
    sequence      INTEGER NOT NULL,    -- 1-based position in chain
    chain_length  INTEGER NOT NULL,    -- total orbs in chain
    tier          TEXT NOT NULL DEFAULT 'hot',  -- hot / warm / cold
    last_accessed TEXT NOT NULL,       -- ISO8601, updated on any access
    embedding     BLOB,                -- nomic embed of 100-word summary, null if cold
    format        TEXT NOT NULL DEFAULT 'text'  -- output format: md / json / text
);

CREATE TABLE access_marks (
    parent_id    TEXT NOT NULL,
    accessed_at  TEXT NOT NULL,       -- ISO8601, written on any orb retrieval in this chain
    PRIMARY KEY (parent_id)           -- one mark per chain, upsert on access
);

CREATE INDEX idx_parent_id ON orbs(parent_id);
CREATE INDEX idx_tier ON orbs(tier);
CREATE INDEX idx_last_accessed ON orbs(last_accessed);
```

---

## Knowledge Graph (graph.db)

```sql
CREATE TABLE kg_nodes (
    node_id      TEXT PRIMARY KEY,   -- UUID v4
    entity       TEXT NOT NULL,      -- extracted entity or concept name
    entity_type  TEXT,               -- e.g. "person", "org", "concept", "location"
    created_at   TEXT NOT NULL       -- ISO8601
);

CREATE TABLE kg_edges (
    edge_id          TEXT PRIMARY KEY,   -- UUID v4
    from_node_id     TEXT NOT NULL,
    to_node_id       TEXT NOT NULL,
    relationship     TEXT NOT NULL,      -- e.g. "treats", "regulates", "references"
    parent_id        TEXT NOT NULL,      -- which orb chain observed this relationship
    created_at       TEXT NOT NULL,
    FOREIGN KEY (from_node_id) REFERENCES kg_nodes(node_id),
    FOREIGN KEY (to_node_id)   REFERENCES kg_nodes(node_id)
);

CREATE TABLE kg_node_refs (
    node_id    TEXT NOT NULL,
    parent_id  TEXT NOT NULL,    -- orb chain that mentions this entity
    PRIMARY KEY (node_id, parent_id)
);

CREATE INDEX idx_kg_entity ON kg_nodes(entity);
CREATE INDEX idx_kg_edges_from ON kg_edges(from_node_id);
CREATE INDEX idx_kg_edges_to ON kg_edges(to_node_id);
CREATE INDEX idx_kg_refs_parent ON kg_node_refs(parent_id);
```

**On 90-day expiry:** When a chain ejects, delete all `kg_node_refs` rows for that `parent_id`. Delete all `kg_edges` rows for that `parent_id`. Delete any `kg_nodes` that have zero remaining refs.

---

## Exit Gate Keystore (keystore.db — separate file, separate directory, chmod 600)

```sql
CREATE TABLE keys (
    key_id     TEXT PRIMARY KEY,   -- matches key_id in Librarian
    parent_id  TEXT NOT NULL,      -- chain this key belongs to
    raw_key    BLOB NOT NULL,      -- ChaCha20-Poly1305 key bytes
    created_at TEXT NOT NULL       -- ISO8601
);
```

---

## Ingestion Journal (journal.db)

```sql
CREATE TABLE ingestions (
    parent_id    TEXT PRIMARY KEY,
    agent_id     TEXT NOT NULL,
    expected_n   INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'IN_PROGRESS',  -- IN_PROGRESS / COMPLETE / FAILED
    failure_reason TEXT,           -- populated on rollback
    opened_at    TEXT NOT NULL,    -- ISO8601
    closed_at    TEXT,            -- ISO8601, null until complete or failed
    request_json TEXT,            -- IngestionRequest minus chunks; lets reconcile_catalog()
                                   -- replay a failed catalog registration without the
                                   -- original caller. Added via ALTER TABLE, so rows from
                                   -- before that migration have this NULL.
    catalog_registered INTEGER NOT NULL DEFAULT 0  -- set once register_catalog() (Librarian +
                                   -- graph + audit record) actually lands. Gatekeeper::
                                   -- reconcile_catalog() finds and replays COMPLETE rows
                                   -- where this is still 0 — otherwise a failed catalog
                                   -- write left the orb durable on disk but unfindable
                                   -- forever, with no way back.
);

CREATE TABLE ingestion_orbs (
    parent_id  TEXT NOT NULL,
    orb_id     TEXT NOT NULL,
    sequence   INTEGER NOT NULL,
    written_at TEXT NOT NULL,
    PRIMARY KEY (parent_id, sequence)
);
```

---

## Scribe Log (scribe.db — append-only)

```sql
CREATE TABLE events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,   -- INGESTION / SHELF / RETRIEVAL / EXIT
    timestamp  TEXT NOT NULL,   -- ISO8601
    payload    TEXT NOT NULL    -- JSON blob per event type
);
```

**INGESTION payload:**
```json
{
  "agent_id": "...",
  "orb_id": "...",
  "parent_id": "...",
  "chain_length": 3,
  "summary": "...",
  "gatekeeper_result": "pass",
  "failure_reason": null
}
```

**SHELF payload:**
```json
{
  "orb_id": "...",
  "parent_id": "...",
  "tier": "hot"
}
```

**RETRIEVAL payload:**
```json
{
  "query": "...",
  "mode": "context",
  "orbs_matched": ["orb_id_1", "orb_id_2"]
}
```

**EXIT payload:**
```json
{
  "parent_id": "...",
  "orb_ids": ["orb_id_1", "orb_id_2", "orb_id_3"],
  "verdict": 1
}
```

---

## Orb File Format (on disk)

Filename: `{orb_id}` (no extension)  
Location: `{shelf_root}/{orb_id[0:2]}/{orb_id[2:4]}/{orb_id}` (two-level sharding to avoid flat directory bottleneck)

Binary layout:
```
[4 bytes]  magic: 0x56455254  ("VERT")
[4 bytes]  version: u32
[32 bytes] orb_id: SHA256 bytes
[32 bytes] parent_id_hash: SHA256 bytes
[4 bytes]  sequence: u32
[4 bytes]  chain_length: u32
[16 bytes] chacha20 nonce
[4 bytes]  content_length: u32
[N bytes]  encrypted+compressed content
[16 bytes] poly1305 auth tag
```

The content is verified on decryption via the Poly1305 tag — integrity check is automatic on exit gate decrypt. No separate integrity step needed.
