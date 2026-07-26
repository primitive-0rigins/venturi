use crate::storage::permissions::restrict_database_files;
use crate::types::error::TunnelError;
use rusqlite::{params, Connection};

/// Write-ahead log for atomic ingestion.
///
/// Guarantees that a document chain is either fully written to the shelf
/// and Librarian, or fully rolled back — never partially committed.
///
/// On startup, Venturi sweeps for any IN_PROGRESS entries and rolls them
/// back, cleaning orphaned orbs from disk and orphaned keys from the keystore.
pub struct Journal {
    conn: Connection,
}

impl Journal {
    /// Open (or create) the journal database at the given path.
    pub fn open(path: &str) -> Result<Self, TunnelError> {
        let conn = Connection::open(path).map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS ingestions (
                parent_id      TEXT PRIMARY KEY,
                agent_id       TEXT NOT NULL,
                expected_n     INTEGER NOT NULL,
                status         TEXT NOT NULL DEFAULT 'IN_PROGRESS',
                failure_reason TEXT,
                opened_at      TEXT NOT NULL,
                closed_at      TEXT
            );

            CREATE TABLE IF NOT EXISTS ingestion_orbs (
                parent_id  TEXT NOT NULL,
                orb_id     TEXT NOT NULL,
                sequence   INTEGER NOT NULL,
                written_at TEXT NOT NULL,
                PRIMARY KEY (parent_id, sequence)
            );
        ",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        // Added after the original schema: catalog-registration reconciliation
        // needs its own durable copy of the ingestion metadata (everything
        // register_catalog() needs except the raw chunk bytes) and a flag for
        // whether that registration has actually landed in the Librarian.
        // Best-effort ADD COLUMN — errors mean the column already exists.
        let _ = conn.execute_batch("ALTER TABLE ingestions ADD COLUMN request_json TEXT");
        let _ = conn.execute_batch(
            "ALTER TABLE ingestions ADD COLUMN catalog_registered INTEGER NOT NULL DEFAULT 0",
        );
        restrict_database_files(path)?;

        Ok(Self { conn })
    }

    /// Open a new ingestion. Called by Gatekeeper before gates run.
    ///
    /// `request_json` is the ingestion request (minus raw chunk bytes) so a
    /// failed catalog registration can be replayed later by reconcile_catalog()
    /// without needing the original caller to still be around.
    pub fn open_ingestion(
        &self,
        parent_id: &str,
        expected_n: u32,
        agent_id: &str,
        request_json: &str,
    ) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "INSERT INTO ingestions (parent_id, agent_id, expected_n, opened_at, request_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
                params![parent_id, agent_id, expected_n, now, request_json],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Mark a completed ingestion as having its catalog entry (Librarian +
    /// graph + audit record) durably registered. Called after a successful
    /// register_catalog(), whether that happened inline during ingest() or
    /// later during reconcile_catalog().
    pub fn mark_catalog_registered(&self, parent_id: &str) -> Result<(), TunnelError> {
        self.conn
            .execute(
                "UPDATE ingestions SET catalog_registered = 1 WHERE parent_id = ?1",
                params![parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Ingestions that committed (sealed + journaled) but never made it into
    /// the catalog — the "durable on disk but unfindable" gap. Each entry's
    /// orb_ids are in sequence order so the caller can replay register_catalog()
    /// as-is.
    pub fn pending_catalog_registration(&self) -> Result<Vec<PendingCatalogEntry>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT parent_id, expected_n, request_json
             FROM ingestions
             WHERE status = 'COMPLETE' AND catalog_registered = 0",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let heads = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let mut entries = Vec::with_capacity(heads.len());
        for (parent_id, expected_n, request_json) in heads {
            let orb_ids = self.orb_ids_in_sequence(&parent_id)?;
            entries.push(PendingCatalogEntry {
                parent_id,
                expected_n,
                request_json,
                orb_ids,
            });
        }
        Ok(entries)
    }

    fn orb_ids_in_sequence(&self, parent_id: &str) -> Result<Vec<String>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare("SELECT orb_id FROM ingestion_orbs WHERE parent_id = ?1 ORDER BY sequence ASC")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map(params![parent_id], |row| row.get::<_, String>(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    /// Record one orb completing Gate4 (tether). Called per orb after shelf write.
    pub fn record_orb(
        &self,
        parent_id: &str,
        orb_id: &str,
        sequence: u32,
    ) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "INSERT INTO ingestion_orbs (parent_id, orb_id, sequence, written_at)
             VALUES (?1, ?2, ?3, ?4)",
                params![parent_id, orb_id, sequence, now],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Commit: all N orbs written. Librarian and Scribe writes happen after this.
    pub fn commit_ingestion(&self, parent_id: &str) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "UPDATE ingestions SET status = 'COMPLETE', closed_at = ?1
             WHERE parent_id = ?2",
                params![now, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Rollback: something failed. Log the reason. Caller cleans up disk + keystore.
    pub fn rollback_ingestion(&self, parent_id: &str, reason: &str) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "UPDATE ingestions SET status = 'FAILED', failure_reason = ?1, closed_at = ?2
             WHERE parent_id = ?3",
                params![reason, now, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Return all IN_PROGRESS ingestions. Called on startup for cleanup sweep.
    pub fn in_progress(&self) -> Result<Vec<IncompleteIngestion>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT i.parent_id, i.agent_id, i.expected_n,
                    GROUP_CONCAT(o.orb_id || ':' || o.sequence) as orbs
             FROM ingestions i
             LEFT JOIN ingestion_orbs o ON i.parent_id = o.parent_id
             WHERE i.status = 'IN_PROGRESS'
             GROUP BY i.parent_id",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let orbs_str: Option<String> = row.get(3)?;
                let orb_ids: Vec<String> = orbs_str
                    .unwrap_or_default()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.split(':').next().unwrap_or("").to_string())
                    .collect();
                Ok(IncompleteIngestion {
                    parent_id: row.get(0)?,
                    agent_id: row.get(1)?,
                    expected_n: row.get(2)?,
                    orb_ids,
                })
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }
}

/// A committed ingestion whose catalog registration (Librarian + graph +
/// audit record) never landed and needs to be replayed.
pub struct PendingCatalogEntry {
    pub parent_id: String,
    pub expected_n: u32,
    /// Serialized IngestionRequest (minus chunks) captured at open_ingestion time.
    /// None only for ingestions journaled before this column existed.
    pub request_json: Option<String>,
    /// OrbIds in sequence order (1..expected_n).
    pub orb_ids: Vec<String>,
}

/// An ingestion that was in-flight when the process died.
pub struct IncompleteIngestion {
    pub parent_id: String,
    pub agent_id: String,
    pub expected_n: u32,
    /// OrbIds of any orbs that made it to disk before the crash.
    pub orb_ids: Vec<String>,
}

fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // ISO8601 approximation — full datetime library added later with chrono
    format!("{}Z", secs)
}
