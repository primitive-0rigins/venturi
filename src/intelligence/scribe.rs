use crate::storage::permissions::restrict_database_files;
use crate::types::error::TunnelError;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// End-to-end event recorder. Append-only — nothing is ever deleted from Scribe.
///
/// Records every lifecycle event from ingestion entry to exit verdict.
/// This log is the primary dataset source for training improved embed models
/// and for compliance audit trails in regulated environments.
///
/// Events: INGESTION | SHELF | RETRIEVAL | EXIT
pub struct Scribe {
    conn: Connection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditExport {
    pub format: String,
    pub final_hash: String,
    pub public_key_hex: String,
    pub signature_hex: String,
    pub jsonl: String,
}

impl Scribe {
    pub fn open(path: &str) -> Result<Self, TunnelError> {
        let conn = Connection::open(path).map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS events (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                timestamp  TEXT NOT NULL,
                payload    TEXT NOT NULL
                ,prev_hash TEXT
                ,entry_hash TEXT
            );
        ",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        // Existing audit databases predate the hash-chain columns. Adding
        // nullable columns is rollback-safe; a wholly legacy log is then
        // backfilled once before its first chained append.
        let _ = conn.execute_batch("ALTER TABLE events ADD COLUMN prev_hash TEXT");
        let _ = conn.execute_batch("ALTER TABLE events ADD COLUMN entry_hash TEXT");
        backfill_legacy_hash_chain(&conn)?;
        restrict_database_files(path)?;

        Ok(Self { conn })
    }

    /// Verify the append-only hash chain. This detects database alteration;
    /// customers should additionally protect exported copies with their configured signing key.
    pub fn verify_integrity(&self) -> Result<bool, TunnelError> {
        let mut stmt = self.conn.prepare("SELECT event_type, timestamp, payload, prev_hash, entry_hash FROM events ORDER BY id")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let mut previous = String::new();
        for row in rows {
            let (kind, timestamp, payload, prev, hash) =
                row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            if prev.as_deref().unwrap_or("") != previous {
                return Ok(false);
            }
            let expected = audit_hash(&previous, &kind, &timestamp, &payload);
            if hash.as_deref() != Some(expected.as_str()) {
                return Ok(false);
            }
            previous = expected;
        }
        Ok(true)
    }

    /// Produce a detached Ed25519-signed JSONL export. The 32-byte private
    /// key belongs in a root-readable secret file, never in the database.
    pub fn export_jsonl(&self, private_key: &[u8; 32]) -> Result<AuditExport, TunnelError> {
        if !self.verify_integrity()? {
            return Err(TunnelError::DatabaseError(
                "audit hash chain verification failed".into(),
            ));
        }
        let mut stmt = self.conn.prepare("SELECT id, event_type, timestamp, payload, prev_hash, entry_hash FROM events ORDER BY id")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt.query_map([], |row| {
            Ok(serde_json::json!({"id": row.get::<_, i64>(0)?, "event_type": row.get::<_, String>(1)?, "timestamp": row.get::<_, String>(2)?, "payload": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?).unwrap_or(serde_json::Value::Null), "prev_hash": row.get::<_, Option<String>>(4)?, "entry_hash": row.get::<_, Option<String>>(5)?}))
        }).map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let mut jsonl = String::new();
        let mut final_hash = String::new();
        for row in rows {
            let value = row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            final_hash = value["entry_hash"].as_str().unwrap_or_default().to_string();
            jsonl.push_str(
                &serde_json::to_string(&value)
                    .map_err(|e| TunnelError::Serialization(e.to_string()))?,
            );
            jsonl.push('\n');
        }
        let key = SigningKey::from_bytes(private_key);
        let signature = key.sign(jsonl.as_bytes());
        Ok(AuditExport {
            format: "venturi-audit-jsonl-v1".into(),
            final_hash,
            public_key_hex: hex(&key.verifying_key().to_bytes()),
            signature_hex: hex(&signature.to_bytes()),
            jsonl,
        })
    }

    pub fn verify_export(export: &AuditExport) -> bool {
        let Some(public) = decode_hex_32(&export.public_key_hex) else {
            return false;
        };
        let Some(signature) = decode_hex_64(&export.signature_hex) else {
            return false;
        };
        VerifyingKey::from_bytes(&public).ok().is_some_and(|key| {
            key.verify_strict(export.jsonl.as_bytes(), &Signature::from_bytes(&signature))
                .is_ok()
        })
    }

    /// Record an ingestion attempt (pass or fail) at the Gatekeeper.
    pub fn record_ingestion(
        &self,
        agent_id: &str,
        parent_id: &str,
        chain_length: u32,
        summary: &str,
        result: GatekeeperResult,
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "parent_id": parent_id,
            "chain_length": chain_length,
            "summary": summary,
            "gatekeeper_result": result.as_str(),
            "failure_reason": result.reason(),
        });
        self.append("INGESTION", &payload.to_string())
    }

    /// Record an orb landing on the shelf.
    pub fn record_shelf(
        &self,
        orb_id: &str,
        parent_id: &str,
        tier: &str,
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "orb_id": orb_id,
            "parent_id": parent_id,
            "tier": tier,
        });
        self.append("SHELF", &payload.to_string())
    }

    /// Record a retrieval call.
    pub fn record_retrieval(
        &self,
        query: &str,
        mode: &str,
        actor_id: Option<&str>,
        orbs_matched: &[String],
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "query": if crate::auth::hipaa_profile() { None } else { Some(query) },
            "mode": mode,
            "actor_id": actor_id,
            "orbs_matched": orbs_matched,
        });
        self.append("RETRIEVAL", &payload.to_string())
    }

    /// Record a retrieval proof and return its audit id.
    pub fn record_retrieval_proof(&self, mut proof: RetrievalProof) -> Result<String, TunnelError> {
        let audit_id = proof.retrieval_audit_id.clone();
        if crate::auth::hipaa_profile() {
            // The proof remains useful for reproducibility by its selected IDs,
            // but audit storage must not become a second content/query store.
            proof.query.clear();
            proof.filters_applied = serde_json::json!({"minimized": true});
        }
        let payload =
            serde_json::to_string(&proof).map_err(|e| TunnelError::Serialization(e.to_string()))?;
        self.append("RETRIEVAL_PROOF", &payload)?;
        Ok(audit_id)
    }

    /// Read a retrieval proof by audit id.
    pub fn retrieval_proof(
        &self,
        retrieval_audit_id: &str,
    ) -> Result<Option<RetrievalProof>, TunnelError> {
        let result = self.conn.query_row(
            "SELECT payload FROM events
             WHERE event_type = 'RETRIEVAL_PROOF'
               AND json_extract(payload, '$.retrieval_audit_id') = ?1
             ORDER BY id DESC LIMIT 1",
            params![retrieval_audit_id],
            |row| row.get::<_, String>(0),
        );

        match result {
            Ok(payload) => serde_json::from_str(&payload)
                .map(Some)
                .map_err(|e| TunnelError::Serialization(e.to_string())),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(TunnelError::DatabaseError(e.to_string())),
        }
    }

    /// Record retrieval failures that did not return authenticated content for
    /// one or more requested orbs.
    pub fn record_retrieval_failure(
        &self,
        query: &str,
        mode: &str,
        actor_id: Option<&str>,
        orbs_matched: &[String],
        warnings: &[String],
        failure_categories: &[String],
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "query": query,
            "mode": mode,
            "actor_id": actor_id,
            "orbs_matched": orbs_matched,
            "warnings": warnings,
            "failure_categories": failure_categories,
        });
        self.append("RETRIEVAL_FAILURE", &payload.to_string())
    }

    pub fn record_daemon_health(
        &self,
        daemon: &str,
        status: &str,
        consecutive_failures: u8,
        details: Option<&str>,
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "daemon": daemon,
            "status": status,
            "consecutive_failures": consecutive_failures,
            "details": details,
        });
        self.append("DAEMON_HEALTH", &payload.to_string())
    }

    /// Retention decisions contain identifiers only; never content or summaries.
    pub fn record_retention_decision(
        &self,
        parent_id: &str,
        outcome: &str,
    ) -> Result<(), TunnelError> {
        self.append(
            "RETENTION",
            &serde_json::json!({"parent_id": parent_id, "outcome": outcome}).to_string(),
        )
    }

    /// Record a content-free administrative action. The authenticated service
    /// principal and namespace are sufficient for review without turning the
    /// audit store into a second content store.
    pub fn record_administrative_action(
        &self,
        principal: &str,
        action: &str,
        namespace: &str,
        target_id: &str,
        outcome: &str,
    ) -> Result<(), TunnelError> {
        let payload = serde_json::json!({
            "principal": principal,
            "principal_type": "service",
            "namespace": namespace,
            "action": action,
            "target_id": target_id,
            "outcome": outcome,
        });
        self.append("ADMINISTRATIVE", &payload.to_string())
    }

    /// Record the exit verdict. 1 = yes this is what I wanted, 0 = no.
    /// In document mode this fires once and applies to all orbs in the chain.
    /// expected_orb_ids is optional — if provided, recall is computed as
    /// |intersection(expected, actual)| / |expected|.
    pub fn record_exit(
        &self,
        parent_id: &str,
        orb_ids: &[String],
        expected_orb_ids: &[String],
        verdict: u8,
    ) -> Result<(), TunnelError> {
        let recall: Option<f32> = if expected_orb_ids.is_empty() {
            None
        } else {
            let expected_set: std::collections::HashSet<&str> =
                expected_orb_ids.iter().map(|s| s.as_str()).collect();
            let actual_set: std::collections::HashSet<&str> =
                orb_ids.iter().map(|s| s.as_str()).collect();
            let intersection = expected_set.intersection(&actual_set).count();
            Some(intersection as f32 / expected_set.len() as f32)
        };

        let payload = serde_json::json!({
            "parent_id": parent_id,
            "orb_ids": orb_ids,
            "expected_orb_ids": expected_orb_ids,
            "verdict": verdict,
            "recall": recall,
        });
        self.append("EXIT", &payload.to_string())
    }

    /// Read all EXIT events after a checkpoint cursor. New cursors include the event row ID as
    /// well as the timestamp, so events written in the same second are not skipped. Legacy
    /// timestamp-only checkpoints remain supported. Consumed by
    /// `Sweeper::sweep_lifecycle` to feed `Librarian::apply_exit_feedback` — see
    /// `spec/math-application-proposal-usefulness-score-tiering.md`.
    pub fn exit_events_since(
        &self,
        cursor: &str,
    ) -> Result<(Vec<ExitEvent>, Option<String>), TunnelError> {
        let (since_ts, since_id) = parse_exit_cursor(cursor);
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, timestamp, payload FROM events
             WHERE event_type = 'EXIT'
               AND (timestamp > ?1 OR (timestamp = ?1 AND id > ?2))
             ORDER BY id ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map(params![since_ts, since_id], |row| {
                let id: i64 = row.get(0)?;
                let timestamp: String = row.get(1)?;
                let payload: String = row.get(2)?;
                Ok((id, timestamp, payload))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let mut events = Vec::new();
        let mut last_cursor = None;
        for row in rows {
            let (id, timestamp, payload) =
                row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            last_cursor = Some(format!("{timestamp}#{id}"));
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&payload) {
                let parent_id = v["parent_id"].as_str().unwrap_or("").to_string();
                let orb_ids = v["orb_ids"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|id| id.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let verdict = v["verdict"].as_u64().unwrap_or(0) as u8;
                let recall = v["recall"].as_f64().map(|f| f as f32);
                events.push(ExitEvent {
                    parent_id,
                    orb_ids,
                    verdict,
                    recall,
                });
            }
        }
        Ok((events, last_cursor))
    }

    fn append(&self, event_type: &str, payload: &str) -> Result<(), TunnelError> {
        let now = now_iso();
        let previous: String = self
            .conn
            .query_row(
                "SELECT COALESCE(entry_hash, '') FROM events ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap_or_default();
        let hash = audit_hash(&previous, event_type, &now, payload);
        self.conn
            .execute(
                "INSERT INTO events (event_type, timestamp, payload, prev_hash, entry_hash) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![event_type, now, payload, previous, hash],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }
}

/// A pre-hash-chain database has no integrity anchors at all. Backfill it once
/// as a migration before the first chained append, preserving every event and
/// making subsequent signed exports possible. A partially chained database is
/// deliberately left untouched so integrity verification can detect damage.
fn backfill_legacy_hash_chain(conn: &Connection) -> Result<(), TunnelError> {
    let chained: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE prev_hash IS NOT NULL OR entry_hash IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
    if chained != 0 {
        return Ok(());
    }
    let mut statement = conn
        .prepare("SELECT id, event_type, timestamp, payload FROM events ORDER BY id")
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
    let mut entries = Vec::new();
    for row in rows {
        entries.push(row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?);
    }
    drop(statement);
    let mut previous = String::new();
    for (id, kind, timestamp, payload) in entries {
        let entry_hash = audit_hash(&previous, &kind, &timestamp, &payload);
        conn.execute(
            "UPDATE events SET prev_hash = ?1, entry_hash = ?2 WHERE id = ?3",
            params![previous, entry_hash, id],
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        previous = entry_hash;
    }
    Ok(())
}

fn audit_hash(previous: &str, event_type: &str, timestamp: &str, payload: &str) -> String {
    let mut digest = Sha256::new();
    for field in [previous, event_type, timestamp, payload] {
        digest.update(field.as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut out = [0; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}
fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    decode_hex(value)
}
fn decode_hex_64(value: &str) -> Option<[u8; 64]> {
    decode_hex(value)
}

pub struct ExitEvent {
    pub parent_id: String,
    pub orb_ids: Vec<String>,
    pub verdict: u8,
    pub recall: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalProof {
    pub retrieval_audit_id: String,
    pub actor_id: Option<String>,
    pub mode: String,
    pub query: String,
    pub filters_applied: serde_json::Value,
    pub candidate_count: usize,
    pub selected_orb_ids: Vec<String>,
    pub selected_parent_ids: Vec<String>,
    pub key_ids_used: Vec<String>,
    pub embedding_model_version: Option<String>,
    pub chain_complete: bool,
    pub retrieval_timestamp: String,
}

impl RetrievalProof {
    pub fn new(
        actor_id: Option<&str>,
        mode: &str,
        query: &str,
        filters_applied: serde_json::Value,
        selected_orb_ids: Vec<String>,
        selected_parent_ids: Vec<String>,
        chain_complete: bool,
    ) -> Self {
        Self {
            retrieval_audit_id: Uuid::new_v4().to_string(),
            actor_id: actor_id.map(String::from),
            mode: mode.to_string(),
            query: query.to_string(),
            filters_applied,
            candidate_count: selected_orb_ids.len(),
            selected_orb_ids,
            selected_parent_ids,
            key_ids_used: Vec::new(),
            embedding_model_version: None,
            chain_complete,
            retrieval_timestamp: now_iso(),
        }
    }
}

pub enum GatekeeperResult {
    Pass,
    Fail(String),
}

impl GatekeeperResult {
    fn as_str(&self) -> &str {
        match self {
            Self::Pass => "pass",
            Self::Fail(_) => "fail",
        }
    }
    fn reason(&self) -> Option<&str> {
        match self {
            Self::Pass => None,
            Self::Fail(r) => Some(r.as_str()),
        }
    }
}

fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}Z", secs)
}

fn parse_exit_cursor(cursor: &str) -> (&str, i64) {
    cursor
        .rsplit_once('#')
        .and_then(|(timestamp, id)| id.parse().ok().map(|id| (timestamp, id)))
        .unwrap_or((cursor, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn recall_uses_unique_expected_orb_ids() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scribe.db");
        let scribe = Scribe::open(path.to_str().unwrap()).unwrap();
        let actual = vec!["orb-1".to_owned()];
        let expected = vec!["orb-1".to_owned(), "orb-1".to_owned()];

        scribe
            .record_exit("parent-1", &actual, &expected, 1)
            .unwrap();

        let (events, _) = scribe.exit_events_since("").unwrap();
        assert_eq!(events[0].recall, Some(1.0));
    }

    #[test]
    fn cursor_does_not_skip_events_with_the_same_timestamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scribe.db");
        let scribe = Scribe::open(path.to_str().unwrap()).unwrap();
        let payload = r#"{"parent_id":"parent-1","orb_ids":["orb-1"],"verdict":1,"recall":null}"#;
        scribe
            .conn
            .execute(
                "INSERT INTO events (event_type, timestamp, payload) VALUES ('EXIT', '123Z', ?1)",
                params![payload],
            )
            .unwrap();

        let (first_events, cursor) = scribe.exit_events_since("0").unwrap();
        assert_eq!(first_events.len(), 1);

        scribe
            .conn
            .execute(
                "INSERT INTO events (event_type, timestamp, payload) VALUES ('EXIT', '123Z', ?1)",
                params![payload],
            )
            .unwrap();

        let (second_events, _) = scribe
            .exit_events_since(cursor.as_deref().unwrap())
            .unwrap();
        assert_eq!(second_events.len(), 1);
    }

    #[test]
    fn integrity_chain_detects_altered_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scribe.db");
        let scribe = Scribe::open(path.to_str().unwrap()).unwrap();
        scribe.record_daemon_health("sweep", "ok", 0, None).unwrap();
        assert!(scribe.verify_integrity().unwrap());
        scribe
            .conn
            .execute("UPDATE events SET payload = '{}' WHERE id = 1", [])
            .unwrap();
        assert!(!scribe.verify_integrity().unwrap());
    }

    #[test]
    fn signed_jsonl_export_verifies_and_detects_alteration() {
        let dir = tempdir().unwrap();
        let scribe = Scribe::open(dir.path().join("scribe.db").to_str().unwrap()).unwrap();
        scribe.record_daemon_health("sweep", "ok", 0, None).unwrap();
        let mut export = scribe.export_jsonl(&[7; 32]).unwrap();
        assert!(Scribe::verify_export(&export));
        export.jsonl.push('x');
        assert!(!Scribe::verify_export(&export));
    }
}
