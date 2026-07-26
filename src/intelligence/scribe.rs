use crate::storage::permissions::restrict_database_files;
use crate::types::error::TunnelError;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
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
            );
        ",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        restrict_database_files(path)?;

        Ok(Self { conn })
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
            "query": query,
            "mode": mode,
            "actor_id": actor_id,
            "orbs_matched": orbs_matched,
        });
        self.append("RETRIEVAL", &payload.to_string())
    }

    /// Record a retrieval proof and return its audit id.
    pub fn record_retrieval_proof(&self, proof: RetrievalProof) -> Result<String, TunnelError> {
        let audit_id = proof.retrieval_audit_id.clone();
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
            Some(intersection as f32 / expected_orb_ids.len() as f32)
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

    /// Read all EXIT events since a given timestamp, with the timestamp of the last event read
    /// (if any) so the caller can advance its own checkpoint. Consumed by
    /// `Sweeper::sweep_lifecycle` to feed `Librarian::apply_exit_feedback` — see
    /// `spec/math-application-proposal-usefulness-score-tiering.md`.
    pub fn exit_events_since(
        &self,
        since_ts: &str,
    ) -> Result<(Vec<ExitEvent>, Option<String>), TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT timestamp, payload FROM events
             WHERE event_type = 'EXIT' AND timestamp > ?1
             ORDER BY id ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map(params![since_ts], |row| {
                let timestamp: String = row.get(0)?;
                let payload: String = row.get(1)?;
                Ok((timestamp, payload))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let mut events = Vec::new();
        let mut last_ts = None;
        for row in rows {
            let (timestamp, payload) =
                row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            last_ts = Some(timestamp);
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
        Ok((events, last_ts))
    }

    fn append(&self, event_type: &str, payload: &str) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "INSERT INTO events (event_type, timestamp, payload) VALUES (?1, ?2, ?3)",
                params![event_type, now, payload],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }
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
