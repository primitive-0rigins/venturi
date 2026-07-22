use crate::intelligence::community::CommunityDetector;
use crate::pipeline::sweep::SweepReport;
use crate::types::error::TunnelError;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Upper bound on cluster count for spectral community detection (roadmap: 5-10).
const COMMUNITY_MAX_K: usize = 8;

/// Knowledge Graph — concept relationship map across all ingested documents.
///
/// Builds incrementally as orbs are ingested. Entity extraction runs on the
/// 100-word summary via local Ollama (qwen3 3B). Each node is a concept or
/// named entity. Each edge is a named relationship between two nodes.
/// Both reference the parent_id chain where they were observed.
///
/// Graph mode traversal: match query concepts to nodes → follow edges →
/// return connected parent_id chains → retrieve and reassemble.
pub struct KnowledgeGraph {
    conn: Connection,
    ollama_url: String,
}

impl KnowledgeGraph {
    pub fn open(db_path: &str, ollama_url: &str) -> Result<Self, TunnelError> {
        let conn =
            Connection::open(db_path).map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS kg_nodes (
                node_id     TEXT PRIMARY KEY,
                entity      TEXT NOT NULL,
                entity_type TEXT,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kg_edges (
                edge_id      TEXT PRIMARY KEY,
                from_node_id TEXT NOT NULL,
                to_node_id   TEXT NOT NULL,
                relationship TEXT NOT NULL,
                parent_id    TEXT NOT NULL,
                created_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kg_node_refs (
                node_id   TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                PRIMARY KEY (node_id, parent_id)
            );

            CREATE TABLE IF NOT EXISTS hyperedges (
                edge_id   TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL,
                members   TEXT NOT NULL,
                weight    REAL NOT NULL DEFAULT 1.0
            );

            CREATE INDEX IF NOT EXISTS idx_kg_entity    ON kg_nodes(entity);
            CREATE INDEX IF NOT EXISTS idx_kg_edges_from ON kg_edges(from_node_id);
            CREATE INDEX IF NOT EXISTS idx_kg_edges_to   ON kg_edges(to_node_id);
            CREATE INDEX IF NOT EXISTS idx_kg_refs_parent ON kg_node_refs(parent_id);
            CREATE INDEX IF NOT EXISTS idx_he_parent ON hyperedges(parent_id);
        ",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        // Migration: community_id added by R1 spectral community detection.
        // SQLite has no `ADD COLUMN IF NOT EXISTS`; ignore the error on repeat opens.
        let _ = conn.execute_batch("ALTER TABLE kg_nodes ADD COLUMN community_id TEXT");

        Ok(Self {
            conn,
            ollama_url: ollama_url.to_string(),
        })
    }

    /// Extract entities from a 100-word summary and write them to the graph.
    /// Called by Gatekeeper at ingestion commit time — not inside gate transit.
    pub fn ingest_summary(&self, parent_id: &str, summary: &str) -> Result<(), TunnelError> {
        let extracted = self.extract_entities(summary);
        let now = now_iso();
        let mut node_records = Vec::new();

        for entity_info in &extracted {
            // Upsert node (same entity name = same node, accumulates refs)
            let node_id = self.get_or_create_node(
                &entity_info.entity,
                entity_info.entity_type.as_deref(),
                &now,
            )?;

            // Link node to this chain
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO kg_node_refs (node_id, parent_id) VALUES (?1, ?2)",
                    params![node_id, parent_id],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

            node_records.push(NodeRecord {
                node_id,
                relationship: entity_info
                    .relationship
                    .clone()
                    .unwrap_or_else(|| "relates_to".to_string()),
            });
        }

        self.write_pairwise_edges(parent_id, &now, &node_records)?;
        self.write_hyperedge(parent_id, &node_records)?;

        Ok(())
    }

    fn write_pairwise_edges(
        &self,
        parent_id: &str,
        now: &str,
        node_records: &[NodeRecord],
    ) -> Result<(), TunnelError> {
        for i in 0..node_records.len() {
            for j in (i + 1)..node_records.len() {
                self.insert_pairwise_edge(parent_id, now, &node_records[i], &node_records[j])?;
            }
        }
        Ok(())
    }

    /// `edge_id` is derived deterministically from `(parent_id, from, to, relationship)` rather
    /// than a fresh UUID, so `INSERT OR IGNORE` can actually catch a repeat (the gatekeeper's
    /// crash recovery path replays `ingest_summary` for an already-processed parent_id — see
    /// `write_hyperedge`'s doc comment for the same hazard). With a fresh UUID every call, `OR
    /// IGNORE` never fired because the primary key never collided, so replay silently duplicated
    /// pairwise edge rows.
    fn insert_pairwise_edge(
        &self,
        parent_id: &str,
        now: &str,
        from: &NodeRecord,
        to: &NodeRecord,
    ) -> Result<(), TunnelError> {
        let edge_id =
            deterministic_edge_id(parent_id, &from.node_id, &to.node_id, &from.relationship);
        self.conn
            .execute(
                "INSERT OR IGNORE INTO kg_edges
                 (edge_id, from_node_id, to_node_id, relationship, parent_id, created_at)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    edge_id,
                    from.node_id,
                    to.node_id,
                    from.relationship,
                    parent_id,
                    now
                ],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Writes (or reinforces) the hyperedge over this observation's member set.
    ///
    /// `edge_id` is derived deterministically from `(parent_id, sorted members)` rather than a
    /// fresh UUID, so a repeat call for the same chain and member set (the gatekeeper's crash
    /// recovery path replays `ingest_summary` for a parent_id it already processed — see
    /// `gatekeeper.rs::reconcile_catalog`) increments `weight` as an observation count instead of
    /// inserting a duplicate row. Dedup is scoped to `(parent_id, members)`, not just `members`,
    /// because `eject_chain` deletes hyperedges by `parent_id`; merging across parent_ids would
    /// make ejecting one chain silently delete observations that belong to another.
    fn write_hyperedge(
        &self,
        parent_id: &str,
        node_records: &[NodeRecord],
    ) -> Result<(), TunnelError> {
        let mut members: Vec<&str> = node_records.iter().map(|r| r.node_id.as_str()).collect();
        members.sort_unstable();
        members.dedup();
        if members.len() < 2 {
            return Ok(());
        }
        let members_json = serde_json::to_string(&members)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let edge_id = deterministic_hyperedge_id(parent_id, &members_json);

        self.conn
            .execute(
                "INSERT INTO hyperedges (edge_id, parent_id, members, weight)
                 VALUES (?1, ?2, ?3, 1.0)
                 ON CONFLICT(edge_id) DO UPDATE SET weight = weight + 1.0",
                params![edge_id, parent_id, members_json],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Graph mode: find nodes matching query concepts, traverse edges,
    /// return all parent_id chains reachable from matched nodes.
    ///
    /// Two passes: BFS out to `max_hops` (local, hop-limited), plus a
    /// community-membership pass that adds every node sharing a
    /// `community_id` with a matched anchor, regardless of hop distance (see
    /// `detect_communities` / VENTURI_ROADMAP.md R1). BFS alone misses
    /// concepts that are semantically related but many hops away; the
    /// community pass catches those once a sweep has populated
    /// `community_id`. Nodes with no `community_id` yet (sweep hasn't run,
    /// or the node was isolated) simply get no extra pass — behavior is
    /// unchanged from pre-R1 BFS-only traversal.
    pub fn traverse(&self, query: &str, max_hops: u32) -> Result<Vec<String>, TunnelError> {
        let matched_nodes = self.matching_nodes(query)?;
        if matched_nodes.is_empty() {
            return Ok(Vec::new());
        }

        let mut visited = self.expand_nodes(matched_nodes.clone(), max_hops)?;

        let community_ids = self.community_ids_for_nodes(&matched_nodes)?;
        if !community_ids.is_empty() {
            for node_id in self.nodes_in_communities(&community_ids)? {
                if !visited.contains(&node_id) {
                    visited.push(node_id);
                }
            }
        }

        self.parent_ids_for_nodes(&visited)
    }

    fn community_ids_for_nodes(&self, node_ids: &[String]) -> Result<Vec<String>, TunnelError> {
        let mut ids = Vec::new();
        for node_id in node_ids {
            let community_id: Option<String> = self
                .conn
                .query_row(
                    "SELECT community_id FROM kg_nodes WHERE node_id = ?1",
                    params![node_id],
                    |r| r.get(0),
                )
                .ok()
                .flatten();
            if let Some(community_id) = community_id {
                if !ids.contains(&community_id) {
                    ids.push(community_id);
                }
            }
        }
        Ok(ids)
    }

    fn nodes_in_communities(&self, community_ids: &[String]) -> Result<Vec<String>, TunnelError> {
        let mut nodes = Vec::new();
        for community_id in community_ids {
            let mut stmt = self
                .conn
                .prepare("SELECT node_id FROM kg_nodes WHERE community_id = ?1")
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            let ids: Vec<String> = stmt
                .query_map(params![community_id], |r| r.get(0))
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            nodes.extend(ids);
        }
        nodes.dedup();
        Ok(nodes)
    }

    /// Spectral community detection sweep (R1). Recomputes `community_id`
    /// for every node with at least one edge, from the current
    /// `kg_edges` + `hyperedges` state. Scheduled as a periodic background
    /// sweep alongside the other maintenance sweeps (same pattern as
    /// `sweep_tiers`) — cheap enough for Venturi's typical graph sizes
    /// (hundreds to low thousands of concepts), not run per-request.
    pub fn detect_communities(&self) -> Result<SweepReport, TunnelError> {
        let nodes = self.all_node_ids()?;
        let edges = self.all_weighted_edges()?;

        let detector = CommunityDetector::new(COMMUNITY_MAX_K);
        let assignments = detector.detect(&nodes, &edges);

        for (node_id, community_id) in &assignments {
            self.conn
                .execute(
                    "UPDATE kg_nodes SET community_id = ?1 WHERE node_id = ?2",
                    params![community_id, node_id],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        }

        Ok(SweepReport {
            sweep: "communities",
            chains_affected: assignments.len() as u32,
            orbs_ejected: 0,
        })
    }

    fn all_node_ids(&self) -> Result<Vec<String>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare("SELECT node_id FROM kg_nodes")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let ids: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(ids)
    }

    /// Pairwise edges weighted by observation count (how many parent chains
    /// contributed the same pair), plus hyperedges expanded into pairwise
    /// weight contributions across every member pair — a hyperedge over
    /// {A, B, C} strengthens A-B, A-C, and B-C alike, preserving the
    /// co-occurrence signal `write_hyperedge` recorded.
    fn all_weighted_edges(&self) -> Result<Vec<(String, String, f64)>, TunnelError> {
        let mut edges = Vec::new();

        let mut stmt = self
            .conn
            .prepare(
                "SELECT from_node_id, to_node_id, COUNT(*) as cnt
                 FROM kg_edges GROUP BY from_node_id, to_node_id",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let pairwise: Vec<(String, String, f64)> = stmt
            .query_map([], |r| {
                let count: i64 = r.get(2)?;
                Ok((r.get(0)?, r.get(1)?, count as f64))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        edges.extend(pairwise);

        let mut stmt = self
            .conn
            .prepare("SELECT members, weight FROM hyperedges")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let hyperedge_rows: Vec<(String, f64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        for (members_json, weight) in hyperedge_rows {
            let members: Vec<String> = serde_json::from_str(&members_json).unwrap_or_default();
            for i in 0..members.len() {
                for j in (i + 1)..members.len() {
                    edges.push((members[i].clone(), members[j].clone(), weight));
                }
            }
        }

        Ok(edges)
    }

    fn matching_nodes(&self, query: &str) -> Result<Vec<String>, TunnelError> {
        let mut matched = Vec::new();

        for word in query.split_whitespace() {
            let pattern = format!("%{}%", word.to_lowercase());
            let mut stmt = self
                .conn
                .prepare("SELECT node_id FROM kg_nodes WHERE LOWER(entity) LIKE ?1")
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            let ids: Vec<String> = stmt
                .query_map(params![pattern], |r| r.get(0))
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            matched.extend(ids);
        }

        matched.dedup();
        Ok(matched)
    }

    fn expand_nodes(
        &self,
        matched_nodes: Vec<String>,
        max_hops: u32,
    ) -> Result<Vec<String>, TunnelError> {
        let mut visited = matched_nodes.clone();
        let mut frontier = matched_nodes.clone();

        for _ in 0..max_hops {
            let mut next = Vec::new();
            for node_id in &frontier {
                let neighbors = self.neighbors(node_id)?;
                for n in neighbors {
                    if !visited.contains(&n) {
                        visited.push(n.clone());
                        next.push(n);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }

        Ok(visited)
    }

    fn parent_ids_for_nodes(&self, node_ids: &[String]) -> Result<Vec<String>, TunnelError> {
        let mut chains = Vec::new();

        for node_id in node_ids {
            let mut stmt = self
                .conn
                .prepare("SELECT DISTINCT parent_id FROM kg_node_refs WHERE node_id = ?1")
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

            let ids: Vec<String> = stmt
                .query_map(params![node_id], |r| r.get(0))
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            chains.extend(ids);
        }
        chains.dedup();
        Ok(chains)
    }

    /// Remove all graph entries for a chain (called on 90-day expiry).
    pub fn eject_chain(&self, parent_id: &str) -> Result<(), TunnelError> {
        // Remove edges referencing this chain
        self.conn
            .execute(
                "DELETE FROM kg_edges WHERE parent_id = ?1",
                params![parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        self.conn
            .execute(
                "DELETE FROM hyperedges WHERE parent_id = ?1",
                params![parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        // Remove node refs for this chain
        self.conn
            .execute(
                "DELETE FROM kg_node_refs WHERE parent_id = ?1",
                params![parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        // Remove orphaned nodes (no remaining refs)
        self.conn
            .execute(
                "DELETE FROM kg_nodes WHERE node_id NOT IN
             (SELECT DISTINCT node_id FROM kg_node_refs)",
                [],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    fn get_or_create_node(
        &self,
        entity: &str,
        entity_type: Option<&str>,
        now: &str,
    ) -> Result<String, TunnelError> {
        // Check if node exists
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT node_id FROM kg_nodes WHERE LOWER(entity) = LOWER(?1)",
                params![entity],
                |r| r.get(0),
            )
            .ok();

        if let Some(id) = existing {
            return Ok(id);
        }

        let node_id = Uuid::new_v4().to_string();
        self.conn
            .execute(
                "INSERT INTO kg_nodes (node_id, entity, entity_type, created_at)
             VALUES (?1, ?2, ?3, ?4)",
                params![node_id, entity, entity_type, now],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(node_id)
    }

    fn neighbors(&self, node_id: &str) -> Result<Vec<String>, TunnelError> {
        let mut ids = self.edge_neighbors(node_id)?;
        ids.extend(self.hyperedge_neighbors(node_id)?);
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn edge_neighbors(&self, node_id: &str) -> Result<Vec<String>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT to_node_id FROM kg_edges WHERE from_node_id = ?1
             UNION
             SELECT DISTINCT from_node_id FROM kg_edges WHERE to_node_id = ?1",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let ids: Vec<String> = stmt
            .query_map(params![node_id], |r| r.get(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(ids)
    }

    fn hyperedge_neighbors(&self, node_id: &str) -> Result<Vec<String>, TunnelError> {
        let pattern = format!("%{}%", node_id);
        let mut stmt = self
            .conn
            .prepare("SELECT members FROM hyperedges WHERE members LIKE ?1")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let member_lists: Vec<String> = stmt
            .query_map(params![pattern], |r| r.get(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let mut ids = Vec::new();
        for members_json in member_lists {
            ids.extend(Self::parse_hyperedge_members(&members_json, node_id));
        }
        Ok(ids)
    }

    fn parse_hyperedge_members(members_json: &str, node_id: &str) -> Vec<String> {
        serde_json::from_str::<Vec<String>>(members_json)
            .unwrap_or_default()
            .into_iter()
            .filter(|member| member != node_id)
            .collect()
    }

    /// Extract entities from summary text via Ollama qwen3 3B.
    /// Falls back to simple noun extraction if Ollama is unavailable.
    fn extract_entities(&self, summary: &str) -> Vec<EntityInfo> {
        self.extract_via_ollama(summary)
            .unwrap_or_else(|_| simple_extract(summary))
    }

    fn extract_via_ollama(&self, summary: &str) -> Result<Vec<EntityInfo>, TunnelError> {
        let url = format!("{}/api/generate", self.ollama_url);
        let prompt = format!(
            "Extract named entities and concepts from this text. \
             Return JSON array only, no explanation. \
             Format: [{{\"entity\":\"name\",\"type\":\"person|org|concept|location\",\"relationship\":\"verb\"}}]\n\n{}",
            summary
        );

        let body = serde_json::json!({
            "model": "qwen2.5:3b",
            "prompt": prompt,
            "stream": false,
            "format": "json"
        });

        let resp = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let json: serde_json::Value = resp
            .into_json()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let response_text = json["response"].as_str().unwrap_or("[]");
        let entities: Vec<serde_json::Value> =
            serde_json::from_str(response_text).unwrap_or_default();

        Ok(entities
            .iter()
            .filter_map(|e| {
                Some(EntityInfo {
                    entity: e["entity"].as_str()?.to_string(),
                    entity_type: e["type"].as_str().map(String::from),
                    relationship: e["relationship"].as_str().map(String::from),
                })
            })
            .collect())
    }
}

/// Simple fallback: extract capitalized words as entities when Ollama is down.
fn simple_extract(text: &str) -> Vec<EntityInfo> {
    text.split_whitespace()
        .filter(|w| w.chars().next().map(|c| c.is_uppercase()).unwrap_or(false))
        .filter(|w| w.len() > 2)
        .map(|w| EntityInfo {
            entity: w.trim_matches(|c: char| !c.is_alphanumeric()).to_string(),
            entity_type: None,
            relationship: None,
        })
        .collect()
}

struct EntityInfo {
    entity: String,
    entity_type: Option<String>,
    relationship: Option<String>,
}

struct NodeRecord {
    node_id: String,
    relationship: String,
}

fn deterministic_hyperedge_id(parent_id: &str, members_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(parent_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(members_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn deterministic_edge_id(
    parent_id: &str,
    from_node_id: &str,
    to_node_id: &str,
    relationship: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(parent_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(from_node_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(to_node_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(relationship.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}Z", secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `reconcile_catalog` (gatekeeper.rs) can replay `ingest_summary` for a parent_id it
    /// already processed after a crash. That must reinforce the existing hyperedge, not
    /// duplicate it.
    #[test]
    fn write_hyperedge_dedups_by_parent_and_members_incrementing_weight() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta chain")
            .unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta chain")
            .unwrap();

        let (count, weight): (i64, f64) = graph
            .conn
            .query_row(
                "SELECT COUNT(*), MAX(weight) FROM hyperedges WHERE parent_id = 'parent-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(weight, 2.0);
    }

    /// The same crash-recovery replay hazard applies to pairwise edges: a repeat call for the
    /// same chain must reinforce the existing kg_edges row, not duplicate it.
    #[test]
    fn insert_pairwise_edge_dedups_on_replay() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta chain")
            .unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta chain")
            .unwrap();

        let count: i64 = graph
            .conn
            .query_row("SELECT COUNT(*) FROM kg_edges", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 1);
    }

    /// A summary naming the same entity twice must not produce a degenerate 1-member
    /// hyperedge — the `< 2` guard has to run after member dedup, not before.
    #[test]
    fn write_hyperedge_skips_degenerate_single_member_after_dedup() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Alpha chain")
            .unwrap();

        let count: i64 = graph
            .conn
            .query_row("SELECT COUNT(*) FROM hyperedges", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 0);
    }

    /// Dedup must stay scoped per parent_id — `eject_chain` deletes hyperedges by parent_id,
    /// so merging the same member set across different chains would let ejecting one chain
    /// delete an observation that belongs to another.
    #[test]
    fn write_hyperedge_keeps_separate_rows_across_parent_ids() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta chain")
            .unwrap();
        graph
            .ingest_summary("parent-2", "Alpha Beta chain")
            .unwrap();

        let count: i64 = graph
            .conn
            .query_row("SELECT COUNT(*) FROM hyperedges", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 2);
    }

    /// R1: `detect_communities` should populate `community_id` for every node
    /// that has at least one edge, from the current kg_edges/hyperedges state.
    #[test]
    fn detect_communities_populates_community_id_for_connected_nodes() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph
            .ingest_summary("parent-1", "Alpha Beta Gamma Delta chain")
            .unwrap();

        let report = graph.detect_communities().unwrap();
        assert_eq!(report.sweep, "communities");
        assert_eq!(report.chains_affected, 4);

        let assigned: i64 = graph
            .conn
            .query_row(
                "SELECT COUNT(*) FROM kg_nodes WHERE community_id IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(assigned, 4);
    }

    /// R1's value proposition: `traverse()` must find a concept that shares a
    /// community with a matched anchor even when it's unreachable by BFS at
    /// the caller's `max_hops` limit — here, 0 hops, and no shared edge at all.
    #[test]
    fn traverse_finds_community_linked_node_beyond_bfs_hop_limit() {
        let graph = KnowledgeGraph::open(":memory:", "http://127.0.0.1:9").unwrap();
        graph.ingest_summary("parent-gamma", "Gamma alone").unwrap();
        graph.ingest_summary("parent-delta", "Delta alone").unwrap();

        // Plain BFS (0 hops) only finds Gamma's own chain — no edge links them.
        let bfs_only = graph.traverse("Gamma", 0).unwrap();
        assert_eq!(bfs_only, vec!["parent-gamma".to_string()]);

        // Simulate what a `detect_communities` sweep would find once the wider
        // graph ties Gamma and Delta into the same community.
        let gamma_id: String = graph
            .conn
            .query_row(
                "SELECT node_id FROM kg_nodes WHERE entity = 'Gamma'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let delta_id: String = graph
            .conn
            .query_row(
                "SELECT node_id FROM kg_nodes WHERE entity = 'Delta'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        graph
            .conn
            .execute(
                "UPDATE kg_nodes SET community_id = 'c0' WHERE node_id IN (?1, ?2)",
                params![gamma_id, delta_id],
            )
            .unwrap();

        let mut with_community = graph.traverse("Gamma", 0).unwrap();
        with_community.sort();
        assert_eq!(
            with_community,
            vec!["parent-delta".to_string(), "parent-gamma".to_string()]
        );
    }
}
