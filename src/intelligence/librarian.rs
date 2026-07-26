use crate::intelligence::scribe::ExitEvent;
use crate::storage::permissions::restrict_database_files;
use crate::types::error::TunnelError;
use crate::types::fact::{AnswerFact, Foresight};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::env;

const HYPE_FACT_IDX_OFFSET: i64 = 1_000_000;
const DEFAULT_EMBED_MODEL: &str = "nomic-embed-text";
const DEFAULT_EMBED_DIM: usize = 768;

/// Minimum Beta-Bernoulli posterior mean (`alpha / (alpha + beta)`) over EXIT verdicts for an
/// orb's recency-stale content to be exempted from cold demotion. See
/// `spec/math-application-proposal-usefulness-score-tiering.md`.
const USEFULNESS_COLD_FLOOR: f64 = 0.75;
/// Minimum evidence (`alpha + beta`) before `USEFULNESS_COLD_FLOOR` can apply — an orb with no
/// feedback sits at the neutral Beta(1,1) prior (alpha+beta = 2.0) and must never be treated as
/// "proven useful" on that basis alone.
const USEFULNESS_MIN_EVIDENCE: f64 = 4.0;

/// The Librarian — SQLite catalog + durable retrieval indexes.
///
/// Does not hold raw content. Does not hold raw keys.
/// Holds: orb metadata, key_id pointers, 100-word summary embeddings.
///
/// Embedding is now async — register_orb enqueues into embedding_queue.
/// A background worker calls process_embedding_batch() on a timer.
/// Until embedding completes, orbs are retrievable by metadata/parent_id and
/// FTS5 keyword search, but not by semantic similarity.
pub struct Librarian {
    conn: Connection,
    embedding_index_loaded: bool,
    sqlite_vec_loaded: bool,
    hype_enabled: bool,
    ollama_url: String,
    embed_model: String,
    embedding_dim: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleConfig {
    pub enabled: bool,
    pub t_warm_secs: u64,
    pub t_cold_secs: u64,
    pub max_hot_orbs: usize,
    pub sweep_interval: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        // Per VENTURI_ROADMAP.md (B15 — Context Lifecycle Manager): a fast,
        // in-memory RAM-eviction cache scoped per actor, deliberately on a
        // much shorter cycle than the long-term retention tiering in
        // `pipeline/sweep.rs` (Hot <= 7 days, Warm 7-30 days, Cold 30+ days)
        // — the two serve different purposes despite sharing `orbs.tier`.
        // See `promote_active`'s embedding re-queue for why cold demotion
        // here is safe to run this aggressively.
        Self {
            enabled: true,
            t_warm_secs: 5 * 60,
            t_cold_secs: 10 * 60,
            max_hot_orbs: 500,
            sweep_interval: 60,
        }
    }
}

impl Librarian {
    pub fn open(
        db_path: &str,
        ollama_url: &str,
        embedding_model: Option<&str>,
        embedding_dim: Option<usize>,
    ) -> Result<Self, TunnelError> {
        let conn =
            Connection::open(db_path).map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        init_schema(&conn)?;
        let sqlite_vec_loaded = load_sqlite_vec_if_configured(&conn);
        if sqlite_vec_loaded {
            backfill_sqlite_vec_index(&conn);
        }
        restrict_database_files(db_path)?;
        Ok(Self {
            conn,
            embedding_index_loaded: sqlite_vec_loaded,
            sqlite_vec_loaded,
            hype_enabled: hype_enabled(),
            ollama_url: ollama_url.to_string(),
            embed_model: embedding_model.unwrap_or(DEFAULT_EMBED_MODEL).to_string(),
            embedding_dim: embedding_dim.unwrap_or(DEFAULT_EMBED_DIM),
        })
    }

    pub fn embedding_model_version(&self) -> String {
        format!("{}:{}", self.embed_model, self.embedding_dim)
    }

    /// Register a new orb in the catalog after a successful ingestion commit.
    ///
    /// Embedding is NOT done inline — the orb is enqueued for background processing.
    /// This keeps the ingest hot path fast regardless of Ollama availability.
    pub fn register_orb(&mut self, entry: OrbEntry) -> Result<(), TunnelError> {
        let now = now_iso();
        self.insert_orb_row(&entry, &now)?;

        if entry.classification == "secret" {
            return Ok(());
        }

        self.conn
            .execute(
                "INSERT OR IGNORE INTO embedding_queue (orb_id, summary, queued_at)
             VALUES (?1, ?2, ?3)",
                params![entry.orb_id, entry.summary, now],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        self.index_fts(&entry.orb_id, &entry.summary, "summary")?;

        for (idx, fact) in entry.answer_facts.iter().enumerate() {
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO fact_queue
                     (orb_id, fact_idx, fact_text, source, queued_at)
                     VALUES (?1, ?2, ?3, 'fact', ?4)",
                    params![entry.orb_id, idx as i64, fact.fact, now],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            self.index_fts(&entry.orb_id, &fact.fact, "fact")?;
        }

        if entry.sequence == 1 {
            self.register_foresights(&entry)?;
        }

        Ok(())
    }

    fn index_fts(&self, orb_id: &str, body: &str, source: &str) -> Result<(), TunnelError> {
        if body.trim().is_empty() {
            return Ok(());
        }
        self.conn
            .execute(
                "INSERT INTO fts_orbs (orb_id, source, body)
                 SELECT ?1, ?2, ?3
                 WHERE NOT EXISTS (
                     SELECT 1 FROM fts_orbs
                     WHERE orb_id = ?1 AND source = ?2 AND body = ?3
                 )",
                params![orb_id, source, body],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn insert_orb_row(&self, entry: &OrbEntry, now: &str) -> Result<(), TunnelError> {
        let facts_json =
            serde_json::to_string(&entry.answer_facts).unwrap_or_else(|_| "[]".to_string());
        self.conn
            .execute(
                "INSERT INTO orbs
             (orb_id, key_id, topic, domain, date, parent_id, sequence, chain_length,
              tier, last_accessed, last_accessed_at, access_count, usefulness_score,
              pinned, owner_agent_id, embedding, format, classification, content_type, answer_facts,
              summary_author, summary_model, summary_verified, summary_verified_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'hot',?9,?9,0,1.0,?10,?11,NULL,?12,?13,?14,?15,?16,?17,?18,?19)
             ON CONFLICT(orb_id) DO NOTHING",
                params![
                    entry.orb_id,
                    entry.key_id,
                    entry.topic,
                    entry.domain,
                    entry.date,
                    entry.parent_id,
                    entry.sequence,
                    entry.chain_length,
                    now,
                    if entry.pinned { 1 } else { 0 },
                    entry.owner_agent_id,
                    entry.format,
                    entry.classification,
                    entry.content_type,
                    facts_json,
                    entry.summary_author,
                    entry.summary_model,
                    if entry.summary_verified { 1 } else { 0 },
                    entry.summary_verified_at
                ],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn register_foresights(&self, entry: &OrbEntry) -> Result<(), TunnelError> {
        for (idx, foresight) in entry.foresights.iter().enumerate() {
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO foresights
                     (parent_id, foresight_idx, foresight_text, relevant_from,
                      relevant_until, duration_days, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        entry.parent_id,
                        idx as i64,
                        foresight.foresight_text,
                        foresight.relevant_from,
                        foresight.relevant_until,
                        foresight.duration_days as i64,
                        now_iso()
                    ],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    /// Process up to 10 summary + 5 fact embeddings from the durable queues.
    ///
    /// Called by the background embedding worker every 30 seconds.
    /// Returns total count of items successfully embedded this batch.
    pub fn process_embedding_batch(&mut self) -> Result<u32, TunnelError> {
        if !self.embedding_index_loaded {
            self.load_embedding_index()?;
        }

        let n = self.process_summary_queue()?;
        let m = self.process_fact_queue()?;
        Ok(n + m)
    }

    pub fn embedding_ready(&self) -> bool {
        self.embedding_index_loaded
    }

    fn load_embedding_index(&mut self) -> Result<(), TunnelError> {
        self.embedding_index_loaded = true;
        Ok(())
    }

    fn process_summary_queue(&mut self) -> Result<u32, TunnelError> {
        let pending: Vec<(String, String)> = {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT q.orb_id, q.summary
                     FROM embedding_queue q
                     JOIN orbs o ON o.orb_id = q.orb_id
                     WHERE q.attempts < 3
                       AND o.classification != 'secret'
                       AND o.expired_at IS NULL
                     LIMIT 10",
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            rows
        };
        let mut processed = 0u32;
        for (orb_id, summary) in pending {
            match self.embed(&summary) {
                Ok(embedding) => {
                    self.validate_embedding_dim(&embedding)?;
                    let blob = floats_to_bytes(&embedding);
                    self.conn
                        .execute(
                            "UPDATE orbs SET embedding = ?1 WHERE orb_id = ?2",
                            params![blob, orb_id],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                    self.sync_sqlite_vec_embedding(&summary_vector_id(&orb_id), &embedding)?;
                    self.enqueue_hype_questions(&orb_id, &summary)?;
                    self.conn
                        .execute(
                            "DELETE FROM embedding_queue WHERE orb_id = ?1",
                            params![orb_id],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                    processed += 1;
                }
                Err(_) => {
                    self.conn
                        .execute(
                            "UPDATE embedding_queue SET attempts = attempts + 1 WHERE orb_id = ?1",
                            params![orb_id],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                }
            }
        }
        Ok(processed)
    }

    fn process_fact_queue(&mut self) -> Result<u32, TunnelError> {
        let pending = self.pending_fact_queue()?;
        let mut processed = 0u32;
        for (orb_id, fact_idx, fact_text, source) in pending {
            match self.embed(&fact_text) {
                Ok(embedding) => {
                    self.validate_embedding_dim(&embedding)?;
                    let blob = floats_to_bytes(&embedding);
                    self.conn
                        .execute(
                            "INSERT OR REPLACE INTO fact_embeddings
                             (orb_id, fact_idx, source, embedding)
                             VALUES (?1, ?2, ?3, ?4)",
                            params![orb_id, fact_idx, source, blob],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                    self.sync_sqlite_vec_embedding(
                        &fact_vector_id(&orb_id, &source, fact_idx),
                        &embedding,
                    )?;
                    self.conn
                        .execute(
                            "DELETE FROM fact_queue
                             WHERE orb_id = ?1 AND fact_idx = ?2 AND source = ?3",
                            params![orb_id, fact_idx, source],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                    processed += 1;
                }
                Err(_) => {
                    self.conn
                        .execute(
                            "UPDATE fact_queue SET attempts = attempts + 1
                             WHERE orb_id = ?1 AND fact_idx = ?2 AND source = ?3",
                            params![orb_id, fact_idx, source],
                        )
                        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                }
            }
        }
        Ok(processed)
    }

    fn pending_fact_queue(&self) -> Result<Vec<(String, i64, String, String)>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT q.orb_id, q.fact_idx, q.fact_text, q.source
                 FROM fact_queue q
                 JOIN orbs o ON o.orb_id = q.orb_id
                 WHERE q.attempts < 3
                   AND o.classification != 'secret'
                   AND o.expired_at IS NULL
                 LIMIT 5",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(rows)
    }

    fn enqueue_hype_questions(&self, orb_id: &str, summary: &str) -> Result<(), TunnelError> {
        if !self.hype_enabled {
            return Ok(());
        }
        let questions = match self.generate_hype_questions(summary) {
            Ok(questions) => questions,
            Err(_) => return Ok(()),
        };
        for (idx, question) in questions.iter().enumerate() {
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO fact_queue
                     (orb_id, fact_idx, fact_text, source, queued_at)
                     VALUES (?1, ?2, ?3, 'hype', ?4)",
                    params![
                        orb_id,
                        HYPE_FACT_IDX_OFFSET + idx as i64,
                        question,
                        now_iso()
                    ],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            self.index_fts(orb_id, question, "hype")?;
        }
        Ok(())
    }

    fn fts_search(&self, query: &str, limit: usize) -> Result<Vec<String>, TunnelError> {
        let Some(match_query) = fts_match_query(query) else {
            return Ok(Vec::new());
        };
        let mut stmt = self
            .conn
            .prepare(
                "SELECT fts_orbs.orb_id
                 FROM fts_orbs
                 JOIN orbs o ON o.orb_id = fts_orbs.orb_id
                 WHERE fts_orbs MATCH ?1
                   AND o.expired_at IS NULL
                   AND o.classification != 'secret'
                 ORDER BY bm25(fts_orbs) ASC
                 LIMIT ?2",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map(params![match_query, limit as i64], |row| row.get(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let mut seen = HashMap::new();
        let mut ranked = Vec::new();
        for row in rows {
            let orb_id: String = row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            if seen.insert(orb_id.clone(), ()).is_none() {
                ranked.push(orb_id);
            }
        }
        Ok(ranked)
    }

    fn vector_scan_search(
        &self,
        query_emb: &[f32],
        limit: usize,
    ) -> Result<Vec<String>, TunnelError> {
        if let Some(ranked) = self.sqlite_vec_search(query_emb, limit)? {
            return Ok(ranked);
        }

        let mut best: HashMap<String, f32> = HashMap::new();
        for (orb_id, emb) in self.stored_embeddings()? {
            let score = cosine_similarity(query_emb, &emb);
            let entry = best.entry(orb_id).or_insert(f32::NEG_INFINITY);
            if score > *entry {
                *entry = score;
            }
        }
        let mut scored: Vec<(f32, String)> = best.into_iter().map(|(id, s)| (s, id)).collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        Ok(scored.into_iter().take(limit).map(|(_, id)| id).collect())
    }

    fn stored_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>, TunnelError> {
        load_embeddings(&self.conn)
    }

    fn sqlite_vec_search(
        &self,
        query_emb: &[f32],
        limit: usize,
    ) -> Result<Option<Vec<String>>, TunnelError> {
        if !self.sqlite_vec_loaded || sqlite_vec_dim(&self.conn)? != Some(query_emb.len()) {
            return Ok(None);
        }

        let query_blob = floats_to_bytes(query_emb);
        let mut stmt = match self.conn.prepare(
            "SELECT id
             FROM embeddings_vec
             ORDER BY vec_distance_cosine(embedding, ?1) ASC
             LIMIT ?2",
        ) {
            Ok(stmt) => stmt,
            Err(_) => return Ok(None),
        };
        let rows = match stmt.query_map(params![query_blob, (limit * 4) as i64], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(rows) => rows,
            Err(_) => return Ok(None),
        };

        let mut seen = HashMap::new();
        let mut ranked = Vec::new();
        for row in rows {
            let vector_id = row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            let Some(orb_id) = orb_id_from_vector_id(&vector_id) else {
                continue;
            };
            if seen.insert(orb_id.clone(), ()).is_none() && self.active_public_orb(&orb_id)? {
                ranked.push(orb_id);
                if ranked.len() >= limit {
                    break;
                }
            }
        }
        Ok(Some(ranked))
    }

    fn sync_sqlite_vec_embedding(
        &self,
        vector_id: &str,
        embedding: &[f32],
    ) -> Result<(), TunnelError> {
        if !self.sqlite_vec_loaded || !ensure_sqlite_vec_table(&self.conn, embedding.len())? {
            return Ok(());
        }

        self.conn
            .execute(
                "INSERT OR REPLACE INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
                params![vector_id, floats_to_bytes(embedding)],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn active_public_orb(&self, orb_id: &str) -> Result<bool, TunnelError> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM orbs
                 WHERE orb_id = ?1
                   AND expired_at IS NULL
                   AND classification != 'secret'",
                params![orb_id],
                |row| row.get(0),
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(count > 0)
    }

    fn generate_hype_questions(&self, summary: &str) -> Result<Vec<String>, TunnelError> {
        let prompt = format!(
            "Generate 5 concise questions that this text is the answer to. \
             Return only one question per line.\n\n{}",
            summary
        );
        let body = serde_json::json!({
            "model": self.embed_model,
            "prompt": prompt,
            "stream": false
        });

        let resp = ureq::post(&format!("{}/api/generate", self.ollama_url))
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| TunnelError::DatabaseError(format!("ollama hype failed: {}", e)))?;
        let json: serde_json::Value = resp
            .into_json()
            .map_err(|e| TunnelError::DatabaseError(format!("hype parse failed: {}", e)))?;
        let text = json["response"].as_str().unwrap_or_default();
        Ok(parse_hype_questions(text))
    }

    fn validate_embedding_dim(&self, embedding: &[f32]) -> Result<(), TunnelError> {
        if embedding.len() == self.embedding_dim {
            return Ok(());
        }
        Err(TunnelError::DatabaseError(format!(
            "embedding dimension mismatch: model {} returned {}, expected {}",
            self.embed_model,
            embedding.len(),
            self.embedding_dim
        )))
    }

    /// Return count of orbs still waiting for embeddings.
    pub fn embedding_queue_depth(&self) -> Result<u32, TunnelError> {
        let n: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM embedding_queue WHERE attempts < 3",
                [],
                |row| row.get(0),
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(n as u32)
    }

    /// Context mode: embed query → cosine similarity → top-K orb_ids.
    /// Deduplicates by orb_id — takes the highest score when multiple embeddings
    /// (summary + fact atoms) map to the same orb.
    pub fn similarity_search(&self, query: &str, top_k: usize) -> Result<Vec<String>, TunnelError> {
        let keyword_ranked = self.fts_search(query, top_k * 4)?;
        let vector_ranked = match self.embed(query) {
            Ok(query_emb) => self.vector_scan_search(&query_emb, top_k * 4)?,
            Err(error) if keyword_ranked.is_empty() => return Err(error),
            Err(_) => Vec::new(),
        };
        Ok(rrf_fuse(&keyword_ranked, &vector_ranked, top_k))
    }

    /// Look up a single orb row by its orb_id. Returns None if not catalogued.
    pub fn fetch_by_orb_id(&self, orb_id: &str) -> Result<Option<OrbRow>, TunnelError> {
        let result = self.conn.query_row(
            "SELECT orb_id, key_id, parent_id, sequence, chain_length, tier, format
             FROM orbs WHERE orb_id = ?1",
            params![orb_id],
            orb_row_from_row,
        );
        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(TunnelError::DatabaseError(e.to_string())),
        }
    }

    /// Document mode: fetch all orbs in a chain, ordered by sequence.
    pub fn fetch_chain(&self, parent_id: &str) -> Result<Vec<OrbRow>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT orb_id, key_id, parent_id, sequence, chain_length, tier, format
             FROM orbs WHERE parent_id = ?1 ORDER BY sequence ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map(params![parent_id], orb_row_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    /// Temporal mode: fetch all chains touching a subject within a date range.
    pub fn fetch_temporal(
        &self,
        subject: &str,
        from: &str,
        to: &str,
        agent_id_filter: Option<&str>,
    ) -> Result<Vec<OrbRow>, TunnelError> {
        let _ = agent_id_filter;
        let pattern = format!("%{}%", subject);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT orb_id, key_id, parent_id, sequence, chain_length, tier, format
             FROM orbs
             WHERE (topic LIKE ?1 OR domain LIKE ?1)
               AND date >= ?2 AND date <= ?3
               AND expired_at IS NULL
               AND classification != 'secret'
             ORDER BY date ASC, sequence ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map(params![pattern, from, to], orb_row_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    /// Structured mode: exact metadata filter — pure SQL, no semantic search.
    pub fn fetch_structured(&self, filter: StructuredFilter) -> Result<Vec<OrbRow>, TunnelError> {
        let (where_clause, values) = build_where(&filter);
        let sql = format!(
            "SELECT orb_id, key_id, parent_id, sequence, chain_length, tier, format
             FROM orbs WHERE {} AND expired_at IS NULL ORDER BY date ASC, sequence ASC",
            where_clause
        );

        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let params: Vec<&dyn rusqlite::ToSql> =
            values.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let rows = stmt
            .query_map(params.as_slice(), orb_row_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    /// Metadata mode: return catalog rows without triggering decryption.
    /// Same filter API as structured mode but returns richer metadata, no key_id exposed.
    pub fn fetch_metadata(&self, filter: StructuredFilter) -> Result<Vec<MetaRow>, TunnelError> {
        let (where_clause, values) = build_where(&filter);
        let sql = format!(
            "SELECT orb_id, parent_id, topic, domain, date, format, tier, sequence, chain_length,
                    classification, content_type, summary_author, summary_model,
                    summary_verified, summary_verified_at, answer_facts
             FROM orbs WHERE {} AND expired_at IS NULL ORDER BY date ASC, sequence ASC",
            where_clause
        );

        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let params: Vec<&dyn rusqlite::ToSql> =
            values.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let rows = stmt
            .query_map(params.as_slice(), meta_row_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    pub fn mark_accessed(&self, parent_id: &str) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO access_marks (parent_id, accessed_at) VALUES (?1, ?2)",
                params![parent_id, now],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        self.conn
            .execute(
                "UPDATE orbs
                 SET last_accessed = ?1,
                     last_accessed_at = ?1,
                     access_count = access_count + 1
                 WHERE parent_id = ?2 AND expired_at IS NULL",
                params![now, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub fn tiers_for_orbs(&self, orb_ids: &[String]) -> Result<Vec<String>, TunnelError> {
        let mut tiers = Vec::new();
        for orb_id in orb_ids {
            let tier = self
                .conn
                .query_row(
                    "SELECT tier FROM orbs WHERE orb_id = ?1 AND expired_at IS NULL",
                    params![orb_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            tiers.push(tier);
        }
        Ok(tiers)
    }

    /// Reads the last-processed EXIT cursor for a named sweep checkpoint (`None` on first run).
    pub fn sweep_checkpoint(&self, name: &str) -> Result<Option<String>, TunnelError> {
        self.conn
            .query_row(
                "SELECT last_ts FROM sweep_checkpoints WHERE name = ?1",
                params![name],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    /// Advances a named sweep checkpoint. Used so a restart resumes from the last processed
    /// EXIT event instead of reprocessing or dropping events. The stored value may include an
    /// event row ID in addition to its timestamp.
    pub fn set_sweep_checkpoint(&self, name: &str, last_ts: &str) -> Result<(), TunnelError> {
        self.conn
            .execute(
                "INSERT INTO sweep_checkpoints (name, last_ts) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET last_ts = excluded.last_ts",
                params![name, last_ts],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Aggregates EXIT-verdict feedback into each referenced orb's usefulness posterior
    /// (Beta-Bernoulli: verdict=1 increments alpha, anything else increments beta), then
    /// refreshes the cached `usefulness_score = alpha/(alpha+beta)`. Orbs referenced by an event
    /// that no longer exist (already ejected) are silently skipped — the `UPDATE` simply matches
    /// no row. See `spec/math-application-proposal-usefulness-score-tiering.md`.
    pub fn apply_exit_feedback(&self, events: &[ExitEvent]) -> Result<u32, TunnelError> {
        let mut updated = 0u32;
        for event in events {
            let success: f64 = if event.verdict == 1 { 1.0 } else { 0.0 };
            let failure: f64 = 1.0 - success;
            for orb_id in &event.orb_ids {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE orbs
                         SET usefulness_alpha = usefulness_alpha + ?1,
                             usefulness_beta = usefulness_beta + ?2,
                             usefulness_score =
                                 (usefulness_alpha + ?1) / (usefulness_alpha + usefulness_beta + ?1 + ?2)
                         WHERE orb_id = ?3 AND expired_at IS NULL",
                        params![success, failure, orb_id],
                    )
                    .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
                updated += changed as u32;
            }
        }
        Ok(updated)
    }

    pub fn lifecycle_sweep(&self, cfg: &LifecycleConfig) -> Result<u32, TunnelError> {
        if !cfg.enabled {
            return Ok(0);
        }

        let now = now_secs();
        let warm_cutoff = format!("{}Z", now.saturating_sub(cfg.t_warm_secs));
        let cold_cutoff = format!("{}Z", now.saturating_sub(cfg.t_cold_secs));

        let cold = self.demote_cold(&cold_cutoff)?;
        let warm = self.demote_warm(&warm_cutoff, &cold_cutoff)?;
        let hot = self.promote_active(&warm_cutoff)?;
        let capped = self.cap_hot_tier(cfg.max_hot_orbs)?;
        Ok((cold + warm + hot + capped) as u32)
    }

    /// Recency-stale orbs are demoted to cold unless real EXIT-verdict feedback has proven them
    /// useful (`usefulness_alpha + usefulness_beta >= USEFULNESS_MIN_EVIDENCE`, ruling out the
    /// neutral no-feedback prior, and a posterior mean at or above `USEFULNESS_COLD_FLOOR`). This
    /// only ever protects an orb from cold demotion — it does not promote, and does not affect
    /// `demote_warm`/`cap_hot_tier`. See
    /// `spec/math-application-proposal-usefulness-score-tiering.md`.
    fn demote_cold(&self, cutoff: &str) -> Result<usize, TunnelError> {
        self.conn
            .execute(
                "UPDATE orbs
                 SET tier = 'cold', embedding = NULL
                 WHERE pinned = 0
                   AND expired_at IS NULL
                   AND COALESCE(last_accessed_at, last_accessed) < ?1
                   AND access_count < 2
                   AND tier != 'cold'
                   AND NOT (
                       (usefulness_alpha + usefulness_beta) >= ?2
                       AND (usefulness_alpha / (usefulness_alpha + usefulness_beta)) >= ?3
                   )",
                params![cutoff, USEFULNESS_MIN_EVIDENCE, USEFULNESS_COLD_FLOOR],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    fn demote_warm(&self, warm_cutoff: &str, cold_cutoff: &str) -> Result<usize, TunnelError> {
        self.conn
            .execute(
                "UPDATE orbs
                 SET tier = 'warm'
                 WHERE pinned = 0
                   AND expired_at IS NULL
                   AND COALESCE(last_accessed_at, last_accessed) < ?1
                   AND COALESCE(last_accessed_at, last_accessed) >= ?2
                   AND tier != 'warm'",
                params![warm_cutoff, cold_cutoff],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    fn promote_active(&self, warm_cutoff: &str) -> Result<usize, TunnelError> {
        let promoted = self
            .conn
            .execute(
                "UPDATE orbs
                 SET tier = 'hot'
                 WHERE expired_at IS NULL
                   AND (pinned = 1 OR COALESCE(last_accessed_at, last_accessed) >= ?1)
                   AND tier != 'hot'",
                params![warm_cutoff],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        if promoted > 0 {
            self.requeue_embeddings_for_hot_orbs()?;
        }
        Ok(promoted)
    }

    /// `demote_cold` drops `embedding` to free memory — B15's documented
    /// design is that the orb "stays on disk, reloaded on demand." Nothing
    /// else ever recomputes a dropped embedding, so without this, promotion
    /// back to hot is silent and the orb never re-enters semantic search
    /// (`load_summary_embeddings` requires `embedding IS NOT NULL`),
    /// permanently losing recall for content that was simply idle for a
    /// while. Re-queues through the same async `embedding_queue` used at
    /// ingest, picked up by the existing embedding sweep, rather than
    /// embedding synchronously inside this sweep.
    fn requeue_embeddings_for_hot_orbs(&self) -> Result<(), TunnelError> {
        let now = now_iso();
        self.conn
            .execute(
                "INSERT OR IGNORE INTO embedding_queue (orb_id, summary, queued_at)
                 SELECT o.orb_id, f.body, ?1
                 FROM orbs o
                 JOIN fts_orbs f ON f.orb_id = o.orb_id AND f.source = 'summary'
                 WHERE o.tier = 'hot' AND o.embedding IS NULL AND o.expired_at IS NULL",
                params![now],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn cap_hot_tier(&self, max_hot_orbs: usize) -> Result<usize, TunnelError> {
        if max_hot_orbs == 0 {
            return Ok(0);
        }
        let owners = self.hot_tier_owners()?;
        let mut demoted = 0usize;
        for owner in owners {
            demoted += self.cap_owner_hot_tier(&owner, max_hot_orbs)?;
        }
        Ok(demoted)
    }

    fn hot_tier_owners(&self) -> Result<Vec<String>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT COALESCE(owner_agent_id, 'unknown')
                 FROM orbs
                 WHERE tier = 'hot' AND pinned = 0 AND expired_at IS NULL",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let owners = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(owners)
    }

    fn cap_owner_hot_tier(&self, owner: &str, max_hot_orbs: usize) -> Result<usize, TunnelError> {
        self.conn
            .execute(
                "UPDATE orbs
                 SET tier = 'warm'
                 WHERE orb_id IN (
                     SELECT orb_id FROM orbs
                     WHERE tier = 'hot'
                       AND pinned = 0
                       AND expired_at IS NULL
                       AND COALESCE(owner_agent_id, 'unknown') = ?1
                     ORDER BY COALESCE(last_accessed_at, last_accessed) DESC, orb_id ASC
                     LIMIT -1 OFFSET ?2
                 )",
                params![owner, max_hot_orbs as i64],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    pub fn update_tier(&self, parent_id: &str, tier: &str) -> Result<(), TunnelError> {
        self.conn
            .execute(
                "UPDATE orbs SET tier = ?1 WHERE parent_id = ?2 AND expired_at IS NULL",
                params![tier, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub fn expired_chains(&self, days: u64) -> Result<Vec<String>, TunnelError> {
        let cutoff = now_secs().saturating_sub(days * 86400);
        let cutoff_ts = format!("{}Z", cutoff);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT parent_id FROM orbs
                 WHERE last_accessed < ?1 AND expired_at IS NULL AND pinned = 0",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let rows = stmt
            .query_map(params![cutoff_ts], |row| row.get::<_, String>(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    pub fn eject_chain(&self, parent_id: &str) -> Result<Vec<String>, TunnelError> {
        if self.chain_on_legal_hold(parent_id)? {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare("SELECT orb_id FROM orbs WHERE parent_id = ?1 AND expired_at IS NULL")
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        let orb_ids: Vec<String> = stmt
            .query_map(params![parent_id], |row| row.get(0))
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        // Remove pending/completed fact data for ejected orbs before deleting the orbs rows
        for orb_id in &orb_ids {
            self.conn
                .execute("DELETE FROM fact_queue WHERE orb_id = ?1", params![orb_id])
                .ok();
            self.conn
                .execute(
                    "DELETE FROM fact_embeddings WHERE orb_id = ?1",
                    params![orb_id],
                )
                .ok();
            self.conn
                .execute(
                    "DELETE FROM embedding_queue WHERE orb_id = ?1",
                    params![orb_id],
                )
                .ok();
            self.conn
                .execute("DELETE FROM fts_orbs WHERE orb_id = ?1", params![orb_id])
                .ok();
            if self.sqlite_vec_loaded {
                self.conn
                    .execute(
                        "DELETE FROM embeddings_vec WHERE id = ?1 OR id GLOB ?2",
                        params![summary_vector_id(orb_id), format!("f:*:{}:*", orb_id)],
                    )
                    .ok();
            }
        }

        let now = now_iso();
        self.conn
            .execute(
                "UPDATE orbs
                 SET expired_at = ?1, tier = 'expired', embedding = NULL
                 WHERE parent_id = ?2 AND legal_hold = 0",
                params![now, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        Ok(orb_ids)
    }

    pub fn set_legal_hold(&self, parent_id: &str, reason: &str) -> Result<(), TunnelError> {
        let changed = self
            .conn
            .execute(
                "UPDATE orbs SET legal_hold = 1, legal_hold_reason = ?1 WHERE parent_id = ?2",
                params![reason, parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        if changed == 0 {
            return Err(TunnelError::OrbNotFound {
                id: parent_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn release_legal_hold(&self, parent_id: &str) -> Result<(), TunnelError> {
        let changed = self
            .conn
            .execute(
                "UPDATE orbs SET legal_hold = 0, legal_hold_reason = NULL WHERE parent_id = ?1",
                params![parent_id],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        if changed == 0 {
            return Err(TunnelError::OrbNotFound {
                id: parent_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn chain_on_legal_hold(&self, parent_id: &str) -> Result<bool, TunnelError> {
        let held: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM orbs WHERE parent_id = ?1 AND legal_hold = 1",
                params![parent_id],
                |row| row.get(0),
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(held > 0)
    }

    pub fn link_chains(
        &self,
        from_parent_id: &str,
        to_parent_id: &str,
        reference_type: &str,
    ) -> Result<(), TunnelError> {
        validate_reference_type(reference_type)?;
        if from_parent_id == to_parent_id {
            return Err(TunnelError::GatekeeperRejected {
                reason: "chain reference cannot target itself".to_string(),
            });
        }
        if !self.chain_exists(from_parent_id)? || !self.chain_exists(to_parent_id)? {
            return Err(TunnelError::GatekeeperRejected {
                reason: "chain reference parent_id not found".to_string(),
            });
        }

        self.conn
            .execute(
                "INSERT OR REPLACE INTO chain_references
                 (from_parent_id, to_parent_id, reference_type, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![from_parent_id, to_parent_id, reference_type, now_iso()],
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub fn chain_references(&self, parent_id: &str) -> Result<Vec<ChainReference>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT from_parent_id, to_parent_id, reference_type, created_at
                 FROM chain_references
                 WHERE from_parent_id = ?1 OR to_parent_id = ?1
                 ORDER BY created_at ASC, from_parent_id ASC, to_parent_id ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map(params![parent_id], chain_reference_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    pub fn active_foresights(&self, on: &str) -> Result<Vec<ForesightRow>, TunnelError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT f.parent_id, f.foresight_text, f.relevant_from,
                        f.relevant_until, f.duration_days, f.created_at
                 FROM foresights f
                 WHERE f.relevant_from <= ?1
                   AND f.relevant_until >= ?1
                   AND EXISTS (
                       SELECT 1 FROM orbs o
                       WHERE o.parent_id = f.parent_id
                         AND o.expired_at IS NULL
                         AND o.classification != 'secret'
                   )
                 ORDER BY f.relevant_until ASC, f.parent_id ASC, f.foresight_idx ASC",
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        let rows = stmt
            .query_map(params![on], foresight_row_from_row)
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))
    }

    fn chain_exists(&self, parent_id: &str) -> Result<bool, TunnelError> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM orbs WHERE parent_id = ?1",
                params![parent_id],
                |row| row.get(0),
            )
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        Ok(count > 0)
    }

    pub fn flush_access_marks(&self) -> Result<u32, TunnelError> {
        let marks: Vec<(String, String)> = {
            let mut stmt = self
                .conn
                .prepare("SELECT parent_id, accessed_at FROM access_marks")
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
            rows
        };

        for (parent_id, accessed_at) in &marks {
            self.conn
                .execute(
                    "UPDATE orbs SET last_accessed = ?1, last_accessed_at = ?1
                 WHERE parent_id = ?2 AND expired_at IS NULL",
                    params![accessed_at, parent_id],
                )
                .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        }

        self.conn
            .execute("DELETE FROM access_marks", [])
            .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

        Ok(marks.len() as u32)
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>, TunnelError> {
        let url = format!("{}/api/embeddings", self.ollama_url);
        let body = serde_json::json!({
            "model": self.embed_model,
            "prompt": text
        });

        let resp = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| TunnelError::DatabaseError(format!("ollama embed failed: {}", e)))?;

        let json: serde_json::Value = resp
            .into_json()
            .map_err(|e| TunnelError::DatabaseError(format!("embed parse failed: {}", e)))?;

        let embedding = json["embedding"]
            .as_array()
            .ok_or_else(|| TunnelError::DatabaseError("no embedding field in response".into()))?
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();

        Ok(embedding)
    }
}

// ── WHERE clause builder (shared by structured and metadata modes) ─────────────

fn build_where(filter: &StructuredFilter) -> (String, Vec<String>) {
    let mut conditions = Vec::new();
    let mut values: Vec<String> = Vec::new();

    if let Some(v) = &filter.topic {
        conditions.push("topic = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.domain {
        conditions.push("domain = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.tier {
        conditions.push("tier = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.parent_id {
        conditions.push("parent_id = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.format {
        conditions.push("format = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.classification {
        conditions.push("classification = ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.date_from {
        conditions.push("date >= ?");
        values.push(v.clone());
    }
    if let Some(v) = &filter.date_to {
        conditions.push("date <= ?");
        values.push(v.clone());
    }
    if filter.parent_id.is_none() {
        conditions.push("classification != 'secret'");
    }

    let clause = if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    };
    (clause, values)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn init_schema(conn: &Connection) -> Result<(), TunnelError> {
    init_orbs_schema(conn)?;
    init_queue_schema(conn)?;
    init_search_schema(conn)?;
    init_reference_schema(conn)?;
    init_foresight_schema(conn)?;
    init_indexes(conn)?;
    apply_schema_migrations(conn);
    backfill_search_index(conn);
    Ok(())
}

fn init_orbs_schema(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS orbs (
            orb_id        TEXT PRIMARY KEY,
            key_id        TEXT NOT NULL,
            topic         TEXT NOT NULL,
            domain        TEXT NOT NULL,
            date          TEXT NOT NULL,
            parent_id     TEXT NOT NULL,
            sequence      INTEGER NOT NULL,
            chain_length  INTEGER NOT NULL,
            tier          TEXT NOT NULL DEFAULT 'hot',
            last_accessed TEXT NOT NULL,
            last_accessed_at TEXT,
            access_count INTEGER NOT NULL DEFAULT 0,
            usefulness_score REAL NOT NULL DEFAULT 1.0,
            usefulness_alpha REAL NOT NULL DEFAULT 1.0,
            usefulness_beta  REAL NOT NULL DEFAULT 1.0,
            pinned       INTEGER NOT NULL DEFAULT 0,
            owner_agent_id TEXT NOT NULL DEFAULT 'unknown',
            embedding     BLOB,
            format        TEXT NOT NULL DEFAULT 'text',
            classification TEXT NOT NULL DEFAULT 'internal',
            content_type  TEXT NOT NULL DEFAULT 'text',
            legal_hold    INTEGER NOT NULL DEFAULT 0,
            legal_hold_reason TEXT,
            expired_at    TEXT,
            summary_author TEXT NOT NULL DEFAULT 'unknown',
            summary_model TEXT,
            summary_verified INTEGER NOT NULL DEFAULT 0,
            summary_verified_at TEXT
        );
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn init_queue_schema(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS access_marks (
            parent_id   TEXT PRIMARY KEY,
            accessed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sweep_checkpoints (
            name    TEXT PRIMARY KEY,
            last_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS embedding_queue (
            orb_id     TEXT PRIMARY KEY,
            summary    TEXT NOT NULL,
            attempts   INTEGER NOT NULL DEFAULT 0,
            queued_at  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fact_queue (
            orb_id    TEXT NOT NULL,
            fact_idx  INTEGER NOT NULL,
            fact_text TEXT NOT NULL,
            source    TEXT NOT NULL DEFAULT 'fact',
            attempts  INTEGER NOT NULL DEFAULT 0,
            queued_at TEXT NOT NULL,
            PRIMARY KEY (orb_id, source, fact_idx)
        );
        CREATE TABLE IF NOT EXISTS fact_embeddings (
            orb_id    TEXT NOT NULL,
            fact_idx  INTEGER NOT NULL,
            source    TEXT NOT NULL DEFAULT 'fact',
            embedding BLOB NOT NULL,
            PRIMARY KEY (orb_id, source, fact_idx)
        );
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn init_search_schema(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS embedding_vector_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_orbs USING fts5(
            orb_id UNINDEXED,
            source UNINDEXED,
            body
        );
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn init_reference_schema(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS chain_references (
            from_parent_id TEXT NOT NULL,
            to_parent_id   TEXT NOT NULL,
            reference_type TEXT NOT NULL,
            created_at     TEXT NOT NULL,
            PRIMARY KEY (from_parent_id, to_parent_id, reference_type)
        );
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn init_foresight_schema(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS foresights (
            parent_id       TEXT NOT NULL,
            foresight_idx   INTEGER NOT NULL,
            foresight_text  TEXT NOT NULL,
            relevant_from   TEXT NOT NULL,
            relevant_until  TEXT NOT NULL,
            duration_days   INTEGER NOT NULL,
            created_at      TEXT NOT NULL,
            PRIMARY KEY (parent_id, foresight_idx)
        );
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn init_indexes(conn: &Connection) -> Result<(), TunnelError> {
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_parent_id     ON orbs(parent_id);
        CREATE INDEX IF NOT EXISTS idx_tier          ON orbs(tier);
        CREATE INDEX IF NOT EXISTS idx_last_accessed ON orbs(last_accessed);
        CREATE INDEX IF NOT EXISTS idx_last_accessed_at ON orbs(last_accessed_at);
        CREATE INDEX IF NOT EXISTS idx_owner_agent_id ON orbs(owner_agent_id);
        CREATE INDEX IF NOT EXISTS idx_eq_attempts   ON embedding_queue(attempts);
        CREATE INDEX IF NOT EXISTS idx_fq_attempts   ON fact_queue(attempts);
        CREATE INDEX IF NOT EXISTS idx_cr_from       ON chain_references(from_parent_id);
        CREATE INDEX IF NOT EXISTS idx_cr_to         ON chain_references(to_parent_id);
        CREATE INDEX IF NOT EXISTS idx_foresight_window
            ON foresights(relevant_from, relevant_until);
    ",
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))
}

fn apply_schema_migrations(conn: &Connection) {
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN answer_facts TEXT");
    let _ = conn.execute_batch(
        "ALTER TABLE orbs ADD COLUMN classification TEXT NOT NULL DEFAULT 'internal'",
    );
    let _ =
        conn.execute_batch("ALTER TABLE orbs ADD COLUMN content_type TEXT NOT NULL DEFAULT 'text'");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN legal_hold INTEGER NOT NULL DEFAULT 0");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN legal_hold_reason TEXT");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN expired_at TEXT");
    let _ = conn.execute_batch(
        "ALTER TABLE orbs ADD COLUMN summary_author TEXT NOT NULL DEFAULT 'unknown'",
    );
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN summary_model TEXT");
    let _ = conn
        .execute_batch("ALTER TABLE orbs ADD COLUMN summary_verified INTEGER NOT NULL DEFAULT 0");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN summary_verified_at TEXT");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN last_accessed_at TEXT");
    let _ =
        conn.execute_batch("ALTER TABLE orbs ADD COLUMN access_count INTEGER NOT NULL DEFAULT 0");
    let _ = conn
        .execute_batch("ALTER TABLE orbs ADD COLUMN usefulness_score REAL NOT NULL DEFAULT 1.0");
    let _ = conn
        .execute_batch("ALTER TABLE orbs ADD COLUMN usefulness_alpha REAL NOT NULL DEFAULT 1.0");
    let _ =
        conn.execute_batch("ALTER TABLE orbs ADD COLUMN usefulness_beta REAL NOT NULL DEFAULT 1.0");
    let _ = conn.execute_batch("ALTER TABLE orbs ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0");
    let _ = conn.execute_batch(
        "ALTER TABLE orbs ADD COLUMN owner_agent_id TEXT NOT NULL DEFAULT 'unknown'",
    );
    let _ =
        conn.execute_batch("ALTER TABLE fact_queue ADD COLUMN source TEXT NOT NULL DEFAULT 'fact'");
    let _ = conn.execute_batch(
        "ALTER TABLE fact_embeddings ADD COLUMN source TEXT NOT NULL DEFAULT 'fact'",
    );
}

fn backfill_search_index(conn: &Connection) {
    let _ = conn.execute(
        "INSERT INTO fts_orbs (orb_id, source, body)
         SELECT q.orb_id, 'summary', q.summary
         FROM embedding_queue q
         JOIN orbs o ON o.orb_id = q.orb_id
         WHERE q.summary != ''
           AND o.expired_at IS NULL
           AND o.classification != 'secret'
           AND NOT EXISTS (
               SELECT 1 FROM fts_orbs f
               WHERE f.orb_id = q.orb_id AND f.source = 'summary'
           )",
        [],
    );

    let _ = conn.execute(
        "INSERT INTO fts_orbs (orb_id, source, body)
         SELECT q.orb_id, q.source, q.fact_text
         FROM fact_queue q
         JOIN orbs o ON o.orb_id = q.orb_id
         WHERE q.fact_text != ''
           AND o.expired_at IS NULL
           AND o.classification != 'secret'
           AND NOT EXISTS (
               SELECT 1 FROM fts_orbs f
               WHERE f.orb_id = q.orb_id
                 AND f.source = q.source
                 AND f.body = q.fact_text
           )",
        [],
    );
}

fn load_sqlite_vec_if_configured(conn: &Connection) -> bool {
    let Ok(path) = env::var("VENTURI_SQLITE_VEC_EXTENSION") else {
        return false;
    };
    unsafe {
        if conn.load_extension_enable().is_err() {
            return false;
        }
        let loaded = conn.load_extension(path, None).is_ok();
        let _ = conn.load_extension_disable();
        loaded
    }
}

fn hype_enabled() -> bool {
    !matches!(
        env::var("VENTURI_HYPE_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "off" | "no"
    )
}

fn backfill_sqlite_vec_index(conn: &Connection) {
    if let Ok(summaries) = load_summary_embeddings(conn) {
        for (orb_id, embedding) in summaries {
            if !ensure_sqlite_vec_table(conn, embedding.len()).unwrap_or(false) {
                return;
            }
            let _ = conn.execute(
                "INSERT OR REPLACE INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
                params![summary_vector_id(&orb_id), floats_to_bytes(&embedding)],
            );
        }
    }

    if let Ok(facts) = load_fact_embeddings(conn) {
        for fact in facts {
            if !ensure_sqlite_vec_table(conn, fact.embedding.len()).unwrap_or(false) {
                return;
            }
            let _ = conn.execute(
                "INSERT OR REPLACE INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
                params![
                    fact_vector_id(&fact.orb_id, &fact.source, fact.fact_idx),
                    floats_to_bytes(&fact.embedding)
                ],
            );
        }
    }
}

fn ensure_sqlite_vec_table(conn: &Connection, dim: usize) -> Result<bool, TunnelError> {
    if dim == 0 {
        return Ok(false);
    }
    if let Some(existing_dim) = sqlite_vec_dim(conn)? {
        return Ok(existing_dim == dim);
    }

    let sql = format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS embeddings_vec USING vec0(
            id TEXT PRIMARY KEY,
            embedding FLOAT[{}]
        );",
        dim
    );
    if conn.execute_batch(&sql).is_err() {
        return Ok(false);
    }
    conn.execute(
        "INSERT OR REPLACE INTO embedding_vector_meta (key, value) VALUES ('dim', ?1)",
        params![dim.to_string()],
    )
    .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
    Ok(true)
}

fn sqlite_vec_dim(conn: &Connection) -> Result<Option<usize>, TunnelError> {
    let result = conn.query_row(
        "SELECT value FROM embedding_vector_meta WHERE key = 'dim'",
        [],
        |row| row.get::<_, String>(0),
    );
    match result {
        Ok(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|e| TunnelError::DatabaseError(e.to_string())),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(TunnelError::DatabaseError(e.to_string())),
    }
}

fn load_embeddings(conn: &Connection) -> Result<Vec<(String, Vec<f32>)>, TunnelError> {
    let mut result = load_summary_embeddings(conn)?;
    result.extend(
        load_fact_embeddings(conn)?
            .into_iter()
            .map(|fact| (fact.orb_id, fact.embedding)),
    );
    Ok(result)
}

fn load_summary_embeddings(conn: &Connection) -> Result<Vec<(String, Vec<f32>)>, TunnelError> {
    let mut stmt = conn
        .prepare(
            "SELECT orb_id, embedding FROM orbs
         WHERE tier IN ('hot','warm')
           AND embedding IS NOT NULL
           AND expired_at IS NULL
           AND classification != 'secret'",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

    let rows = stmt
        .query_map([], |row| {
            let orb_id: String = row.get(0)?;
            let blob: Vec<u8> = row.get(1)?;
            Ok((orb_id, blob))
        })
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

    let mut result = Vec::new();
    for row in rows {
        let (orb_id, blob) = row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        result.push((orb_id, bytes_to_floats(&blob)));
    }

    Ok(result)
}

struct FactEmbeddingRow {
    orb_id: String,
    fact_idx: i64,
    source: String,
    embedding: Vec<f32>,
}

fn load_fact_embeddings(conn: &Connection) -> Result<Vec<FactEmbeddingRow>, TunnelError> {
    let mut fact_stmt = conn
        .prepare(
            "SELECT f.orb_id, f.fact_idx, f.source, f.embedding
             FROM fact_embeddings f
             JOIN orbs o ON o.orb_id = f.orb_id
             WHERE o.expired_at IS NULL AND o.classification != 'secret'",
        )
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

    let fact_rows = fact_stmt
        .query_map([], |row| {
            let orb_id: String = row.get(0)?;
            let fact_idx: i64 = row.get(1)?;
            let source: String = row.get(2)?;
            let blob: Vec<u8> = row.get(3)?;
            Ok((orb_id, fact_idx, source, blob))
        })
        .map_err(|e| TunnelError::DatabaseError(e.to_string()))?;

    let mut result = Vec::new();
    for row in fact_rows {
        let (orb_id, fact_idx, source, blob) =
            row.map_err(|e| TunnelError::DatabaseError(e.to_string()))?;
        result.push(FactEmbeddingRow {
            orb_id,
            fact_idx,
            source,
            embedding: bytes_to_floats(&blob),
        });
    }

    Ok(result)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        0.0
    } else {
        dot / (mag_a * mag_b)
    }
}

fn floats_to_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn bytes_to_floats(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

fn fts_match_query(query: &str) -> Option<String> {
    let terms: Vec<String> = query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| format!("\"{}\"", term))
        .collect();

    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" OR "))
    }
}

fn parse_hype_questions(text: &str) -> Vec<String> {
    let mut questions = Vec::new();
    for line in text.lines() {
        let question = line
            .trim()
            .trim_start_matches(|ch: char| {
                ch.is_ascii_digit() || matches!(ch, '.' | ')' | '-' | '*' | ' ')
            })
            .trim();
        if question.len() < 8 || !question.ends_with('?') {
            continue;
        }
        let normalized = question.to_string();
        if !questions.contains(&normalized) {
            questions.push(normalized);
        }
        if questions.len() >= 5 {
            break;
        }
    }
    questions
}

fn rrf_fuse(keyword_ranked: &[String], vector_ranked: &[String], top_k: usize) -> Vec<String> {
    const RRF_K: f32 = 60.0;
    let mut scores: HashMap<String, (f32, usize)> = HashMap::new();
    let mut next_order = 0usize;

    for ranked in [keyword_ranked, vector_ranked] {
        for (rank, orb_id) in ranked.iter().enumerate() {
            let entry = scores.entry(orb_id.clone()).or_insert_with(|| {
                let order = next_order;
                next_order += 1;
                (0.0, order)
            });
            entry.0 += 1.0 / (RRF_K + rank as f32 + 1.0);
        }
    }

    let mut fused: Vec<(String, f32, usize)> = scores
        .into_iter()
        .map(|(orb_id, (score, order))| (orb_id, score, order))
        .collect();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.2.cmp(&b.2))
    });
    fused
        .into_iter()
        .take(top_k)
        .map(|(orb_id, _, _)| orb_id)
        .collect()
}

fn summary_vector_id(orb_id: &str) -> String {
    format!("s:{}", orb_id)
}

fn fact_vector_id(orb_id: &str, source: &str, fact_idx: i64) -> String {
    format!("f:{}:{}:{}", source, orb_id, fact_idx)
}

fn orb_id_from_vector_id(vector_id: &str) -> Option<String> {
    if let Some(orb_id) = vector_id.strip_prefix("s:") {
        return Some(orb_id.to_string());
    }
    let fact_id = vector_id.strip_prefix("f:")?;
    let (_, rest) = fact_id.split_once(':')?;
    rest.rsplit_once(':').map(|(orb_id, _)| orb_id.to_string())
}

fn orb_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<OrbRow> {
    Ok(OrbRow {
        orb_id: row.get(0)?,
        key_id: row.get(1)?,
        parent_id: row.get(2)?,
        sequence: row.get(3)?,
        chain_length: row.get(4)?,
        tier: row.get(5)?,
        format: row.get(6)?,
    })
}

fn chain_reference_from_row(row: &rusqlite::Row) -> rusqlite::Result<ChainReference> {
    Ok(ChainReference {
        from_parent_id: row.get(0)?,
        to_parent_id: row.get(1)?,
        reference_type: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn foresight_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<ForesightRow> {
    Ok(ForesightRow {
        parent_id: row.get(0)?,
        foresight_text: row.get(1)?,
        relevant_from: row.get(2)?,
        relevant_until: row.get(3)?,
        duration_days: row.get::<_, i64>(4)? as u32,
        created_at: row.get(5)?,
    })
}

fn meta_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<MetaRow> {
    let facts_json: Option<String> = row.get(15)?;
    Ok(MetaRow {
        orb_id: row.get(0)?,
        parent_id: row.get(1)?,
        topic: row.get(2)?,
        domain: row.get(3)?,
        date: row.get(4)?,
        format: row.get(5)?,
        tier: row.get(6)?,
        sequence: row.get(7)?,
        chain_length: row.get(8)?,
        classification: row.get(9)?,
        content_type: row.get(10)?,
        summary_author: row.get(11)?,
        summary_model: row.get(12)?,
        summary_verified: row.get::<_, i64>(13)? != 0,
        summary_verified_at: row.get(14)?,
        verified_facts: verified_facts_from_json(facts_json.as_deref()),
    })
}

fn verified_facts_from_json(facts_json: Option<&str>) -> Vec<AnswerFact> {
    facts_json
        .and_then(|json| serde_json::from_str::<Vec<AnswerFact>>(json).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|fact| fact.verified)
        .collect()
}

fn validate_reference_type(reference_type: &str) -> Result<(), TunnelError> {
    match reference_type {
        "supersedes" | "supports" | "contradicts" | "cites" => Ok(()),
        _ => Err(TunnelError::GatekeeperRejected {
            reason: "reference_type must be one of supersedes, supports, contradicts, cites"
                .to_string(),
        }),
    }
}

fn now_iso() -> String {
    format!("{}Z", now_secs())
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Public types ──────────────────────────────────────────────────────────────

pub struct OrbEntry {
    pub orb_id: String,
    pub key_id: String,
    pub topic: String,
    pub domain: String,
    pub date: String,
    pub parent_id: String,
    pub sequence: u32,
    pub chain_length: u32,
    pub format: String,
    pub classification: String,
    pub content_type: String,
    pub pinned: bool,
    pub owner_agent_id: String,
    pub summary: String,
    /// Optional atomic verifiable facts derived from the content.
    /// Each string is one independently-verifiable statement.
    /// Example: ["patient was admitted", "admission date: 2026-05-01", "symptom: chest pain"]
    /// If empty, only the summary is embedded (existing behavior).
    pub answer_facts: Vec<AnswerFact>,
    pub summary_author: String,
    pub summary_model: Option<String>,
    pub summary_verified: bool,
    pub summary_verified_at: Option<String>,
    pub foresights: Vec<Foresight>,
}

pub struct OrbRow {
    pub orb_id: String,
    pub key_id: String,
    pub parent_id: String,
    pub sequence: u32,
    pub chain_length: u32,
    pub tier: String,
    pub format: String,
}

/// Catalog metadata returned without decrypting content.
/// key_id is intentionally excluded — metadata access must not expose key pointers.
#[derive(Clone)]
pub struct MetaRow {
    pub orb_id: String,
    pub parent_id: String,
    pub topic: String,
    pub domain: String,
    pub date: String,
    pub format: String,
    pub content_type: String,
    pub tier: String,
    pub sequence: u32,
    pub chain_length: u32,
    pub classification: String,
    pub summary_author: String,
    pub summary_model: Option<String>,
    pub summary_verified: bool,
    pub summary_verified_at: Option<String>,
    pub verified_facts: Vec<AnswerFact>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChainReference {
    pub from_parent_id: String,
    pub to_parent_id: String,
    pub reference_type: String,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ForesightRow {
    pub parent_id: String,
    pub foresight_text: String,
    pub relevant_from: String,
    pub relevant_until: String,
    pub duration_days: u32,
    pub created_at: String,
}

#[derive(Default)]
pub struct StructuredFilter {
    pub topic: Option<String>,
    pub domain: Option<String>,
    pub tier: Option<String>,
    pub parent_id: Option<String>,
    pub format: Option<String>,
    pub classification: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_orb_entry() -> OrbEntry {
        OrbEntry {
            orb_id: "orb-1".to_string(),
            key_id: "key-1".to_string(),
            topic: "topic".to_string(),
            domain: "domain".to_string(),
            date: "2026-01-01".to_string(),
            parent_id: "parent-1".to_string(),
            sequence: 1,
            chain_length: 1,
            format: "text".to_string(),
            classification: "internal".to_string(),
            content_type: "document".to_string(),
            pinned: false,
            owner_agent_id: "agent-1".to_string(),
            summary: "replay-safe summary".to_string(),
            answer_facts: Vec::new(),
            summary_author: "agent-1".to_string(),
            summary_model: None,
            summary_verified: false,
            summary_verified_at: None,
            foresights: Vec::new(),
        }
    }

    #[test]
    fn vector_ids_roundtrip_to_orb_ids() {
        assert_eq!(
            orb_id_from_vector_id(&summary_vector_id("orb-1")).as_deref(),
            Some("orb-1")
        );
        assert_eq!(
            orb_id_from_vector_id(&fact_vector_id("orb-2", "hype", 3)).as_deref(),
            Some("orb-2")
        );
        assert!(orb_id_from_vector_id("bad").is_none());
    }

    #[test]
    fn replayed_catalog_registration_preserves_orb_state_and_fts_rows() {
        let mut lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();
        lib.register_orb(test_orb_entry()).unwrap();
        lib.conn
            .execute(
                "UPDATE orbs SET access_count = 7, usefulness_score = 0.8 WHERE orb_id = 'orb-1'",
                [],
            )
            .unwrap();

        lib.register_orb(test_orb_entry()).unwrap();

        let (access_count, usefulness_score): (u32, f64) = lib
            .conn
            .query_row(
                "SELECT access_count, usefulness_score FROM orbs WHERE orb_id = 'orb-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let fts_rows: u32 = lib
            .conn
            .query_row(
                "SELECT COUNT(*) FROM fts_orbs WHERE orb_id = 'orb-1' AND source = 'summary'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(access_count, 7);
        assert_eq!(usefulness_score, 0.8);
        assert_eq!(fts_rows, 1);
    }

    #[test]
    fn parse_hype_questions_keeps_only_questions() {
        let parsed = parse_hype_questions(
            "1. What medication was documented?\n\
             - not a question\n\
             2) Which admission symptom was recorded?\n\
             What medication was documented?\n\
             Why was follow-up requested?",
        );

        assert_eq!(
            parsed,
            vec![
                "What medication was documented?".to_string(),
                "Which admission symptom was recorded?".to_string(),
                "Why was follow-up requested?".to_string()
            ]
        );
    }

    #[test]
    fn cosine_similarity_identical_vectors_is_one() {
        assert!((cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_orthogonal_vectors_is_zero() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), 0.0);
    }

    #[test]
    fn cosine_similarity_opposite_vectors_is_negative_one() {
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) - -1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_mismatched_lengths_is_zero() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0, 0.0]), 0.0);
    }

    #[test]
    fn cosine_similarity_empty_vectors_is_zero() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
    }

    #[test]
    fn cosine_similarity_zero_vector_is_zero() {
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
    }

    #[test]
    fn rrf_fuse_agreement_outranks_single_list_hit() {
        let keyword = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let vector = vec!["b".to_string(), "a".to_string(), "d".to_string()];

        let fused = rrf_fuse(&keyword, &vector, 4);

        assert_eq!(fused[0], "a");
        assert_eq!(fused[1], "b");
        assert!(fused.contains(&"c".to_string()));
        assert!(fused.contains(&"d".to_string()));
    }

    #[test]
    fn rrf_fuse_respects_top_k() {
        let keyword = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let vector: Vec<String> = Vec::new();

        let fused = rrf_fuse(&keyword, &vector, 2);

        assert_eq!(fused, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn rrf_fuse_empty_inputs_yields_empty_result() {
        let empty: Vec<String> = Vec::new();
        assert!(rrf_fuse(&empty, &empty, 10).is_empty());
    }

    fn insert_bare_orb(lib: &Librarian, orb_id: &str, parent_id: &str) {
        lib.conn
            .execute(
                "INSERT INTO orbs
                 (orb_id, key_id, topic, domain, date, parent_id, sequence, chain_length, last_accessed)
                 VALUES (?1, 'key', 't', 'd', '2026-01-01', ?2, 0, 1, '0Z')",
                params![orb_id, parent_id],
            )
            .unwrap();
    }

    #[test]
    fn apply_exit_feedback_updates_beta_bernoulli_posterior() {
        let lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();
        insert_bare_orb(&lib, "orb-1", "parent-1");

        let events = vec![
            ExitEvent {
                parent_id: "parent-1".to_string(),
                orb_ids: vec!["orb-1".to_string()],
                verdict: 1,
                recall: None,
            },
            ExitEvent {
                parent_id: "parent-1".to_string(),
                orb_ids: vec!["orb-1".to_string()],
                verdict: 1,
                recall: None,
            },
            ExitEvent {
                parent_id: "parent-1".to_string(),
                orb_ids: vec!["orb-1".to_string()],
                verdict: 0,
                recall: None,
            },
        ];

        let updated = lib.apply_exit_feedback(&events).unwrap();
        assert_eq!(updated, 3);

        let (alpha, beta, score): (f64, f64, f64) = lib
            .conn
            .query_row(
                "SELECT usefulness_alpha, usefulness_beta, usefulness_score
                 FROM orbs WHERE orb_id = 'orb-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(alpha, 3.0);
        assert_eq!(beta, 2.0);
        assert!((score - 0.6).abs() < 1e-9);
    }

    #[test]
    fn demote_cold_protects_orb_with_sufficient_evidence_and_high_score() {
        let lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();
        insert_bare_orb(&lib, "orb-1", "parent-1");
        lib.conn
            .execute(
                "UPDATE orbs SET usefulness_alpha = 4.0, usefulness_beta = 1.0
                 WHERE orb_id = 'orb-1'",
                [],
            )
            .unwrap();

        lib.lifecycle_sweep(&LifecycleConfig {
            enabled: true,
            t_warm_secs: 1,
            t_cold_secs: 2,
            max_hot_orbs: 500,
            sweep_interval: 60,
        })
        .unwrap();

        let tier: String = lib
            .conn
            .query_row("SELECT tier FROM orbs WHERE orb_id = 'orb-1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_ne!(tier, "cold");
    }

    #[test]
    fn demote_cold_ignores_high_score_with_insufficient_evidence() {
        let lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();
        insert_bare_orb(&lib, "orb-1", "parent-1");
        lib.conn
            .execute(
                "UPDATE orbs SET usefulness_alpha = 3.0, usefulness_beta = 0.0
                 WHERE orb_id = 'orb-1'",
                [],
            )
            .unwrap();

        lib.lifecycle_sweep(&LifecycleConfig {
            enabled: true,
            t_warm_secs: 1,
            t_cold_secs: 2,
            max_hot_orbs: 500,
            sweep_interval: 60,
        })
        .unwrap();

        let tier: String = lib
            .conn
            .query_row("SELECT tier FROM orbs WHERE orb_id = 'orb-1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(tier, "cold");
    }

    #[test]
    fn apply_exit_feedback_skips_orb_that_no_longer_exists() {
        let lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();

        let events = vec![ExitEvent {
            parent_id: "parent-1".to_string(),
            orb_ids: vec!["missing-orb".to_string()],
            verdict: 1,
            recall: None,
        }];

        let updated = lib.apply_exit_feedback(&events).unwrap();
        assert_eq!(updated, 0);
    }

    #[test]
    fn sweep_checkpoint_roundtrips_and_advances() {
        let lib = Librarian::open(":memory:", "http://127.0.0.1:9", None, None).unwrap();

        assert_eq!(lib.sweep_checkpoint("usefulness_feedback").unwrap(), None);

        lib.set_sweep_checkpoint("usefulness_feedback", "100")
            .unwrap();
        assert_eq!(
            lib.sweep_checkpoint("usefulness_feedback").unwrap(),
            Some("100".to_string())
        );

        lib.set_sweep_checkpoint("usefulness_feedback", "200")
            .unwrap();
        assert_eq!(
            lib.sweep_checkpoint("usefulness_feedback").unwrap(),
            Some("200".to_string())
        );
    }

    #[test]
    fn sqlite_vec_smoke_when_extension_configured() {
        if env::var("VENTURI_SQLITE_VEC_EXTENSION").is_err() {
            return;
        }

        let conn = Connection::open_in_memory().expect("open in-memory sqlite");
        init_search_schema(&conn).expect("init search schema");
        assert!(load_sqlite_vec_if_configured(&conn));
        assert!(ensure_sqlite_vec_table(&conn, 4).expect("create vec table"));

        conn.execute(
            "INSERT INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
            params![
                summary_vector_id("a"),
                floats_to_bytes(&[1.0, 0.0, 0.0, 0.0])
            ],
        )
        .expect("insert first vector");
        conn.execute(
            "INSERT INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
            params![
                summary_vector_id("b"),
                floats_to_bytes(&[0.0, 1.0, 0.0, 0.0])
            ],
        )
        .expect("insert second vector");

        let nearest: String = conn
            .query_row(
                "SELECT id
                 FROM embeddings_vec
                 ORDER BY vec_distance_cosine(embedding, ?1) ASC
                 LIMIT 1",
                params![floats_to_bytes(&[1.0, 0.0, 0.0, 0.0])],
                |row| row.get(0),
            )
            .expect("query nearest vector");

        assert_eq!(nearest, summary_vector_id("a"));
    }

    #[test]
    fn eject_chain_fact_vector_cleanup_glob_matches_source_prefixed_ids() {
        if env::var("VENTURI_SQLITE_VEC_EXTENSION").is_err() {
            return;
        }

        let conn = Connection::open_in_memory().expect("open in-memory sqlite");
        init_search_schema(&conn).expect("init search schema");
        assert!(load_sqlite_vec_if_configured(&conn));
        assert!(ensure_sqlite_vec_table(&conn, 4).expect("create vec table"));

        // fact_vector_id ids are shaped "f:<source>:<orb_id>:<fact_idx>" — the
        // source (fact/hype) comes before the orb_id, not after.
        let ejected_fact = fact_vector_id("orb-1", "fact", 0);
        let ejected_hype = fact_vector_id("orb-1", "hype", 3);
        let kept_fact = fact_vector_id("orb-2", "fact", 0);
        for id in [&ejected_fact, &ejected_hype, &kept_fact] {
            conn.execute(
                "INSERT INTO embeddings_vec (id, embedding) VALUES (?1, ?2)",
                params![id, floats_to_bytes(&[1.0, 0.0, 0.0, 0.0])],
            )
            .expect("insert fact vector");
        }

        conn.execute(
            "DELETE FROM embeddings_vec WHERE id = ?1 OR id GLOB ?2",
            params![summary_vector_id("orb-1"), format!("f:*:{}:*", "orb-1")],
        )
        .expect("delete orb-1 vectors");

        let remaining: Vec<String> = conn
            .prepare("SELECT id FROM embeddings_vec ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(remaining, vec![kept_fact]);
    }
}
