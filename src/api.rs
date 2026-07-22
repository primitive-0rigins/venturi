use crate::intelligence::gatekeeper::{
    Gatekeeper, GatekeeperOpenConfig, IngestionRequest, IngestionResult,
};
use crate::intelligence::librarian::{
    ChainReference, ForesightRow, LifecycleConfig, MetaRow, OrbRow, StructuredFilter,
};
use crate::intelligence::scribe::RetrievalProof;
use crate::pipeline::retrieval::RetrievalPipeline;
use crate::pipeline::sweep::{SweepReport, Sweeper};
use crate::types::error::{NotFoundReason, TunnelError};
use crate::types::orb::OrbId;

/// Hard resource limits enforced on every retrieval and ingestion call.
///
/// These are security requirements, not optional tuning. They prevent
/// runaway agents from exhausting memory or overwhelming the retrieval path.
pub struct StorageLimits {
    /// Maximum bytes per content chunk at ingest time (default: 64 KB).
    pub max_chunk_bytes: usize,
    /// Maximum number of orbs in a single ingestion chain (default: 1 000).
    pub max_chain_length: usize,
    /// Maximum number of orbs returned by any single retrieval query (default: 100).
    pub max_orbs_per_query: usize,
    /// Maximum total bytes rehydrated per retrieval call (default: 50 MB).
    pub max_rehydration_bytes: usize,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 65_536,
            max_chain_length: 1_000,
            max_orbs_per_query: 100,
            max_rehydration_bytes: 52_428_800,
        }
    }
}

/// The public entry point for Venturi.
///
/// Owns a Gatekeeper (which holds all subsystem DB connections) and a
/// RetrievalPipeline. The retrieval methods borrow subsystems through
/// Gatekeeper accessors — no duplicate DB connections are opened.
///
/// Ingestion path — caller calls ingest():
///   Venturi → Gatekeeper → Wormhole → Shelf + Keystore + Librarian + Graph
///
/// Retrieval modes:
///   1. context()              — embed query → similarity → rehydrated chunks for prompt injection
///   2. document()             — find chain → decrypt all orbs → reassemble as single byte stream
///   3. document_by_parent_id() — reassemble by known parent_id (no Ollama)
///   4. graph_query()          — BFS from query entities → parent_id chains → rehydrated chunks
///   5. temporal()             — date-range + subject filter → rehydrated chunks
///   6. structured()           — exact metadata filter → rehydrated chunks
///   7. metadata()             — catalog rows only, no decryption (cheapest mode)
///
/// All retrieval modes return (content, warnings). Warnings are non-empty only
/// when one or more orbs failed rehydration after 3 retry attempts. The
/// document assembles what it can; corrupted orb positions are replaced with
/// a corruption marker so callers can identify the gap.
///
/// Exit signal — caller calls record_verdict() after consuming retrieval output.
///   Fires the Scribe EXIT event. Drives the dataset flywheel.
pub struct Venturi {
    gatekeeper: Gatekeeper,
    pipeline: RetrievalPipeline,
    lifecycle: LifecycleConfig,
    limits: StorageLimits,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SystemCapabilities {
    pub embedding: CapabilityState,
    pub graph: CapabilityState,
    pub retrieval: CapabilityState,
    pub ingest: CapabilityState,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CapabilityState {
    Ready,
    Degraded,
}

impl CapabilityState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
        }
    }
}

/// Result of overlay consensus retrieval.
///
/// `core_chunks` appeared in every successful mode. `supplementary_chunks`
/// appeared in at least one successful mode but not all of them.
#[derive(Debug)]
pub struct ConsensusResult {
    pub core_chunks: Vec<Vec<u8>>,
    pub supplementary_chunks: Vec<Vec<u8>>,
    pub modes_run: Vec<String>,
    pub warnings: Vec<String>,
    pub retrieval_audit_id: String,
}

#[derive(Debug)]
pub struct RetrievalWithProof<T> {
    pub value: T,
    pub warnings: Vec<String>,
    pub retrieval_audit_id: String,
    pub token_budget_applied: bool,
    pub stability: Option<String>,
    pub cache_tier: Option<String>,
}

/// Config for opening Venturi. All paths are absolute filesystem paths.
pub struct VenturiConfig {
    /// Root directory for sealed orb files (e.g. /var/venturi/shelf/).
    pub shelf_root: String,
    /// SQLite path for the write-ahead journal (e.g. /var/venturi/journal.db).
    pub journal_db: String,
    /// SQLite path for the exit-gate keystore. Keep in a separate directory
    /// with chmod 600 (e.g. /var/venturi/keys/keystore.db).
    pub keystore_db: String,
    /// SQLite path for the Librarian catalog (e.g. /var/venturi/librarian.db).
    pub librarian_db: String,
    /// SQLite path for the Scribe event log (e.g. /var/venturi/scribe.db).
    pub scribe_db: String,
    /// SQLite path for the Knowledge Graph (e.g. /var/venturi/graph.db).
    pub graph_db: String,
    /// Ollama API base URL (e.g. http://localhost:11434).
    pub ollama_url: String,
    /// Embedding sidecar model name. Defaults to nomic-embed-text when unset.
    pub embedding_model: Option<String>,
    /// Expected embedding vector dimension. Defaults to 768 when unset.
    pub embedding_dim: Option<usize>,
    /// Hot/warm/cold lifecycle behavior. Defaults are used when unset.
    pub lifecycle: Option<LifecycleConfig>,
    /// Hard resource limits. Defaults are suitable for most deployments.
    pub limits: StorageLimits,
}

impl Venturi {
    /// Open all subsystems and recover any incomplete ingestions from a prior crash.
    pub fn open(cfg: VenturiConfig) -> Result<Self, TunnelError> {
        let mut gatekeeper = Gatekeeper::open(GatekeeperOpenConfig {
            shelf_root: &cfg.shelf_root,
            journal_db: &cfg.journal_db,
            keystore_db: &cfg.keystore_db,
            librarian_db: &cfg.librarian_db,
            scribe_db: &cfg.scribe_db,
            graph_db: &cfg.graph_db,
            ollama_url: &cfg.ollama_url,
            embedding_model: cfg.embedding_model.as_deref(),
            embedding_dim: cfg.embedding_dim,
        })?;

        gatekeeper.recover_incomplete()?;
        gatekeeper.reconcile_catalog()?;

        Ok(Self {
            gatekeeper,
            pipeline: RetrievalPipeline::new(),
            lifecycle: cfg.lifecycle.unwrap_or_default(),
            limits: cfg.limits,
        })
    }

    pub fn capabilities(&self) -> SystemCapabilities {
        SystemCapabilities {
            embedding: if self.gatekeeper.librarian().embedding_ready() {
                CapabilityState::Ready
            } else {
                CapabilityState::Degraded
            },
            graph: CapabilityState::Ready,
            retrieval: CapabilityState::Ready,
            ingest: CapabilityState::Ready,
        }
    }

    // ── Ingestion ─────────────────────────────────────────────────────────────

    /// Ingest content. See IngestionRequest for field documentation.
    ///
    /// Enforces hard limits:
    ///   - each chunk must be ≤ max_chunk_bytes
    ///   - chain length (number of chunks) must be ≤ max_chain_length
    pub fn ingest(&mut self, req: IngestionRequest) -> Result<IngestionResult, TunnelError> {
        if req.chunks.len() > self.limits.max_chain_length {
            return Err(TunnelError::GatekeeperRejected {
                reason: format!(
                    "chain length {} exceeds limit {}",
                    req.chunks.len(),
                    self.limits.max_chain_length
                ),
            });
        }
        for (i, chunk) in req.chunks.iter().enumerate() {
            if chunk.len() > self.limits.max_chunk_bytes {
                return Err(TunnelError::GatekeeperRejected {
                    reason: format!(
                        "chunk {} size {} exceeds limit {}",
                        i,
                        chunk.len(),
                        self.limits.max_chunk_bytes
                    ),
                });
            }
        }
        self.gatekeeper.ingest(req)
    }

    // ── Retrieval: Context mode ───────────────────────────────────────────────

    /// Context mode: embed query → cosine similarity → return up to top_k rehydrated
    /// content chunks ready for prompt injection.
    ///
    /// Results are capped at max_orbs_per_query and max_rehydration_bytes.
    /// Each element in the returned Vec is the raw content of one orb.
    /// agent_id is recorded in the Scribe audit event.
    pub fn context(
        &self,
        query: &str,
        top_k: usize,
        agent_id: Option<&str>,
    ) -> Result<(Vec<Vec<u8>>, Vec<String>), TunnelError> {
        let result = self.context_with_proof(query, top_k, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn context_with_proof(
        &self,
        query: &str,
        top_k: usize,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        self.context_with_budget_and_proof(query, top_k, None, agent_id)
    }

    pub fn context_with_budget_and_proof(
        &self,
        query: &str,
        top_k: usize,
        max_tokens: Option<u32>,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        self.context_with_options_and_proof(query, top_k, max_tokens, false, agent_id)
    }

    pub fn context_with_options_and_proof(
        &self,
        query: &str,
        top_k: usize,
        max_tokens: Option<u32>,
        check_stability: bool,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        let effective_k = top_k.min(self.limits.max_orbs_per_query);
        let orb_ids = self
            .gatekeeper
            .librarian()
            .similarity_search(query, effective_k)
            .map_err(|_| TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::EmbeddingUnavailable,
            })?;
        if orb_ids.is_empty() {
            return Err(TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::NoSimilarContent,
            });
        }
        let (stability, mut stability_warnings) =
            self.context_stability(query, effective_k, &orb_ids, check_stability);

        let (results, mut warnings, token_budget_applied) =
            self.rehydrate_orb_ids(&orb_ids, max_tokens);
        warnings.append(&mut stability_warnings);
        let _ = self
            .gatekeeper
            .scribe()
            .record_retrieval(query, "context", agent_id, &orb_ids);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "context",
            query,
            serde_json::json!({"top_k": effective_k}),
            &orb_ids,
            warnings.is_empty(),
        )?;
        Ok(RetrievalWithProof {
            value: results,
            warnings,
            retrieval_audit_id: audit_id,
            token_budget_applied,
            stability,
            cache_tier: self.cache_tier_for_orbs(&orb_ids),
        })
    }

    // ── Retrieval: Document mode ──────────────────────────────────────────────

    /// Document mode: find a chain containing an orb matching the query,
    /// decrypt all orbs in sequence, and reassemble into the original document.
    ///
    /// Orbs that fail after 3 retry attempts are replaced with a corruption
    /// marker at the gap position. Warnings list names every failed orb.
    /// Rehydration never aborts due to a single orb failure.
    pub fn document(
        &self,
        query: &str,
        agent_id: Option<&str>,
    ) -> Result<(Vec<u8>, Vec<String>), TunnelError> {
        let result = self.document_with_proof(query, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn document_with_proof(
        &self,
        query: &str,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, TunnelError> {
        self.document_with_budget_and_proof(query, None, agent_id)
    }

    pub fn document_with_budget_and_proof(
        &self,
        query: &str,
        max_tokens: Option<u32>,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, TunnelError> {
        let anchor_ids = self
            .gatekeeper
            .librarian()
            .similarity_search(query, 1)
            .map_err(|_| TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::EmbeddingUnavailable,
            })?;
        if anchor_ids.is_empty() {
            return Err(TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::NoSimilarContent,
            });
        }

        let anchor_row = self
            .gatekeeper
            .librarian()
            .fetch_by_orb_id(&anchor_ids[0])?
            .ok_or_else(|| TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::NoSimilarContent,
            })?;

        let parent_id = anchor_row.parent_id.clone();
        self.assemble_chain_by_parent_id(&parent_id, query, "document", max_tokens, agent_id)
    }

    // ── Retrieval: Document by parent_id ─────────────────────────────────────

    /// Document mode (direct): fetch a chain by known parent_id and reassemble.
    ///
    /// No Ollama dependency — bypasses similarity search entirely.
    /// Partial corruption handling is identical to document().
    pub fn document_by_parent_id(
        &self,
        parent_id: &str,
        agent_id: Option<&str>,
    ) -> Result<(Vec<u8>, Vec<String>), TunnelError> {
        let result = self.document_by_parent_id_with_proof(parent_id, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn document_by_parent_id_with_proof(
        &self,
        parent_id: &str,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, TunnelError> {
        self.assemble_chain_by_parent_id(parent_id, parent_id, "document", None, agent_id)
    }

    // ── Retrieval: Graph mode ─────────────────────────────────────────────────

    /// Graph mode: BFS traversal from query-matched concept nodes → collect all
    /// reachable parent_id chains → return rehydrated content from those chains.
    pub fn graph_query(
        &self,
        query: &str,
        max_hops: u32,
        agent_id: Option<&str>,
    ) -> Result<(Vec<Vec<u8>>, Vec<String>), TunnelError> {
        let result = self.graph_query_with_proof(query, max_hops, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn graph_query_with_proof(
        &self,
        query: &str,
        max_hops: u32,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        let chain_ids = self.gatekeeper.graph().traverse(query, max_hops)?;
        if chain_ids.is_empty() {
            return Err(TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::GraphNoMatch,
            });
        }

        let (all_orb_ids, mut warnings) = self.orb_ids_for_chains(&chain_ids)?;
        let (results, mut rehydrate_warnings, token_budget_applied) =
            self.rehydrate_orb_ids(&all_orb_ids, None);
        warnings.append(&mut rehydrate_warnings);

        let _ = self
            .gatekeeper
            .scribe()
            .record_retrieval(query, "graph", agent_id, &all_orb_ids);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "graph",
            query,
            serde_json::json!({"max_hops": max_hops}),
            &all_orb_ids,
            warnings.is_empty(),
        )?;
        Ok(RetrievalWithProof {
            value: results,
            warnings,
            retrieval_audit_id: audit_id,
            token_budget_applied,
            stability: None,
            cache_tier: self.cache_tier_for_orbs(&all_orb_ids),
        })
    }

    // ── Retrieval: Consensus mode ─────────────────────────────────────────────

    /// Consensus mode: run multiple retrieval overlays and split results into
    /// high-confidence core vs supplementary hits.
    ///
    /// Supported modes: "context", "graph". Unknown modes are ignored with a
    /// warning. When only one mode succeeds, all hits are supplementary because
    /// no cross-mode agreement can be established.
    pub fn consensus(
        &self,
        query: &str,
        modes: &[String],
        top_k: usize,
        max_hops: u32,
        agent_id: Option<&str>,
    ) -> Result<ConsensusResult, TunnelError> {
        let mut warnings = Vec::new();
        let mode_hits = self.collect_consensus_hits(query, modes, top_k, max_hops, &mut warnings);

        if mode_hits.is_empty() {
            return Err(TunnelError::MemoryNotFound {
                query: query.to_string(),
                reason: NotFoundReason::NoSimilarContent,
            });
        }

        let modes_run: Vec<String> = mode_hits.iter().map(|(mode, _)| mode.clone()).collect();
        let (core_ids, supplementary_ids) = Self::split_consensus_ids(&mode_hits);

        let (core_chunks, mut core_warnings, _) = self.rehydrate_orb_ids(&core_ids, None);
        let (supplementary_chunks, mut supplementary_warnings, _) =
            self.rehydrate_orb_ids(&supplementary_ids, None);
        warnings.append(&mut core_warnings);
        warnings.append(&mut supplementary_warnings);

        let mut all_ids = core_ids;
        all_ids.extend(supplementary_ids);
        let _ = self
            .gatekeeper
            .scribe()
            .record_retrieval(query, "consensus", agent_id, &all_ids);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "consensus",
            query,
            serde_json::json!({"modes_run": modes_run, "top_k": top_k, "max_hops": max_hops}),
            &all_ids,
            warnings.is_empty(),
        )?;

        Ok(ConsensusResult {
            core_chunks,
            supplementary_chunks,
            modes_run,
            warnings,
            retrieval_audit_id: audit_id,
        })
    }

    fn collect_consensus_hits(
        &self,
        query: &str,
        modes: &[String],
        top_k: usize,
        max_hops: u32,
        warnings: &mut Vec<String>,
    ) -> Vec<(String, Vec<String>)> {
        let mut mode_hits = Vec::new();

        for mode in Self::requested_consensus_modes(modes) {
            match self.consensus_mode_hits(&mode, query, top_k, max_hops) {
                Ok(ids) if !ids.is_empty() => mode_hits.push((mode, ids)),
                Ok(_) => warnings.push(format!("{} returned no hits", mode)),
                Err(warning) => warnings.push(warning),
            }
        }

        mode_hits
    }

    fn requested_consensus_modes(modes: &[String]) -> Vec<String> {
        if modes.is_empty() {
            vec!["context".to_string(), "graph".to_string()]
        } else {
            modes.iter().map(|m| m.to_lowercase()).collect()
        }
    }

    fn consensus_mode_hits(
        &self,
        mode: &str,
        query: &str,
        top_k: usize,
        max_hops: u32,
    ) -> Result<Vec<String>, String> {
        match mode {
            "context" => self
                .context_candidates(query, top_k)
                .map_err(|e| format!("context failed: {}", e)),
            "graph" => self
                .graph_candidates(query, max_hops)
                .map_err(|e| format!("graph failed: {}", e)),
            other => Err(format!("unsupported consensus mode: {}", other)),
        }
    }

    fn split_consensus_ids(mode_hits: &[(String, Vec<String>)]) -> (Vec<String>, Vec<String>) {
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut order = Vec::new();

        for (_, ids) in mode_hits {
            Self::count_mode_hits(ids, &mut counts, &mut order);
        }

        let mut core_ids = Vec::new();
        let mut supplementary_ids = Vec::new();
        let successful_mode_count = mode_hits.len();

        for id in order {
            if counts.get(&id).copied().unwrap_or_default() == successful_mode_count
                && successful_mode_count > 1
            {
                core_ids.push(id);
            } else {
                supplementary_ids.push(id);
            }
        }

        (core_ids, supplementary_ids)
    }

    fn count_mode_hits(
        ids: &[String],
        counts: &mut std::collections::HashMap<String, usize>,
        order: &mut Vec<String>,
    ) {
        let mut seen_in_mode = std::collections::HashSet::new();
        for id in ids {
            if seen_in_mode.insert(id.clone()) {
                if !counts.contains_key(id) {
                    order.push(id.clone());
                }
                *counts.entry(id.clone()).or_insert(0) += 1;
            }
        }
    }

    // ── Retrieval: Temporal mode ──────────────────────────────────────────────

    /// Temporal mode: all content touching `subject` within a date range.
    ///
    /// from/to: ISO date strings e.g. "2026-01-01", "2026-05-29".
    pub fn temporal(
        &self,
        subject: &str,
        from: &str,
        to: &str,
        agent_id: Option<&str>,
    ) -> Result<(Vec<Vec<u8>>, Vec<String>), TunnelError> {
        let result = self.temporal_with_proof(subject, from, to, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn temporal_with_proof(
        &self,
        subject: &str,
        from: &str,
        to: &str,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        self.temporal_with_budget_and_proof(subject, from, to, None, agent_id)
    }

    pub fn temporal_with_budget_and_proof(
        &self,
        subject: &str,
        from: &str,
        to: &str,
        max_tokens: Option<u32>,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        let rows = self
            .gatekeeper
            .librarian()
            .fetch_temporal(subject, from, to, None)?;
        let rows = &rows[..rows.len().min(self.limits.max_orbs_per_query)];
        let orb_ids: Vec<String> = rows.iter().map(|r| r.orb_id.clone()).collect();
        let (results, warnings, token_budget_applied) = self.rehydrate_rows(rows, max_tokens);

        let _ = self
            .gatekeeper
            .scribe()
            .record_retrieval(subject, "temporal", agent_id, &orb_ids);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "temporal",
            subject,
            serde_json::json!({"from": from, "to": to}),
            &orb_ids,
            warnings.is_empty(),
        )?;
        Ok(RetrievalWithProof {
            value: results,
            warnings,
            retrieval_audit_id: audit_id,
            token_budget_applied,
            stability: None,
            cache_tier: self.cache_tier_for_orbs(&orb_ids),
        })
    }

    // ── Retrieval: Structured mode ────────────────────────────────────────────

    /// Structured mode: exact metadata filter SQL — no semantic search.
    ///
    /// All filter fields are optional. An empty StructuredFilter returns up to
    /// max_orbs_per_query orbs (ordered by most recently accessed).
    pub fn structured(
        &self,
        filter: StructuredFilter,
        agent_id: Option<&str>,
    ) -> Result<(Vec<Vec<u8>>, Vec<String>), TunnelError> {
        let result = self.structured_with_proof(filter, agent_id)?;
        Ok((result.value, result.warnings))
    }

    pub fn structured_with_proof(
        &self,
        filter: StructuredFilter,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        self.structured_with_budget_and_proof(filter, None, agent_id)
    }

    pub fn structured_with_budget_and_proof(
        &self,
        filter: StructuredFilter,
        max_tokens: Option<u32>,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, TunnelError> {
        let filters_applied = filter_json(&filter);
        let rows = self.gatekeeper.librarian().fetch_structured(filter)?;
        let rows = &rows[..rows.len().min(self.limits.max_orbs_per_query)];
        let orb_ids: Vec<String> = rows.iter().map(|r| r.orb_id.clone()).collect();
        let (results, warnings, token_budget_applied) = self.rehydrate_rows(rows, max_tokens);

        let _ = self.gatekeeper.scribe().record_retrieval(
            "structured",
            "structured",
            agent_id,
            &orb_ids,
        );
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "structured",
            "structured",
            filters_applied,
            &orb_ids,
            warnings.is_empty(),
        )?;
        Ok(RetrievalWithProof {
            value: results,
            warnings,
            retrieval_audit_id: audit_id,
            token_budget_applied,
            stability: None,
            cache_tier: self.cache_tier_for_orbs(&orb_ids),
        })
    }

    // ── Retrieval: Metadata mode (no decrypt) ────────────────────────────────

    /// Metadata mode: return catalog rows only — no sealed orb is touched,
    /// no key is fetched, no decryption occurs.
    ///
    /// This is the cheapest retrieval mode (< 50 ms target). Use it when
    /// the caller only needs to know what exists, not what it contains.
    pub fn metadata(
        &self,
        filter: StructuredFilter,
        agent_id: Option<&str>,
    ) -> Result<Vec<MetaRow>, TunnelError> {
        Ok(self.metadata_with_proof(filter, agent_id)?.value)
    }

    pub fn metadata_with_proof(
        &self,
        filter: StructuredFilter,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<MetaRow>>, TunnelError> {
        let filters_applied = filter_json(&filter);
        let rows = self.gatekeeper.librarian().fetch_metadata(filter)?;
        let rows = &rows[..rows.len().min(self.limits.max_orbs_per_query)];

        // Audit that metadata was accessed — no orb_ids since nothing was decrypted
        let orb_ids: Vec<String> = rows.iter().map(|r| r.orb_id.clone()).collect();
        let _ = self
            .gatekeeper
            .scribe()
            .record_retrieval("metadata", "metadata", agent_id, &orb_ids);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            "metadata",
            "metadata",
            filters_applied,
            &orb_ids,
            true,
        )?;

        Ok(RetrievalWithProof {
            value: rows.to_vec(),
            warnings: Vec::new(),
            retrieval_audit_id: audit_id,
            token_budget_applied: false,
            stability: None,
            cache_tier: Self::aggregate_cache_tier(rows.iter().map(|r| r.tier.as_str())),
        })
    }

    // ── Exit signal ───────────────────────────────────────────────────────────

    /// Record the agent or user verdict on a retrieval result.
    /// verdict: 1 = this is what I wanted, 0 = not useful.
    ///
    /// Fires the Scribe EXIT event.
    pub fn record_verdict(
        &self,
        parent_id: &str,
        orb_ids: &[String],
        expected_orb_ids: &[String],
        verdict: u8,
    ) -> Result<(), TunnelError> {
        self.gatekeeper
            .scribe()
            .record_exit(parent_id, orb_ids, expected_orb_ids, verdict)
    }

    pub fn retrieval_proof(
        &self,
        retrieval_audit_id: &str,
    ) -> Result<Option<RetrievalProof>, TunnelError> {
        self.gatekeeper.scribe().retrieval_proof(retrieval_audit_id)
    }

    pub fn set_legal_hold(&self, parent_id: &str, reason: &str) -> Result<(), TunnelError> {
        self.gatekeeper
            .librarian()
            .set_legal_hold(parent_id, reason)
    }

    pub fn release_legal_hold(&self, parent_id: &str) -> Result<(), TunnelError> {
        self.gatekeeper.librarian().release_legal_hold(parent_id)
    }

    pub fn link_chains(
        &self,
        from_parent_id: &str,
        to_parent_id: &str,
        reference_type: &str,
    ) -> Result<(), TunnelError> {
        self.gatekeeper
            .librarian()
            .link_chains(from_parent_id, to_parent_id, reference_type)
    }

    pub fn chain_references(&self, parent_id: &str) -> Result<Vec<ChainReference>, TunnelError> {
        self.gatekeeper.librarian().chain_references(parent_id)
    }

    pub fn foresights(&self, on: &str) -> Result<Vec<ForesightRow>, TunnelError> {
        self.gatekeeper.librarian().active_foresights(on)
    }

    // ── Embedding queue ───────────────────────────────────────────────────────

    /// Process up to 10 pending embedding jobs from the durable queue.
    ///
    /// Called by the background embedding worker in main.rs every 30 seconds.
    /// Returns the number of orbs successfully embedded this batch.
    pub fn process_embedding_queue(&mut self) -> Result<u32, TunnelError> {
        self.gatekeeper.librarian_mut().process_embedding_batch()
    }

    // ── Background sweep methods ──────────────────────────────────────────────

    /// Run the sibling refresh sweep. Call every ~5 minutes.
    pub fn sweep_access_marks(&self) -> Result<SweepReport, TunnelError> {
        Sweeper::new(
            self.gatekeeper.librarian(),
            self.gatekeeper.keystore(),
            self.gatekeeper.shelf(),
            self.gatekeeper.graph(),
            self.gatekeeper.scribe(),
        )
        .sweep_access_marks()
    }

    /// Run the tier update sweep. Call every ~15 minutes.
    pub fn sweep_tiers(&self) -> Result<SweepReport, TunnelError> {
        Sweeper::new(
            self.gatekeeper.librarian(),
            self.gatekeeper.keystore(),
            self.gatekeeper.shelf(),
            self.gatekeeper.graph(),
            self.gatekeeper.scribe(),
        )
        .sweep_tiers()
    }

    /// Run the 90-day expiry sweep. Call once daily.
    pub fn sweep_expiry(&self) -> Result<SweepReport, TunnelError> {
        Sweeper::new(
            self.gatekeeper.librarian(),
            self.gatekeeper.keystore(),
            self.gatekeeper.shelf(),
            self.gatekeeper.graph(),
            self.gatekeeper.scribe(),
        )
        .sweep_expiry()
    }

    /// Run the hot/warm/cold lifecycle manager sweep.
    pub fn lifecycle_sweep(&self) -> Result<SweepReport, TunnelError> {
        Sweeper::new(
            self.gatekeeper.librarian(),
            self.gatekeeper.keystore(),
            self.gatekeeper.shelf(),
            self.gatekeeper.graph(),
            self.gatekeeper.scribe(),
        )
        .sweep_lifecycle(&self.lifecycle)
    }

    /// Run the spectral community detection sweep. Call every ~30 minutes.
    pub fn sweep_communities(&self) -> Result<SweepReport, TunnelError> {
        Sweeper::new(
            self.gatekeeper.librarian(),
            self.gatekeeper.keystore(),
            self.gatekeeper.shelf(),
            self.gatekeeper.graph(),
            self.gatekeeper.scribe(),
        )
        .sweep_communities()
    }

    pub fn record_daemon_health(
        &self,
        daemon: &str,
        status: &str,
        consecutive_failures: u8,
        details: Option<&str>,
    ) -> Result<(), TunnelError> {
        self.gatekeeper
            .scribe()
            .record_daemon_health(daemon, status, consecutive_failures, details)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Attempt to load and rehydrate an orb up to 3 times.
    ///
    /// Returns Ok(content) on any success, or Err(warning_string) after 3 failures.
    fn load_and_rehydrate_with_retry(&self, orb_id_hex: &str) -> Result<Vec<u8>, String> {
        let mut last_err = String::new();
        for _attempt in 0..3 {
            match self.load_and_rehydrate(orb_id_hex) {
                Ok(content) => return Ok(content),
                Err(e) => last_err = e.to_string(),
            }
        }
        Err(format!(
            "orb {} failed after 3 attempts: {}",
            orb_id_hex, last_err
        ))
    }

    fn load_and_rehydrate(&self, orb_id_hex: &str) -> Result<Vec<u8>, TunnelError> {
        let orb_id = OrbId::from_hex(orb_id_hex).ok_or_else(|| TunnelError::OrbNotFound {
            id: orb_id_hex.to_string(),
        })?;

        let row = self
            .gatekeeper
            .librarian()
            .fetch_by_orb_id(orb_id_hex)?
            .ok_or_else(|| TunnelError::OrbNotFound {
                id: orb_id_hex.to_string(),
            })?;
        let orb = self.gatekeeper.shelf().load(&orb_id, row.parent_id)?;
        let chain_key = self.gatekeeper.keystore().retrieve(&row.key_id)?;
        self.pipeline.rehydrate(orb, &chain_key)
    }

    fn context_candidates(&self, query: &str, top_k: usize) -> Result<Vec<String>, TunnelError> {
        let effective_k = top_k.min(self.limits.max_orbs_per_query);
        self.gatekeeper
            .librarian()
            .similarity_search(query, effective_k)
    }

    fn context_stability(
        &self,
        query: &str,
        top_k: usize,
        first_ids: &[String],
        check_stability: bool,
    ) -> (Option<String>, Vec<String>) {
        if !check_stability {
            return (None, Vec::new());
        }
        let Ok(second_ids) = self.context_candidates(query, top_k) else {
            return (
                Some("unstable".to_string()),
                vec!["stability: unstable; replay failed".to_string()],
            );
        };
        let score = jaccard_similarity(first_ids, &second_ids);
        if score < 0.8 {
            return (
                Some("unstable".to_string()),
                vec![format!("stability: unstable; jaccard={:.2}", score)],
            );
        }
        (Some("stable".to_string()), Vec::new())
    }

    fn graph_candidates(&self, query: &str, max_hops: u32) -> Result<Vec<String>, TunnelError> {
        let chain_ids = self.gatekeeper.graph().traverse(query, max_hops)?;
        Ok(self.orb_ids_for_chains(&chain_ids)?.0)
    }

    fn orb_ids_for_chains(
        &self,
        chain_ids: &[String],
    ) -> Result<(Vec<String>, Vec<String>), TunnelError> {
        let mut orb_ids = Vec::new();
        let mut warnings = Vec::new();

        'outer: for parent_id in chain_ids {
            let rows = self.gatekeeper.librarian().fetch_chain(parent_id)?;
            for row in rows {
                if orb_ids.len() >= self.limits.max_orbs_per_query {
                    warnings.push(format!(
                        "retrieval stopped at max_orbs_per_query ({})",
                        self.limits.max_orbs_per_query
                    ));
                    break 'outer;
                }
                orb_ids.push(row.orb_id);
            }
        }

        Ok((orb_ids, warnings))
    }

    fn approx_tokens(content: &[u8]) -> u32 {
        let words = String::from_utf8_lossy(content).split_whitespace().count() as u32;
        (words * 13).div_ceil(10)
    }

    fn rehydrate_orb_ids(
        &self,
        orb_ids: &[String],
        max_tokens: Option<u32>,
    ) -> (Vec<Vec<u8>>, Vec<String>, bool) {
        let mut chunks = Vec::new();
        let mut warnings = Vec::new();
        let mut total_bytes = 0usize;
        let mut token_budget = TokenBudget::new(max_tokens);
        let mut marked: std::collections::HashSet<String> = std::collections::HashSet::new();

        for orb_id in orb_ids {
            if let Ok(Some(row)) = self.gatekeeper.librarian().fetch_by_orb_id(orb_id) {
                if marked.insert(row.parent_id.clone()) {
                    let _ = self.gatekeeper.librarian().mark_accessed(&row.parent_id);
                }
            }

            match self.load_and_rehydrate_with_retry(orb_id) {
                Ok(content) => {
                    if token_budget.exhausted_by(&content, &mut warnings) {
                        break;
                    }
                    total_bytes += content.len();
                    if total_bytes > self.limits.max_rehydration_bytes {
                        warnings.push(format!(
                            "retrieval stopped at max_rehydration_bytes ({})",
                            self.limits.max_rehydration_bytes
                        ));
                        break;
                    }
                    chunks.push(content);
                }
                Err(w) => warnings.push(w),
            }
        }

        (chunks, warnings, token_budget.applied())
    }

    fn rehydrate_rows(
        &self,
        rows: &[OrbRow],
        max_tokens: Option<u32>,
    ) -> (Vec<Vec<u8>>, Vec<String>, bool) {
        let mut results = Vec::new();
        let mut warnings = Vec::new();
        let mut total_bytes = 0usize;
        let mut token_budget = TokenBudget::new(max_tokens);
        let mut marked: std::collections::HashSet<String> = std::collections::HashSet::new();

        for row in rows {
            if marked.insert(row.parent_id.clone()) {
                let _ = self.gatekeeper.librarian().mark_accessed(&row.parent_id);
            }

            match self.load_and_rehydrate_with_retry(&row.orb_id) {
                Ok(content) => {
                    if token_budget.exhausted_by(&content, &mut warnings) {
                        break;
                    }
                    total_bytes += content.len();
                    if total_bytes > self.limits.max_rehydration_bytes {
                        warnings.push(format!(
                            "retrieval stopped at max_rehydration_bytes ({})",
                            self.limits.max_rehydration_bytes
                        ));
                        break;
                    }
                    results.push(content);
                }
                Err(w) => warnings.push(w),
            }
        }

        (results, warnings, token_budget.applied())
    }

    fn cache_tier_for_orbs(&self, orb_ids: &[String]) -> Option<String> {
        self.gatekeeper
            .librarian()
            .tiers_for_orbs(orb_ids)
            .ok()
            .and_then(|tiers| Self::aggregate_cache_tier(tiers.iter().map(String::as_str)))
    }

    fn aggregate_cache_tier<'a>(tiers: impl Iterator<Item = &'a str>) -> Option<String> {
        let mut first = None;
        for tier in tiers {
            match first {
                None => first = Some(tier.to_string()),
                Some(ref seen) if seen == tier => {}
                Some(_) => return Some("mixed".to_string()),
            }
        }
        first
    }

    /// Assemble a full chain document from a parent_id.
    ///
    /// Orbs that fail after 3 retries are replaced with a corruption marker
    /// at their gap position. Returns (assembled_bytes, warnings).
    fn assemble_chain_by_parent_id(
        &self,
        parent_id: &str,
        audit_query: &str,
        mode: &str,
        max_tokens: Option<u32>,
        agent_id: Option<&str>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, TunnelError> {
        let rows = self.gatekeeper.librarian().fetch_chain(parent_id)?;
        if rows.is_empty() {
            return Err(TunnelError::OrbNotFound {
                id: parent_id.to_string(),
            });
        }

        let rows = &rows[..rows.len().min(self.limits.max_orbs_per_query)];
        let all_orb_ids: Vec<String> = rows.iter().map(|r| r.orb_id.clone()).collect();
        let _ = self.gatekeeper.librarian().mark_accessed(parent_id);

        let (document, warnings, token_budget_applied) = self.assemble_rows(rows, max_tokens);

        let _ =
            self.gatekeeper
                .scribe()
                .record_retrieval(audit_query, mode, agent_id, &all_orb_ids);
        self.audit_retrieval_failures(audit_query, mode, agent_id, &all_orb_ids, &warnings);
        let audit_id = self.record_retrieval_proof(
            agent_id,
            mode,
            audit_query,
            serde_json::json!({"parent_id": parent_id}),
            &all_orb_ids,
            warnings.is_empty(),
        )?;
        Ok(RetrievalWithProof {
            value: document,
            warnings,
            retrieval_audit_id: audit_id,
            token_budget_applied,
            stability: None,
            cache_tier: Self::aggregate_cache_tier(rows.iter().map(|r| r.tier.as_str())),
        })
    }

    fn assemble_rows(
        &self,
        rows: &[OrbRow],
        max_tokens: Option<u32>,
    ) -> (Vec<u8>, Vec<String>, bool) {
        let mut document = Vec::new();
        let mut warnings = Vec::new();
        let mut total_bytes = 0usize;
        let mut token_budget = TokenBudget::new(max_tokens);

        for row in rows {
            if self.document_limit_reached(total_bytes, &mut warnings) {
                break;
            }
            match self.load_and_rehydrate_with_retry(&row.orb_id) {
                Ok(content) => {
                    if token_budget.exhausted_by(&content, &mut warnings) {
                        break;
                    }
                    total_bytes += content.len();
                    document.extend_from_slice(&content);
                }
                Err(w) => {
                    document.extend_from_slice(&Self::make_corruption_marker(&row.orb_id));
                    warnings.push(w);
                }
            }
        }

        (document, warnings, token_budget.applied())
    }

    fn document_limit_reached(&self, total_bytes: usize, warnings: &mut Vec<String>) -> bool {
        if total_bytes <= self.limits.max_rehydration_bytes {
            return false;
        }
        warnings.push(format!(
            "document assembly stopped at max_rehydration_bytes ({})",
            self.limits.max_rehydration_bytes
        ));
        true
    }

    fn audit_retrieval_failures(
        &self,
        audit_query: &str,
        mode: &str,
        agent_id: Option<&str>,
        all_orb_ids: &[String],
        warnings: &[String],
    ) {
        if warnings.is_empty() {
            return;
        }
        let failure_categories = Self::warning_categories(warnings);
        let _ = self.gatekeeper.scribe().record_retrieval_failure(
            audit_query,
            mode,
            agent_id,
            all_orb_ids,
            warnings,
            &failure_categories,
        );
    }

    /// Produce a clearly-labelled corruption marker for a failed orb position.
    fn make_corruption_marker(orb_id_hex: &str) -> Vec<u8> {
        format!("[VENTURI:CORRUPTED_ORB:{}:END]\n", orb_id_hex).into_bytes()
    }

    fn warning_categories(warnings: &[String]) -> Vec<String> {
        let mut categories: Vec<String> = warnings
            .iter()
            .map(|warning| {
                if warning.contains("orb not found") {
                    "chain_incomplete"
                } else if warning.contains("chain assembly failed") || warning.contains("wrong key")
                {
                    "wrong_key"
                } else if warning.contains("corrupted") || warning.contains("integrity") {
                    "orb_corrupt"
                } else {
                    "retrieval_failed"
                }
                .to_string()
            })
            .collect();
        categories.sort();
        categories.dedup();
        categories
    }

    fn record_retrieval_proof(
        &self,
        agent_id: Option<&str>,
        mode: &str,
        query: &str,
        filters_applied: serde_json::Value,
        selected_orb_ids: &[String],
        chain_complete: bool,
    ) -> Result<String, TunnelError> {
        let selected_parent_ids = self.parent_ids_for_orbs(selected_orb_ids)?;
        let embedding_model_version = Some(self.gatekeeper.librarian().embedding_model_version());
        let mut proof = RetrievalProof::new(
            agent_id,
            mode,
            query,
            filters_applied,
            selected_orb_ids.to_vec(),
            selected_parent_ids,
            chain_complete,
        );
        proof.embedding_model_version = embedding_model_version;
        self.gatekeeper.scribe().record_retrieval_proof(proof)
    }

    fn parent_ids_for_orbs(&self, orb_ids: &[String]) -> Result<Vec<String>, TunnelError> {
        let mut parent_ids = Vec::new();
        for orb_id in orb_ids {
            if let Some(row) = self.gatekeeper.librarian().fetch_by_orb_id(orb_id)? {
                if !parent_ids.contains(&row.parent_id) {
                    parent_ids.push(row.parent_id);
                }
            }
        }
        Ok(parent_ids)
    }
}

struct TokenBudget {
    remaining: Option<u32>,
    applied: bool,
}

impl TokenBudget {
    fn new(max_tokens: Option<u32>) -> Self {
        Self {
            remaining: max_tokens,
            applied: false,
        }
    }

    fn exhausted_by(&mut self, content: &[u8], warnings: &mut Vec<String>) -> bool {
        let Some(remaining) = self.remaining else {
            return false;
        };
        let tokens = Venturi::approx_tokens(content);
        if tokens > remaining {
            self.applied = true;
            warnings.push(format!("retrieval stopped at max_tokens ({})", remaining));
            return true;
        }
        self.remaining = Some(remaining - tokens);
        false
    }

    fn applied(&self) -> bool {
        self.applied
    }
}

fn jaccard_similarity(a: &[String], b: &[String]) -> f32 {
    let left: std::collections::HashSet<&String> = a.iter().collect();
    let right: std::collections::HashSet<&String> = b.iter().collect();
    if left.is_empty() && right.is_empty() {
        return 1.0;
    }
    let intersection = left.intersection(&right).count() as f32;
    let union = left.union(&right).count() as f32;
    intersection / union
}

fn filter_json(filter: &StructuredFilter) -> serde_json::Value {
    serde_json::json!({
        "topic": filter.topic,
        "domain": filter.domain,
        "tier": filter.tier,
        "parent_id": filter.parent_id,
        "format": filter.format,
        "classification": filter.classification,
        "date_from": filter.date_from,
        "date_to": filter.date_to,
    })
}

#[cfg(test)]
mod tests {
    use super::jaccard_similarity;

    #[test]
    fn jaccard_similarity_scores_overlap() {
        let left = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let right = vec!["b".to_string(), "c".to_string(), "d".to_string()];

        assert_eq!(jaccard_similarity(&[], &[]), 1.0);
        assert!((jaccard_similarity(&left, &right) - 0.5).abs() < f32::EPSILON);
    }
}
