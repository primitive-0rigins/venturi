use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{Json, Response},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
/// HTTP server — exposes Venturi over localhost JSON API.
///
/// All calling agents talk to it over HTTP.
/// One Venturi process, one port (default 9271), all agents share it.
///
/// Endpoints:
///   POST /ingest               — store content (returns parent_id + orb_ids)
///   POST /retrieve/context     — similarity search → prompt-ready chunks
///   POST /retrieve/document    — reassemble full document by query
///   GET  /retrieve/document/:parent_id — reassemble by known parent_id (no Ollama)
///   GET  /retrieve/document/:parent_id/stream — same, delivered as newline-delimited JSON
///   POST /retrieve/graph       — BFS traversal from query concepts
///   POST /retrieve/consensus   — overlay retrieval core + supplementary split
///   POST /retrieve/temporal    — date-range + subject filter
///   POST /retrieve/structured  — exact metadata filter
///   POST /retrieve/metadata    — catalog rows only, no decryption (cheapest)
///   POST /verdict              — record agent feedback (1=good, 0=bad)
///   GET  /health               — liveness check
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use venturi::auth::{configured_keys, required_scope, ApiKey};
use venturi::{
    AnswerFact, ChainReference, ContentType, Foresight, ForesightRow, IngestionRequest, MetaRow,
    RetrievalProof, StructuredFilter, SystemCapabilities, TunnelError,
};

use crate::worker::{CommandSender, WorkerError};

// ── Shared state ──────────────────────────────────────────────────────────────

/// Handle to the single-owner Venturi worker thread (see `src/worker.rs`).
/// Cheaply cloneable — no `Arc` wrapper needed on top.
pub type SharedVenturi = CommandSender;

/// Lock a mutex, recovering from poisoning instead of panicking.
///
/// A plain `.lock().unwrap()` turns any single panic under the lock into a
/// permanent outage: the mutex stays poisoned forever, so every future
/// request across every endpoint panics too, with no way back short of a
/// process restart. The data behind the lock (the rate limiter) is still
/// perfectly usable after a poisoning panic — the panic just means one
/// request didn't finish, not that the state is corrupt — so recovering the
/// guard and carrying on is the safe default here.
pub(crate) fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone)]
pub struct ServerState {
    venturi: SharedVenturi,
    limiter: Arc<Mutex<RateLimiter>>,
    rate_limits: RateLimitConfig,
    api_keys: Arc<Vec<ApiKey>>,
}

impl ServerState {
    pub fn new(venturi: SharedVenturi) -> Self {
        Self::with_rate_limits(venturi, RateLimitConfig::default())
    }

    pub fn with_rate_limits(venturi: SharedVenturi, rate_limits: RateLimitConfig) -> Self {
        Self {
            venturi,
            limiter: Arc::new(Mutex::new(RateLimiter::new())),
            rate_limits,
            api_keys: Arc::new(configured_keys()),
        }
    }

    /// Bypasses env-var key loading so tests can inject keys directly instead
    /// of mutating process-wide state.
    #[cfg(test)]
    fn with_api_keys(venturi: SharedVenturi, api_keys: Vec<ApiKey>) -> Self {
        Self {
            venturi,
            limiter: Arc::new(Mutex::new(RateLimiter::new())),
            rate_limits: RateLimitConfig::default(),
            api_keys: Arc::new(api_keys),
        }
    }
}

async fn require_api_key(
    State(state): State<ServerState>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let allowed = token.is_some_and(|token| {
        state
            .api_keys
            .iter()
            .any(|key| key.value == token && key.scope.allows(required_scope(request.uri().path())))
    });
    if !allowed {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

#[derive(Clone, Copy)]
pub struct RateLimitConfig {
    pub ingest_limit: usize,
    pub retrieval_limit: usize,
    pub window: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            ingest_limit: 120,
            retrieval_limit: 600,
            window: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RateLimitedOp {
    Ingest,
    Retrieval,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct RateLimitKey {
    agent_id: String,
    op: RateLimitedOp,
}

#[derive(Debug, PartialEq)]
struct RateLimitDecision {
    retry_after_ms: u64,
}

struct RateLimiter {
    events: HashMap<RateLimitKey, VecDeque<Instant>>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            events: HashMap::new(),
        }
    }

    fn check(
        &mut self,
        agent_id: &str,
        op: RateLimitedOp,
        now: Instant,
        config: RateLimitConfig,
    ) -> Option<RateLimitDecision> {
        let limit = match op {
            RateLimitedOp::Ingest => config.ingest_limit,
            RateLimitedOp::Retrieval => config.retrieval_limit,
        };
        if limit == 0 {
            return Some(RateLimitDecision {
                retry_after_ms: config.window.as_millis().max(1) as u64,
            });
        }

        let key = RateLimitKey {
            agent_id: agent_id.to_string(),
            op,
        };
        let events = self.events.entry(key).or_default();
        while events
            .front()
            .is_some_and(|front| now.duration_since(*front) >= config.window)
        {
            events.pop_front();
        }

        if events.len() >= limit {
            let oldest = *events.front().expect("limited queues are non-empty");
            let retry_after = config.window.saturating_sub(now.duration_since(oldest));
            return Some(RateLimitDecision {
                retry_after_ms: retry_after.as_millis().max(1) as u64,
            });
        }

        events.push_back(now);
        None
    }
}

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct IngestBody {
    pub agent_id: String,
    pub topic: String,
    pub domain: String,
    pub date: String,
    pub format: String,
    #[serde(default = "default_classification")]
    pub classification: String,
    pub summary: String,
    #[serde(default)]
    pub pinned: Option<bool>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub table_summary: Option<String>,
    /// Content chunks as base64-encoded strings.
    /// One element = one orb. Caller splits large docs before calling.
    pub chunks: Vec<String>,
    /// Optional atomic verifiable facts for semantic search precision.
    #[serde(default)]
    pub answer_facts: Option<Vec<String>>,
    #[serde(default)]
    pub answer_fact_atoms: Vec<AnswerFact>,
    #[serde(default)]
    pub foresights: Vec<Foresight>,
    #[serde(default)]
    pub summary_author: Option<String>,
    #[serde(default)]
    pub summary_model: Option<String>,
    #[serde(default)]
    pub summary_verified: bool,
    #[serde(default)]
    pub summary_verified_at: Option<String>,
}

#[derive(Serialize)]
pub struct IngestResponse {
    pub parent_id: String,
    pub orb_ids: Vec<String>,
    pub orb_count: usize,
}

#[derive(Deserialize)]
pub struct ContextBody {
    pub query: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub check_stability: bool,
    pub agent_id: Option<String>,
}
fn default_top_k() -> usize {
    5
}
fn default_classification() -> String {
    "internal".to_string()
}

#[derive(Deserialize)]
pub struct GraphBody {
    pub query: String,
    #[serde(default = "default_hops")]
    pub max_hops: u32,
    pub agent_id: Option<String>,
}
fn default_hops() -> u32 {
    2
}

#[derive(Deserialize)]
pub struct ConsensusBody {
    pub query: String,
    #[serde(default)]
    pub modes: Vec<String>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default = "default_hops")]
    pub max_hops: u32,
    pub agent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct DocumentBody {
    pub query: String,
    pub max_tokens: Option<u32>,
    pub agent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct TemporalBody {
    pub subject: String,
    pub from: String,
    pub to: String,
    pub max_tokens: Option<u32>,
    pub agent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct StructuredBody {
    pub topic: Option<String>,
    pub domain: Option<String>,
    pub tier: Option<String>,
    pub parent_id: Option<String>,
    pub format: Option<String>,
    pub classification: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub max_tokens: Option<u32>,
    pub agent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct MetadataBody {
    pub topic: Option<String>,
    pub domain: Option<String>,
    pub tier: Option<String>,
    pub parent_id: Option<String>,
    pub format: Option<String>,
    pub classification: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub agent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct VerdictBody {
    pub parent_id: String,
    pub orb_ids: Vec<String>,
    /// 1 = useful, 0 = not useful
    pub verdict: u8,
    /// Orbs the agent expected to receive. Optional — used to compute recall.
    #[serde(default)]
    pub expected_orb_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct HoldBody {
    pub parent_id: String,
    pub reason: String,
}

#[derive(Deserialize)]
pub struct ChainLinkBody {
    pub from_parent_id: String,
    pub to_parent_id: String,
    pub reference_type: String,
}

#[derive(Deserialize)]
pub struct ForesightQuery {
    pub on: String,
}

/// Generic response wrapper: content chunks encoded as base64.
/// `warnings` is non-empty when one or more orbs were corrupted or truncated.
#[derive(Serialize)]
pub struct ChunksResponse {
    pub chunks: Vec<String>,
    pub count: usize,
    pub warnings: Vec<String>,
    pub retrieval_audit_id: String,
    pub token_budget_applied: bool,
    pub stability: Option<String>,
    pub cache_tier: Option<String>,
}

#[derive(Serialize)]
pub struct ConsensusResponse {
    pub core_chunks: Vec<String>,
    pub supplementary_chunks: Vec<String>,
    pub core_count: usize,
    pub supplementary_count: usize,
    pub modes_run: Vec<String>,
    pub warnings: Vec<String>,
    pub retrieval_audit_id: String,
}

/// Full document response: single base64-encoded blob.
/// `warnings` is non-empty when partial corruption occurred.
#[derive(Serialize)]
pub struct DocumentResponse {
    pub content: String,
    pub bytes: usize,
    pub warnings: Vec<String>,
    pub retrieval_audit_id: String,
    pub token_budget_applied: bool,
    pub cache_tier: Option<String>,
}

/// Metadata-only response — no content, no decryption.
#[derive(Serialize)]
pub struct MetadataResponse {
    pub rows: Vec<MetaRowJson>,
    pub count: usize,
    pub retrieval_audit_id: String,
}

#[derive(Serialize)]
pub struct AuditResponse {
    pub proof: RetrievalProof,
}

#[derive(Serialize)]
pub struct ChainReferencesResponse {
    pub references: Vec<ChainReferenceJson>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct ForesightsResponse {
    pub foresights: Vec<ForesightJson>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct ForesightJson {
    pub parent_id: String,
    pub foresight_text: String,
    pub relevant_from: String,
    pub relevant_until: String,
    pub duration_days: u32,
    pub created_at: String,
}

impl From<ForesightRow> for ForesightJson {
    fn from(row: ForesightRow) -> Self {
        Self {
            parent_id: row.parent_id,
            foresight_text: row.foresight_text,
            relevant_from: row.relevant_from,
            relevant_until: row.relevant_until,
            duration_days: row.duration_days,
            created_at: row.created_at,
        }
    }
}

#[derive(Serialize)]
pub struct ChainReferenceJson {
    pub from_parent_id: String,
    pub to_parent_id: String,
    pub reference_type: String,
    pub created_at: String,
}

impl From<ChainReference> for ChainReferenceJson {
    fn from(r: ChainReference) -> Self {
        Self {
            from_parent_id: r.from_parent_id,
            to_parent_id: r.to_parent_id,
            reference_type: r.reference_type,
            created_at: r.created_at,
        }
    }
}

/// JSON-serializable form of a MetaRow.
#[derive(Serialize)]
pub struct MetaRowJson {
    pub orb_id: String,
    pub topic: String,
    pub domain: String,
    pub date: String,
    pub format: String,
    pub content_type: String,
    pub classification: String,
    pub tier: String,
    pub parent_id: String,
    pub sequence: u32,
    pub chain_length: u32,
    pub summary_author: String,
    pub summary_model: Option<String>,
    pub summary_verified: bool,
    pub summary_verified_at: Option<String>,
    pub verified_facts: Vec<AnswerFactJson>,
}

impl From<MetaRow> for MetaRowJson {
    fn from(r: MetaRow) -> Self {
        Self {
            orb_id: r.orb_id,
            topic: r.topic,
            domain: r.domain,
            date: r.date,
            format: r.format,
            content_type: r.content_type,
            classification: r.classification,
            tier: r.tier,
            parent_id: r.parent_id,
            sequence: r.sequence,
            chain_length: r.chain_length,
            summary_author: r.summary_author,
            summary_model: r.summary_model,
            summary_verified: r.summary_verified,
            summary_verified_at: r.summary_verified_at,
            verified_facts: r
                .verified_facts
                .into_iter()
                .map(AnswerFactJson::from)
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub struct AnswerFactJson {
    pub fact: String,
    pub verified: bool,
    pub verified_by: Option<String>,
}

impl From<AnswerFact> for AnswerFactJson {
    fn from(fact: AnswerFact) -> Self {
        Self {
            fact: fact.fact,
            verified: fact.verified,
            verified_by: fact.verified_by,
        }
    }
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub capabilities: CapabilityResponse,
}

#[derive(Serialize)]
pub struct CapabilityResponse {
    pub embedding: String,
    pub graph: String,
    pub retrieval: String,
    pub ingest: String,
}

impl From<SystemCapabilities> for CapabilityResponse {
    fn from(c: SystemCapabilities) -> Self {
        Self {
            embedding: c.embedding.as_str().to_string(),
            graph: c.graph.as_str().to_string(),
            retrieval: c.retrieval.as_str().to_string(),
            ingest: c.ingest.as_str().to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct OkResponse {
    pub ok: bool,
}

// ── Route handlers ────────────────────────────────────────────────────────────

async fn health(
    State(state): State<ServerState>,
) -> Result<Json<HealthResponse>, (StatusCode, String)> {
    let capabilities = state.venturi.capabilities().await.map_err(worker_error)?;
    Ok(Json(HealthResponse {
        ok: true,
        capabilities: capabilities.into(),
    }))
}

async fn ingest(
    State(state): State<ServerState>,
    Json(body): Json<IngestBody>,
) -> Result<Json<IngestResponse>, (StatusCode, String)> {
    enforce_rate_limit(&state, &body.agent_id, RateLimitedOp::Ingest)?;

    // Decode base64 chunks → raw bytes
    let chunks: Vec<Vec<u8>> = body
        .chunks
        .iter()
        .map(|s| base64_decode(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("base64 decode: {}", e)))?;

    let summary_author = body.summary_author.unwrap_or_else(|| body.agent_id.clone());
    let content_type_value = body.content_type.as_deref().unwrap_or("text");
    let content_type = ContentType::parse(content_type_value).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "content_type must be one of text, table, time_series, code".to_string(),
        )
    })?;

    let req = IngestionRequest {
        agent_id: body.agent_id,
        topic: body.topic,
        domain: body.domain,
        date: body.date,
        format: body.format,
        classification: body.classification,
        summary: body.summary,
        answer_facts: body.answer_facts.unwrap_or_default(),
        answer_fact_atoms: body.answer_fact_atoms,
        foresights: body.foresights,
        summary_author,
        summary_model: body.summary_model,
        summary_verified: body.summary_verified,
        summary_verified_at: body.summary_verified_at,
        pinned: body.pinned,
        content_type: Some(content_type),
        table_summary: body.table_summary,
        chunks,
    };

    let result = state.venturi.ingest(req).await.map_err(worker_error)?;

    Ok(Json(IngestResponse {
        orb_count: result.orb_ids.len(),
        parent_id: result.parent_id,
        orb_ids: result.orb_ids,
    }))
}

async fn retrieve_context(
    State(state): State<ServerState>,
    Json(body): Json<ContextBody>,
) -> Result<Json<ChunksResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let result = state
        .venturi
        .context_with_options_and_proof(
            body.query,
            body.top_k,
            body.max_tokens,
            body.check_stability,
            body.agent_id,
        )
        .await
        .map_err(worker_error)?;

    let encoded: Vec<String> = result.value.iter().map(|c| base64_encode(c)).collect();
    let count = encoded.len();
    Ok(Json(ChunksResponse {
        chunks: encoded,
        count,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        stability: result.stability,
        cache_tier: result.cache_tier,
    }))
}

async fn retrieve_document(
    State(state): State<ServerState>,
    Json(body): Json<DocumentBody>,
) -> Result<Json<DocumentResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let result = state
        .venturi
        .document_with_budget_and_proof(body.query, body.max_tokens, body.agent_id)
        .await
        .map_err(worker_error)?;

    let size = result.value.len();
    Ok(Json(DocumentResponse {
        content: base64_encode(&result.value),
        bytes: size,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        cache_tier: result.cache_tier,
    }))
}

async fn retrieve_document_by_id(
    State(state): State<ServerState>,
    Path(parent_id): Path<String>,
) -> Result<Json<DocumentResponse>, (StatusCode, String)> {
    enforce_rate_limit(&state, "anonymous", RateLimitedOp::Retrieval)?;

    let result = state
        .venturi
        .document_by_parent_id_with_proof(parent_id, None)
        .await
        .map_err(worker_error)?;

    let size = result.value.len();
    Ok(Json(DocumentResponse {
        content: base64_encode(&result.value),
        bytes: size,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        cache_tier: result.cache_tier,
    }))
}

/// One line of the `application/x-ndjson` body returned by
/// `retrieve_document_stream`. `chunk` lines carry one rehydrated orb each;
/// the stream always ends with exactly one `done` or `error` line so a
/// caller can tell a truncated response (connection dropped mid-chunk) apart
/// from a complete one.
#[derive(Serialize)]
#[serde(tag = "type")]
enum DocumentStreamLine {
    #[serde(rename = "chunk")]
    Chunk {
        index: usize,
        total: usize,
        orb_id: String,
        content: String,
    },
    #[serde(rename = "done")]
    Done {
        warnings: Vec<String>,
        retrieval_audit_id: String,
        cache_tier: Option<String>,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

fn stream_line_bytes(line: &DocumentStreamLine) -> Bytes {
    let mut json = serde_json::to_string(line).expect("DocumentStreamLine always serializes");
    json.push('\n');
    Bytes::from(json.into_bytes())
}

/// State threaded through the `futures_util::stream::unfold` that backs
/// `retrieve_document_stream`. `orb_ids` is fetched once up front; each
/// `unfold` step sends exactly one command over the worker channel (either
/// one orb rehydration or the final bookkeeping call) so a slow-draining
/// client never holds the single worker thread — see src/worker.rs.
struct DocumentStreamState {
    venturi: SharedVenturi,
    parent_id: String,
    orb_ids: Vec<String>,
    next_index: usize,
    warnings: Vec<String>,
    finished: bool,
}

async fn document_stream_next(
    mut state: DocumentStreamState,
) -> Option<(Result<Bytes, std::convert::Infallible>, DocumentStreamState)> {
    if state.finished {
        return None;
    }

    if state.next_index < state.orb_ids.len() {
        let index = state.next_index;
        let orb_id = state.orb_ids[index].clone();
        state.next_index += 1;

        match state.venturi.rehydrate_orb_for_stream(orb_id.clone()).await {
            Ok((content, warning)) => {
                if let Some(warning) = warning {
                    state.warnings.push(warning);
                }
                let line = DocumentStreamLine::Chunk {
                    index,
                    total: state.orb_ids.len(),
                    orb_id,
                    content: base64_encode(&content),
                };
                Some((Ok(stream_line_bytes(&line)), state))
            }
            Err(error) => {
                state.finished = true;
                let line = DocumentStreamLine::Error {
                    message: error.to_string(),
                };
                Some((Ok(stream_line_bytes(&line)), state))
            }
        }
    } else {
        state.finished = true;
        let line = match state
            .venturi
            .finalize_document_stream(
                state.parent_id.clone(),
                state.orb_ids.clone(),
                state.warnings.clone(),
                None,
            )
            .await
        {
            Ok((retrieval_audit_id, cache_tier)) => DocumentStreamLine::Done {
                warnings: state.warnings.clone(),
                retrieval_audit_id,
                cache_tier,
            },
            Err(error) => DocumentStreamLine::Error {
                message: error.to_string(),
            },
        };
        Some((Ok(stream_line_bytes(&line)), state))
    }
}

/// Streamed sibling of `retrieve_document_by_id`: same chain, delivered as
/// newline-delimited JSON (one `chunk` line per orb) instead of one large
/// JSON body, so a caller can start processing a large document chain
/// before the whole thing is rehydrated. See roadmap item B2.
async fn retrieve_document_stream(
    State(state): State<ServerState>,
    Path(parent_id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    enforce_rate_limit(&state, "anonymous", RateLimitedOp::Retrieval)?;

    let orb_ids = state
        .venturi
        .document_chain_orb_ids(parent_id.clone())
        .await
        .map_err(worker_error)?;

    let initial = DocumentStreamState {
        venturi: state.venturi.clone(),
        parent_id,
        orb_ids,
        next_index: 0,
        warnings: Vec::new(),
        finished: false,
    };
    let body = Body::from_stream(futures_util::stream::unfold(
        initial,
        document_stream_next,
    ));

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
        .expect("static response builder call never fails"))
}

async fn retrieve_graph(
    State(state): State<ServerState>,
    Json(body): Json<GraphBody>,
) -> Result<Json<ChunksResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let result = state
        .venturi
        .graph_query_with_proof(body.query, body.max_hops, body.agent_id)
        .await
        .map_err(worker_error)?;

    let encoded: Vec<String> = result.value.iter().map(|c| base64_encode(c)).collect();
    let count = encoded.len();
    Ok(Json(ChunksResponse {
        chunks: encoded,
        count,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        stability: result.stability,
        cache_tier: result.cache_tier,
    }))
}

async fn retrieve_consensus(
    State(state): State<ServerState>,
    Json(body): Json<ConsensusBody>,
) -> Result<Json<ConsensusResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let result = state
        .venturi
        .consensus(
            body.query,
            body.modes,
            body.top_k,
            body.max_hops,
            body.agent_id,
        )
        .await
        .map_err(worker_error)?;

    let core_chunks: Vec<String> = result
        .core_chunks
        .iter()
        .map(|c| base64_encode(c))
        .collect();
    let supplementary_chunks: Vec<String> = result
        .supplementary_chunks
        .iter()
        .map(|c| base64_encode(c))
        .collect();
    let core_count = core_chunks.len();
    let supplementary_count = supplementary_chunks.len();

    Ok(Json(ConsensusResponse {
        core_chunks,
        supplementary_chunks,
        core_count,
        supplementary_count,
        modes_run: result.modes_run,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
    }))
}

async fn retrieve_temporal(
    State(state): State<ServerState>,
    Json(body): Json<TemporalBody>,
) -> Result<Json<ChunksResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let result = state
        .venturi
        .temporal_with_budget_and_proof(body.subject, body.from, body.to, body.max_tokens, body.agent_id)
        .await
        .map_err(worker_error)?;

    let encoded: Vec<String> = result.value.iter().map(|c| base64_encode(c)).collect();
    let count = encoded.len();
    Ok(Json(ChunksResponse {
        chunks: encoded,
        count,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        stability: result.stability,
        cache_tier: result.cache_tier,
    }))
}

async fn retrieve_structured(
    State(state): State<ServerState>,
    Json(body): Json<StructuredBody>,
) -> Result<Json<ChunksResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let filter = StructuredFilter {
        topic: body.topic,
        domain: body.domain,
        tier: body.tier,
        parent_id: body.parent_id,
        format: body.format,
        classification: body.classification,
        date_from: body.date_from,
        date_to: body.date_to,
    };

    let result = state
        .venturi
        .structured_with_budget_and_proof(filter, body.max_tokens, body.agent_id)
        .await
        .map_err(worker_error)?;

    let encoded: Vec<String> = result.value.iter().map(|c| base64_encode(c)).collect();
    let count = encoded.len();
    Ok(Json(ChunksResponse {
        chunks: encoded,
        count,
        warnings: result.warnings,
        retrieval_audit_id: result.retrieval_audit_id,
        token_budget_applied: result.token_budget_applied,
        stability: result.stability,
        cache_tier: result.cache_tier,
    }))
}

async fn retrieve_metadata(
    State(state): State<ServerState>,
    Json(body): Json<MetadataBody>,
) -> Result<Json<MetadataResponse>, (StatusCode, String)> {
    enforce_rate_limit(
        &state,
        body.agent_id.as_deref().unwrap_or("anonymous"),
        RateLimitedOp::Retrieval,
    )?;

    let filter = StructuredFilter {
        topic: body.topic,
        domain: body.domain,
        tier: body.tier,
        parent_id: body.parent_id,
        format: body.format,
        classification: body.classification,
        date_from: body.date_from,
        date_to: body.date_to,
    };

    let result = state
        .venturi
        .metadata_with_proof(filter, body.agent_id)
        .await
        .map_err(worker_error)?;

    let count = result.value.len();
    Ok(Json(MetadataResponse {
        rows: result.value.into_iter().map(MetaRowJson::from).collect(),
        count,
        retrieval_audit_id: result.retrieval_audit_id,
    }))
}

async fn verdict(
    State(state): State<ServerState>,
    Json(body): Json<VerdictBody>,
) -> Result<Json<OkResponse>, (StatusCode, String)> {
    state
        .venturi
        .record_verdict(
            body.parent_id,
            body.orb_ids,
            body.expected_orb_ids,
            body.verdict,
        )
        .await
        .map_err(worker_error)?;

    Ok(Json(OkResponse { ok: true }))
}

async fn audit(
    State(state): State<ServerState>,
    Path(retrieval_audit_id): Path<String>,
) -> Result<Json<AuditResponse>, (StatusCode, String)> {
    enforce_rate_limit(&state, "anonymous", RateLimitedOp::Retrieval)?;

    let proof = state
        .venturi
        .retrieval_proof(retrieval_audit_id)
        .await
        .map_err(worker_error)?
        .ok_or_else(|| {
            let body = serde_json::json!({
                "ok": false,
                "category": "audit_not_found",
                "error": "retrieval proof not found",
            });
            (StatusCode::NOT_FOUND, body.to_string())
        })?;

    Ok(Json(AuditResponse { proof }))
}

async fn hold(
    State(state): State<ServerState>,
    Json(body): Json<HoldBody>,
) -> Result<Json<OkResponse>, (StatusCode, String)> {
    state
        .venturi
        .set_legal_hold(body.parent_id, body.reason)
        .await
        .map_err(worker_error)?;

    Ok(Json(OkResponse { ok: true }))
}

async fn release_hold(
    State(state): State<ServerState>,
    Path(parent_id): Path<String>,
) -> Result<Json<OkResponse>, (StatusCode, String)> {
    state
        .venturi
        .release_legal_hold(parent_id)
        .await
        .map_err(worker_error)?;

    Ok(Json(OkResponse { ok: true }))
}

async fn link_chain(
    State(state): State<ServerState>,
    Json(body): Json<ChainLinkBody>,
) -> Result<Json<OkResponse>, (StatusCode, String)> {
    state
        .venturi
        .link_chains(body.from_parent_id, body.to_parent_id, body.reference_type)
        .await
        .map_err(worker_error)?;

    Ok(Json(OkResponse { ok: true }))
}

async fn chain_references(
    State(state): State<ServerState>,
    Path(parent_id): Path<String>,
) -> Result<Json<ChainReferencesResponse>, (StatusCode, String)> {
    let references = state
        .venturi
        .chain_references(parent_id)
        .await
        .map_err(worker_error)?;
    let count = references.len();

    Ok(Json(ChainReferencesResponse {
        references: references
            .into_iter()
            .map(ChainReferenceJson::from)
            .collect(),
        count,
    }))
}

async fn retrieve_foresights(
    State(state): State<ServerState>,
    Query(query): Query<ForesightQuery>,
) -> Result<Json<ForesightsResponse>, (StatusCode, String)> {
    enforce_rate_limit(&state, "anonymous", RateLimitedOp::Retrieval)?;
    let foresights = state
        .venturi
        .foresights(query.on)
        .await
        .map_err(worker_error)?;
    let count = foresights.len();

    Ok(Json(ForesightsResponse {
        foresights: foresights.into_iter().map(ForesightJson::from).collect(),
        count,
    }))
}

fn worker_error(error: WorkerError) -> (StatusCode, String) {
    match error {
        WorkerError::Tunnel(error) => api_error(error),
        WorkerError::Overloaded { retry_after_ms } => overloaded_error(retry_after_ms),
        WorkerError::Unavailable => {
            let body = serde_json::json!({
                "ok": false,
                "category": "internal_error",
                "error": "venturi worker unavailable",
            });
            (StatusCode::INTERNAL_SERVER_ERROR, body.to_string())
        }
    }
}

fn enforce_rate_limit(
    state: &ServerState,
    agent_id: &str,
    op: RateLimitedOp,
) -> Result<(), (StatusCode, String)> {
    let decision =
        lock_mutex(&state.limiter).check(agent_id, op, Instant::now(), state.rate_limits);

    match decision {
        Some(decision) => Err(overloaded_error(decision.retry_after_ms)),
        None => Ok(()),
    }
}

fn overloaded_error(retry_after_ms: u64) -> (StatusCode, String) {
    let body = serde_json::json!({
        "ok": false,
        "category": "overloaded",
        "error": "rate limit exceeded",
        "retry_after_ms": retry_after_ms,
    });
    (StatusCode::TOO_MANY_REQUESTS, body.to_string())
}

fn api_error(error: TunnelError) -> (StatusCode, String) {
    let status = status_for_error(&error);
    let body = match &error {
        TunnelError::MemoryNotFound { query, reason } => serde_json::json!({
            "ok": false,
            "found": false,
            "category": error.category(),
            "reason": reason.to_string(),
            "query": query,
            "error": error.to_string(),
        }),
        _ => serde_json::json!({
            "ok": false,
            "category": error.category(),
            "error": error.to_string(),
        }),
    };
    (status, body.to_string())
}

fn status_for_error(error: &TunnelError) -> StatusCode {
    match error {
        TunnelError::MemoryNotFound { .. } | TunnelError::OrbNotFound { .. } => {
            StatusCode::NOT_FOUND
        }
        TunnelError::GatekeeperRejected { .. } | TunnelError::InvalidConfiguration(_) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        TunnelError::KeystoreInaccessible | TunnelError::DatabaseError(_) | TunnelError::Io(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── Server builder ────────────────────────────────────────────────────────────

/// Build the axum Router from a pre-created SharedVenturi.
/// main.rs creates the Arc so it can also pass it to background sweep tasks.
pub fn build_router(state: SharedVenturi) -> Router {
    build_router_with_state(ServerState::new(state))
}

fn build_router_with_state(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/ingest", post(ingest))
        .route("/retrieve/context", post(retrieve_context))
        .route("/retrieve/document", post(retrieve_document))
        .route(
            "/retrieve/document/:parent_id",
            get(retrieve_document_by_id),
        )
        .route(
            "/retrieve/document/:parent_id/stream",
            get(retrieve_document_stream),
        )
        .route("/retrieve/graph", post(retrieve_graph))
        .route("/retrieve/consensus", post(retrieve_consensus))
        .route("/retrieve/temporal", post(retrieve_temporal))
        .route("/retrieve/structured", post(retrieve_structured))
        .route("/retrieve/metadata", post(retrieve_metadata))
        .route("/verdict", post(verdict))
        .route("/audit/:retrieval_audit_id", get(audit))
        .route("/hold", post(hold))
        .route("/hold/:parent_id", axum::routing::delete(release_hold))
        .route("/chain/link", post(link_chain))
        .route("/chain/references/:parent_id", get(chain_references))
        .route("/retrieve/foresights", get(retrieve_foresights))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

// ── base64 helpers (no extra dep — hand-rolled using std) ─────────────────────

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 {
            chunk[1] as usize
        } else {
            0
        };
        let b2 = if chunk.len() > 2 {
            chunk[2] as usize
        } else {
            0
        };
        out.push(TABLE[(b0 >> 2) & 0x3f]);
        out.push(TABLE[((b0 << 4) | (b1 >> 4)) & 0x3f]);
        out.push(if chunk.len() > 1 {
            TABLE[((b1 << 2) | (b2 >> 6)) & 0x3f]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            TABLE[b2 & 0x3f]
        } else {
            b'='
        });
    }
    String::from_utf8(out).unwrap()
}

fn base64_decode(s: &str) -> Result<Vec<u8>, &'static str> {
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;

    for &b in s.as_bytes() {
        let val = match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return Err("invalid base64"),
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    #[test]
    fn lock_mutex_recovers_after_a_panic_under_the_lock() {
        let mutex = Arc::new(Mutex::new(0i32));

        let poisoner = Arc::clone(&mutex);
        let _ = panic::catch_unwind(move || {
            let mut guard = lock_mutex(&poisoner);
            *guard = 1;
            panic!("simulated panic while holding the lock");
        });
        assert!(mutex.is_poisoned());

        // A plain `.lock().unwrap()` would panic on every call from here on.
        // lock_mutex() must keep working — the whole point of this fix.
        let guard = lock_mutex(&mutex);
        assert_eq!(*guard, 1);
    }

    #[test]
    fn rate_limiter_is_per_agent_and_operation() {
        let config = RateLimitConfig {
            ingest_limit: 1,
            retrieval_limit: 1,
            window: Duration::from_secs(10),
        };
        let now = Instant::now();
        let mut limiter = RateLimiter::new();

        assert_eq!(
            limiter.check("agent-a", RateLimitedOp::Ingest, now, config),
            None
        );
        assert_eq!(
            limiter.check("agent-a", RateLimitedOp::Ingest, now, config),
            Some(RateLimitDecision {
                retry_after_ms: 10_000,
            })
        );
        assert_eq!(
            limiter.check("agent-a", RateLimitedOp::Retrieval, now, config),
            None
        );
        assert_eq!(
            limiter.check("agent-b", RateLimitedOp::Ingest, now, config),
            None
        );
    }

    #[test]
    fn rate_limiter_reopens_after_window() {
        let config = RateLimitConfig {
            ingest_limit: 1,
            retrieval_limit: 1,
            window: Duration::from_secs(10),
        };
        let now = Instant::now();
        let mut limiter = RateLimiter::new();

        assert_eq!(
            limiter.check("agent-a", RateLimitedOp::Retrieval, now, config),
            None
        );
        assert_eq!(
            limiter.check(
                "agent-a",
                RateLimitedOp::Retrieval,
                now + Duration::from_secs(11),
                config
            ),
            None
        );
    }

    #[test]
    fn overload_error_is_machine_readable() {
        let (status, body) = overloaded_error(2500);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(parsed["category"], "overloaded");
        assert_eq!(parsed["retry_after_ms"], 2500);
    }

    // ── auth middleware ────────────────────────────────────────────────────

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;
    use venturi::auth::Scope;
    use venturi::{StorageLimits, Venturi, VenturiConfig};

    fn test_router(api_keys: Vec<ApiKey>) -> (Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let config = VenturiConfig {
            shelf_root: format!("{root}/shelf"),
            journal_db: format!("{root}/journal.db"),
            keystore_db: format!("{root}/keystore.db"),
            librarian_db: format!("{root}/librarian.db"),
            scribe_db: format!("{root}/scribe.db"),
            graph_db: format!("{root}/graph.db"),
            ollama_url: "http://localhost:11434".to_string(),
            embedding_model: None,
            embedding_dim: None,
            lifecycle: None,
            limits: StorageLimits::default(),
        };
        let venturi = Venturi::open(config).unwrap();
        let sender = crate::worker::spawn_worker(venturi);
        let state = ServerState::with_api_keys(sender, api_keys);
        (build_router_with_state(state), dir)
    }

    fn request(method: &str, path: &str, auth: Option<&str>) -> HttpRequest<Body> {
        let mut builder = HttpRequest::builder().method(method).uri(path);
        if let Some(token) = auth {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap()
    }

    #[tokio::test]
    async fn health_requires_no_key() {
        let (router, _dir) = test_router(vec![]);
        let response = router.oneshot(request("GET", "/health", None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_without_key_is_rejected() {
        let (router, _dir) = test_router(vec![]);
        let response = router
            .oneshot(request("POST", "/ingest", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_wrong_key_is_rejected() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "correct-key".to_string(),
            scope: Scope::Admin,
        }]);
        let response = router
            .oneshot(request("POST", "/ingest", Some("wrong-key")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_key_passes_middleware_on_write_route() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "admin-key".to_string(),
            scope: Scope::Admin,
        }]);
        let response = router
            .oneshot(request("POST", "/ingest", Some("admin-key")))
            .await
            .unwrap();
        // The malformed body will fail validation past the middleware, but it
        // must not be UNAUTHORIZED — proves the key was accepted.
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn read_scoped_key_is_rejected_on_write_route() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "read-key".to_string(),
            scope: Scope::Read,
        }]);
        let response = router
            .oneshot(request("POST", "/ingest", Some("read-key")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn read_scoped_key_passes_middleware_on_read_route() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "read-key".to_string(),
            scope: Scope::Read,
        }]);
        let response = router
            .oneshot(request(
                "GET",
                "/retrieve/document/some-parent-id",
                Some("read-key"),
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // ── streamed document retrieval (roadmap B2) ──────────────────────────

    fn json_request(method: &str, path: &str, key: &str, body: serde_json::Value) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {key}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn body_bytes(response: Response) -> Vec<u8> {
        use http_body_util::BodyExt;
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec()
    }

    fn ndjson_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
        std::str::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn streamed_document_matches_non_streaming_reassembly() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "admin-key".to_string(),
            scope: Scope::Admin,
        }]);

        let ingest_body = serde_json::json!({
            "agent_id": "stream-test-agent",
            "topic": "streaming",
            "domain": "test",
            "date": "2026-07-21",
            "format": "text",
            "summary": "a three-orb chain for streaming tests",
            "chunks": [
                base64_encode(b"first orb content "),
                base64_encode(b"second orb content "),
                base64_encode(b"third orb content"),
            ],
        });
        let ingest_response = router
            .clone()
            .oneshot(json_request("POST", "/ingest", "admin-key", ingest_body))
            .await
            .unwrap();
        assert_eq!(ingest_response.status(), StatusCode::OK);
        let ingest_json: serde_json::Value =
            serde_json::from_slice(&body_bytes(ingest_response).await).unwrap();
        let parent_id = ingest_json["parent_id"].as_str().unwrap().to_string();
        assert_eq!(ingest_json["orb_count"], 3);

        let stream_response = router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/retrieve/document/{parent_id}/stream"),
                Some("admin-key"),
            ))
            .await
            .unwrap();
        assert_eq!(stream_response.status(), StatusCode::OK);
        assert_eq!(
            stream_response.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );
        let lines = ndjson_lines(&body_bytes(stream_response).await);

        // Exactly 3 chunk lines followed by exactly 1 done line, in order.
        assert_eq!(lines.len(), 4);
        let mut reassembled = Vec::new();
        for (i, line) in lines[..3].iter().enumerate() {
            assert_eq!(line["type"], "chunk");
            assert_eq!(line["index"], i);
            assert_eq!(line["total"], 3);
            reassembled.extend(base64_decode(line["content"].as_str().unwrap()).unwrap());
        }
        assert_eq!(lines[3]["type"], "done");
        assert!(lines[3]["warnings"].as_array().unwrap().is_empty());
        let retrieval_audit_id = lines[3]["retrieval_audit_id"].as_str().unwrap().to_string();
        assert!(!retrieval_audit_id.is_empty());

        let non_streaming_response = router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/retrieve/document/{parent_id}"),
                Some("admin-key"),
            ))
            .await
            .unwrap();
        assert_eq!(non_streaming_response.status(), StatusCode::OK);
        let non_streaming_json: serde_json::Value =
            serde_json::from_slice(&body_bytes(non_streaming_response).await).unwrap();
        let non_streaming_content =
            base64_decode(non_streaming_json["content"].as_str().unwrap()).unwrap();

        assert_eq!(reassembled, non_streaming_content);
        assert_eq!(
            reassembled,
            b"first orb content second orb content third orb content".to_vec()
        );

        // The proof the stream reported is fetchable, like any other retrieval proof.
        let audit_response = router
            .oneshot(request(
                "GET",
                &format!("/audit/{retrieval_audit_id}"),
                Some("admin-key"),
            ))
            .await
            .unwrap();
        assert_eq!(audit_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn streaming_unknown_parent_id_returns_not_found_without_a_body() {
        let (router, _dir) = test_router(vec![ApiKey {
            value: "admin-key".to_string(),
            scope: Scope::Admin,
        }]);

        let response = router
            .oneshot(request(
                "GET",
                "/retrieve/document/does-not-exist/stream",
                Some("admin-key"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
