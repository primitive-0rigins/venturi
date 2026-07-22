//! Single-owner worker for Venturi's SQLite-backed state (roadmap R2).
//!
//! `Venturi` holds `!Sync` rusqlite connections, so it can only ever be driven
//! from one place at a time. The previous design shared it behind
//! `Arc<Mutex<Venturi>>` and had every async handler lock that same mutex —
//! correct, but it serializes all requests and reads block writes and each
//! other.
//!
//! Instead, one dedicated OS thread owns the `Venturi` instance outright.
//! Callers (HTTP handlers, background sweeps) send a `VenturiCommand` over a
//! bounded channel and await a paired oneshot reply. Ingest ordering is
//! preserved for free — a single mpsc channel drained by a single worker
//! processes commands strictly in send order — and backpressure becomes
//! explicit: a full channel returns `WorkerError::Overloaded` instead of
//! queuing without bound.

use std::thread;

use tokio::sync::{mpsc, oneshot};

use venturi::pipeline::sweep::SweepReport;
use venturi::{
    ChainReference, ConsensusResult, ForesightRow, IngestionRequest, IngestionResult, MetaRow,
    RetrievalProof, RetrievalWithProof, StructuredFilter, SystemCapabilities, TunnelError,
    Venturi,
};

/// Bounded channel capacity. A single worker thread processing SQLite
/// operations drains fast; this only fills under sustained overload, not
/// normal bursts.
const CHANNEL_CAPACITY: usize = 256;

/// Fixed backoff hint returned when the channel is full. Unlike the per-agent
/// rate limiter, there is no rolling window to compute an exact wait from —
/// a short fixed value is a reasonable default since a single fast worker
/// drains its queue quickly.
const OVERLOADED_RETRY_AFTER_MS: u64 = 250;

/// Error surfaced to callers of `CommandSender`. Wraps the same `TunnelError`
/// the direct `Venturi` API would have returned, plus the two failure modes
/// unique to going through a channel and a worker thread.
#[derive(Debug)]
pub enum WorkerError {
    Tunnel(TunnelError),
    /// Channel is at capacity — caller should back off and retry.
    Overloaded { retry_after_ms: u64 },
    /// The worker thread is gone (send failed, or it dropped a reply sender
    /// without responding — only possible if the worker itself panicked).
    Unavailable,
}

impl From<TunnelError> for WorkerError {
    fn from(error: TunnelError) -> Self {
        Self::Tunnel(error)
    }
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tunnel(error) => write!(f, "{error}"),
            Self::Overloaded { retry_after_ms } => {
                write!(f, "worker overloaded, retry after {retry_after_ms}ms")
            }
            Self::Unavailable => write!(f, "venturi worker unavailable"),
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, TunnelError>>;

pub enum VenturiCommand {
    Capabilities(oneshot::Sender<SystemCapabilities>),
    Ingest(Box<IngestionRequest>, Reply<IngestionResult>),
    ContextWithOptionsAndProof {
        query: String,
        top_k: usize,
        max_tokens: Option<u32>,
        check_stability: bool,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<Vec<u8>>>>,
    },
    DocumentWithBudgetAndProof {
        query: String,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<u8>>>,
    },
    DocumentByParentIdWithProof {
        parent_id: String,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<u8>>>,
    },
    GraphQueryWithProof {
        query: String,
        max_hops: u32,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<Vec<u8>>>>,
    },
    Consensus {
        query: String,
        modes: Vec<String>,
        top_k: usize,
        max_hops: u32,
        agent_id: Option<String>,
        reply: Reply<ConsensusResult>,
    },
    TemporalWithBudgetAndProof {
        subject: String,
        from: String,
        to: String,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<Vec<u8>>>>,
    },
    StructuredWithBudgetAndProof {
        filter: StructuredFilter,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<Vec<u8>>>>,
    },
    MetadataWithProof {
        filter: StructuredFilter,
        agent_id: Option<String>,
        reply: Reply<RetrievalWithProof<Vec<MetaRow>>>,
    },
    RecordVerdict {
        parent_id: String,
        orb_ids: Vec<String>,
        expected_orb_ids: Vec<String>,
        verdict: u8,
        reply: Reply<()>,
    },
    RetrievalProof {
        retrieval_audit_id: String,
        reply: Reply<Option<RetrievalProof>>,
    },
    SetLegalHold {
        parent_id: String,
        reason: String,
        reply: Reply<()>,
    },
    ReleaseLegalHold {
        parent_id: String,
        reply: Reply<()>,
    },
    LinkChains {
        from_parent_id: String,
        to_parent_id: String,
        reference_type: String,
        reply: Reply<()>,
    },
    ChainReferences {
        parent_id: String,
        reply: Reply<Vec<ChainReference>>,
    },
    Foresights {
        on: String,
        reply: Reply<Vec<ForesightRow>>,
    },
    ProcessEmbeddingQueue(Reply<u32>),
    SweepAccessMarks(Reply<SweepReport>),
    SweepTiers(Reply<SweepReport>),
    SweepExpiry(Reply<SweepReport>),
    LifecycleSweep(Reply<SweepReport>),
    SweepCommunities(Reply<SweepReport>),
    RecordDaemonHealth {
        daemon: String,
        status: String,
        consecutive_failures: u8,
        details: Option<String>,
        reply: Reply<()>,
    },
}

/// Cheaply cloneable handle to the worker thread's command channel.
/// `mpsc::Sender` is internally reference-counted, so this needs no extra
/// `Arc` wrapper to be shared across handlers.
#[derive(Clone)]
pub struct CommandSender {
    tx: mpsc::Sender<VenturiCommand>,
}

impl CommandSender {
    fn try_send(&self, cmd: VenturiCommand) -> Result<(), WorkerError> {
        self.tx.try_send(cmd).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => WorkerError::Overloaded {
                retry_after_ms: OVERLOADED_RETRY_AFTER_MS,
            },
            mpsc::error::TrySendError::Closed(_) => WorkerError::Unavailable,
        })
    }

    /// Send a command whose reply carries a `Result<T, TunnelError>` and
    /// flatten both failure paths (channel, worker-side) into `WorkerError`.
    async fn call<T>(
        &self,
        make_cmd: impl FnOnce(Reply<T>) -> VenturiCommand,
    ) -> Result<T, WorkerError> {
        let (reply, rx) = oneshot::channel();
        self.try_send(make_cmd(reply))?;
        Ok(rx.await.map_err(|_| WorkerError::Unavailable)??)
    }

    /// Send a command whose reply is infallible on the worker side
    /// (`capabilities()` never fails) — only the channel itself can fail.
    async fn call_infallible<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<T>) -> VenturiCommand,
    ) -> Result<T, WorkerError> {
        let (reply, rx) = oneshot::channel();
        self.try_send(make_cmd(reply))?;
        rx.await.map_err(|_| WorkerError::Unavailable)
    }

    pub async fn capabilities(&self) -> Result<SystemCapabilities, WorkerError> {
        self.call_infallible(VenturiCommand::Capabilities).await
    }

    pub async fn ingest(&self, req: IngestionRequest) -> Result<IngestionResult, WorkerError> {
        self.call(|reply| VenturiCommand::Ingest(Box::new(req), reply))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn context_with_options_and_proof(
        &self,
        query: String,
        top_k: usize,
        max_tokens: Option<u32>,
        check_stability: bool,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, WorkerError> {
        self.call(|reply| VenturiCommand::ContextWithOptionsAndProof {
            query,
            top_k,
            max_tokens,
            check_stability,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn document_with_budget_and_proof(
        &self,
        query: String,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, WorkerError> {
        self.call(|reply| VenturiCommand::DocumentWithBudgetAndProof {
            query,
            max_tokens,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn document_by_parent_id_with_proof(
        &self,
        parent_id: String,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<u8>>, WorkerError> {
        self.call(|reply| VenturiCommand::DocumentByParentIdWithProof {
            parent_id,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn graph_query_with_proof(
        &self,
        query: String,
        max_hops: u32,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, WorkerError> {
        self.call(|reply| VenturiCommand::GraphQueryWithProof {
            query,
            max_hops,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn consensus(
        &self,
        query: String,
        modes: Vec<String>,
        top_k: usize,
        max_hops: u32,
        agent_id: Option<String>,
    ) -> Result<ConsensusResult, WorkerError> {
        self.call(|reply| VenturiCommand::Consensus {
            query,
            modes,
            top_k,
            max_hops,
            agent_id,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn temporal_with_budget_and_proof(
        &self,
        subject: String,
        from: String,
        to: String,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, WorkerError> {
        self.call(|reply| VenturiCommand::TemporalWithBudgetAndProof {
            subject,
            from,
            to,
            max_tokens,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn structured_with_budget_and_proof(
        &self,
        filter: StructuredFilter,
        max_tokens: Option<u32>,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<Vec<u8>>>, WorkerError> {
        self.call(|reply| VenturiCommand::StructuredWithBudgetAndProof {
            filter,
            max_tokens,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn metadata_with_proof(
        &self,
        filter: StructuredFilter,
        agent_id: Option<String>,
    ) -> Result<RetrievalWithProof<Vec<MetaRow>>, WorkerError> {
        self.call(|reply| VenturiCommand::MetadataWithProof {
            filter,
            agent_id,
            reply,
        })
        .await
    }

    pub async fn record_verdict(
        &self,
        parent_id: String,
        orb_ids: Vec<String>,
        expected_orb_ids: Vec<String>,
        verdict: u8,
    ) -> Result<(), WorkerError> {
        self.call(|reply| VenturiCommand::RecordVerdict {
            parent_id,
            orb_ids,
            expected_orb_ids,
            verdict,
            reply,
        })
        .await
    }

    pub async fn retrieval_proof(
        &self,
        retrieval_audit_id: String,
    ) -> Result<Option<RetrievalProof>, WorkerError> {
        self.call(|reply| VenturiCommand::RetrievalProof {
            retrieval_audit_id,
            reply,
        })
        .await
    }

    pub async fn set_legal_hold(
        &self,
        parent_id: String,
        reason: String,
    ) -> Result<(), WorkerError> {
        self.call(|reply| VenturiCommand::SetLegalHold {
            parent_id,
            reason,
            reply,
        })
        .await
    }

    pub async fn release_legal_hold(&self, parent_id: String) -> Result<(), WorkerError> {
        self.call(|reply| VenturiCommand::ReleaseLegalHold { parent_id, reply })
            .await
    }

    pub async fn link_chains(
        &self,
        from_parent_id: String,
        to_parent_id: String,
        reference_type: String,
    ) -> Result<(), WorkerError> {
        self.call(|reply| VenturiCommand::LinkChains {
            from_parent_id,
            to_parent_id,
            reference_type,
            reply,
        })
        .await
    }

    pub async fn chain_references(
        &self,
        parent_id: String,
    ) -> Result<Vec<ChainReference>, WorkerError> {
        self.call(|reply| VenturiCommand::ChainReferences { parent_id, reply })
            .await
    }

    pub async fn foresights(&self, on: String) -> Result<Vec<ForesightRow>, WorkerError> {
        self.call(|reply| VenturiCommand::Foresights { on, reply })
            .await
    }

    pub async fn process_embedding_queue(&self) -> Result<u32, WorkerError> {
        self.call(VenturiCommand::ProcessEmbeddingQueue).await
    }

    pub async fn sweep_access_marks(&self) -> Result<SweepReport, WorkerError> {
        self.call(VenturiCommand::SweepAccessMarks).await
    }

    pub async fn sweep_tiers(&self) -> Result<SweepReport, WorkerError> {
        self.call(VenturiCommand::SweepTiers).await
    }

    pub async fn sweep_expiry(&self) -> Result<SweepReport, WorkerError> {
        self.call(VenturiCommand::SweepExpiry).await
    }

    pub async fn lifecycle_sweep(&self) -> Result<SweepReport, WorkerError> {
        self.call(VenturiCommand::LifecycleSweep).await
    }

    pub async fn sweep_communities(&self) -> Result<SweepReport, WorkerError> {
        self.call(VenturiCommand::SweepCommunities).await
    }

    pub async fn record_daemon_health(
        &self,
        daemon: String,
        status: String,
        consecutive_failures: u8,
        details: Option<String>,
    ) -> Result<(), WorkerError> {
        self.call(|reply| VenturiCommand::RecordDaemonHealth {
            daemon,
            status,
            consecutive_failures,
            details,
            reply,
        })
        .await
    }
}

/// Spawn the single owner-worker thread and return a handle to its channel.
/// `venturi` is moved in — from this point on it is only ever touched from
/// the worker thread.
pub fn spawn_worker(venturi: Venturi) -> CommandSender {
    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
    thread::Builder::new()
        .name("venturi-worker".to_string())
        .spawn(move || {
            let mut venturi = venturi;
            while let Some(cmd) = rx.blocking_recv() {
                handle_command(&mut venturi, cmd);
            }
        })
        .expect("failed to spawn venturi worker thread");
    CommandSender { tx }
}

/// A dropped reply means the caller stopped waiting (e.g. request cancelled)
/// — nothing to do, the worker just moves on to the next command.
fn handle_command(venturi: &mut Venturi, cmd: VenturiCommand) {
    match cmd {
        VenturiCommand::Capabilities(reply) => {
            let _ = reply.send(venturi.capabilities());
        }
        VenturiCommand::Ingest(req, reply) => {
            let _ = reply.send(venturi.ingest(*req));
        }
        VenturiCommand::ContextWithOptionsAndProof {
            query,
            top_k,
            max_tokens,
            check_stability,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.context_with_options_and_proof(
                &query,
                top_k,
                max_tokens,
                check_stability,
                agent_id.as_deref(),
            ));
        }
        VenturiCommand::DocumentWithBudgetAndProof {
            query,
            max_tokens,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.document_with_budget_and_proof(
                &query,
                max_tokens,
                agent_id.as_deref(),
            ));
        }
        VenturiCommand::DocumentByParentIdWithProof {
            parent_id,
            agent_id,
            reply,
        } => {
            let _ = reply.send(
                venturi.document_by_parent_id_with_proof(&parent_id, agent_id.as_deref()),
            );
        }
        VenturiCommand::GraphQueryWithProof {
            query,
            max_hops,
            agent_id,
            reply,
        } => {
            let _ =
                reply.send(venturi.graph_query_with_proof(&query, max_hops, agent_id.as_deref()));
        }
        VenturiCommand::Consensus {
            query,
            modes,
            top_k,
            max_hops,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.consensus(&query, &modes, top_k, max_hops, agent_id.as_deref()));
        }
        VenturiCommand::TemporalWithBudgetAndProof {
            subject,
            from,
            to,
            max_tokens,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.temporal_with_budget_and_proof(
                &subject,
                &from,
                &to,
                max_tokens,
                agent_id.as_deref(),
            ));
        }
        VenturiCommand::StructuredWithBudgetAndProof {
            filter,
            max_tokens,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.structured_with_budget_and_proof(
                filter,
                max_tokens,
                agent_id.as_deref(),
            ));
        }
        VenturiCommand::MetadataWithProof {
            filter,
            agent_id,
            reply,
        } => {
            let _ = reply.send(venturi.metadata_with_proof(filter, agent_id.as_deref()));
        }
        VenturiCommand::RecordVerdict {
            parent_id,
            orb_ids,
            expected_orb_ids,
            verdict,
            reply,
        } => {
            let _ = reply.send(venturi.record_verdict(
                &parent_id,
                &orb_ids,
                &expected_orb_ids,
                verdict,
            ));
        }
        VenturiCommand::RetrievalProof {
            retrieval_audit_id,
            reply,
        } => {
            let _ = reply.send(venturi.retrieval_proof(&retrieval_audit_id));
        }
        VenturiCommand::SetLegalHold {
            parent_id,
            reason,
            reply,
        } => {
            let _ = reply.send(venturi.set_legal_hold(&parent_id, &reason));
        }
        VenturiCommand::ReleaseLegalHold { parent_id, reply } => {
            let _ = reply.send(venturi.release_legal_hold(&parent_id));
        }
        VenturiCommand::LinkChains {
            from_parent_id,
            to_parent_id,
            reference_type,
            reply,
        } => {
            let _ =
                reply.send(venturi.link_chains(&from_parent_id, &to_parent_id, &reference_type));
        }
        VenturiCommand::ChainReferences { parent_id, reply } => {
            let _ = reply.send(venturi.chain_references(&parent_id));
        }
        VenturiCommand::Foresights { on, reply } => {
            let _ = reply.send(venturi.foresights(&on));
        }
        VenturiCommand::ProcessEmbeddingQueue(reply) => {
            let _ = reply.send(venturi.process_embedding_queue());
        }
        VenturiCommand::SweepAccessMarks(reply) => {
            let _ = reply.send(venturi.sweep_access_marks());
        }
        VenturiCommand::SweepTiers(reply) => {
            let _ = reply.send(venturi.sweep_tiers());
        }
        VenturiCommand::SweepExpiry(reply) => {
            let _ = reply.send(venturi.sweep_expiry());
        }
        VenturiCommand::LifecycleSweep(reply) => {
            let _ = reply.send(venturi.lifecycle_sweep());
        }
        VenturiCommand::SweepCommunities(reply) => {
            let _ = reply.send(venturi.sweep_communities());
        }
        VenturiCommand::RecordDaemonHealth {
            daemon,
            status,
            consecutive_failures,
            details,
            reply,
        } => {
            let _ = reply.send(venturi.record_daemon_health(
                &daemon,
                &status,
                consecutive_failures,
                details.as_deref(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venturi::{StorageLimits, VenturiConfig};

    /// A full channel with nobody draining it — deterministic, no timing
    /// dependency on how fast a real worker thread happens to run.
    #[tokio::test]
    async fn channel_full_returns_overloaded() {
        let (tx, _rx) = mpsc::channel(1);
        let sender = CommandSender { tx };

        let (reply, _first) = oneshot::channel();
        sender
            .try_send(VenturiCommand::Capabilities(reply))
            .expect("first send fills the only slot");

        let (reply, _second) = oneshot::channel();
        let error = sender
            .try_send(VenturiCommand::Capabilities(reply))
            .expect_err("channel is now at capacity");

        assert!(matches!(error, WorkerError::Overloaded { .. }));
    }

    fn spawn_test_worker() -> (CommandSender, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let venturi = Venturi::open(VenturiConfig {
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
        })
        .unwrap();
        (spawn_worker(venturi), dir)
    }

    fn ingest_request(topic: &str, chunks: Vec<Vec<u8>>) -> IngestionRequest {
        IngestionRequest {
            agent_id: "concurrent-agent".to_string(),
            topic: topic.to_string(),
            domain: "test".to_string(),
            date: "2026-05-29".to_string(),
            format: "text".to_string(),
            classification: "internal".to_string(),
            summary: format!("summary for {topic}"),
            answer_facts: vec![],
            answer_fact_atoms: vec![],
            foresights: vec![],
            summary_author: "concurrent-agent".to_string(),
            summary_model: None,
            summary_verified: false,
            summary_verified_at: None,
            pinned: None,
            content_type: None,
            table_summary: None,
            chunks,
        }
    }

    /// The single-owner worker replaces `Mutex<Venturi>` as the thing that
    /// keeps concurrent requests from corrupting each other's state. Fire two
    /// ingests from the same agent without awaiting either first, then prove
    /// neither chain picked up bytes from the other.
    #[tokio::test]
    async fn concurrent_ingests_do_not_interleave_or_corrupt_chains() {
        let (sender, _dir) = spawn_test_worker();

        let req_a = ingest_request("topic-a", vec![b"chunk-a0".to_vec(), b"chunk-a1".to_vec()]);
        let req_b = ingest_request("topic-b", vec![b"chunk-b0".to_vec(), b"chunk-b1".to_vec()]);

        let (result_a, result_b) = tokio::join!(sender.ingest(req_a), sender.ingest(req_b));
        let result_a = result_a.expect("ingest a succeeds");
        let result_b = result_b.expect("ingest b succeeds");

        let doc_a = sender
            .document_by_parent_id_with_proof(result_a.parent_id, None)
            .await
            .expect("chain a reassembles");
        let doc_b = sender
            .document_by_parent_id_with_proof(result_b.parent_id, None)
            .await
            .expect("chain b reassembles");

        assert_eq!(doc_a.value, b"chunk-a0chunk-a1");
        assert_eq!(doc_b.value, b"chunk-b0chunk-b1");
    }
}
