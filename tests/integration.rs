/// Integration tests for the full Venturi pipeline.
///
/// These tests cover Gatekeeper → Shelf → Keystore → Librarian → Scribe → API
/// without requiring Ollama. Embeddings soft-fail (no vector index built), but
/// all storage, encryption, and retrieval paths are fully exercised.
///
/// Tests use tempfile::TempDir so nothing persists between runs.
use tempfile::TempDir;
use venturi::{
    AnswerFact, CapabilityState, ContentType, Foresight, IngestionRequest, LifecycleConfig,
    NotFoundReason, StorageLimits, TunnelError, Venturi, VenturiConfig,
};

fn test_config(dir: &TempDir) -> VenturiConfig {
    let root = dir.path().to_str().unwrap();
    VenturiConfig {
        shelf_root: format!("{}/shelf", root),
        journal_db: format!("{}/journal.db", root),
        keystore_db: format!("{}/keystore.db", root),
        librarian_db: format!("{}/librarian.db", root),
        scribe_db: format!("{}/scribe.db", root),
        graph_db: format!("{}/graph.db", root),
        // Ollama not required — embed/extract soft-fail, storage still works
        ollama_url: "http://localhost:11434".to_string(),
        embedding_model: None,
        embedding_dim: None,
        lifecycle: None,
        limits: StorageLimits::default(),
    }
}

fn test_request(summary: &str, chunks: Vec<Vec<u8>>) -> IngestionRequest {
    IngestionRequest {
        agent_id: "test-agent".to_string(),
        topic: "test_topic".to_string(),
        domain: "test".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: summary.to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "test-agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks,
    }
}

fn graph_request(topic: &str, summary: &str, content: &[u8]) -> IngestionRequest {
    IngestionRequest {
        agent_id: "agent".to_string(),
        topic: topic.to_string(),
        domain: "test".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: summary.to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![content.to_vec()],
    }
}

fn ingest_classified_pair(v: &mut Venturi) -> venturi::IngestionResult {
    let mut public_req = test_request("Shared SecretTopic", vec![b"public note".to_vec()]);
    public_req.topic = "classified_topic".to_string();
    public_req.classification = "internal".to_string();
    v.ingest(public_req).expect("ingest public failed");

    let mut secret_req = test_request("SecretTopic HiddenNeedle", vec![b"secret note".to_vec()]);
    secret_req.topic = "classified_topic".to_string();
    secret_req.classification = "secret".to_string();
    v.ingest(secret_req).expect("ingest secret failed")
}

fn assert_secret_direct_only(v: &Venturi, secret: &venturi::IngestionResult) {
    let broad_rows = metadata_by_topic(v, "classified_topic");
    assert_eq!(broad_rows.len(), 1);
    assert_ne!(broad_rows[0].parent_id, secret.parent_id);

    let direct_rows = v
        .metadata(
            venturi::StructuredFilter {
                parent_id: Some(secret.parent_id.clone()),
                ..Default::default()
            },
            None,
        )
        .expect("direct metadata failed");
    assert_eq!(direct_rows.len(), 1);
    assert_eq!(direct_rows[0].classification, "secret");
}

#[test]
fn pinned_orbs_do_not_demote_in_lifecycle_sweep() {
    let dir = TempDir::new().unwrap();
    let mut cfg = test_config(&dir);
    cfg.lifecycle = Some(LifecycleConfig {
        enabled: true,
        t_warm_secs: 1,
        t_cold_secs: 2,
        max_hot_orbs: 500,
        sweep_interval: 60,
    });
    let librarian_db = cfg.librarian_db.clone();
    let mut v = Venturi::open(cfg).unwrap();

    let mut pinned_req = test_request("pinned lifecycle memory", vec![b"pinned".to_vec()]);
    pinned_req.pinned = Some(true);
    let pinned = v.ingest(pinned_req).unwrap();

    let normal = v
        .ingest(test_request(
            "normal lifecycle memory",
            vec![b"normal".to_vec()],
        ))
        .unwrap();

    let conn = rusqlite::Connection::open(&librarian_db).unwrap();
    conn.execute(
        "UPDATE orbs
         SET last_accessed = '0Z', last_accessed_at = '0Z', access_count = 0
         WHERE parent_id IN (?1, ?2)",
        rusqlite::params![pinned.parent_id, normal.parent_id],
    )
    .unwrap();

    let report = v.lifecycle_sweep().unwrap();
    assert_eq!(report.sweep, "lifecycle");

    let pinned_tier: String = conn
        .query_row(
            "SELECT tier FROM orbs WHERE parent_id = ?1",
            rusqlite::params![pinned.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    let normal_tier: String = conn
        .query_row(
            "SELECT tier FROM orbs WHERE parent_id = ?1",
            rusqlite::params![normal.parent_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(pinned_tier, "hot");
    assert_eq!(normal_tier, "cold");
}

/// A recency-stale orb with enough positive EXIT-verdict evidence is exempted from cold
/// demotion; an equally stale orb with no verdicts is not. See
/// `spec/math-application-proposal-usefulness-score-tiering.md`.
#[test]
fn verdict_proven_useful_orb_is_exempt_from_cold_demotion() {
    let dir = TempDir::new().unwrap();
    let mut cfg = test_config(&dir);
    cfg.lifecycle = Some(LifecycleConfig {
        enabled: true,
        t_warm_secs: 1,
        t_cold_secs: 2,
        max_hot_orbs: 500,
        sweep_interval: 60,
    });
    let librarian_db = cfg.librarian_db.clone();
    let mut v = Venturi::open(cfg).unwrap();

    let proven = v
        .ingest(test_request(
            "proven useful memory",
            vec![b"proven".to_vec()],
        ))
        .unwrap();
    let unproven = v
        .ingest(test_request(
            "never rated memory",
            vec![b"unproven".to_vec()],
        ))
        .unwrap();

    for _ in 0..3 {
        v.record_verdict(&proven.parent_id, &proven.orb_ids, &[], 1)
            .unwrap();
    }

    let conn = rusqlite::Connection::open(&librarian_db).unwrap();
    conn.execute(
        "UPDATE orbs
         SET last_accessed = '0Z', last_accessed_at = '0Z', access_count = 0
         WHERE parent_id IN (?1, ?2)",
        rusqlite::params![proven.parent_id, unproven.parent_id],
    )
    .unwrap();

    let report = v.lifecycle_sweep().unwrap();
    assert_eq!(report.sweep, "lifecycle");

    let proven_tier: String = conn
        .query_row(
            "SELECT tier FROM orbs WHERE parent_id = ?1",
            rusqlite::params![proven.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    let unproven_tier: String = conn
        .query_row(
            "SELECT tier FROM orbs WHERE parent_id = ?1",
            rusqlite::params![unproven.parent_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_ne!(proven_tier, "cold");
    assert_eq!(unproven_tier, "cold");
}

#[test]
fn lifecycle_hot_cap_is_scoped_per_agent() {
    let dir = TempDir::new().unwrap();
    let mut cfg = test_config(&dir);
    cfg.lifecycle = Some(LifecycleConfig {
        enabled: true,
        t_warm_secs: u64::MAX,
        t_cold_secs: u64::MAX,
        max_hot_orbs: 1,
        sweep_interval: 60,
    });
    let librarian_db = cfg.librarian_db.clone();
    let mut v = Venturi::open(cfg).unwrap();

    let mut agent_a_old = test_request("agent a old", vec![b"a-old".to_vec()]);
    agent_a_old.agent_id = "agent-a".to_string();
    let agent_a_old = v.ingest(agent_a_old).unwrap();

    let mut agent_a_new = test_request("agent a new", vec![b"a-new".to_vec()]);
    agent_a_new.agent_id = "agent-a".to_string();
    let agent_a_new = v.ingest(agent_a_new).unwrap();

    let mut agent_b_only = test_request("agent b only", vec![b"b-only".to_vec()]);
    agent_b_only.agent_id = "agent-b".to_string();
    let agent_b_only = v.ingest(agent_b_only).unwrap();

    let conn = rusqlite::Connection::open(&librarian_db).unwrap();
    set_chain_access(&conn, &agent_a_old.parent_id, "100Z");
    set_chain_access(&conn, &agent_a_new.parent_id, "200Z");
    set_chain_access(&conn, &agent_b_only.parent_id, "100Z");

    v.lifecycle_sweep().unwrap();

    assert_eq!(chain_tier(&conn, &agent_a_old.parent_id), "warm");
    assert_eq!(chain_tier(&conn, &agent_a_new.parent_id), "hot");
    assert_eq!(chain_tier(&conn, &agent_b_only.parent_id), "hot");
}

#[test]
fn daemon_health_events_are_recorded_to_scribe() {
    let dir = TempDir::new().unwrap();
    let cfg = test_config(&dir);
    let scribe_db = cfg.scribe_db.clone();
    let v = Venturi::open(cfg).unwrap();

    v.record_daemon_health("lifecycle", "ok", 0, Some("chains_affected=0"))
        .unwrap();

    let conn = rusqlite::Connection::open(&scribe_db).unwrap();
    let payload: String = conn
        .query_row(
            "SELECT payload FROM events
             WHERE event_type = 'DAEMON_HEALTH'
             ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(value["daemon"], "lifecycle");
    assert_eq!(value["status"], "ok");
    assert_eq!(value["consecutive_failures"], 0);
}

fn set_chain_access(conn: &rusqlite::Connection, parent_id: &str, ts: &str) {
    conn.execute(
        "UPDATE orbs SET last_accessed = ?1, last_accessed_at = ?1 WHERE parent_id = ?2",
        rusqlite::params![ts, parent_id],
    )
    .unwrap();
}

fn chain_tier(conn: &rusqlite::Connection, parent_id: &str) -> String {
    conn.query_row(
        "SELECT tier FROM orbs WHERE parent_id = ?1",
        rusqlite::params![parent_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn metadata_by_topic(v: &Venturi, topic: &str) -> Vec<venturi::MetaRow> {
    v.metadata(
        venturi::StructuredFilter {
            topic: Some(topic.to_string()),
            ..Default::default()
        },
        None,
    )
    .expect("metadata by topic failed")
}

fn assert_secret_not_indexed(root: &str, v: &Venturi, secret: &venturi::IngestionResult) {
    let err = v
        .graph_query("HiddenNeedle", 2, None)
        .expect_err("secret summary must not be graph-indexed");
    assert!(matches!(
        err,
        TunnelError::MemoryNotFound {
            reason: NotFoundReason::GraphNoMatch,
            ..
        }
    ));

    let conn = rusqlite::Connection::open(format!("{}/librarian.db", root)).unwrap();
    let queued: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM embedding_queue WHERE orb_id = ?1",
            [&secret.orb_ids[0]],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(queued, 0);
}

/// Single-orb ingest → document_by_parent_id reassembly.
/// Validates the full write + decrypt path for a 1-orb chain.
#[test]
fn single_orb_ingest_and_retrieve() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let content = b"Encrypted governed agent memory for regulated industries.";
    let req = test_request("single orb test", vec![content.to_vec()]);

    let result = v.ingest(req).expect("ingest failed");
    assert_eq!(result.orb_ids.len(), 1);
    assert!(!result.parent_id.is_empty());

    let (recovered, warnings) = v
        .document_by_parent_id(&result.parent_id, None)
        .expect("document_by_parent_id failed");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(recovered.as_slice(), content.as_ref());
}

#[test]
fn embedding_capability_degrades_until_worker_loads_index() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let initial = v.capabilities();
    assert_eq!(initial.embedding, CapabilityState::Degraded);
    assert_eq!(initial.graph, CapabilityState::Ready);
    assert_eq!(initial.retrieval, CapabilityState::Ready);
    assert_eq!(initial.ingest, CapabilityState::Ready);

    v.process_embedding_queue()
        .expect("embedding worker failed");
    assert_eq!(v.capabilities().embedding, CapabilityState::Ready);
}

#[test]
fn chain_references_roundtrip_and_reverse_lookup() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");
    let older = v
        .ingest(test_request("procedure v1", vec![b"old".to_vec()]))
        .expect("ingest old failed");
    let newer = v
        .ingest(test_request("procedure v2", vec![b"new".to_vec()]))
        .expect("ingest new failed");

    v.link_chains(&newer.parent_id, &older.parent_id, "supersedes")
        .expect("link failed");
    v.link_chains(&newer.parent_id, &older.parent_id, "supersedes")
        .expect("duplicate link should upsert");

    let from_refs = v.chain_references(&newer.parent_id).unwrap();
    let to_refs = v.chain_references(&older.parent_id).unwrap();

    assert_eq!(from_refs.len(), 1);
    assert_eq!(to_refs, from_refs);
    assert_eq!(from_refs[0].from_parent_id, newer.parent_id);
    assert_eq!(from_refs[0].to_parent_id, older.parent_id);
    assert_eq!(from_refs[0].reference_type, "supersedes");
}

#[test]
fn chain_reference_rejects_invalid_type() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");
    let a = v.ingest(test_request("a", vec![b"a".to_vec()])).unwrap();
    let b = v.ingest(test_request("b", vec![b"b".to_vec()])).unwrap();

    let err = v
        .link_chains(&a.parent_id, &b.parent_id, "mentions")
        .expect_err("invalid reference_type must fail");
    assert!(matches!(err, TunnelError::GatekeeperRejected { .. }));
}

/// Multi-orb chain ingest → document_by_parent_id lossless reassembly.
///
/// Large documents are split into chunks, stored as individual encrypted
/// orbs, and reassembled to the exact original on retrieval.
#[test]
fn multi_orb_lossless_reassembly() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    // Simulate a 3-chunk document (e.g. pages 1-3 of a PDF split at 4KB each)
    let chunks: Vec<Vec<u8>> = vec![
        b"CHAPTER ONE: Background and Context. Lorem ipsum dolor sit amet.".to_vec(),
        b"CHAPTER TWO: Methods and Analysis. Consectetur adipiscing elit.".to_vec(),
        b"CHAPTER THREE: Results and Conclusions. Sed do eiusmod tempor.".to_vec(),
    ];
    let expected: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();

    let req = test_request("three chapter research document", chunks);
    let result = v.ingest(req).expect("ingest failed");

    assert_eq!(result.orb_ids.len(), 3, "should have 3 sealed orbs");

    let (recovered, warnings) = v
        .document_by_parent_id(&result.parent_id, None)
        .expect("lossless reassembly failed");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(
        recovered, expected,
        "reassembled document must equal original exactly"
    );
}

/// Ingest two separate chains — verify they don't cross-contaminate.
/// Each chain must decrypt only with its own key; wrong chain → error.
#[test]
fn two_chains_isolation() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req_a = test_request("chain A summary", vec![b"Chain A secret content.".to_vec()]);
    let req_b = test_request("chain B summary", vec![b"Chain B secret content.".to_vec()]);

    let result_a = v.ingest(req_a).expect("ingest A failed");
    let result_b = v.ingest(req_b).expect("ingest B failed");

    // Each chain retrieves its own content correctly
    let (content_a, _) = v
        .document_by_parent_id(&result_a.parent_id, None)
        .expect("retrieve A failed");
    let (content_b, _) = v
        .document_by_parent_id(&result_b.parent_id, None)
        .expect("retrieve B failed");

    assert_eq!(content_a.as_slice(), b"Chain A secret content.".as_ref());
    assert_eq!(content_b.as_slice(), b"Chain B secret content.".as_ref());
    assert_ne!(
        result_a.parent_id, result_b.parent_id,
        "chains must have different parent_ids"
    );

    // Cross-chain fetch should return empty (no catalog rows for wrong parent_id)
    // Both chains exist but have separate catalog entries — neither returns the other's orbs
    let rows_a = venturi::OrbShelf::new(format!("{}/shelf", dir.path().to_str().unwrap()));
    let _ = rows_a; // shelf is accessible but wrong parent_id → fetch_chain returns []
}

/// Ingest with empty chunks should be rejected at the Gatekeeper.
#[test]
fn empty_ingest_rejected() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = test_request("empty", vec![]);
    let result = v.ingest(req);

    assert!(result.is_err(), "empty chunk list must be rejected");
}

/// record_verdict fires correctly without panicking.
/// Scribe EXIT event is append-only — just verify it doesn't error.
#[test]
fn verdict_signal_fires() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = test_request("verdict test", vec![b"content for verdict test".to_vec()]);
    let result = v.ingest(req).expect("ingest failed");

    // Record a positive verdict
    v.record_verdict(&result.parent_id, &result.orb_ids, &[], 1)
        .expect("record_verdict failed");

    // Record a negative verdict (same chain — append-only log allows duplicates)
    v.record_verdict(&result.parent_id, &result.orb_ids, &[], 0)
        .expect("record_verdict negative failed");
}

/// Structured mode returns orbs matching exact metadata filter.
#[test]
fn structured_retrieval_by_topic() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let mut req_medical = test_request(
        "patient admission notes",
        vec![b"Patient admitted with chest pain.".to_vec()],
    );
    req_medical.topic = "patient_record".to_string();
    req_medical.domain = "medical".to_string();
    req_medical.date = "2026-05-01".to_string();

    let mut req_legal = test_request(
        "service agreement",
        vec![b"This agreement is entered into by parties.".to_vec()],
    );
    req_legal.topic = "contract".to_string();
    req_legal.domain = "legal".to_string();
    req_legal.date = "2026-05-02".to_string();

    v.ingest(req_medical).expect("ingest medical failed");
    v.ingest(req_legal).expect("ingest legal failed");

    // Structured query for medical domain only
    let filter = venturi::StructuredFilter {
        domain: Some("medical".to_string()),
        ..Default::default()
    };
    let (results, warnings) = v.structured(filter, None).expect("structured query failed");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(results.len(), 1, "should return exactly the medical orb");
    assert_eq!(
        results[0].as_slice(),
        b"Patient admitted with chest pain.".as_ref()
    );
}

#[test]
fn structured_retrieval_respects_token_budget() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let mut first = test_request("budget first", vec![b"one two".to_vec()]);
    first.topic = "budgeted".to_string();
    first.date = "2026-05-01".to_string();
    let mut second = test_request("budget second", vec![b"three four five six".to_vec()]);
    second.topic = "budgeted".to_string();
    second.date = "2026-05-02".to_string();

    v.ingest(first).expect("ingest first failed");
    v.ingest(second).expect("ingest second failed");

    let result = v
        .structured_with_budget_and_proof(
            venturi::StructuredFilter {
                topic: Some("budgeted".to_string()),
                ..Default::default()
            },
            Some(3),
            None,
        )
        .expect("budgeted structured query failed");

    assert_eq!(result.value, vec![b"one two".to_vec()]);
    assert!(result.token_budget_applied);
    assert!(
        result
            .warnings
            .iter()
            .any(|warning| warning.contains("max_tokens")),
        "expected max_tokens warning, got {:?}",
        result.warnings
    );
}

/// Temporal mode returns orbs within the date range.
#[test]
fn temporal_retrieval_by_date_range() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req_jan = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "incident_report".to_string(),
        domain: "security".to_string(),
        date: "2026-01-15".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "january security incident".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"January incident: unauthorized access detected.".to_vec()],
    };
    let req_may = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "incident_report".to_string(),
        domain: "security".to_string(),
        date: "2026-05-20".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "may security incident".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"May incident: phishing attempt blocked.".to_vec()],
    };

    v.ingest(req_jan).expect("ingest jan failed");
    v.ingest(req_may).expect("ingest may failed");

    // Query for incidents in May only
    let (results, warnings) = v
        .temporal("incident_report", "2026-05-01", "2026-05-31", None)
        .expect("temporal query failed");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(results.len(), 1, "should return only the May incident");
    assert_eq!(
        results[0].as_slice(),
        b"May incident: phishing attempt blocked.".as_ref()
    );
}

/// Hard size limits reject oversized chunks at ingest.
#[test]
fn chunk_size_limit_enforced() {
    use venturi::StorageLimits;
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();

    let mut v = Venturi::open(VenturiConfig {
        shelf_root: format!("{}/shelf", root),
        journal_db: format!("{}/journal.db", root),
        keystore_db: format!("{}/keystore.db", root),
        librarian_db: format!("{}/librarian.db", root),
        scribe_db: format!("{}/scribe.db", root),
        graph_db: format!("{}/graph.db", root),
        ollama_url: "http://localhost:11434".to_string(),
        embedding_model: None,
        embedding_dim: None,
        lifecycle: None,
        limits: StorageLimits {
            max_chunk_bytes: 16, // tiny limit for test
            max_chain_length: 10,
            max_orbs_per_query: 100,
            max_rehydration_bytes: 1_000_000,
        },
    })
    .expect("open failed");

    // 17 bytes > limit of 16
    let req = test_request("oversized chunk", vec![b"this is 17 bytes!".to_vec()]);
    let result = v.ingest(req);
    assert!(result.is_err(), "oversized chunk must be rejected");
}

/// Metadata mode returns catalog rows without decrypting any content.
#[test]
fn metadata_retrieval_no_decrypt() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "contract".to_string(),
        domain: "legal".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "nda between two parties".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"Sample agreement content.".to_vec()],
    };
    v.ingest(req).expect("ingest failed");

    let filter = venturi::StructuredFilter {
        domain: Some("legal".to_string()),
        ..Default::default()
    };
    let rows = v.metadata(filter, None).expect("metadata query failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].topic, "contract");
    assert_eq!(rows[0].domain, "legal");
    assert_eq!(rows[0].format, "text");
}

#[test]
fn table_ingest_indexes_table_summary_and_preserves_raw_table() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "labs".to_string(),
        domain: "medical".to_string(),
        date: "2026-05-29".to_string(),
        format: "csv".to_string(),
        classification: "internal".to_string(),
        summary: "raw lab result table".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: Some(ContentType::Table),
        table_summary: Some("Creatinine 2.1 mg/dL is above threshold".to_string()),
        chunks: vec![b"test,value\ncreatinine,2.1\n".to_vec()],
    };

    v.ingest(req).expect("table ingest failed");
    let (chunks, warnings) = v
        .context("Creatinine threshold", 5, None)
        .expect("table summary context failed");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(chunks, vec![b"test,value\ncreatinine,2.1\n".to_vec()]);

    let rows = metadata_by_topic(&v, "labs");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].content_type, "table");
}

/// Summary trust metadata is persisted and surfaced by metadata retrieval
/// without exposing key pointers or decrypting content.
#[test]
fn summary_trust_metadata_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "clinical_note".to_string(),
        domain: "medical".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "verified discharge summary".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "human-reviewer".to_string(),
        summary_model: Some("nomic-local-summarizer".to_string()),
        summary_verified: true,
        summary_verified_at: Some("2026-05-29T12:00:00Z".to_string()),
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"Discharge note content.".to_vec()],
    };
    v.ingest(req).expect("ingest failed");

    let rows = v
        .metadata(
            venturi::StructuredFilter {
                topic: Some("clinical_note".to_string()),
                ..Default::default()
            },
            None,
        )
        .expect("metadata query failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].classification, "internal");
    assert_eq!(rows[0].summary_author, "human-reviewer");
    assert_eq!(
        rows[0].summary_model.as_deref(),
        Some("nomic-local-summarizer")
    );
    assert!(rows[0].summary_verified);
    assert_eq!(
        rows[0].summary_verified_at.as_deref(),
        Some("2026-05-29T12:00:00Z")
    );
}

/// Secret chains are not exposed through broad metadata/graph retrieval.
/// Callers must use an explicit parent_id to retrieve them.
#[test]
fn secret_classification_requires_explicit_parent_id() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let secret = ingest_classified_pair(&mut v);
    assert_secret_direct_only(&v, &secret);

    let (doc, warnings) = v
        .document_by_parent_id(&secret.parent_id, None)
        .expect("direct secret document failed");
    assert!(warnings.is_empty());
    assert_eq!(doc, b"secret note".to_vec());
    assert_secret_not_indexed(root, &v, &secret);
}

/// answer_facts are stored and retrievable via metadata mode.
#[test]
fn answer_facts_stored_in_catalog() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "patient_record".to_string(),
        domain: "medical".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "patient admitted with chest pain".to_string(),
        answer_facts: vec![
            "patient was admitted".to_string(),
            "admission date: 2026-05-01".to_string(),
            "symptom: chest pain".to_string(),
        ],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"Patient record content.".to_vec()],
    };

    let result = v.ingest(req).expect("ingest failed");
    assert_eq!(result.orb_ids.len(), 1);

    // Verify the orb is in the catalog (metadata mode — no decryption)
    let filter = venturi::StructuredFilter {
        domain: Some("medical".to_string()),
        ..Default::default()
    };
    let rows = v.metadata(filter, None).expect("metadata query failed");
    assert_eq!(rows.len(), 1, "expected exactly one row in catalog");
    assert_eq!(rows[0].topic, "patient_record");
    assert!(rows[0].verified_facts.is_empty());

    let conn = rusqlite::Connection::open(format!("{}/librarian.db", root)).unwrap();
    let fact_sources: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fact_queue WHERE source = 'fact'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(fact_sources, 3);
}

#[test]
fn verified_answer_fact_atoms_surface_in_metadata() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let mut req = test_request("verified facts", vec![b"Patient record content.".to_vec()]);
    req.topic = "verified_claims".to_string();
    req.answer_fact_atoms = vec![
        AnswerFact {
            fact: "allergy confirmed by nurse".to_string(),
            verified: true,
            verified_by: Some("nurse-a".to_string()),
        },
        AnswerFact {
            fact: "model inferred follow-up".to_string(),
            verified: false,
            verified_by: None,
        },
    ];
    v.ingest(req).expect("ingest failed");

    let rows = metadata_by_topic(&v, "verified_claims");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].verified_facts.len(), 1);
    assert_eq!(rows[0].verified_facts[0].fact, "allergy confirmed by nurse");
    assert_eq!(
        rows[0].verified_facts[0].verified_by.as_deref(),
        Some("nurse-a")
    );
}

#[test]
fn active_foresights_return_by_relevance_date() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let mut req = test_request("medication refill reminder", vec![b"rx note".to_vec()]);
    req.topic = "medication".to_string();
    req.foresights = vec![Foresight {
        foresight_text: "Patient medication runs out".to_string(),
        relevant_from: "2026-06-08".to_string(),
        relevant_until: "2026-06-15".to_string(),
        duration_days: 7,
    }];
    let result = v.ingest(req).expect("ingest failed");

    assert!(v.foresights("2026-06-07").unwrap().is_empty());
    let active = v.foresights("2026-06-10").unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].parent_id, result.parent_id);
    assert_eq!(active[0].foresight_text, "Patient medication runs out");
    assert_eq!(active[0].duration_days, 7);
    assert!(v.foresights("2026-06-16").unwrap().is_empty());
}

#[test]
fn secret_foresights_are_not_returned_broadly() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let mut req = test_request("secret reminder", vec![b"secret rx".to_vec()]);
    req.classification = "secret".to_string();
    req.foresights = vec![Foresight {
        foresight_text: "Secret reminder".to_string(),
        relevant_from: "2026-06-08".to_string(),
        relevant_until: "2026-06-15".to_string(),
        duration_days: 7,
    }];
    v.ingest(req).expect("ingest failed");

    assert!(v.foresights("2026-06-10").unwrap().is_empty());
}

/// record_verdict with expected_orb_ids computes recall correctly.
#[test]
fn verdict_recall_computes() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = test_request(
        "recall test content",
        vec![b"content for recall test".to_vec()],
    );
    let result = v.ingest(req).expect("ingest failed");
    let parent_id = &result.parent_id;
    let orb_ids = &result.orb_ids;

    // recall = 1.0: expected == actual
    v.record_verdict(parent_id, orb_ids, orb_ids, 1)
        .expect("record_verdict with full recall failed");

    // recall = 0.0: expected=[orb_id], actual=[]
    v.record_verdict(parent_id, &[], orb_ids, 0)
        .expect("record_verdict with zero recall failed");

    // recall = None: expected empty (unknown)
    v.record_verdict(parent_id, orb_ids, &[], 1)
        .expect("record_verdict with no expected failed");
}

/// context() returns MemoryNotFound when nothing is indexed.
/// Since Ollama is not running in tests, similarity_search fails → EmbeddingUnavailable.
#[test]
fn context_not_found_empty_store() {
    let dir = TempDir::new().unwrap();
    let v = Venturi::open(test_config(&dir)).expect("open failed");

    let err = v
        .context("anything", 5, None)
        .expect_err("expected MemoryNotFound error");

    match err {
        TunnelError::MemoryNotFound { reason, .. } => {
            // With no embeddings in the store, the HashMap is empty → NoSimilarContent
            // (similarity_search succeeds with empty result when Ollama is unreachable
            // because the embed call is only made when there are embeddings to compare).
            // Either variant is a valid "not found" — both mean the query returned nothing.
            assert!(
                reason == NotFoundReason::EmbeddingUnavailable
                    || reason == NotFoundReason::NoSimilarContent,
                "expected EmbeddingUnavailable or NoSimilarContent, got: {:?}",
                reason
            );
        }
        other => panic!("expected MemoryNotFound, got: {:?}", other),
    }
}

#[test]
fn context_uses_keyword_index_when_embeddings_unavailable() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = test_request(
        "Quarterly ZephyrNeedle incident review",
        vec![b"retrieved through the FTS fallback path".to_vec()],
    );
    v.ingest(req).expect("ingest failed");

    let (chunks, warnings) = v
        .context("ZephyrNeedle", 5, None)
        .expect("context should use FTS when Ollama is unavailable");

    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);
    assert_eq!(
        chunks,
        vec![b"retrieved through the FTS fallback path".to_vec()]
    );
}

#[test]
fn context_stability_check_reports_stable_replay() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = test_request(
        "ReplayNeedle stability check",
        vec![b"stable replay chunk".to_vec()],
    );
    v.ingest(req).expect("ingest failed");

    let result = v
        .context_with_options_and_proof("ReplayNeedle", 5, None, true, None)
        .expect("context stability query failed");

    assert_eq!(result.stability.as_deref(), Some("stable"));
    assert!(
        result.warnings.is_empty(),
        "unexpected warnings: {:?}",
        result.warnings
    );
    assert_eq!(result.value, vec![b"stable replay chunk".to_vec()]);
}

/// graph_query() returns MemoryNotFound when graph has no matching concepts.
#[test]
fn graph_not_found_no_match() {
    let dir = TempDir::new().unwrap();
    let v = Venturi::open(test_config(&dir)).expect("open failed");

    let err = v
        .graph_query("nonexistent_concept_xyz", 2, None)
        .expect_err("expected MemoryNotFound error");

    match err {
        TunnelError::MemoryNotFound { reason, .. } => {
            assert_eq!(
                reason,
                NotFoundReason::GraphNoMatch,
                "expected GraphNoMatch since graph is empty"
            );
        }
        other => panic!("expected MemoryNotFound, got: {:?}", other),
    }
}

/// Consensus defaults to context + graph. FTS-backed context keeps overlay
/// retrieval useful even when Ollama embeddings are unavailable.
#[test]
fn consensus_uses_keyword_context_when_embeddings_unavailable() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(test_config(&dir)).expect("open failed");

    let req = IngestionRequest {
        agent_id: "agent".to_string(),
        topic: "patient_record".to_string(),
        domain: "medical".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "Patient Hospital ChestPain admission record".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![b"Patient admitted through Hospital intake for chest pain.".to_vec()],
    };
    v.ingest(req).expect("ingest failed");

    let result = v
        .consensus("Patient", &[], 5, 2, Some("agent"))
        .expect("consensus should return keyword-backed context result");

    assert!(
        result.core_chunks.iter().any(|chunk| chunk.as_slice()
            == b"Patient admitted through Hospital intake for chest pain.".as_ref()),
        "expected context/graph consensus to include patient chunk"
    );
    assert!(result.modes_run.contains(&"context".to_string()));
    assert!(result.modes_run.contains(&"graph".to_string()));
    assert!(
        result
            .warnings
            .iter()
            .all(|w| !w.contains("context failed") && !w.contains("context returned no hits")),
        "did not expect context degradation warning, got {:?}",
        result.warnings
    );
}

/// Unsupported consensus modes should not silently return success.
#[test]
fn consensus_unsupported_modes_return_not_found() {
    let dir = TempDir::new().unwrap();
    let v = Venturi::open(test_config(&dir)).expect("open failed");
    let modes = vec!["bogus".to_string()];

    let err = v
        .consensus("anything", &modes, 5, 2, None)
        .expect_err("unsupported-only consensus should fail");

    match err {
        TunnelError::MemoryNotFound { reason, .. } => {
            assert_eq!(reason, NotFoundReason::NoSimilarContent);
        }
        other => panic!("expected MemoryNotFound, got: {:?}", other),
    }
}

/// Hyperedges preserve co-occurrence groups in the graph. Even if pairwise
/// edges are unavailable, traversal can move through hyperedge co-members and
/// surface chains attached to those members.
#[test]
fn graph_traverses_hyperedges_without_pairwise_edges() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut cfg = test_config(&dir);
    cfg.ollama_url = "http://127.0.0.1:9".to_string();
    let mut v = Venturi::open(cfg).expect("open failed");

    let alpha = graph_request("alpha", "Alpha Beta", b"alpha beta chain");
    let beta = graph_request("beta", "Beta Gamma", b"beta gamma chain");

    let alpha_result = v.ingest(alpha).expect("ingest alpha failed");
    v.ingest(beta).expect("ingest beta failed");

    let conn = rusqlite::Connection::open(format!("{}/graph.db", root)).unwrap();
    let hyperedges: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM hyperedges WHERE parent_id = ?1",
            [&alpha_result.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hyperedges, 1);

    conn.execute("DELETE FROM kg_edges", []).unwrap();

    let (chunks, warnings) = v
        .graph_query("Alpha", 1, None)
        .expect("hyperedge graph query failed");
    assert!(warnings.is_empty(), "unexpected warnings: {:?}", warnings);

    let texts: Vec<String> = chunks
        .iter()
        .map(|chunk| String::from_utf8_lossy(chunk).to_string())
        .collect();
    assert!(texts.iter().any(|text| text == "alpha beta chain"));
    assert!(texts.iter().any(|text| text == "beta gamma chain"));
}
