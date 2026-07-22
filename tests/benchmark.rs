use tempfile::TempDir;
use venturi::{
    IngestionRequest, NotFoundReason, StorageLimits, StructuredFilter, TunnelError, Venturi,
    VenturiConfig,
};

fn config(dir: &TempDir) -> VenturiConfig {
    let root = dir.path().to_str().unwrap();
    VenturiConfig {
        shelf_root: format!("{}/shelf", root),
        journal_db: format!("{}/journal.db", root),
        keystore_db: format!("{}/keystore.db", root),
        librarian_db: format!("{}/librarian.db", root),
        scribe_db: format!("{}/scribe.db", root),
        graph_db: format!("{}/graph.db", root),
        ollama_url: "http://127.0.0.1:9".to_string(),
        embedding_model: None,
        embedding_dim: None,
        lifecycle: None,
        limits: StorageLimits::default(),
    }
}

fn request(
    topic: &str,
    domain: &str,
    date: &str,
    summary: &str,
    content: &[u8],
) -> IngestionRequest {
    IngestionRequest {
        agent_id: "benchmark-agent".to_string(),
        topic: topic.to_string(),
        domain: domain.to_string(),
        date: date.to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: summary.to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "benchmark-agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![content.to_vec()],
    }
}

fn open() -> (TempDir, Venturi) {
    let dir = TempDir::new().unwrap();
    let v = Venturi::open(config(&dir)).expect("open failed");
    (dir, v)
}

#[test]
fn bench_01_basic_exact_single_orb() {
    let (_dir, mut v) = open();
    let content = b"basic exact retrieval content";
    let result = v
        .ingest(request(
            "basic",
            "test",
            "2026-05-01",
            "Basic Exact",
            content,
        ))
        .expect("ingest failed");

    let (doc, warnings) = v
        .document_by_parent_id(&result.parent_id, None)
        .expect("document retrieval failed");

    assert!(warnings.is_empty());
    assert_eq!(doc, content);
}

#[test]
fn bench_02_constrained_filter_selects_one() {
    let (_dir, mut v) = open();
    v.ingest(request(
        "case",
        "medical",
        "2026-05-01",
        "Case Medical",
        b"medical case",
    ))
    .unwrap();
    v.ingest(request(
        "case",
        "legal",
        "2026-05-01",
        "Case Legal",
        b"legal case",
    ))
    .unwrap();
    v.ingest(request(
        "case",
        "finance",
        "2026-05-01",
        "Case Finance",
        b"finance case",
    ))
    .unwrap();

    let filter = StructuredFilter {
        domain: Some("legal".to_string()),
        ..Default::default()
    };
    let (chunks, warnings) = v.structured(filter, None).expect("structured failed");

    assert!(warnings.is_empty());
    assert_eq!(chunks, vec![b"legal case".to_vec()]);
}

#[test]
fn bench_03_info_not_found_is_typed() {
    let (_dir, v) = open();
    let err = v
        .graph_query("MissingConcept", 2, None)
        .expect_err("expected no match");

    match err {
        TunnelError::MemoryNotFound { reason, .. } => {
            assert_eq!(reason, NotFoundReason::GraphNoMatch)
        }
        other => panic!("wrong error: {:?}", other),
    }
}

#[test]
fn bench_04_completeness_returns_all_related_orbs() {
    let (_dir, mut v) = open();
    for idx in 1..=3 {
        let content = format!("incident evidence {}", idx);
        v.ingest(request(
            "incident",
            "security",
            "2026-05-01",
            "Incident Evidence",
            content.as_bytes(),
        ))
        .unwrap();
    }

    let filter = StructuredFilter {
        topic: Some("incident".to_string()),
        ..Default::default()
    };
    let (chunks, warnings) = v.structured(filter, None).expect("structured failed");

    assert!(warnings.is_empty());
    assert_eq!(chunks.len(), 3);
}

#[test]
fn bench_05_conflicting_info_returns_both() {
    let (_dir, mut v) = open();
    v.ingest(request(
        "policy",
        "legal",
        "2026-05-01",
        "Policy Active",
        b"policy is active",
    ))
    .unwrap();
    v.ingest(request(
        "policy",
        "legal",
        "2026-05-02",
        "Policy Inactive",
        b"policy is inactive",
    ))
    .unwrap();

    let filter = StructuredFilter {
        topic: Some("policy".to_string()),
        ..Default::default()
    };
    let (chunks, warnings) = v.structured(filter, None).expect("structured failed");

    assert!(warnings.is_empty());
    assert!(chunks.contains(&b"policy is active".to_vec()));
    assert!(chunks.contains(&b"policy is inactive".to_vec()));
}

#[test]
fn bench_06_intra_document_full_chain_assembly() {
    let (_dir, mut v) = open();
    let req = IngestionRequest {
        chunks: vec![
            b"section one ".to_vec(),
            b"section two ".to_vec(),
            b"section three".to_vec(),
        ],
        ..request("long_doc", "legal", "2026-05-01", "Long Document", b"")
    };
    let result = v.ingest(req).expect("ingest failed");

    let (doc, warnings) = v.document_by_parent_id(&result.parent_id, None).unwrap();

    assert!(warnings.is_empty());
    assert_eq!(doc, b"section one section two section three".to_vec());
}

#[test]
fn bench_07_stale_superseded_returns_versions_by_date() {
    let (_dir, mut v) = open();
    v.ingest(request(
        "procedure",
        "medical",
        "2026-01-01",
        "Procedure V1",
        b"procedure v1",
    ))
    .unwrap();
    v.ingest(request(
        "procedure",
        "medical",
        "2026-05-01",
        "Procedure V2",
        b"procedure v2",
    ))
    .unwrap();

    let (chunks, warnings) = v
        .temporal("procedure", "2026-01-01", "2026-12-31", None)
        .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(
        chunks,
        vec![b"procedure v1".to_vec(), b"procedure v2".to_vec()]
    );
}

#[test]
fn bench_08_semantic_context_soft_check() {
    let (_dir, mut v) = open();
    v.ingest(request(
        "cardiology",
        "medical",
        "2026-05-01",
        "Cardiac Admission",
        b"heart intake",
    ))
    .unwrap();

    match v.context("heart patient", 5, None) {
        Ok((chunks, _)) => assert!(!chunks.is_empty()),
        Err(TunnelError::MemoryNotFound { reason, .. }) => {
            assert!(matches!(
                reason,
                NotFoundReason::EmbeddingUnavailable | NotFoundReason::NoSimilarContent
            ));
        }
        Err(other) => panic!("unexpected semantic error: {:?}", other),
    }
}

#[test]
fn bench_09_completeness_recall_signal_records() {
    let (_dir, mut v) = open();
    let result = v
        .ingest(request(
            "recall",
            "test",
            "2026-05-01",
            "Recall",
            b"recall content",
        ))
        .unwrap();

    v.record_verdict(&result.parent_id, &result.orb_ids, &result.orb_ids, 1)
        .expect("verdict failed");
}

#[test]
fn bench_10_high_level_consensus_uses_graph_overlay() {
    let (_dir, mut v) = open();
    v.ingest(request(
        "alpha",
        "test",
        "2026-05-01",
        "Alpha Beta",
        b"alpha beta",
    ))
    .unwrap();
    v.ingest(request(
        "beta",
        "test",
        "2026-05-01",
        "Beta Gamma",
        b"beta gamma",
    ))
    .unwrap();

    let modes = vec!["graph".to_string()];
    let result = v
        .consensus("Alpha", &modes, 5, 1, None)
        .expect("consensus failed");

    assert!(result.core_chunks.is_empty());
    assert_eq!(result.supplementary_chunks.len(), 2);
}
