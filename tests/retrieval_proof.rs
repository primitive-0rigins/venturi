use tempfile::TempDir;
use venturi::{IngestionRequest, StorageLimits, StructuredFilter, Venturi, VenturiConfig};

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
        embedding_model: Some("venturi-test-embed".to_string()),
        embedding_dim: Some(384),
        lifecycle: None,
        limits: StorageLimits::default(),
    }
}

fn request(content: &[u8]) -> IngestionRequest {
    IngestionRequest {
        agent_id: "proof-agent".to_string(),
        topic: "proof".to_string(),
        domain: "test".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "Proof Document".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "proof-agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![content.to_vec()],
    }
}

#[test]
fn document_retrieval_writes_fetchable_proof() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let ingest = v.ingest(request(b"proof content")).unwrap();

    let result = v
        .document_by_parent_id_with_proof(&ingest.parent_id, Some("agent-a"))
        .unwrap();
    let proof = v
        .retrieval_proof(&result.retrieval_audit_id)
        .unwrap()
        .expect("proof missing");

    assert_eq!(result.value, b"proof content".to_vec());
    assert_eq!(proof.retrieval_audit_id, result.retrieval_audit_id);
    assert_eq!(proof.actor_id.as_deref(), Some("agent-a"));
    assert_eq!(proof.mode, "document");
    assert_eq!(proof.query, ingest.parent_id);
    assert_eq!(proof.selected_orb_ids, ingest.orb_ids);
    assert_eq!(proof.selected_parent_ids, vec![ingest.parent_id]);
    assert_eq!(
        proof.embedding_model_version.as_deref(),
        Some("venturi-test-embed:384")
    );
    assert!(proof.key_ids_used.is_empty());
    assert!(proof.chain_complete);
}

#[test]
fn metadata_retrieval_proof_has_filters_and_no_keys() {
    let dir = TempDir::new().unwrap();
    let mut v = Venturi::open(config(&dir)).unwrap();
    v.ingest(request(b"metadata proof")).unwrap();

    let filter = StructuredFilter {
        topic: Some("proof".to_string()),
        ..Default::default()
    };
    let result = v.metadata_with_proof(filter, Some("agent-b")).unwrap();
    let proof = v
        .retrieval_proof(&result.retrieval_audit_id)
        .unwrap()
        .expect("proof missing");

    assert_eq!(result.value.len(), 1);
    assert_eq!(proof.mode, "metadata");
    assert_eq!(proof.filters_applied["topic"], "proof");
    assert_eq!(proof.candidate_count, 1);
    assert!(proof.key_ids_used.is_empty());
    assert!(proof.chain_complete);
}
