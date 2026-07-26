use rusqlite::{params, Connection};
use std::fs;
use tempfile::TempDir;
use venturi::{IngestionRequest, StorageLimits, TunnelError, Venturi, VenturiConfig};

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

fn request(summary: &str, chunks: Vec<Vec<u8>>) -> IngestionRequest {
    IngestionRequest {
        agent_id: "corruption-agent".to_string(),
        topic: "corruption".to_string(),
        domain: "test".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: summary.to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "corruption-agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks,
    }
}

fn orb_path(root: &str, orb_id: &str) -> String {
    format!(
        "{}/shelf/{}/{}/{}",
        root,
        &orb_id[0..2],
        &orb_id[2..4],
        orb_id
    )
}

fn corrupt_orb_file(root: &str, orb_id: &str) {
    let path = orb_path(root, orb_id);
    let mut bytes = fs::read(&path).unwrap();
    let idx = bytes.len() - 1;
    bytes[idx] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

fn count_scribe_failures(root: &str) -> i64 {
    let conn = Connection::open(format!("{}/scribe.db", root)).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM events WHERE event_type = 'RETRIEVAL_FAILURE'",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

fn last_scribe_failure_payload(root: &str) -> serde_json::Value {
    let conn = Connection::open(format!("{}/scribe.db", root)).unwrap();
    let payload: String = conn
        .query_row(
            "SELECT payload FROM events WHERE event_type = 'RETRIEVAL_FAILURE' ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&payload).unwrap()
}

#[test]
fn modified_orb_file_returns_marker_and_audits_failure() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v
        .ingest(request(
            "Modified Orb",
            vec![b"authenticated content".to_vec()],
        ))
        .unwrap();

    corrupt_orb_file(&root, &result.orb_ids[0]);
    let (doc, warnings) = v.document_by_parent_id(&result.parent_id, None).unwrap();

    assert!(!warnings.is_empty());
    assert!(String::from_utf8_lossy(&doc).contains("[VENTURI:CORRUPTED_ORB:"));
    assert!(!doc
        .windows(b"authenticated content".len())
        .any(|w| w == b"authenticated content"));
    assert_eq!(count_scribe_failures(&root), 1);
    assert_eq!(
        last_scribe_failure_payload(&root)["failure_categories"][0],
        "orb_corrupt"
    );
}

#[test]
fn missing_orb_file_returns_marker_and_audits_failure() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v
        .ingest(request("Missing Orb", vec![b"missing content".to_vec()]))
        .unwrap();

    fs::remove_file(orb_path(&root, &result.orb_ids[0])).unwrap();
    let (doc, warnings) = v.document_by_parent_id(&result.parent_id, None).unwrap();

    assert!(!warnings.is_empty());
    assert!(String::from_utf8_lossy(&doc).contains("[VENTURI:CORRUPTED_ORB:"));
    assert_eq!(count_scribe_failures(&root), 1);
}

#[test]
fn wrong_chain_key_returns_marker_and_audits_failure() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v
        .ingest(request("Wrong Key", vec![b"keyed content".to_vec()]))
        .unwrap();

    let librarian = Connection::open(format!("{}/librarian.db", root)).unwrap();
    let key_id: String = librarian
        .query_row(
            "SELECT key_id FROM orbs WHERE orb_id = ?1",
            [&result.orb_ids[0]],
            |row| row.get(0),
        )
        .unwrap();
    let keystore = Connection::open(format!("{}/keystore.db", root)).unwrap();
    keystore
        .execute(
            "UPDATE keys SET raw_key = ?1 WHERE key_id = ?2",
            params![vec![0u8; 32], key_id],
        )
        .unwrap();

    let (doc, warnings) = v.document_by_parent_id(&result.parent_id, None).unwrap();

    assert!(!warnings.is_empty());
    assert!(String::from_utf8_lossy(&doc).contains("[VENTURI:CORRUPTED_ORB:"));
    assert_eq!(count_scribe_failures(&root), 1);
}

#[test]
fn one_corrupted_orb_preserves_other_chain_content() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v
        .ingest(request(
            "Partial Chain",
            vec![b"first ".to_vec(), b"second ".to_vec(), b"third".to_vec()],
        ))
        .unwrap();

    corrupt_orb_file(&root, &result.orb_ids[1]);
    let (doc, warnings) = v.document_by_parent_id(&result.parent_id, None).unwrap();
    let text = String::from_utf8_lossy(&doc);

    assert_eq!(warnings.len(), 1);
    assert!(text.contains("first "));
    assert!(text.contains("[VENTURI:CORRUPTED_ORB:"));
    assert!(text.contains("third"));
}

#[test]
fn startup_recovers_in_progress_journal_entry() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    drop(Venturi::open(config(&dir)).unwrap());

    let conn = Connection::open(format!("{}/journal.db", root)).unwrap();
    conn.execute(
        "INSERT INTO ingestions (parent_id, agent_id, expected_n, status, opened_at)
         VALUES ('stuck-parent', 'agent', 1, 'IN_PROGRESS', '0Z')",
        [],
    )
    .unwrap();

    drop(Venturi::open(config(&dir)).unwrap());
    let status: String = conn
        .query_row(
            "SELECT status FROM ingestions WHERE parent_id = 'stuck-parent'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(status, "FAILED");
}

#[test]
fn invalid_gatekeeper_input_is_rejected_before_journal_open() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let long_summary = (0..101).map(|_| "word").collect::<Vec<_>>().join(" ");

    match v.ingest(request(&long_summary, vec![b"content".to_vec()])) {
        Err(TunnelError::GatekeeperRejected { .. }) => {}
        Err(other) => panic!("wrong error for long summary: {:?}", other),
        Ok(_) => panic!("long summary should be rejected"),
    }
    let conn = Connection::open(format!("{}/journal.db", root)).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ingestions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn invalid_calendar_dates_are_rejected_before_journal_open() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let mut req = request("invalid date", vec![b"content".to_vec()]);
    req.date = "2026-02-29".to_string();

    assert!(matches!(
        v.ingest(req),
        Err(TunnelError::GatekeeperRejected { .. })
    ));

    let conn = Connection::open(format!("{}/journal.db", root)).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ingestions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn multi_orb_ingestion_uses_one_chain_key() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v
        .ingest(request(
            "One Chain Key",
            vec![b"one".to_vec(), b"two".to_vec()],
        ))
        .unwrap();

    let librarian = Connection::open(format!("{}/librarian.db", root)).unwrap();
    let distinct_key_ids: i64 = librarian
        .query_row(
            "SELECT COUNT(DISTINCT key_id) FROM orbs WHERE parent_id = ?1",
            [&result.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    let keystore = Connection::open(format!("{}/keystore.db", root)).unwrap();
    let stored_keys: i64 = keystore
        .query_row(
            "SELECT COUNT(*) FROM keys WHERE parent_id = ?1",
            [&result.parent_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(distinct_key_ids, 1);
    assert_eq!(stored_keys, 1);
}
