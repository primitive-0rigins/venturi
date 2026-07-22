use rusqlite::{params, Connection};
use tempfile::TempDir;
use venturi::{IngestionRequest, OrbId, OrbShelf, StorageLimits, Venturi, VenturiConfig};

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

fn request(content: &[u8]) -> IngestionRequest {
    IngestionRequest {
        agent_id: "hold-agent".to_string(),
        topic: "hold".to_string(),
        domain: "test".to_string(),
        date: "2026-05-29".to_string(),
        format: "text".to_string(),
        classification: "internal".to_string(),
        summary: "Hold Document".to_string(),
        answer_facts: vec![],
        answer_fact_atoms: vec![],
        foresights: vec![],
        summary_author: "hold-agent".to_string(),
        summary_model: None,
        summary_verified: false,
        summary_verified_at: None,
        pinned: None,
        content_type: None,
        table_summary: None,
        chunks: vec![content.to_vec()],
    }
}

fn age_chain(root: &str, parent_id: &str) {
    let conn = Connection::open(format!("{}/librarian.db", root)).unwrap();
    conn.execute(
        "UPDATE orbs SET last_accessed = '0Z' WHERE parent_id = ?1",
        params![parent_id],
    )
    .unwrap();
}

#[test]
fn expiry_tombstones_catalog_and_removes_bytes_and_key() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v.ingest(request(b"expires")).unwrap();
    age_chain(&root, &result.parent_id);

    let report = v.sweep_expiry().unwrap();
    assert_eq!(report.chains_affected, 1);
    assert_eq!(report.orbs_ejected, 1);

    let shelf = OrbShelf::new(format!("{}/shelf", root));
    let orb_id = OrbId::from_hex(&result.orb_ids[0]).unwrap();
    assert!(!shelf.exists(&orb_id));

    let librarian = Connection::open(format!("{}/librarian.db", root)).unwrap();
    let expired_at: Option<String> = librarian
        .query_row(
            "SELECT expired_at FROM orbs WHERE parent_id = ?1",
            [&result.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(expired_at.is_some());

    let keystore = Connection::open(format!("{}/keystore.db", root)).unwrap();
    let key_count: i64 = keystore
        .query_row(
            "SELECT COUNT(*) FROM keys WHERE parent_id = ?1",
            [&result.parent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(key_count, 0);
}

#[test]
fn legal_hold_blocks_expiry_until_released() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let mut v = Venturi::open(config(&dir)).unwrap();
    let result = v.ingest(request(b"held")).unwrap();
    age_chain(&root, &result.parent_id);

    v.set_legal_hold(&result.parent_id, "litigation hold")
        .unwrap();
    let held_report = v.sweep_expiry().unwrap();
    assert_eq!(held_report.chains_affected, 0);
    assert_eq!(held_report.orbs_ejected, 0);

    let shelf = OrbShelf::new(format!("{}/shelf", root));
    let orb_id = OrbId::from_hex(&result.orb_ids[0]).unwrap();
    assert!(shelf.exists(&orb_id));

    v.release_legal_hold(&result.parent_id).unwrap();
    let expired_report = v.sweep_expiry().unwrap();
    assert_eq!(expired_report.chains_affected, 1);
    assert!(!shelf.exists(&orb_id));
}
