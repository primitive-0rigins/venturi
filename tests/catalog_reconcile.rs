/// Regression coverage for the catalog-registration reconciliation gap:
/// previously, an ingestion that sealed content to disk but failed to
/// register in the Librarian catalog was durable-but-unfindable forever —
/// nothing tracked the gap and nothing could replay it.
///
/// Tests use tempfile::TempDir so nothing persists between runs.
use tempfile::TempDir;
use venturi::intelligence::gatekeeper::{Gatekeeper, GatekeeperOpenConfig, IngestionRequest};
use venturi::storage::journal::Journal;
use venturi::storage::keystore::Keystore;

struct Paths {
    shelf_root: String,
    journal_db: String,
    keystore_db: String,
    librarian_db: String,
    scribe_db: String,
    graph_db: String,
}

fn test_paths(dir: &TempDir) -> Paths {
    let root = dir.path().to_str().unwrap();
    Paths {
        shelf_root: format!("{root}/shelf"),
        journal_db: format!("{root}/journal.db"),
        keystore_db: format!("{root}/keystore.db"),
        librarian_db: format!("{root}/librarian.db"),
        scribe_db: format!("{root}/scribe.db"),
        graph_db: format!("{root}/graph.db"),
    }
}

fn gatekeeper_config(paths: &Paths) -> GatekeeperOpenConfig<'_> {
    GatekeeperOpenConfig {
        shelf_root: &paths.shelf_root,
        journal_db: &paths.journal_db,
        keystore_db: &paths.keystore_db,
        librarian_db: &paths.librarian_db,
        scribe_db: &paths.scribe_db,
        graph_db: &paths.graph_db,
        ollama_url: "http://localhost:11434",
        embedding_model: None,
        embedding_dim: None,
    }
}

fn test_request(summary: &str) -> IngestionRequest {
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
        chunks: vec![],
    }
}

/// Forges exactly the state a real register_catalog() failure leaves behind:
/// sealed + journaled + keyed (steps 1-3, which are atomic and already
/// crash-safe), but never cataloged (step 4, which used to be a silent,
/// unrecoverable dead end).
fn forge_uncataloged_ingestion(paths: &Paths, parent_id: &str, orb_id: &str) {
    let request = test_request("forged for reconciliation test");
    let request_json = serde_json::to_string(&request).expect("serialize request");

    let journal = Journal::open(&paths.journal_db).expect("open journal");
    journal
        .open_ingestion(parent_id, 1, &request.agent_id, &request_json)
        .expect("open_ingestion");
    journal
        .record_orb(parent_id, orb_id, 1)
        .expect("record_orb");
    journal
        .commit_ingestion(parent_id)
        .expect("commit_ingestion");
    // catalog_registered is left at its default 0 — this is the gap.

    let keystore = Keystore::open(&paths.keystore_db).expect("open keystore");
    keystore
        .deposit("forged-key-id", parent_id, &[7u8; 32])
        .expect("deposit key");
}

#[test]
fn reconcile_catalog_recovers_orb_missing_from_librarian() {
    let dir = TempDir::new().expect("tempdir");
    let paths = test_paths(&dir);
    let parent_id = "forged-parent-id";
    let orb_id = "forged-orb-id";

    forge_uncataloged_ingestion(&paths, parent_id, orb_id);

    let mut gatekeeper = Gatekeeper::open(gatekeeper_config(&paths)).expect("open gatekeeper");

    // Before reconciliation: durable on disk (journal says COMPLETE) but
    // genuinely unfindable — this is the bug.
    assert!(gatekeeper
        .librarian()
        .fetch_chain(parent_id)
        .expect("fetch_chain")
        .is_empty());

    let reconciled = gatekeeper.reconcile_catalog().expect("reconcile_catalog");
    assert_eq!(reconciled, 1);

    // After reconciliation: findable again.
    let rows = gatekeeper
        .librarian()
        .fetch_chain(parent_id)
        .expect("fetch_chain");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].orb_id, orb_id);
}

#[test]
fn reconcile_catalog_is_idempotent_once_registered() {
    let dir = TempDir::new().expect("tempdir");
    let paths = test_paths(&dir);
    let parent_id = "forged-parent-id-2";
    let orb_id = "forged-orb-id-2";

    forge_uncataloged_ingestion(&paths, parent_id, orb_id);

    let mut gatekeeper = Gatekeeper::open(gatekeeper_config(&paths)).expect("open gatekeeper");
    assert_eq!(gatekeeper.reconcile_catalog().expect("first reconcile"), 1);

    // A second reconcile pass (e.g. the next process restart) must not
    // re-replay an already-registered entry.
    assert_eq!(gatekeeper.reconcile_catalog().expect("second reconcile"), 0);
}

#[test]
fn normal_ingestion_does_not_need_reconciliation() {
    let dir = TempDir::new().expect("tempdir");
    let paths = test_paths(&dir);

    let mut gatekeeper = Gatekeeper::open(gatekeeper_config(&paths)).expect("open gatekeeper");
    let mut request = test_request("normal ingestion");
    request.chunks = vec![b"hello".to_vec()];
    gatekeeper.ingest(request).expect("ingest");

    // A healthy ingestion marks itself registered inline — nothing pending.
    assert_eq!(
        gatekeeper.reconcile_catalog().expect("reconcile_catalog"),
        0
    );
}
