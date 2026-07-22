use venturi::{NotFoundReason, TunnelError};

#[test]
fn tunnel_errors_have_stable_categories() {
    assert_eq!(
        TunnelError::GatekeeperRejected {
            reason: "summary exceeds 100 words".to_string()
        }
        .category(),
        "summary_invalid"
    );
    assert_eq!(
        TunnelError::GatekeeperRejected {
            reason: "agent_id is required".to_string()
        }
        .category(),
        "metadata_invalid"
    );
    assert_eq!(
        TunnelError::MemoryNotFound {
            query: "x".to_string(),
            reason: NotFoundReason::EmbeddingUnavailable,
        }
        .category(),
        "embedding_unavailable"
    );
    assert_eq!(
        TunnelError::OrbNotFound {
            id: "missing".to_string()
        }
        .category(),
        "chain_incomplete"
    );
    assert_eq!(
        TunnelError::KeystoreInaccessible.category(),
        "keystore_unavailable"
    );
}

#[test]
fn not_found_reasons_have_stable_categories() {
    assert_eq!(
        NotFoundReason::NoSimilarContent.category(),
        "no_similar_content"
    );
    assert_eq!(
        NotFoundReason::MetadataFilterEmpty.category(),
        "metadata_filter_empty"
    );
    assert_eq!(NotFoundReason::GraphNoMatch.category(), "graph_no_match");
}
