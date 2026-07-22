use thiserror::Error;

#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("compression failed: {0}")]
    CompressionFailed(String),

    #[error("encryption failed")]
    EncryptionFailed,

    #[error("wrong key for gate {gate_id}")]
    WrongKey { gate_id: u8 },

    #[error("key count mismatch: pipeline expects {expected} keys, got {got}")]
    KeyCountMismatch { expected: usize, got: usize },

    #[error("orb not found: {id}")]
    OrbNotFound { id: String },

    #[error("orb content corrupted: id mismatch for {id}")]
    OrbCorrupted { id: String },

    #[error("rehydration failed: {0}")]
    RehydrationFailed(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("gatekeeper rejected: {reason}")]
    GatekeeperRejected { reason: String },

    #[error("keystore inaccessible")]
    KeystoreInaccessible,

    #[error("chain assembly failed: parent_id={parent_id} sequence={sequence}")]
    ChainAssemblyFailed { parent_id: String, sequence: u32 },

    #[error("database error: {0}")]
    DatabaseError(String),

    #[error("memory not found: {reason} (query: {query})")]
    MemoryNotFound {
        query: String,
        reason: NotFoundReason,
    },
}

impl TunnelError {
    /// Stable machine-readable failure category for API responses, Scribe
    /// payloads, and tests. Display strings may change; these must not change
    /// casually because callers can branch on them.
    pub fn category(&self) -> &'static str {
        match self {
            Self::CompressionFailed(_)
            | Self::EncryptionFailed
            | Self::WrongKey { .. }
            | Self::RehydrationFailed(_) => crypto_category(self),
            Self::KeyCountMismatch { .. }
            | Self::OrbNotFound { .. }
            | Self::OrbCorrupted { .. }
            | Self::ChainAssemblyFailed { .. } => orb_category(self),
            Self::Serialization(_)
            | Self::InvalidConfiguration(_)
            | Self::GatekeeperRejected { .. } => input_category(self),
            Self::Io(_) | Self::KeystoreInaccessible | Self::DatabaseError(_) => {
                storage_category(self)
            }
            Self::MemoryNotFound { reason, .. } => reason.category(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NotFoundReason {
    /// Ollama did not respond — semantic search could not run.
    EmbeddingUnavailable,
    /// Semantic search ran but returned no results above threshold.
    NoSimilarContent,
    /// Metadata filter matched zero catalog rows.
    MetadataFilterEmpty,
    /// Graph traversal found no reachable chains from the query concepts.
    GraphNoMatch,
}

impl std::fmt::Display for NotFoundReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmbeddingUnavailable => write!(f, "embedding_unavailable"),
            Self::NoSimilarContent => write!(f, "no_similar_content"),
            Self::MetadataFilterEmpty => write!(f, "metadata_filter_empty"),
            Self::GraphNoMatch => write!(f, "graph_no_match"),
        }
    }
}

impl NotFoundReason {
    pub fn category(&self) -> &'static str {
        match self {
            Self::EmbeddingUnavailable => "embedding_unavailable",
            Self::NoSimilarContent => "no_similar_content",
            Self::MetadataFilterEmpty => "metadata_filter_empty",
            Self::GraphNoMatch => "graph_no_match",
        }
    }
}

fn gatekeeper_category(reason: &str) -> &'static str {
    if reason.contains("summary") {
        "summary_invalid"
    } else {
        "metadata_invalid"
    }
}

fn crypto_category(error: &TunnelError) -> &'static str {
    match error {
        TunnelError::CompressionFailed(_) => "compression_failed",
        TunnelError::EncryptionFailed => "encryption_failed",
        TunnelError::WrongKey { .. } => "wrong_key",
        TunnelError::RehydrationFailed(_) => "rehydration_failed",
        _ => "internal_error",
    }
}

fn orb_category(error: &TunnelError) -> &'static str {
    match error {
        TunnelError::OrbCorrupted { .. } => "orb_corrupt",
        TunnelError::KeyCountMismatch { .. }
        | TunnelError::OrbNotFound { .. }
        | TunnelError::ChainAssemblyFailed { .. } => "chain_incomplete",
        _ => "internal_error",
    }
}

fn input_category(error: &TunnelError) -> &'static str {
    match error {
        TunnelError::Serialization(_) => "serialization_failed",
        TunnelError::InvalidConfiguration(_) => "metadata_invalid",
        TunnelError::GatekeeperRejected { reason } => gatekeeper_category(reason),
        _ => "internal_error",
    }
}

fn storage_category(error: &TunnelError) -> &'static str {
    match error {
        TunnelError::Io(_) => "shelf_unavailable",
        TunnelError::KeystoreInaccessible => "keystore_unavailable",
        TunnelError::DatabaseError(_) => "catalog_inconsistent",
        _ => "internal_error",
    }
}
