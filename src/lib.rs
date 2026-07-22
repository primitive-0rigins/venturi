// Venturi — encrypted governed agent memory infrastructure

// Foundational types (most imported, no internal deps):
pub mod auth;
pub mod types;

// Storage layer (imports types only):
pub mod storage;

// Gate processors (imports types only):
pub mod gate;

// Intelligence layer (imports types + storage + pipeline):
pub mod intelligence;

// Pipeline layer (imports types + gate + intelligence + storage):
pub mod pipeline;

// Public API (imports all layers through gatekeeper):
pub mod api;

// ── Top-level re-exports ──────────────────────────────────────────────────────
// These allow callers to use `venturi::Venturi` etc. without knowing the layout.

pub use api::{
    CapabilityState, ConsensusResult, RetrievalWithProof, StorageLimits, SystemCapabilities,
    Venturi, VenturiConfig,
};
pub use intelligence::gatekeeper::{ContentType, IngestionRequest, IngestionResult};
pub use intelligence::librarian::{
    ChainReference, ForesightRow, LifecycleConfig, MetaRow, StructuredFilter,
};
pub use intelligence::scribe::RetrievalProof;
pub use pipeline::retrieval::RetrievalPipeline;
pub use pipeline::tunnel::WormholeTunnel;
pub use storage::shelf::OrbShelf;
pub use types::error::{NotFoundReason, TunnelError};
pub use types::fact::{AnswerFact, Foresight};
pub use types::orb::{Orb, OrbId};
