use crate::intelligence::graph::KnowledgeGraph;
use crate::intelligence::librarian::{Librarian, LifecycleConfig};
use crate::intelligence::scribe::Scribe;
use crate::storage::keystore::Keystore;
use crate::storage::shelf::OrbShelf;
use crate::types::error::TunnelError;

/// Checkpoint name under which the lifecycle sweep tracks the last EXIT event it has folded
/// into `usefulness_score`. See `Sweeper::sweep_lifecycle`.
const USEFULNESS_FEEDBACK_CHECKPOINT: &str = "usefulness_feedback";
/// A fresh checkpoint must sort before every real EXIT event. The Scribe cursor accepts this
/// legacy timestamp-only value, then upgrades it to a timestamp-plus-event-ID cursor.
const USEFULNESS_FEEDBACK_EPOCH: &str = "0";

/// Cold tier boundary (seconds since last access). Orbs are warm after seven days and cold
/// after thirty days; the sweep updates only those two non-hot tiers.
const TIER_WARM_SECS: u64 = 30 * 86400;

/// 90-day expiry window. Chains not accessed in 90 days eject to dataset.
const EXPIRY_DAYS: u64 = 90;

/// Background maintenance sweeps for Venturi.
///
/// Run each sweep on a timer in a background thread:
///   sweep_access_marks()  — every ~5 minutes
///   sweep_tiers()         — every ~15 minutes
///   sweep_expiry()        — once daily
///
/// Sweep does NOT write Scribe EXIT events. EXIT events are exclusively for
/// agent/user retrieval verdicts (1=useful, 0=not useful). Expiry is a
/// lifecycle event — the dataset flywheel is driven by verdict signals, not
/// by the system clock. Expired chains are silently ejected.
///
/// `sweep_lifecycle` does read EXIT events (never write) to fold verdict feedback into
/// `usefulness_score` — see its own doc comment and
/// `spec/math-application-proposal-usefulness-score-tiering.md`.
pub struct Sweeper<'a> {
    librarian: &'a Librarian,
    keystore: &'a Keystore,
    shelf: &'a OrbShelf,
    graph: &'a KnowledgeGraph,
    scribe: &'a Scribe,
}

impl<'a> Sweeper<'a> {
    pub fn new(
        librarian: &'a Librarian,
        keystore: &'a Keystore,
        shelf: &'a OrbShelf,
        graph: &'a KnowledgeGraph,
        scribe: &'a Scribe,
    ) -> Self {
        Self {
            librarian,
            keystore,
            shelf,
            graph,
            scribe,
        }
    }

    /// Sibling refresh sweep — run every ~5 minutes.
    ///
    /// Flushes pending access_marks: updates last_accessed on all orbs in any
    /// chain touched since the last sweep. A single orb access refreshes the
    /// entire chain's 90-day expiry clock.
    pub fn sweep_access_marks(&self) -> Result<SweepReport, TunnelError> {
        let chains_affected = self.librarian.flush_access_marks()?;
        Ok(SweepReport {
            sweep: "access_marks",
            chains_affected,
            orbs_ejected: 0,
        })
    }

    /// Tier update sweep — run every ~15 minutes.
    ///
    /// Demotes chains based on last_accessed recency:
    ///   hot  → accessed within 7 days
    ///   warm → accessed 7–30 days ago
    ///   cold → not accessed in 30+ days
    pub fn sweep_tiers(&self) -> Result<SweepReport, TunnelError> {
        // Cold: not accessed in 30+ days
        let cold = self.librarian.expired_chains(TIER_WARM_SECS / 86400)?;
        for parent_id in &cold {
            self.librarian.update_tier(parent_id, "cold")?;
        }

        // Warm: not accessed in 7+ days, but within 30 days
        let stale = self.librarian.expired_chains(7)?;
        let mut warm_count = 0u32;
        for parent_id in &stale {
            if !cold.contains(parent_id) {
                self.librarian.update_tier(parent_id, "warm")?;
                warm_count += 1;
            }
        }

        let chains_affected = cold.len() as u32 + warm_count;
        Ok(SweepReport {
            sweep: "tiers",
            chains_affected,
            orbs_ejected: 0,
        })
    }

    /// Expiry sweep — run once daily.
    ///
    /// Finds chains not accessed in 90 days. For each expired chain:
    ///   1. Ejects catalog entries (returns orb_ids)
    ///   2. Removes sealed orb files from OrbShelf
    ///   3. Removes chain keys from Keystore
    ///   4. Removes graph nodes/edges for this chain
    ///
    /// No Scribe EXIT event is written — expiry is a lifecycle action,
    /// not a retrieval verdict. Downstream dataset tooling reads the
    /// Scribe EXIT events written by agents, not by the sweep clock.
    pub fn sweep_expiry(&self) -> Result<SweepReport, TunnelError> {
        let expired = self.librarian.expired_chains(EXPIRY_DAYS)?;
        let mut total_ejected = 0u32;
        let mut chains_affected = 0u32;

        for parent_id in &expired {
            if self.librarian.chain_on_legal_hold(parent_id)? {
                continue;
            }

            let orb_ids = self.librarian.eject_chain(parent_id)?;
            if orb_ids.is_empty() {
                continue;
            }

            for orb_id in &orb_ids {
                let _ = self.shelf.remove(orb_id);
            }

            self.keystore.remove_by_parent(parent_id)?;
            self.graph.eject_chain(parent_id)?;

            total_ejected += orb_ids.len() as u32;
            chains_affected += 1;
        }

        Ok(SweepReport {
            sweep: "expiry",
            chains_affected,
            orbs_ejected: total_ejected,
        })
    }

    /// Folds new EXIT-verdict feedback into `usefulness_score` before running the recency-based
    /// tier sweep, so a recency-stale but verdict-proven-useful orb can be exempted from cold
    /// demotion (see `Librarian::demote_cold`). This reads Scribe's EXIT events but does not
    /// write any — the write-direction boundary documented above is unchanged. See
    /// `spec/math-application-proposal-usefulness-score-tiering.md`.
    pub fn sweep_lifecycle(&self, cfg: &LifecycleConfig) -> Result<SweepReport, TunnelError> {
        let since = self
            .librarian
            .sweep_checkpoint(USEFULNESS_FEEDBACK_CHECKPOINT)?
            .unwrap_or_else(|| USEFULNESS_FEEDBACK_EPOCH.to_string());
        let (events, last_ts) = self.scribe.exit_events_since(&since)?;
        if !events.is_empty() {
            self.librarian.apply_exit_feedback(&events)?;
        }
        if let Some(last_ts) = last_ts {
            self.librarian
                .set_sweep_checkpoint(USEFULNESS_FEEDBACK_CHECKPOINT, &last_ts)?;
        }

        let orbs_changed = self.librarian.lifecycle_sweep(cfg)?;
        Ok(SweepReport {
            sweep: "lifecycle",
            chains_affected: orbs_changed,
            orbs_ejected: 0,
        })
    }

    /// Spectral community detection sweep (R1) — run every ~30 minutes.
    ///
    /// Recomputes `community_id` on every knowledge-graph node from current
    /// edge/hyperedge weights. `graph_query()`'s BFS traversal then gains a
    /// hop-independent "same community" pass on top of local expansion. See
    /// `KnowledgeGraph::detect_communities`.
    pub fn sweep_communities(&self) -> Result<SweepReport, TunnelError> {
        self.graph.detect_communities()
    }
}

/// Summary of what a sweep did. Useful for logging.
pub struct SweepReport {
    pub sweep: &'static str,
    pub chains_affected: u32,
    pub orbs_ejected: u32,
}
