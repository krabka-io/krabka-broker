//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`).
//!
//! The model state holds the REAL `ReplicaState` and drives the production
//! `install_isr` / `update_follower_leo` / `recompute_hw_for_leader_append`.
//! The BFS checker explores every interleaving of leader append, follower
//! fetch, and ISR shrink/expand. It asserts that the partition-replication
//! safety invariants never break, above all no-committed-data-loss. Design:
//! `crates/broker/docs/replication-isr-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! this module fences each run with `within_boundary` + `target_state_count`.
//! You MUST run each config under the host memory watchdog while you tune the
//! bounds.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    time::Instant,
};

use krabka_log::Offset;
use krabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::ReplicaState;

/// Hard backstop on generated states. It bounds host memory even if
/// `within_boundary` is looser than intended.
const MAX_STATES: usize = 200_000;

/// Depth backstop; must exceed each config's reachable-graph diameter.
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_SAFETY: usize = 174;

const PINNED_UNIQUE_STATES_OVERSHOOT: usize = 174;

/// Bounded model config. It is held here, not in the fingerprinted state.
struct IsrModel {
    /// Constant injected `now`. The model does not model wall-clock time.
    /// ISR shrink/expand is an explicit action, not a time-based decision.
    t0: Instant,
    /// `replicas[0]` is the fixed leader; the rest are followers.
    replicas: Vec<NodeId>,
    /// Leader-LEO / follower-LEO cap.
    max_offset: i64,
    /// When set, followers may report a LEO above `leader_leo`. This is the
    /// clamp test.
    test_overshoot: bool,
}

impl IsrModel {
    fn safety(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            max_offset,
            test_overshoot: false,
        }
    }

    fn overshoot(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            max_offset,
            test_overshoot: true,
        }
    }

    fn leader(&self) -> NodeId {
        self.replicas[0]
    }

    fn followers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas[1..].iter().copied()
    }
}

/// The fingerprinted model state: the REAL core + the leader's own LEO.
#[derive(Clone, Debug)]
struct IsrState {
    rs: ReplicaState,
    leader_leo: Offset,
}

impl IsrState {
    /// Normalized, timestamp-free projection for Eq/Hash. The real state holds
    /// non-`Hash` `HashMap`/`HashSet` and non-deterministic timestamps. Neither
    /// of them is safety-relevant here.
    fn project(&self) -> (Vec<NodeId>, Vec<(NodeId, Offset)>, Offset, i32, Offset) {
        let mut isr: Vec<NodeId> = self.rs.isr.iter().copied().collect();
        isr.sort_unstable();
        let mut pf: Vec<(NodeId, Offset)> = self
            .rs
            .per_follower
            .iter()
            .map(|(k, v)| (*k, v.leo))
            .collect();
        pf.sort_unstable();
        (
            isr,
            pf,
            self.rs.hw,
            self.rs.current_leader_epoch.0,
            self.leader_leo,
        )
    }
}

impl PartialEq for IsrState {
    fn eq(&self, other: &Self) -> bool {
        self.project() == other.project()
    }
}

impl Eq for IsrState {}

impl Hash for IsrState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project().hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum IsrAction {
    /// The leader appends one record and recomputes the HW. `leader_leo`
    /// increases by 1.
    LeaderAppend,
    /// A follower reports `leo` in a fetch.
    FollowerFetch { follower: NodeId, leo: Offset },
    /// The controller installs a new committed ISR.
    InstallIsr { isr: Vec<NodeId> },
}

#[path = "replica_state_model/checker.rs"]
mod checker;

#[path = "replica_state_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "replica_state_model/tests.rs"]
mod tests;
