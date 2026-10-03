//! Exhaustive stateright model of the pure KIP-455 reassignment-completion core
//! (`reassign_one`).
//!
//! The model state holds a single partition's reassignment, and `next_state`
//! drives the real `reassign_one`. The BFS checker explores every interleaving
//! of replica catch-up, broker liveness, and completion ticks, and it asserts
//! the reassignment-safety invariants. The most important one is that the
//! replica set never switches off the leader. Design:
//! `crates/broker/docs/replication-isr-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` and `target_state_count`. While
//! bounds are tuned, every run MUST execute under the host memory watchdog.

use std::collections::{BTreeSet, HashSet};

use krabka_metadata::PartitionRecord;
use krabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::reassign_one;

const MAX_STATES: usize = 200_000;

const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 21;

const PINNED_UNIQUE_STATES_LEADER_HANDOFF: usize = 42;

const PINNED_UNIQUE_STATES_WIDE: usize = 310;

const PINNED_UNIQUE_STATES_RF_DECREASE: usize = 21;

const PINNED_UNIQUE_STATES_RF_DECREASE_LEADER_REMOVED: usize = 120;

/// Bounded config for the reassignment model. It lives here, not in the state.
struct ReassignModel {
    replicas: Vec<NodeId>,
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    initial_isr: Vec<NodeId>,
    leader: NodeId,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReassignState {
    replicas: Vec<NodeId>,
    isr: Vec<NodeId>, // canonical replica order
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    leader: NodeId,
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ReassignAction {
    AdmitToIsr(NodeId),
    Die(NodeId),
    Revive(NodeId),
    ReassignStep,
}

impl ReassignModel {
    fn basic() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            adding: vec![krabka_audit::NodeId(3)],
            removing: vec![krabka_audit::NodeId(2)],
            initial_isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader: krabka_audit::NodeId(1), // not removed → no handoff
            max_epoch: 10,
        }
    }

    fn leader_handoff() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            adding: vec![krabka_audit::NodeId(3)],
            removing: vec![krabka_audit::NodeId(2)],
            initial_isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader: krabka_audit::NodeId(2), // in `removing` → handoff required before completion
            max_epoch: 10,
        }
    }

    /// A replication factor decrease with nothing to add: broker 2, a target
    /// replica, is not in the ISR, and completing early would shrink the ISR.
    fn rf_decrease() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            adding: vec![],
            removing: vec![krabka_audit::NodeId(3)],
            initial_isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(3)],
            leader: krabka_audit::NodeId(1),
            max_epoch: 10,
        }
    }

    /// `[1,2,3,4] -> [3,4]` with broker 4 added and the leader removed. Kafka
    /// hands off and completes only once broker 3 has caught up too, even
    /// though broker 4 alone would already satisfy the additions.
    fn rf_decrease_leader_removed() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
                krabka_audit::NodeId(4),
            ],
            adding: vec![krabka_audit::NodeId(4)],
            removing: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            initial_isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader: krabka_audit::NodeId(1),
            max_epoch: 10,
        }
    }

    fn wide() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
                krabka_audit::NodeId(4),
                krabka_audit::NodeId(5),
            ],
            adding: vec![krabka_audit::NodeId(4), krabka_audit::NodeId(5)],
            removing: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            initial_isr: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            leader: krabka_audit::NodeId(1), // in `removing` → handoff required
            max_epoch: 10,
        }
    }
}

#[path = "reassignment_model/helpers.rs"]
mod helpers;
use helpers::{assert_step, in_flight, pr_of, target_of};

#[path = "reassignment_model/checker.rs"]
mod checker;

#[path = "reassignment_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "reassignment_model/tests.rs"]
mod tests;
