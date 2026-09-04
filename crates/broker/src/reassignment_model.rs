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

fn in_flight(s: &ReassignState) -> bool {
    !s.adding.is_empty() || !s.removing.is_empty()
}

/// The target replica set that the reassignment converges to, which is
/// replicas − removing.
fn target_of(s: &ReassignState) -> Vec<NodeId> {
    s.replicas
        .iter()
        .filter(|r| !s.removing.contains(r))
        .copied()
        .collect()
}

/// Build a `PartitionRecord` from the model state to drive the real
/// `reassign_one`. `directories` does not affect the safety properties.
fn pr_of(s: &ReassignState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: krabka_metadata::LeaderEpoch(s.leader_epoch),
        adding_replicas: s.adding.clone(),
        removing_replicas: s.removing.clone(),
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `reassign_one` decision against the pre-state. These are the
/// safety-critical invariants, and they hold per decision under any ordering.
fn assert_step(pre: &ReassignState, next: &PartitionRecord) {
    assert2::assert!(
        next.leader_epoch >= pre.leader_epoch,
        "leader_epoch regressed: {} -> {}",
        pre.leader_epoch,
        next.leader_epoch
    );
    assert2::assert!(
        pre.adding.iter().all(|n| pre.isr.contains(n)),
        "decision emitted before adding caught up: adding={:?} isr={:?}",
        pre.adding,
        pre.isr
    );
    let target = target_of(pre);
    if next.leader != pre.leader {
        // Handoff.
        assert2::assert!(
            pre.isr.contains(&next.leader),
            "handoff to non-ISR {}",
            next.leader
        );
        assert2::assert!(
            target.contains(&next.leader),
            "handoff to non-target {}",
            next.leader
        );
        assert2::assert!(
            pre.alive.contains(&next.leader),
            "handoff to dead {}",
            next.leader
        );
        assert2::assert!(
            !pre.removing.contains(&next.leader),
            "handoff to a removing replica {}",
            next.leader
        );
        assert2::assert!(
            next.replicas == pre.replicas,
            "handoff changed the replica set"
        );
        assert2::assert!(next.adding_replicas == pre.adding, "handoff changed adding");
        assert2::assert!(
            next.removing_replicas == pre.removing,
            "handoff changed removing"
        );
        assert2::assert!(
            next.leader_epoch == pre.leader_epoch + 1,
            "handoff did not bump leader_epoch by exactly 1"
        );
    } else if next.adding_replicas.is_empty() && next.removing_replicas.is_empty() {
        // Completion.
        assert2::assert!(
            next.replicas.contains(&next.leader),
            "completion switched the replica set off the leader {}: replicas={:?}",
            next.leader,
            next.replicas
        );
        assert2::assert!(
            next.replicas == target,
            "completion replicas != target: {:?} vs {:?}",
            next.replicas,
            target
        );
        assert2::assert!(
            next.isr.iter().all(|n| next.replicas.contains(n)),
            "completion ISR not a subset of replicas"
        );
        assert2::assert!(
            next.leader_epoch == pre.leader_epoch,
            "completion bumped leader_epoch"
        );
    } else {
        panic!("unexpected reassign_one decision shape: {next:?} from {pre:?}");
    }
}

impl Model for ReassignModel {
    type State = ReassignState;
    type Action = ReassignAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReassignState {
            replicas: self.replicas.clone(),
            isr: self.initial_isr.clone(),
            adding: self.adding.clone(),
            removing: self.removing.clone(),
            leader: self.leader,
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // AdmitToIsr: any replica not yet in ISR (models a catch-up + admit).
        for &r in &state.replicas {
            if !state.isr.contains(&r) {
                actions.push(ReassignAction::AdmitToIsr(r));
            }
        }
        // Die / Revive over the replica set (keep >= 1 alive).
        if state.alive.len() > 1 {
            for &r in &state.replicas {
                if state.alive.contains(&r) {
                    actions.push(ReassignAction::Die(r));
                }
            }
        }
        for &r in &state.replicas {
            if !state.alive.contains(&r) {
                actions.push(ReassignAction::Revive(r));
            }
        }
        // ReassignStep when in flight, under the epoch cap.
        if in_flight(state) && state.leader_epoch < self.max_epoch {
            actions.push(ReassignAction::ReassignStep);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ReassignAction::AdmitToIsr(n) => {
                if state.isr.contains(&n) || !state.replicas.contains(&n) {
                    return None;
                }
                // Rebuild ISR in canonical replica order (keeps the space small).
                state.isr = state
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| state.isr.contains(r) || *r == n)
                    .collect();
            }
            ReassignAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            ReassignAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            ReassignAction::ReassignStep => {
                if !in_flight(&state) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                {
                    let next = reassign_one(&pr, &alive)?;
                    assert_step(last, &next);
                    state.leader = next.leader;
                    state.isr = next.isr;
                    state.adding = next.adding_replicas;
                    state.removing = next.removing_replicas;
                    state.replicas = next.replicas;
                    state.leader_epoch = next.leader_epoch.0;
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &ReassignState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &ReassignState| {
                s.replicas.contains(&s.leader)
            }),
            Property::always("leader_in_isr", |_, s: &ReassignState| {
                s.isr.contains(&s.leader)
            }),
            Property::always("adding_subset_replicas", |_, s: &ReassignState| {
                s.adding.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("removing_subset_replicas", |_, s: &ReassignState| {
                s.removing.iter().all(|n| s.replicas.contains(n))
            }),
            Property::sometimes("can_complete", |_, s: &ReassignState| {
                s.adding.is_empty() && s.removing.is_empty()
            }),
            // Config-conditional so it is not vacuously unsatisfiable in the
            // basic config (where no handoff happens).
            Property::sometimes("can_handoff", |m: &ReassignModel, s: &ReassignState| {
                !m.removing.contains(&m.leader) || s.leader != m.leader
            }),
            Property::sometimes("can_wait", |_, s: &ReassignState| {
                in_flight(s) && s.adding.iter().any(|n| !s.isr.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}

fn run(model: ReassignModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn reassign_basic() {
    // Leader not removed: catch-up then completion to the target replica set.
    run(
        ReassignModel::basic(),
        "reassign_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn reassign_leader_handoff() {
    // Leader in `removing`: catch-up, leader handoff, then completion.
    run(
        ReassignModel::leader_handoff(),
        "reassign_leader_handoff",
        PINNED_UNIQUE_STATES_LEADER_HANDOFF,
    );
}

#[test]
fn reassign_wide() {
    // 5 replicas, add 2 + remove 2, leader removed → handoff then completion.
    run(
        ReassignModel::wide(),
        "reassign_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
