//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (Task 3). See
//! `docs/superpowers/specs/2026-06-13-krabka-failover-recovery-model-design.md`.
//!
//! Each failover configuration also runs with a data-bearing witness in the
//! replica set. The witness stays in every emitted ISR, and no reachable state
//! has a witness leader.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! `within_boundary` + `target_state_count` fence each run. You MUST run these
//! models under the host memory watchdog while you tune the bounds.
//!
//! # Module layout
//!
//! This file is the module root. It holds the checked configurations, one per
//! test. Each child holds one concern: [`failover_state`] the failover search
//! space and its projection onto a `PartitionRecord`, [`decision`] the
//! safety invariants of one `failover_one` result, [`failover_model`] the
//! stateright [`Model`](stateright::Model) implementation that drives them,
//! [`recovery_state`] and [`recovery_model`] the same pair for KIP-966 winner
//! selection, and [`runner`] the checker bounds.

// This root is itself reached through a `#[path]` declaration in
// `leader_election`, which makes it a module-directory owner, so a bare
// `mod child;` would resolve against `src/` instead of this file's stem
// directory. Each child therefore names its file explicitly.
#[path = "leader_failover_model/decision.rs"]
mod decision;
#[path = "leader_failover_model/failover_model.rs"]
mod failover_model;
#[path = "leader_failover_model/failover_state.rs"]
mod failover_state;
#[path = "leader_failover_model/recovery_model.rs"]
mod recovery_model;
#[path = "leader_failover_model/recovery_state.rs"]
mod recovery_state;
#[path = "leader_failover_model/runner.rs"]
mod runner;

use self::{
    failover_state::FailoverModel,
    recovery_state::RecoveryModel,
    runner::{run_failover, run_recovery},
};
use crate::config_keys::RecoveryStrategy;

/// The three-site stretch shape: replica 2 is a data-bearing witness. It is
/// not `replicas[0]`, so the initial leader is a data replica.
const WITNESS_REPLICA: [u64; 1] = [2];

#[test]
fn failover_safe() {
    // unclean disabled: a clean election (or unavailability) is the only path;
    // the decision asserts guarantee no out-of-ISR election ever happens.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false, &[]),
        "failover_safe",
    );
}

#[test]
fn failover_unclean() {
    // KIP-841: out-of-ISR election permitted when ISR is empty.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &[]),
        "failover_unclean",
    );
}

#[test]
fn failover_recover() {
    // KIP-966: empty-ISR leader death defers to offset-aware recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false, &[]),
        "failover_recover",
    );
}

#[test]
fn failover_witness_safe() {
    // Same as `failover_safe`, with a witness in the replica set. Leadership
    // skips the witness, and the witness stays in the ISR.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false, &WITNESS_REPLICA),
        "failover_witness_safe",
    );
}

#[test]
fn failover_witness_unclean() {
    // The KIP-841 out-of-ISR pick must skip the witness too.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &WITNESS_REPLICA),
        "failover_witness_unclean",
    );
}

#[test]
fn failover_witness_recover() {
    // With a witness present, an ISR that holds only live witnesses is
    // `Unavailable`, and only a truly empty ISR reaches KIP-966 recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false, &WITNESS_REPLICA),
        "failover_witness_recover",
    );
}

#[test]
fn offset_recovery() {
    run_recovery(RecoveryModel::offset_recovery(), "offset_recovery");
}
