//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (`select_leader`). See
//! `docs/superpowers/specs/2026-06-13-krabka-failover-recovery-model-design.md`.
//!
//! Each failover configuration also runs with a data-bearing witness in the
//! replica set. The witness stays in every emitted ISR, and no reachable state
//! has a witness leader.
//!
//! The winner-selection model runs once per published eligible-leader set, and
//! checks the ordering the KIP-966 rule imposes: which replica is elected out
//! of which group, and whether the election reports itself as losing data. It
//! holds no logs, so it cannot check that the replica so elected really is
//! complete -- that claim is about ELR maintenance, and `data_path_model`'s
//! `data_elr` configuration is where it is checked. See [`recovery_model`] for
//! the split.
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

/// KIP-966 winner selection, over every published eligible-leader set the
/// three-replica partition can have, with and without a witness among them.
///
/// The ELR is not part of the search state -- it was published before the poll
/// began -- so it is swept here instead: each subset of `{1,2,3}` is one
/// exhaustive run of the response fan-out, including the sets that name a
/// replica which never answers and the ones whose only member is the witness.
#[test]
fn offset_recovery() {
    for published in 0u8..8 {
        let eligible: Vec<i32> = (1..=3)
            .filter(|id| published & (1 << (id - 1)) != 0)
            .collect();
        for witness_ids in [&[][..], &WITNESS_REPLICA[..]] {
            let label = format!("offset_recovery elr={eligible:?} witnesses={witness_ids:?}");
            run_recovery(
                RecoveryModel::offset_recovery(&eligible, witness_ids),
                &label,
            );
        }
    }
}
