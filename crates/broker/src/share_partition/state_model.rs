//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`).
//!
//! The model state holds the REAL `AcquisitionState` and drives the production
//! `materialize`, `acquire`, `acknowledge`, `renew`, `expire_locks`,
//! `defer_internal`, `promote_deferred`, `to_persist_batches`, and
//! `load_from`. The BFS checker explores every interleaving of consumer
//! operations, time advance, KFC-1 deferral, and, in the failover config,
//! leader-reload. It asserts that the share-group delivery-safety invariants
//! never break. Design:
//! `docs/superpowers/specs/2026-06-13-krabka-share-group-model-design.md`.
//!
//! The model does not carry delivery times. It defers an arbitrary offset at
//! an arbitrary point instead, which covers every deferral a log and a clock
//! could produce and many they could not. That over-approximation is the point:
//! the safety claims must hold for any of them.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary`, `target_state_count`, and
//! `target_state_count`. While bounds are tuned, every run MUST execute under the host
//! memory watchdog. Never run one unguarded, because a runaway space exhausts
//! host RAM.
//!
//! # Module layout
//!
//! This file is the module root. It declares the children and holds the one
//! test per bounded config. Each child holds one concern: `config` the bounded
//! configurations and the shared lock duration, `state` the fingerprinted
//! state and the action set, `observe` the read-only queries over the real
//! machine, `invariants` the safety claims, `properties` the stateright
//! `Model` implementation, and `runner` the checker bounds.

// Each child is declared with an explicit `#[path]`. The declaration then names
// the same file whether or not this root is itself reached through a `#[path]`.
#[path = "state_model/config.rs"]
mod config;
#[path = "state_model/invariants.rs"]
mod invariants;
#[path = "state_model/observe.rs"]
mod observe;
#[path = "state_model/properties.rs"]
mod properties;
#[path = "state_model/runner.rs"]
mod runner;
#[path = "state_model/state.rs"]
mod state;

use self::{config::ShareModel, runner::run};

#[test]
fn share_concurrency_inflight_full() {
    // max_inflight large enough to pull the whole window in one materialize.
    run(
        ShareModel::concurrency(3, 3),
        "share_concurrency_inflight_full",
    );
}

#[test]
fn share_concurrency_inflight_one() {
    // max_inflight = 1: exercises drain-then-rematerialize across Produce steps.
    run(
        ShareModel::concurrency(3, 1),
        "share_concurrency_inflight_one",
    );
}

#[test]
fn share_failover() {
    // Adds leader-failover Reload; stresses acknowledged-is-terminal durability.
    run(ShareModel::failover(), "share_failover");
}

#[test]
fn share_deferral() {
    // Adds KFC-1 Defer/PromoteDeferred over the failover window, so the
    // deferral invariants are checked across a leader change as well.
    run(ShareModel::deferral(), "share_deferral");
}

#[test]
fn share_deferral_wide() {
    // Three offsets: a deferral can span a range, and a due record can sit two
    // behind a waiting one.
    run(ShareModel::deferral_wide(), "share_deferral_wide");
}
