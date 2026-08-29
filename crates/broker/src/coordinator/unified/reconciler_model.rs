//! Exhaustive stateright model of the KIP-848 reconciliation core.
//!
//! The model drives the real `step_heartbeat`, with its membership and
//! heartbeat actions, and a faithful-client environment that revokes before it
//! adds and trusts the coordinator. It checks the main KIP-848 safety
//! property: no two members ever own the same partition at the same time. The
//! design is in
//! `docs/superpowers/specs/2026-06-14-krabka-kip848-reconciliation-model-design.md`.
//!
//! The faithful client adds and revokes partitions strictly by the
//! **advertised** assignment that the coordinator returned in that member's
//! last heartbeat response (`ReconState::advertised`). It never reads the raw
//! target, and it makes no cross-member check. That is how a real consumer
//! behaves: it trusts the coordinator's assignment. The safety guarantee must
//! therefore come entirely from the coordinator's withholding, in
//! `GroupState::reconcile_member`.
//!
//! Memory safety: stateright BFS keeps every visited unique state in memory.
//! Each run is therefore fenced with `within_boundary`, `target_state_count`,
//! and `timeout`. Run it under the host memory watchdog while you tune the
//! bounds.
//!
//! # Module layout
//!
//! This file is the module root. It holds the one topic the whole model shares
//! and the two tests that run the checker. Each child holds one concern:
//! `config` the bounded model shape and the metadata and coordinator config the
//! driven code needs, `state` the enumerated state and actions, `projection`
//! the two-way mapping onto the real `GroupState`, `heartbeat` the request and
//! advertised-assignment wire helpers, `properties` the stateright
//! [`Model`](stateright::Model) implementation, and `runner` the checker
//! bounds.

// Each child is declared with an explicit `#[path]`, because this root is
// itself reached through a `#[path]` and so owns its declaring directory.
#[path = "reconciler_model/config.rs"]
mod config;
#[path = "reconciler_model/heartbeat.rs"]
mod heartbeat;
#[path = "reconciler_model/projection.rs"]
mod projection;
#[path = "reconciler_model/properties.rs"]
mod properties;
#[path = "reconciler_model/runner.rs"]
mod runner;
#[path = "reconciler_model/state.rs"]
mod state;

use krabka_protocol::primitives::uuid::Uuid;

use self::{config::ReconModel, runner::run};

const TOPIC: Uuid = Uuid([7; 16]);
const TOPIC_NAME: &str = "t";

#[test]
fn recon_basic() {
    // 2 members, 1 topic, 2 partitions: the minimal handoff scenario. Proves the
    // coordinator's `reconcile_member` withholding keeps ownership disjoint
    // across every interleaving of join / leave / heartbeat / client revoke+add.
    run(ReconModel::basic(), "recon_basic");
}

#[test]
fn recon_wide() {
    // 3 members contending for 2 partitions: more handoff interleavings as
    // members join/leave and partitions migrate between live members.
    run(ReconModel::wide(), "recon_wide");
}
