//! In-process integration tests for KIP-101 leader-epoch fencing and the
//! .leader-epoch-checkpoint byte format.
//!
//! Windows-gated like the other multi-broker tests.

use support::cluster_lock;

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `leader_epoch/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "leader_epoch/epoch_checkpoint.rs"]
mod epoch_checkpoint;
#[path = "leader_epoch/epoch_diverge_follower.rs"]
mod epoch_diverge_follower;
#[path = "leader_epoch/epoch_diverge_leader.rs"]
mod epoch_diverge_leader;
#[path = "leader_epoch/epoch_fencing.rs"]
mod epoch_fencing;
#[path = "leader_epoch/epoch_harness.rs"]
mod epoch_harness;
