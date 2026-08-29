//! In-process integration tests for KIP-101 leader-epoch fencing and the
//! .leader-epoch-checkpoint byte format.
//!
//! Windows-gated like the other multi-broker tests.

use std::sync::OnceLock;

use tokio::sync::Mutex;

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

/// Serializes the multi-broker tests in this binary.
///
/// Each test starts a 3-broker loopback cluster. Running them at the same time
/// exhausts the ephemeral ports and starves the openraft election timing. This
/// is the same reason as for `replication.rs::cluster_lock` and
/// `quorum.rs::cluster_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
