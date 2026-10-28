//! Leader epoch boundaries specified through actual wire and storage operations.
use {
    crate::epoch_scenarios::{EpochBoundary, epoch, shutdown},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/leader_epoch/epoch_boundaries.feature",
    name = "A stale fetch epoch is fenced by the current leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_leader_epoch_truncates_zombie_writes(epoch: EpochBoundary) {
    shutdown(epoch).await;
}

#[scenario(
    path = "tests/features/migrated/tests/leader_epoch/epoch_boundaries.feature",
    name = "A future fetch epoch reports unknown leader epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_leader_epoch_on_metadata_lag(epoch: EpochBoundary) { shutdown(epoch).await; }
