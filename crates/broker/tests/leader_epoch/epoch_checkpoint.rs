//! Leader epoch boundaries specified through actual wire and storage operations.
use {
    crate::epoch_scenarios::{EpochBoundary, epoch, shutdown},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/leader_epoch/epoch_boundaries.feature",
    name = "Successive epochs persist Kafka compatible checkpoint rows"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_checkpoint_byte_compat(epoch: EpochBoundary) { shutdown(epoch).await; }
