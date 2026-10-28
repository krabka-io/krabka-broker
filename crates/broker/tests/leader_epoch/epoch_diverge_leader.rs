//! A follower with a stale fetched epoch receives the leader's actual epoch boundary.
use {
    crate::epoch_scenarios::{EpochBoundary, epoch, shutdown},
    rstest_bdd_macros::scenario,
};
#[scenario(
    path = "tests/features/migrated/tests/leader_epoch/epoch_boundaries.feature",
    name = "A follower fetch beyond its claimed epoch receives the actual divergence boundary"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diverging_epoch_returned_on_stale_last_fetched_epoch(epoch: EpochBoundary) {
    shutdown(epoch).await;
}
