//! ListOffsets isolation bounds exercised through real producer and transaction operations.
use {
    crate::isolation_scenarios::{Isolation, isolation, shutdown},
    rstest_bdd_macros::scenario,
};
#[scenario(
    path = "tests/features/migrated/tests/list_offsets_isolation.feature",
    name = "Commit and abort both release the last stable offset past their marker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_latest_stops_at_an_open_transaction(
    isolation: Isolation,
    resolution: String,
    topic: String,
    tid: String,
) {
    let _ = (resolution, topic, tid);
    shutdown(&mut isolation).await;
    drop(isolation);
}
