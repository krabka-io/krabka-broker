//! ListOffsets isolation bounds exercised through real producer and transaction operations.
use {
    crate::isolation_scenarios::{Isolation, isolation, shutdown},
    rstest_bdd_macros::scenario,
};
#[scenario(
    path = "tests/features/migrated/tests/list_offsets_isolation.feature",
    name = "Open transactions fence timestamp matches at or beyond the stable offset"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_transaction_fences_max_timestamp_and_a_timestamp_lookup(isolation: Isolation) {
    shutdown(&mut isolation).await;
    drop(isolation);
}
