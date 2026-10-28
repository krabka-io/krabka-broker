//! ListOffsets isolation bounds exercised through real producer and transaction operations.
use {
    crate::isolation_scenarios::{Isolation, isolation, shutdown},
    rstest_bdd_macros::scenario,
};
#[scenario(
    path = "tests/features/migrated/tests/list_offsets_isolation.feature",
    name = "Both isolation levels stop LATEST at the acknowledged high watermark"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_answers_the_high_watermark_not_the_unreplicated_log_end(isolation: Isolation) {
    shutdown(&mut isolation).await;
    drop(isolation);
}
