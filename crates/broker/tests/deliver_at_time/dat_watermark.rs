//! A scheduled partition exposes the log prefix before its first pending record.
use crate::dat_fixtures::{DeliveryScenario, delivery};

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_watermark.feature",
    name = "list offsets latest lands a seek to end on the first pending record"
)]
#[tokio::test]
async fn list_offsets_latest_lands_a_seek_to_end_on_the_first_pending_record(
    delivery: DeliveryScenario,
    mode: String,
    topic: String,
    latest: i64,
    values: String,
) {
    let _ = (mode, topic, latest, values);
    delivery.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_watermark.feature",
    name = "a batch scheduled far out holds back a later batch that is already due"
)]
#[tokio::test]
async fn a_batch_scheduled_far_out_holds_back_a_later_batch_that_is_already_due(
    delivery: DeliveryScenario,
    mode: String,
    topic: String,
    latest: i64,
    values: String,
) {
    let _ = (mode, topic, latest, values);
    delivery.shutdown().await;
}
