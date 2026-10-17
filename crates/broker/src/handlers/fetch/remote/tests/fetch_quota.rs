//! KIP-956's fetch quota holds a consumer's cold read back once the bytes the
//! tier already served put the rate over the bound, and lets it through again
//! once the bound is raised.

use {
    super::{
        super::{RemoteFetchWait, refused_read, try_remote_read},
        TIERED_HIGH_WATERMARK, committed_tiered_fixture, out_of_range_at,
    },
    crate::codes,
    assert2::check,
    krabka_protocol::owned::fetch_response::PartitionData,
};

#[tokio::test]
async fn a_cold_read_over_the_fetch_quota_is_skipped_until_the_quota_is_raised() {
    let tiered = committed_tiered_fixture().await;
    let quota = &tiered.broker.remote_log_quotas.fetch;
    quota.update_quota(1);
    let mut wait = RemoteFetchWait::default();

    // Nothing has been read yet, so the first read goes ahead and is counted.
    let mut first = out_of_range_at(&tiered.part, 0);
    let first_served = try_remote_read(&tiered.broker, &mut first, &tiered.part, &mut wait).await;
    check!(first_served.is_some_and(|bytes| bytes > 0));

    // Those bytes are far over one byte a second: the next read is skipped,
    // and the partition goes out as `handleOffsetOutOfRangeError`'s throttled
    // row, without an error.
    let mut throttled = out_of_range_at(&tiered.part, 0);
    let throttled_served =
        try_remote_read(&tiered.broker, &mut throttled, &tiered.part, &mut wait).await;
    check!(throttled_served == Some(0));
    check!(
        throttled.out
            == PartitionData {
                high_watermark: TIERED_HIGH_WATERMARK,
                last_stable_offset: TIERED_HIGH_WATERMARK,
                log_start_offset: 0,
                ..refused_read(0, codes::NONE)
            }
    );

    // A raised bound applies to the bytes already recorded.
    quota.update_quota(i64::MAX);
    let mut restored = out_of_range_at(&tiered.part, 0);
    let restored_served =
        try_remote_read(&tiered.broker, &mut restored, &tiered.part, &mut wait).await;
    check!(restored_served.is_some_and(|bytes| bytes > 0));
    check!(restored.out.error_code == codes::NONE);
}
