//! KIP-956's fetch quota holds a consumer's cold read back once the bytes the
//! tier already served put the rate over the bound, and lets it through again
//! once the bound is raised.

use {
    super::{
        super::{RemoteFetchWait, refused_read, try_remote_read},
        TIERED_HIGH_WATERMARK, committed_tiered_fixture, out_of_range_at,
    },
    crate::{
        broker::Broker,
        codes,
        remote_log_manager::quota::{FETCH_MAX_BYTES_PER_SECOND, UNLIMITED_BYTES_PER_SECOND},
    },
    assert2::{assert, check},
    krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord},
    krabka_protocol::owned::fetch_response::PartitionData,
    std::time::Duration,
};

/// Set the cluster-default fetch quota the way an operator's `AlterConfigs`
/// does, and wait until the broker's dynamic-config watcher applies it.
///
/// Setting the bound on the quota directly would race the watcher, which
/// re-applies the image's value, unlimited here, on every image it sees.
async fn set_fetch_quota(broker: &Broker, bytes_per_second: i64) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: FETCH_MAX_BYTES_PER_SECOND.into(),
            config_value: Some(bytes_per_second.to_string()),
        })])
        .await
        .expect("publish the fetch quota");
    let applied = tokio::time::timeout(Duration::from_secs(10), async {
        while broker.remote_log_quotas.fetch.bytes_per_second() != bytes_per_second {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        applied.is_ok(),
        "the fetch quota never became {bytes_per_second}"
    );
}

#[tokio::test]
async fn a_cold_read_over_the_fetch_quota_is_skipped_until_the_quota_is_raised() {
    let tiered = committed_tiered_fixture().await;
    set_fetch_quota(&tiered.broker, 1).await;
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
    set_fetch_quota(&tiered.broker, UNLIMITED_BYTES_PER_SECOND).await;
    let mut restored = out_of_range_at(&tiered.part, 0);
    let restored_served =
        try_remote_read(&tiered.broker, &mut restored, &tiered.part, &mut wait).await;
    check!(restored_served.is_some_and(|bytes| bytes > 0));
    check!(restored.out.error_code == codes::NONE);
}
