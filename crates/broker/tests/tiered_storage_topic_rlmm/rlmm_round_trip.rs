//! The shared copy-then-fetch body that both the PLAINTEXT and the
//! `SASL_PLAINTEXT` round-trip tests drive, plus the two observations it needs.
//!
//! The body creates a tiered topic, waits for the config to reach the
//! partition, produces enough to seal segments, waits for the copy task to
//! tier one through the topic-backed manager, and reads offset 0 back over the
//! wire. It lives in its own module because the only difference between its
//! two callers is how they built their `Client`.

use {
    crate::{
        support::{
            fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
            topic_id_for,
        },
        topic_fixture::{create_configured_topic, tiered_configs},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    std::{
        path::Path,
        time::{Duration, Instant},
    },
    tokio::task,
};

/// Create the tiny-segment tiered workload and append eighty records.
pub async fn prepare_round_trip(broker: &BrokerHandle, client: &Client, topic: &str) {
    // Tiny `internal.segment.bytes` so a modest produce seals several segments;
    // `local.retention.bytes=1` evicts every copied segment from local
    // disk so the read-back must consult the remote tier.
    create_configured_topic(client, topic, tiered_configs(Some("1024"))).await;

    // Wait for the tiered config to flow from the metadata image through
    // the supervisor's reconcile loop into the partition's `LogConfig`.
    // Without this gate the first batches land in a default-config log
    // (1 GiB segments, tiering off) and never roll or copy.
    // intentional: this is a local `LogConfig` override applied by the
    // reconcile loop — there is no awaiter/metric for it, so poll directly.
    let cfg_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(topic, 0)
            && cfg.remote_storage_enable
            && cfg.segment_size == krabka_units::kibibytes(1)
            && cfg.local_retention_size == Some(krabka_units::bytes(1))
        {
            break;
        }
        assert!(
            Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated within 10s; saw {:?}",
            broker.partition_log_config_for_test(topic, 0)
        );
        task::yield_now().await;
    }

    // Single-record batches (~85 bytes each) roll the 1 KiB segment every
    // ~12 records, so 80 records seal several segments for the copy task.
    broker
        .produce_records_for_test(topic, 0, 80)
        .await
        .expect("produce records");
}

/// Wait for a completed segment copy in the local archive.
pub async fn await_archived_round_trip(remote_dir: &Path) {
    // Wait for at least one segment to land in the remote tier. The
    // `LocalTieredStorage` layout writes each copied segment's bytes to a
    // file named `log`; its presence proves the RLM copy task's
    // `CopySegment*` events round-tripped through `__remote_log_metadata`.
    // intentional: remote-tier object presence is filesystem state on the
    // `LocalTieredStorage` backend — it is not in the metadata image and has
    // no broker metric, so poll the remote dir directly (bounded loop).
    let copy_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if !remote_log_files(remote_dir).is_empty() {
            break;
        }
        assert!(
            Instant::now() <= copy_deadline,
            "no segment tiered to remote storage within 30s"
        );
        task::yield_now().await;
    }
}

/// Fetch offset zero through the active topic metadata manager.
pub async fn read_first_value(client: &Client, topic: &str) -> Option<bytes::Bytes> {
    // Read offset 0 back. Whether it is served from a still-local segment
    // or (after eviction) the remote tier, a successful read exercises the
    // full path with the topic-backed RLMM active. Retry to absorb the
    // local-retention eviction race.
    // intentional: this drives the wire Fetch API and inspects the returned
    // records — a wire-response poll with no backing metric/image signal, so
    // keep the bounded retry loop.
    let topic_id = topic_id_for(client, topic).await;
    let fetch_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = client
            .send(
                single_partition_fetch()
                    .topic(topic.into())
                    .topic_id(topic_id)
                    .limits(FetchLimits::wait_for_data(RequestWaitMillis(500)))
                    .call(),
            )
            .await
            .expect("Fetch");
        if let Some(batches) = r
            .responses
            .first()
            .and_then(|t| t.partitions.first())
            .and_then(|p| p.records.as_ref())
            .and_then(|recs| recs.as_v2())
            && let Some(first) = batches.first().and_then(|b| b.records.first())
        {
            return first.value.clone();
        }
        assert!(
            Instant::now() <= fetch_deadline,
            "offset 0 never returned records within 30s"
        );
        task::yield_now().await;
    }
}

pub use crate::support::storage::remote_log_files;
