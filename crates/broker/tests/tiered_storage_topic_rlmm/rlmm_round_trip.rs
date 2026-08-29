//! The shared copy-then-fetch body that both the PLAINTEXT and the
//! `SASL_PLAINTEXT` round-trip tests drive, plus the two observations it needs.
//!
//! The body creates a tiered topic, waits for the config to reach the
//! partition, produces enough to seal segments, waits for the copy task to
//! tier one through the topic-backed manager, and reads offset 0 back over the
//! wire. It lives in its own module because the only difference between its
//! two callers is how they built their `Client`.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Shared copy→metadata→read body: create a tiered topic, wait for the
/// config to propagate, produce enough to seal segments, wait for the RLM
/// copy task to tier one through the topic-backed RLMM, then read offset 0
/// back. Both the plaintext loopback test and the `SASL_PLAINTEXT` variant use
/// this body. The only difference is how the caller built `client`.
pub(crate) async fn copy_then_fetch_round_trip(
    broker: &BrokerHandle,
    client: &Client,
    remote_dir: &std::path::Path,
    topic: &str,
) {
    // Tiny `segment.bytes` so a modest produce seals several segments;
    // `local.retention.bytes=1` evicts every copied segment from local
    // disk so the read-back must consult the remote tier.
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "segment.bytes".into(),
                        value: Some("1024".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    // `produce_records_for_test` stamps no record timestamp, so
                    // sealed segments carry max_timestamp_ms=0; the default 7-day
                    // `retention.ms` would then immediately evict every tiered
                    // segment (`now - 0 > 7d`). Disable time retention so the
                    // copied segments survive for the read-back.
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        resp.topics[0].error_message
    );

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
        tokio::task::yield_now().await;
    }

    // Single-record batches (~85 bytes each) roll the 1 KiB segment every
    // ~12 records, so 80 records seal several segments for the copy task.
    broker
        .produce_records_for_test(topic, 0, 80)
        .await
        .expect("produce records");

    // Wait for at least one segment to land in the remote tier. The
    // `LocalTieredStorage` layout writes each copied segment's bytes to a
    // file named `log`; its presence proves the RLM copy task's
    // `CopySegment*` events round-tripped through `__remote_log_metadata`.
    // intentional: remote-tier object presence is filesystem state on the
    // `LocalTieredStorage` backend — it is not in the metadata image and has
    // no broker metric, so poll the remote dir directly (bounded loop).
    let copy_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if count_remote_log_files(remote_dir) >= 1 {
            break;
        }
        assert!(
            Instant::now() <= copy_deadline,
            "no segment tiered to remote storage within 30s"
        );
        tokio::task::yield_now().await;
    }

    // Read offset 0 back. Whether it is served from a still-local segment
    // or (after eviction) the remote tier, a successful read exercises the
    // full path with the topic-backed RLMM active. Retry to absorb the
    // local-retention eviction race.
    // intentional: this drives the wire Fetch API and inspects the returned
    // records — a wire-response poll with no backing metric/image signal, so
    // keep the bounded retry loop.
    let topic_id = topic_id_for(client, topic).await;
    let fetch_deadline = Instant::now() + Duration::from_secs(30);
    let value = loop {
        let r = client
            .send(FetchRequest {
                max_wait_ms: 500,
                min_bytes: 1,
                topics: vec![FetchTopic {
                    topic: topic.into(),
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition: 0,
                        fetch_offset: 0,
                        partition_max_bytes: 1_048_576,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
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
            break first.value.clone();
        }
        assert!(
            Instant::now() <= fetch_deadline,
            "offset 0 never returned records within 30s"
        );
        tokio::task::yield_now().await;
    };

    assert!(
        value.as_deref() == Some(b"test-record-0".as_slice()),
        "offset 0 should read back the first produced record"
    );
}

async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Count current `*.log` files and legacy files named `log` under `root`.
/// Each one is the `LocalTieredStorage` segment-bytes object for a copied
/// segment.
pub(crate) fn count_remote_log_files(root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("log")
                || path.file_name().and_then(|name| name.to_str()) == Some("log")
            {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(root, &mut count);
    count
}
