//! The produce-side workload: creating the tiered topic, filling it until the
//! leader's copy task has tiered segments, and counting what landed in the
//! shared remote directory.
//!
//! These three steps run against the leader before it is killed, and they are
//! the only part of the suite that writes anything. Keeping them together
//! leaves the failover test with a two-call setup and puts the
//! `remote.storage.enable` / `local.retention.bytes` configuration that makes
//! the eviction happen in one place.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

use crate::{RECORDS, TOPIC, multi_client::topic_id_for};

/// Counts current `*.log` files and legacy files named `log` under `root`.
/// Each one is the `LocalTieredStorage` segment-bytes object of a copied
/// segment. This is the same helper as in `tiered_storage_topic_rlmm.rs`.
fn count_remote_log_files(root: &std::path::Path) -> usize {
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

pub(crate) async fn create_tiered_topic(admin: &Client, b1: &BrokerHandle, b2: &BrokerHandle) {
    let response = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 2,
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
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {response:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ready = |broker: &BrokerHandle| {
            broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && config.segment_size == krabka_units::kibibytes(1)
                        && config.local_retention_size == Some(krabka_units::bytes(1))
                })
        };
        if ready(b1) || ready(b2) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "tiered config did not propagate"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn produce_and_await_remote_segments(
    admin: &Client,
    remote_dir: &std::path::Path,
) {
    let topic_id = topic_id_for(admin, TOPIC).await;
    for index in 0..RECORDS {
        let batch = RecordBatch {
            records: vec![Record {
                value: Some(bytes::Bytes::from(format!("test-record-{index}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = admin
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 10_000,
                topic_data: vec![TopicProduceData {
                    name: TOPIC.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        assert!(
            response.responses[0].partition_responses[0].error_code == 0,
            "Produce failed: {response:?}"
        );
    }

    let deadline = Instant::now() + Duration::from_mins(1);
    while count_remote_log_files(remote_dir) < 2 {
        assert!(
            Instant::now() <= deadline,
            "fewer than two segments were tiered"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
