//! Fixtures shared by the unit tests of several `broker` submodules: the
//! [`crate::metadata_source::MetadataSource`] they read from, a locally
//! spawned partition seeded with records, and the metadata records that tests
//! submit. They live in one module so no submodule owns a helper its siblings
//! also need.

use std::sync::Arc;

use assert2::assert;
use krabka_ids::PartitionIndex;

use crate::{
    broker::{BrokerHandle, partition_spawn::spawn_partition},
    partition::Partition,
    test_support::FakeMetadataSource,
};

/// A metadata source over `image`, with `leader` as the controller leader and
/// a loopback controller listener for the gauges and adapter paths to report.
pub(super) fn fake_source(
    image: krabka_metadata::MetadataImage,
    leader: Option<krabka_raft::NodeId>,
) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(leader)
        .controller_bound_addr("127.0.0.1:9093".parse().expect("loopback controller addr"))
        .build()
}

pub(super) fn local_partition_with_records(
    log_dir: &std::path::Path,
    topic: &str,
    partition: i32,
    values: &[&'static [u8]],
) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default())
        .expect("open partition log");
    let part = spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    if !values.is_empty() {
        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
            records: values
                .iter()
                .enumerate()
                .map(|(idx, value)| krabka_protocol::records::Record {
                    offset_delta: i32::try_from(idx).expect("offset delta fits"),
                    value: Some(bytes::Bytes::from_static(value)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        part.log
            .lock()
            .expect("partition log lock")
            .append(&mut batch)
            .expect("append records");
    }
    part
}

pub(super) fn metadata_topic_record(
    topic: &str,
    topic_id: u128,
) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: topic.to_string(),
        topic_id: uuid::Uuid::from_u128(topic_id),
        partitions: 1,
        replication_factor: 1,
    })
}

pub(super) fn metadata_partition_record(
    topic: &str,
    partition: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    leader_epoch: i32,
) -> krabka_metadata::PartitionRecord {
    krabka_metadata::PartitionRecord {
        topic: topic.to_string(),
        partition,
        leader: krabka_audit::NodeId(leader),
        replicas: replicas.iter().copied().map(krabka_audit::NodeId).collect(),
        isr: isr.iter().copied().map(krabka_audit::NodeId).collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: vec![uuid::Uuid::nil(); replicas.len()],
        partition_epoch: 0,
    }
}

pub(super) async fn submit_metadata_topic_partition(
    handle: &BrokerHandle,
    topic_spec: (&str, u128),
    partition: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    leader_epoch: i32,
) {
    let (topic, topic_id) = topic_spec;
    handle
        .submit_metadata_record_for_test(metadata_topic_record(topic, topic_id))
        .await
        .expect("submit topic record");
    let partition_record =
        metadata_partition_record(topic, partition, leader, replicas, isr, leader_epoch);
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
            partition_record.clone(),
        ))
        .await
        .expect("submit partition record");

    let image = handle.controller_image_for_test();
    assert!(image.topic(topic).is_some());
    assert!(image.partition(topic, partition) == Some(&partition_record));
}
