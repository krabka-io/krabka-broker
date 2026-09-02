//! Test doubles that the barrier unit tests share: the metadata records of a
//! topic, the metadata source the fixtures read them through, and the helper
//! that opens a real partition with a live writer. Both the fan-out tests and
//! the coordinator tests need them.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use uuid::Uuid;

use crate::{partition_registry::PartitionRegistry, test_support::FakeMetadataSource};

/// The topic and partition records of one topic, with one leader for every
/// partition and a leader epoch of 3.
pub(crate) fn topic_records(topic: &str, partitions: i32, leader: NodeId) -> Vec<MetadataRecord> {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: topic.to_owned(),
        topic_id: Uuid::new_v4(),
        partitions,
        replication_factor: 1,
    })];
    for p in 0..partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: p,
            leader,
            replicas: vec![leader],
            isr: vec![leader],
            leader_epoch: krabka_metadata::LeaderEpoch(3),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    records
}

/// Open a real partition with a live writer, and register it.
pub(crate) fn open_partition(registry: &PartitionRegistry, dir: &Path, topic: &str, index: i32) {
    let partition_dir = crate::log_dir::partition_dir(dir, topic, index);
    std::fs::create_dir_all(&partition_dir).expect("create the partition directory");
    let log = Log::open(&partition_dir, LogConfig::default()).expect("open the log");
    let partition = crate::broker::spawn_partition(
        topic.to_owned(),
        PartitionIndex(index),
        dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    registry.insert(topic.to_owned(), PartitionIndex(index), partition);
}

/// A metadata source over `records` that fails the test if the code under test
/// submits a metadata change.
///
/// No barrier path writes controller metadata: the coordinator drives cuts
/// through the state partitions and the injector writes markers to data
/// partitions. The hand-rolled double these fixtures used to share enforced
/// that by panicking from `submit_change`, and the shared fake's successful
/// default would let a coordinator that started writing metadata pass
/// unnoticed, because no barrier test reads `submitted()`. The rejecting
/// `on_submit` keeps the invariant.
pub(crate) fn metadata_source(records: &[MetadataRecord]) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .records(records)
        .on_submit(|batch| panic!("the barrier tests submit no metadata change, got {batch:?}"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::metadata_source;
    use crate::metadata_source::MetadataSource;

    #[tokio::test]
    #[should_panic(expected = "the barrier tests submit no metadata change")]
    async fn the_shared_source_rejects_a_metadata_write() {
        let source = metadata_source(&[]);
        let _ = source.submit_change(Vec::new()).await;
    }
}
