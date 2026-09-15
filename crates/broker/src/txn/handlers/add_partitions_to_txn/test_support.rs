//! Fixtures shared by the `AddPartitionsToTxn` unit tests: a request topic
//! entry, and the fully pinned response row that a whole-value comparison
//! checks the handler's output against.

use krabka_protocol::owned::common::{
    add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
        add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};

pub(super) fn topic(name: &str, partitions: &[i32]) -> AddPartitionsToTxnTopic {
    AddPartitionsToTxnTopic {
        name: name.into(),
        partitions: partitions.to_vec(),
        ..Default::default()
    }
}

/// Builds a fully pinned expected topic-result row. Every field is
/// explicit, so that whole-value comparisons kill field-drop mutants.
pub(super) fn topic_result(name: &str, rows: &[(i32, i16)]) -> AddPartitionsToTxnTopicResult {
    AddPartitionsToTxnTopicResult {
        name: name.into(),
        results_by_partition: rows
            .iter()
            .map(
                |&(partition_index, partition_error_code)| AddPartitionsToTxnPartitionResult {
                    partition_index,
                    partition_error_code,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            )
            .collect(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
    }
}

/// Adds `topic` with `partitions` partitions, each led by this broker, to the
/// metadata image, so the existence check of `AddPartitionsToTxn` finds it.
pub(super) async fn seed_topic(broker: &crate::broker::Broker, topic: &str, partitions: i32) {
    let mut records = vec![krabka_metadata::MetadataRecord::V1Topic(
        krabka_metadata::TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::new_v4(),
            partitions,
            replication_factor: 1,
        },
    )];
    records.extend((0..partitions).map(|partition| {
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: topic.to_owned(),
            partition,
            leader: broker.config.node_id,
            replicas: vec![broker.config.node_id],
            isr: vec![broker.config.node_id],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil()],
            partition_epoch: 0,
        })
    }));
    broker
        .controller
        .submit_change(records)
        .await
        .unwrap_or_else(|error| panic!("seed topic {topic}: {error}"));
}
