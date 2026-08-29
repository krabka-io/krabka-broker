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
