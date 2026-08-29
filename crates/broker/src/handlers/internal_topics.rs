//! The list of topic names that the broker owns.
//!
//! `Metadata` and `DescribeTopicPartitions` both set the `is_internal` flag of
//! a topic row from this one list, so a client sees the same answer whichever
//! RPC it asks with.

/// Kafka topics owned by the broker rather than an application.
pub(crate) fn is_internal_topic(name: &str) -> bool {
    matches!(
        name,
        "__consumer_offsets" | "__transaction_state" | "__remote_log_metadata"
    )
}
