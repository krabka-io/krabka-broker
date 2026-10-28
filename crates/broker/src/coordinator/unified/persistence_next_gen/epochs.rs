//! The two next-gen records whose value is a single epoch counter.
//!
//! The group metadata record at key version 3 carries the group epoch, and the
//! target-assignment metadata record at key version 6 carries the assignment
//! epoch.
//!
//! # Layout
//!
//! Both come from the Apache Kafka schemas at tag `4.3.1`, and both declare
//! `"flexibleVersions": "0+"`:
//!
//! - `ConsumerGroupMetadataValue`: `Epoch` (int32), then the tagged
//!   `MetadataHash` (int64, tag 0, default 0), which KIP-1101 added for
//!   Kafka's own rebalance trigger. The group keeps it as Kafka does (see
//!   [`topic_hash`](crate::coordinator::unified::topic_hash)).
//! - `ConsumerGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0), which
//!   KIP-1263 added.
//!
//! Kafka's generated writer leaves a tagged field out when it holds its
//! default, and its reader restores the default for an absent one. A record
//! whose tagged fields all hold their defaults therefore ends in the empty
//! tagged-field trailer, the single `0` byte whose absence is what makes
//! Kafka's reader underflow.

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FlexibleEpochValue)]
#[epoch_value(record = "ConsumerGroupMetadataValue")]
pub struct GroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: the hash of the subscribed topics' metadata
    /// when the epoch was written.
    pub metadata_hash: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FlexibleEpochValue)]
#[epoch_value(record = "ConsumerGroupTargetAssignmentMetadataValue")]
pub struct TargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
    /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
    /// which the assignment calculation finished, or 0 when it is unknown.
    pub assignment_timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::test_support::wire_bytes,
        assert2::assert,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    /// `ConsumerGroupMetadataValue` as Kafka 4.3.1's generated writer lays it
    /// out: i16 version 0, i32 `Epoch`, then the tagged-field trailer, which
    /// holds `MetadataHash` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn group_metadata_value_rejects_a_missing_tagged_trailer() {
        // This is the byte whose absence made Kafka's generated reader throw
        // BufferUnderflowException over the whole topic.
        let full = GroupMetadataValue {
            epoch: 7,
            metadata_hash: 0,
        }
        .encode();
        let truncated = &full[..full.len() - 1];
        assert!(GroupMetadataValue::decode(truncated).is_err());
    }

    /// `ConsumerGroupTargetAssignmentMetadataValue` as Kafka 4.3.1's
    /// generated writer lays it out: i16 version 0, i32 `AssignmentEpoch`,
    /// then the tagged-field trailer, which holds `AssignmentTimestamp` as tag
    /// 0 with an eight-byte payload unless it is the default 0.
    #[derive(Debug, PartialEq)]
    enum ConsumerEpochRecord {
        Group(GroupMetadataValue),
        Target(TargetAssignmentMetadataValue),
    }
    use crate::coordinator::unified::codec_observations::KafkaRecordRoundTrip;
    type ConsumerEpochCompatibility = KafkaRecordRoundTrip<ConsumerEpochRecord>;
    #[rstest::fixture]
    fn epochs() -> ConsumerEpochCompatibility {
        // Start this scenario with fresh inputs and observations.
        ConsumerEpochCompatibility::default()
    }
    #[given("the consumer group epoch is {epoch} with metadata hash {hash}")]
    fn group_epoch_input(epochs: &mut ConsumerEpochCompatibility, epoch: i32, hash: i64) {
        epochs.input = Some(ConsumerEpochRecord::Group(GroupMetadataValue {
            epoch,
            metadata_hash: hash,
        }));
    }
    #[given("the consumer assignment epoch is {epoch} with completion timestamp {timestamp}")]
    fn target_epoch_input(epochs: &mut ConsumerEpochCompatibility, epoch: i32, timestamp: i64) {
        epochs.input = Some(ConsumerEpochRecord::Target(TargetAssignmentMetadataValue {
            assignment_epoch: epoch,
            assignment_timestamp_ms: timestamp,
        }));
    }
    #[given("Kafka encodes that consumer epoch record as {hex}")]
    fn kafka_epoch_bytes(epochs: &mut ConsumerEpochCompatibility, hex: String) {
        epochs.golden = wire_bytes(&[&hex]);
    }
    #[when("the coordinator writes the consumer epoch record and reads Kafka bytes")]
    fn epoch_codec(epochs: &mut ConsumerEpochCompatibility) {
        let (encoded, decoded) = match epochs.input.as_ref().expect("epoch record prepared") {
            ConsumerEpochRecord::Group(value) => (
                value.encode(),
                ConsumerEpochRecord::Group(
                    GroupMetadataValue::decode(&epochs.golden).expect("read Kafka group epoch"),
                ),
            ),
            ConsumerEpochRecord::Target(value) => (
                value.encode(),
                ConsumerEpochRecord::Target(
                    TargetAssignmentMetadataValue::decode(&epochs.golden)
                        .expect("read Kafka assignment epoch"),
                ),
            ),
        };
        epochs.encoded = Some(encoded);
        epochs.decoded = Some(decoded);
    }
    #[then("the consumer epoch bytes match Kafka exactly and all epoch fields are recovered")]
    fn epoch_golden(epochs: &ConsumerEpochCompatibility) { epochs.assert_compatible(); }
    scenarios!("tests/features/migrated/src/coordinator/unified/persistence_next_gen/epochs.feature",fixtures=[epochs:ConsumerEpochCompatibility]);
}
