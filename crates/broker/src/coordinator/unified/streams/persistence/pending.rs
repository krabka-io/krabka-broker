//! [`PendingStreamsRecords`], the set of record mutations for one group-state
//! transition.
//!
//! One heartbeat can change the group epoch, the topology, and several members
//! at once. The actor collects the whole change here and encodes it as a
//! single `RecordBatch` that is ready for `OffsetsLog::append`, so the
//! transition lands in the log atomically.

use krabka_protocol::records::RecordBatch;

use super::{
    assignment::{
        StreamsGroupCurrentMemberAssignmentValue, StreamsGroupTargetAssignmentMemberValue,
    },
    epochs::{StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMetadataValue},
    keys::{
        encode_current_member_assignment_key, encode_group_metadata_key,
        encode_member_metadata_key, encode_partition_metadata_key,
        encode_target_assignment_member_key, encode_target_assignment_metadata_key,
        encode_topology_key,
    },
    member::StreamsGroupMemberMetadataValue,
    partition_metadata::StreamsGroupPartitionMetadataValue,
    topology::StreamsGroupTopologyValue,
};
use crate::{coordinator::unified::OffsetRecordBatchBuilder, error::BrokerError};

#[derive(Debug, Default)]
pub struct PendingStreamsRecords {
    pub group_metadata: Option<StreamsGroupMetadataValue>,
    /// `Some(value)` writes the record. `None` writes a tombstone (null
    /// value).
    pub member_metadata: Vec<(String, Option<StreamsGroupMemberMetadataValue>)>,
    pub topology: Option<StreamsGroupTopologyValue>,
    pub partition_metadata: Option<StreamsGroupPartitionMetadataValue>,
    pub target_metadata: Option<StreamsGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<StreamsGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<StreamsGroupCurrentMemberAssignmentValue>)>,
}

impl PendingStreamsRecords {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.topology.is_none()
            && self.partition_metadata.is_none()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
    }

    /// Encodes the delta as the one batch that `OffsetsLog::append` takes.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the group id or a member id is
    /// longer than 32767 bytes, which a non-flexible key string cannot carry.
    pub fn into_batch(self, group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
        let mut batch = OffsetRecordBatchBuilder::default();

        if let Some(v) = self.group_metadata {
            batch.push(encode_group_metadata_key(group_id)?, Some(v.encode()));
        }
        for (member_id, v) in self.member_metadata {
            batch.push(
                encode_member_metadata_key(group_id, &member_id)?,
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.topology {
            batch.push(encode_topology_key(group_id)?, Some(v.encode()));
        }
        if let Some(v) = self.partition_metadata {
            batch.push(encode_partition_metadata_key(group_id)?, Some(v.encode()));
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                encode_target_assignment_metadata_key(group_id)?,
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            batch.push(
                encode_target_assignment_member_key(group_id, &member_id)?,
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            batch.push(
                encode_current_member_assignment_key(group_id, &member_id)?,
                v.map(|x| x.encode()),
            );
        }

        Ok(batch.finish(now_ms))
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::streams::persistence::DescriptionEpochs;

    #[test]
    fn pending_records_into_batch_emits_one_record_per_key() {
        let mut pending = PendingStreamsRecords {
            group_metadata: Some(StreamsGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
                description: DescriptionEpochs::default(),
            }),
            topology: Some(StreamsGroupTopologyValue::default()),
            ..Default::default()
        };
        pending.member_metadata.push(("m1".into(), None)); // tombstone
        let batch = pending.into_batch("g1", 123).unwrap();
        // group_metadata + topology + one member tombstone = 3 records.
        check!(batch.records.len() == 3);
        check!(batch.max_timestamp == 123);
        check!(batch.last_offset_delta == 2);
        // The tombstone record carries a null value.
        let tombstone = batch.records.iter().find(|r| r.value.is_none()).unwrap();
        assert!(tombstone.key.is_some());
    }

    /// The keys of the streams records write the group id and the member id
    /// with an `INT16` length. A string of 32767 bytes encodes, and one of
    /// 32768 bytes makes the whole batch an error, not a panic in the actor.
    #[test]
    fn a_key_string_over_32767_bytes_does_not_encode() {
        fn every_record() -> PendingStreamsRecords {
            PendingStreamsRecords {
                group_metadata: Some(StreamsGroupMetadataValue {
                    epoch: 1,
                    metadata_hash: 0,
                    description: DescriptionEpochs::default(),
                }),
                member_metadata: vec![("m".into(), None)],
                topology: Some(StreamsGroupTopologyValue::default()),
                target_per_member: vec![("m".into(), None)],
                current_per_member: vec![("m".into(), None)],
                ..Default::default()
            }
        }
        type Field = (&'static str, fn(&mut PendingStreamsRecords, String));
        let fields: [Field; 3] = [
            ("member id of a member record", |p, s| {
                p.member_metadata[0].0 = s;
            }),
            ("member id of a target assignment", |p, s| {
                p.target_per_member[0].0 = s;
            }),
            ("member id of a current assignment", |p, s| {
                p.current_per_member[0].0 = s;
            }),
        ];
        let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
        for (name, set) in fields {
            for (length, encodes) in [(limit, true), (limit + 1, false)] {
                let mut pending = every_record();
                set(&mut pending, "a".repeat(length));

                let outcome = pending.into_batch("g", 0);

                assert!(outcome.is_ok() == encodes, "{name} of {length} bytes");
                assert!(
                    encodes || matches!(outcome, Err(BrokerError::Protocol(_))),
                    "{name} of {length} bytes is a protocol error"
                );
            }
        }
        for (length, encodes) in [(limit, true), (limit + 1, false)] {
            let outcome = every_record().into_batch(&"g".repeat(length), 0);

            assert!(outcome.is_ok() == encodes, "group id of {length} bytes");
        }
    }
}
