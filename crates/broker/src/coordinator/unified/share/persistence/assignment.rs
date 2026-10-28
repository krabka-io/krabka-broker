//! The KIP-932 target and current assignment records, and the topic-partition
//! list codec they share.
//!
//! [`ShareGroupTargetAssignmentMemberValue`] at key version 13 holds the
//! assignment the coordinator computed for one member, and
//! [`ShareGroupCurrentMemberAssignmentValue`] at key version 14 holds what that
//! member has converged on at its member epoch. Share groups never revoke, so
//! the broker keeps no revocation list and no reconciliation state of its own.
//!
//! # Layout
//!
//! From `ShareGroupTargetAssignmentMemberValue.json` and
//! `ShareGroupCurrentMemberAssignmentValue.json` at Apache Kafka tag `4.3.1`.
//! Both declare `"flexibleVersions": "0+"`.
//!
//! - Target: `TopicPartitions` (`[]TopicPartition{TopicId uuid, Partitions
//!   []int32}`).
//! - Current: `MemberEpoch` (int32), `PreviousMemberEpoch` (int32), `State`
//!   (int8), `AssignedPartitions` (`[]TopicPartitions{TopicId uuid, Partitions
//!   []int32}`).
//!
//! The current record's `State` field is plain, not tagged, so it is always on
//! the wire. A share member that never revokes is at `MemberState.STABLE`,
//! whose discriminant is 0, and the decoder drops it again.
//! `PreviousMemberEpoch` is the member epoch before the last bump, which the
//! heartbeat still accepts (`throwIfShareGroupMemberEpochIsInvalid`).

use {
    crate::{
        coordinator::unified::persistence::{
            flex::{get_assigned_topics, get_i8, put_assigned_topics},
            get_i32,
        },
        error::BrokerError,
    },
    bytes::{BufMut, BytesMut},
    krabka_protocol::primitives::uuid::Uuid,
};

/// `org.apache.kafka.coordinator.group.modern.MemberState.STABLE`.
const MEMBER_STATE_STABLE: i8 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Default, krabka_macros::FlexibleArrayValue)]
#[array_value(
    record = "ShareGroupTargetAssignmentMemberValue",
    encode = encode_topic_partitions,
    decode = decode_topic_partitions,
)]
pub struct ShareGroupTargetAssignmentMemberValue {
    pub topic_partitions: Vec<(Uuid, Vec<i32>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub assigned_partitions: Vec<(Uuid, Vec<i32>)>,
}

#[krabka_macros::flexible_value(record = "ShareGroupCurrentMemberAssignmentValue")]
impl ShareGroupCurrentMemberAssignmentValue {
    fn encode_fields(&self, buf: &mut BytesMut) {
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(MEMBER_STATE_STABLE);
        encode_topic_partitions(buf, &self.assigned_partitions);
    }

    fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let member_epoch = get_i32(buf)?;
        let previous_member_epoch = get_i32(buf)?;
        let _state = get_i8(buf)?;
        let assigned_partitions = decode_topic_partitions(buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            assigned_partitions,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[(Uuid, Vec<i32>)]) {
    put_assigned_topics(buf, items, |(topic, partitions)| (topic, partitions));
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<(Uuid, Vec<i32>)>, BrokerError> {
    get_assigned_topics(buf, |topic, partitions| (topic, partitions))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            codec_observations::EncodedRecord,
            share::persistence::{
                KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT, KEY_SHARE_TARGET_ASSIGNMENT_MEMBER,
                ShareGroupKey,
            },
            test_support::wire_bytes,
        },
        assert2::assert,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    #[test]
    fn target_assignment_member_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMember {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_TARGET_ASSIGNMENT_MEMBER),
        );

        let v = ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(Uuid([1; 16]), vec![0, 1, 2]), (Uuid([2; 16]), vec![])],
        };
        assert!(ShareGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let key = ShareGroupKey::CurrentMemberAssignment {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT),
        );

        let v = ShareGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 2,
            assigned_partitions: vec![(Uuid([3; 16]), vec![0, 1])],
        };
        assert!(ShareGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn assignment_records_reject_a_missing_tagged_trailer() {
        let t = ShareGroupTargetAssignmentMemberValue::default().encode();
        assert!(ShareGroupTargetAssignmentMemberValue::decode(&t[..t.len() - 1]).is_err());
        let c = ShareGroupCurrentMemberAssignmentValue::default().encode();
        assert!(ShareGroupCurrentMemberAssignmentValue::decode(&c[..c.len() - 1]).is_err());
    }
    mod bdd_target_assignment_member {
        use {super::*, assert2::assert};
        type EncodedValue = EncodedRecord<ShareGroupTargetAssignmentMemberValue>;
        #[rstest::fixture]
        fn stored_value() -> EncodedValue {
            // Start this scenario with fresh inputs and observations.
            EncodedValue::default()
        }
        #[given("a Share target assignment assigns partition 3 of the all-one topic UUID")]
        fn prepared_value(stored_value: &mut EncodedValue) {
            let v = ShareGroupTargetAssignmentMemberValue {
                topic_partitions: vec![(Uuid([1; 16]), vec![3])],
            };

            stored_value.value = Some(v);
        }
        #[when(
            "the coordinator persists the Share target member assignment in the compacted group log"
        )]
        fn encode_group_record(stored_value: &mut EncodedValue) {
            stored_value.encoded = Some(
                stored_value
                    .value
                    .as_ref()
                    .expect("group record prepared")
                    .encode(),
            );
        }
        #[then(
            "the persisted Share target member assignment matches Kafka schema bytes including compact lengths and tagged trailers"
        )]
        fn kafka_record_layout(stored_value: &EncodedValue) {
            let want = wire_bytes(&[
                "000002",
                "01010101010101010101010101010101",
                "02",
                "00000003",
                "00", // TopicPartition tagged fields
                "00", // message tagged fields
            ]);

            assert!(&stored_value.encoded.as_ref().expect("record encoded")[..] == &want[..]);
        }
        scenarios!("tests/features/migrated/src/coordinator/unified/share/persistence/assignment.feature",tags="@target_assignment_member",fixtures=[stored_value: EncodedValue]);
    }
    mod bdd_current_member_assignment {
        use {super::*, assert2::assert};
        type EncodedValue = EncodedRecord<ShareGroupCurrentMemberAssignmentValue>;
        #[rstest::fixture]
        fn stored_value() -> EncodedValue {
            // Start this scenario with fresh inputs and observations.
            EncodedValue::default()
        }
        #[given("a Share current assignment at epochs 5 and 4 has no assigned partitions")]
        fn prepared_value(stored_value: &mut EncodedValue) {
            let v = ShareGroupCurrentMemberAssignmentValue {
                member_epoch: 5,
                previous_member_epoch: 4,
                assigned_partitions: vec![],
            };

            stored_value.value = Some(v);
        }
        #[when(
            "the coordinator encodes this ShareGroupCurrentMemberAssignmentValue record for the compacted group log"
        )]
        fn encode_group_record(stored_value: &mut EncodedValue) {
            stored_value.encoded = Some(
                stored_value
                    .value
                    .as_ref()
                    .expect("group record prepared")
                    .encode(),
            );
        }
        #[then(
            "the ShareGroupCurrentMemberAssignmentValue record matches Kafka schema bytes including compact lengths and tagged trailers"
        )]
        fn kafka_record_layout(stored_value: &EncodedValue) {
            let want = wire_bytes(&[
                "0000", "00000005", // MemberEpoch
                "00000004", // PreviousMemberEpoch
                "00",       // MemberState.STABLE
                "01",       // empty AssignedPartitions
                "00",       // message tagged fields
            ]);

            assert!(&stored_value.encoded.as_ref().expect("record encoded")[..] == &want[..]);
        }
        scenarios!("tests/features/migrated/src/coordinator/unified/share/persistence/assignment.feature",tags="@current_member_assignment",fixtures=[stored_value: EncodedValue]);
    }
}
