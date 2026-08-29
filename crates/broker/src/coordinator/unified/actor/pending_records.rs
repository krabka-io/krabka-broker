//! The durable delta for one group-state transition.
//!
//! [`PendingRecords`] collects the mutations a transition makes, encodes them
//! as the single `RecordBatch` that `OffsetsLog::append` takes, and applies the
//! same delta to the coordinator's respawn cache once the append succeeds.

use krabka_protocol::records::RecordBatch;

use crate::coordinator::unified::{
    GroupCoordinator, OffsetRecordBatchBuilder,
    persistence_next_gen::{
        CurrentMemberAssignmentValue, GroupMetadataValue, MemberMetadataValue, NextGenKey,
        TargetAssignmentMemberValue, TargetAssignmentMetadataValue, encode_key,
    },
};

#[derive(Debug, Default)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// `Some(value)` writes the record. `None` writes a tombstone (null
    /// value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
    /// When set, the batch also tombstones the classic k2 `GroupMetadata`
    /// record for this group. An upgrade flip sets it.
    pub classic_group_metadata_tombstone: bool,
    /// Tombstone the next-gen k3 `GroupMetadata` (downgrade flip).
    pub next_gen_group_metadata_tombstone: bool,
    /// Tombstone the next-gen k6 `TargetAssignmentMetadata` (downgrade flip).
    pub next_gen_target_metadata_tombstone: bool,
    /// Write the classic k2 `GroupMetadata` value (downgrade flip).
    pub classic_group_metadata:
        Option<crate::coordinator::unified::persistence::GroupMetadataValue>,
}

impl PendingRecords {
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && !self.classic_group_metadata_tombstone
            && !self.next_gen_group_metadata_tombstone
            && !self.next_gen_target_metadata_tombstone
            && self.classic_group_metadata.is_none()
    }

    pub fn to_batch(&self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut batch = OffsetRecordBatchBuilder::default();

        if let Some(v) = self.group_metadata {
            batch.push(
                encode_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in &self.member_metadata {
            batch.push(
                encode_key(&NextGenKey::MemberMetadata {
                    group_id: group_id.into(),
                    member_id: member_id.clone(),
                }),
                v.as_ref().map(MemberMetadataValue::encode),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                encode_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in &self.target_per_member {
            batch.push(
                encode_key(&NextGenKey::TargetAssignmentMember {
                    group_id: group_id.into(),
                    member_id: member_id.clone(),
                }),
                v.as_ref().map(TargetAssignmentMemberValue::encode),
            );
        }
        for (member_id, v) in &self.current_per_member {
            batch.push(
                encode_key(&NextGenKey::CurrentMemberAssignment {
                    group_id: group_id.into(),
                    member_id: member_id.clone(),
                }),
                v.as_ref().map(CurrentMemberAssignmentValue::encode),
            );
        }
        if self.classic_group_metadata_tombstone {
            batch.push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group_id.into(),
                    },
                ),
                None,
            );
        }
        if self.next_gen_group_metadata_tombstone {
            batch.push(
                encode_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
        }
        if self.next_gen_target_metadata_tombstone {
            batch.push(
                encode_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
        }
        if let Some(v) = &self.classic_group_metadata {
            batch.push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group_id.into(),
                    },
                ),
                Some(v.encode_value()),
            );
        }

        batch.finish(now_ms)
    }

    /// Apply exactly this durable next-gen record delta to the respawn cache.
    pub(super) fn apply_to_cache(self, coordinator: &GroupCoordinator, group_id: &str) {
        if self.next_gen_group_metadata_tombstone {
            coordinator.remove_cached_seed(group_id);
            return;
        }
        coordinator.update_cached_seed(group_id, |seed| {
            if let Some(value) = self.group_metadata {
                seed.group_epoch = value.epoch;
            }
            for (member_id, value) in self.member_metadata {
                if let Some(value) = value {
                    seed.members.insert(member_id, value);
                } else {
                    seed.members.remove(&member_id);
                }
            }
            if let Some(value) = self.target_metadata {
                seed.target_epoch = value.assignment_epoch;
            }
            if self.next_gen_target_metadata_tombstone {
                seed.target_epoch = 0;
            }
            for (member_id, value) in self.target_per_member {
                if let Some(value) = value {
                    seed.target_per_member.insert(member_id, value);
                } else {
                    seed.target_per_member.remove(&member_id);
                }
            }
            for (member_id, value) in self.current_per_member {
                if let Some(value) = value {
                    seed.current_per_member.insert(member_id, value);
                } else {
                    seed.current_per_member.remove(&member_id);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    use assert2::assert;
    use bytes::Bytes;
    use krabka_protocol::{
        owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        primitives::uuid::Uuid,
    };

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            member_state::build_member, persistence::snapshot_pending_after_change,
            test_support::make_coordinator,
        },
        consumer_state::GroupState,
        persistence_next_gen::MemberAssignmentState,
    };

    #[test]
    fn pending_records_empty_yields_empty_batch() {
        let p = PendingRecords::default();
        let batch = p.to_batch("g", 0);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn pending_records_offset_deltas_are_sequential() {
        let p = PendingRecords {
            group_metadata: Some(GroupMetadataValue { epoch: 1 }),
            member_metadata: vec![(
                "m1".into(),
                Some(MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c".into(),
                    client_host: "h".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: None,
                }),
            )],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
            }),
            ..Default::default()
        };
        let batch = p.to_batch("g", 0);
        assert!(batch.records.len() == 3);
        let deltas: Vec<i32> = batch.records.iter().map(|r| r.offset_delta).collect();
        assert!(deltas == vec![0, 1, 2]);
        assert!(batch.last_offset_delta == 2);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.to_batch("g", 0);
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    #[test]
    fn pending_delta_populates_cache_including_classic_facade() {
        use crate::coordinator::unified::persistence_next_gen as p;

        let topic = {
            let mut b = [0u8; 16];
            b[15] = 0xEF;
            krabka_protocol::primitives::uuid::Uuid(b)
        };
        let mut state = GroupState::new("g");
        state.group_epoch = 7;
        state.target.epoch = 6;

        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        m.member_epoch = 7;
        m.previous_member_epoch = 6;
        m.assigned_partitions.insert(topic, vec![0, 1]);
        m.classic = Some(
            crate::coordinator::unified::consumer_state::ClassicMemberFacade {
                generation_id: 7,
                supported_protocols: vec![(
                    "range".to_string(),
                    bytes::Bytes::from_static(b"meta"),
                )],
                session_timeout: Duration::from_secs(45),
                last_synced_assignment: bytes::Bytes::from_static(b"assigned"),
                awaiting_sync: false,
            },
        );
        state
            .target
            .per_member
            .insert("m1".to_string(), HashMap::from([(topic, vec![0, 1, 2])]));
        state.add_or_update_member(m);

        let pending = snapshot_pending_after_change(&state, &["m1".to_string()], true);
        let (coordinator, _) = make_coordinator();
        pending.apply_to_cache(&coordinator, "g");
        let seed = coordinator.cached_seed("g").expect("cached seed");

        let expected = crate::coordinator::unified::GroupSeed {
            group_epoch: 7,
            target_epoch: 6,
            members: HashMap::from([(
                "m1".to_string(),
                p::MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "client-a".to_string(),
                    client_host: "h".to_string(),
                    subscribed_topic_names: vec!["t".to_string()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: Some(p::ClassicMemberMetadata {
                        session_timeout_ms: 45_000,
                        supported_protocols: vec![(
                            "range".to_string(),
                            bytes::Bytes::from_static(b"meta"),
                        )],
                        last_synced_assignment: bytes::Bytes::from_static(b"assigned"),
                    }),
                },
            )]),
            target_per_member: HashMap::from([(
                "m1".to_string(),
                p::TargetAssignmentMemberValue {
                    topic_partitions: vec![p::AssignedTopicPartitions {
                        topic_id: topic,
                        partitions: vec![0, 1, 2],
                    }],
                },
            )]),
            current_per_member: HashMap::from([(
                "m1".to_string(),
                p::CurrentMemberAssignmentValue {
                    member_epoch: 7,
                    previous_member_epoch: 6,
                    state: MemberAssignmentState::Stable,
                    assigned_partitions: vec![p::AssignedTopicPartitions {
                        topic_id: topic,
                        partitions: vec![0, 1],
                    }],
                    partitions_pending_revocation: vec![],
                },
            )]),
        };
        assert!(seed == expected);
    }

    #[test]
    fn pending_group_tombstone_removes_cached_seed() {
        let (coordinator, _) = make_coordinator();
        coordinator.update_cached_seed("g", |seed| seed.group_epoch = 7);
        PendingRecords {
            next_gen_group_metadata_tombstone: true,
            ..Default::default()
        }
        .apply_to_cache(&coordinator, "g");

        assert!(coordinator.cached_seed("g").is_none());
    }
}
