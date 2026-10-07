//! The durable delta for one group-state transition.
//!
//! [`PendingRecords`] collects the mutations a transition makes, encodes them
//! as the single `RecordBatch` that `OffsetsLog::append` takes, and applies the
//! same delta to the coordinator's respawn cache once the append succeeds.

use crate::{
    coordinator::unified::{
        GroupCoordinator,
        persistence_next_gen::{
            CurrentMemberAssignmentValue, GroupMetadataValue, MemberMetadataValue, NextGenKey,
            RegularExpressionValue, TargetAssignmentMemberValue, TargetAssignmentMetadataValue,
            encode_key,
        },
    },
    error::BrokerError,
};

/// The key of the deprecated k4 `ConsumerGroupPartitionMetadata` record.
fn partition_metadata_key(group_id: &str) -> Result<bytes::Bytes, BrokerError> {
    encode_key(&NextGenKey::PartitionMetadata {
        group_id: group_id.into(),
    })
}

/// What a transition writes for the deprecated k4
/// `ConsumerGroupPartitionMetadata` record.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionMetadataWrite {
    /// Nothing.
    #[default]
    Keep,
    /// Its tombstone, right after the k3 epoch record.
    Tombstone,
}

#[derive(Debug, Default)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// The regular expressions whose resolution the transition writes
    /// (`Some(value)`) or tombstones (`None`), by regular expression.
    pub resolved_regexes: Vec<(String, Option<RegularExpressionValue>)>,
    /// `Some(value)` writes the record. `None` writes a tombstone (null
    /// value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
    /// When set, the batch also tombstones the classic k2 `GroupMetadata`
    /// record for this group. An upgrade flip sets it.
    pub classic_group_metadata_tombstone: bool,
    /// Whether the batch tombstones the deprecated k4
    /// `ConsumerGroupPartitionMetadata` on a metadata update, as Kafka 4.3.1
    /// does for a group that holds one. A k3 tombstone carries a k4 tombstone
    /// whatever this says, as Kafka's `createGroupTombstoneRecords` does.
    pub partition_metadata: PartitionMetadataWrite,
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
            && self.resolved_regexes.is_empty()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && !self.classic_group_metadata_tombstone
            && self.partition_metadata == PartitionMetadataWrite::Keep
            && !self.next_gen_group_metadata_tombstone
            && !self.next_gen_target_metadata_tombstone
            && self.classic_group_metadata.is_none()
    }

    crate::coordinator::unified::persistence::encode_membership_records! {
        @method
        /// Encodes the delta as the one batch that `OffsetsLog::append` takes.
        ///
        /// # Errors
        ///
        /// Returns [`crate::error::BrokerError::Protocol`] when a string of a record key or of the
        /// classic group value is longer than 32767 bytes, which a non-flexible
        /// field cannot carry.
        fn to_batch(&self);
        batch, self, group_id, now_ms, borrowed;
            (typed, encode_key, NextGenKey);
            before_members {
                // Kafka's `updateSubscriptionMetadata` adds the k4 tombstone right
                // after the epoch record.
                if self.group_metadata.is_some()
                    && self.partition_metadata == PartitionMetadataWrite::Tombstone
                {
                    batch.push(partition_metadata_key(group_id)?, None);
                }
                batch.extend_values(
                    self.resolved_regexes
                        .iter()
                        .map(|(id, value)| (id, value.as_ref())),
                    |regex| {
                        encode_key(&NextGenKey::RegularExpression {
                            group_id: group_id.into(),
                            regex: regex.clone(),
                        })
                    },
                    RegularExpressionValue::encode,
                )?;
            }
            before_target {}
            after_members {
                if self.classic_group_metadata_tombstone {
                    batch.push(
                        crate::coordinator::unified::persistence::encode_key(
                            &crate::coordinator::unified::persistence::Key::GroupMetadata {
                                group_id: group_id.into(),
                            },
                        )?,
                        None,
                    );
                }
                // A deletion or a downgrade tombstones k4 just before k3, as Kafka's
                // `createGroupTombstoneRecords` does.
                if self.next_gen_group_metadata_tombstone {
                    batch.push(partition_metadata_key(group_id)?, None);
                }
                if self.next_gen_group_metadata_tombstone {
                    batch.push(
                        encode_key(&NextGenKey::GroupMetadata {
                            group_id: group_id.into(),
                        })?,
                        None,
                    );
                }
                if self.next_gen_target_metadata_tombstone {
                    batch.push(
                        encode_key(&NextGenKey::TargetAssignmentMetadata {
                            group_id: group_id.into(),
                        })?,
                        None,
                    );
                }
                if let Some(v) = &self.classic_group_metadata {
                    batch.push(
                        crate::coordinator::unified::persistence::encode_key(
                            &crate::coordinator::unified::persistence::Key::GroupMetadata {
                                group_id: group_id.into(),
                            },
                        )?,
                        Some(v.encode_value()?),
                    );
                }
            }
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
                seed.metadata_hash = value.metadata_hash;
            }
            if self.partition_metadata == PartitionMetadataWrite::Tombstone {
                seed.has_subscription_metadata_record = false;
            }
            for (regex, value) in self.resolved_regexes {
                if let Some(value) = value {
                    seed.resolved_regexes.insert(regex, value);
                } else {
                    seed.resolved_regexes.remove(&regex);
                }
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
                seed.assignment_timestamp_ms = value.assignment_timestamp_ms;
            }
            if self.next_gen_target_metadata_tombstone {
                seed.target_epoch = 0;
                seed.assignment_timestamp_ms = 0;
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
    use std::time::{Duration, Instant};

    use assert2::assert;

    use super::*;
    use crate::{
        coordinator::unified::{
            actor::{
                persistence::snapshot_pending_after_change,
                test_support::{make_coordinator, subscribed_member},
            },
            consumer_state::GroupState,
            persistence_next_gen::MemberAssignmentState,
        },
        error::BrokerError,
    };

    #[test]
    fn pending_records_empty_yields_empty_batch() {
        let p = PendingRecords::default();
        let batch = p.to_batch("g", 0).unwrap();
        assert!(batch.records.is_empty());
    }

    #[test]
    fn pending_records_offset_deltas_are_sequential() {
        let p = PendingRecords {
            group_metadata: Some(GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
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
                assignment_timestamp_ms: 0,
            }),
            ..Default::default()
        };
        let batch = p.to_batch("g", 0).unwrap();
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
        let batch = p.to_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    /// A delta with one record of every kind that writes a string with an
    /// `INT16` length: the keys of the next-gen records, the classic group
    /// value, and the classic group tombstone.
    fn every_record() -> PendingRecords {
        use crate::coordinator::unified::persistence::{
            GroupMetadataValue as ClassicGroupValue, MemberMetadata,
        };

        PendingRecords {
            group_metadata: Some(GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            resolved_regexes: vec![("r".into(), None)],
            member_metadata: vec![("m".into(), None)],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }),
            target_per_member: vec![("m".into(), None)],
            current_per_member: vec![("m".into(), None)],
            classic_group_metadata_tombstone: true,
            partition_metadata: PartitionMetadataWrite::Tombstone,
            next_gen_group_metadata_tombstone: true,
            next_gen_target_metadata_tombstone: true,
            classic_group_metadata: Some(ClassicGroupValue {
                protocol_type: "consumer".into(),
                generation: 1,
                protocol_name: Some("range".into()),
                leader: Some("m".into()),
                current_state_timestamp_ms: 0,
                members: vec![MemberMetadata {
                    member_id: "m".into(),
                    group_instance_id: Some("i".into()),
                    client_id: "c".into(),
                    client_host: "h".into(),
                    rebalance_timeout_ms: 1,
                    session_timeout_ms: 1,
                    subscription: bytes::Bytes::new(),
                    assignment: bytes::Bytes::new(),
                }],
            }),
        }
    }

    /// Every string that a group request or the broker supplies to these
    /// records is written with an `INT16` length. A string of 32767 bytes
    /// encodes, and one of 32768 bytes makes the whole batch an error, not a
    /// panic in the actor that writes it.
    #[test]
    fn a_record_string_over_32767_bytes_does_not_encode() {
        type Field = (&'static str, fn(&mut PendingRecords, String));
        let fields: [Field; 11] = [
            ("member id of a member record", |p, s| {
                p.member_metadata[0].0 = s;
            }),
            ("member id of a target assignment", |p, s| {
                p.target_per_member[0].0 = s;
            }),
            ("member id of a current assignment", |p, s| {
                p.current_per_member[0].0 = s;
            }),
            ("regular expression", |p, s| p.resolved_regexes[0].0 = s),
            ("classic protocol type", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().protocol_type = s;
            }),
            ("classic protocol name", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().protocol_name = Some(s);
            }),
            ("classic leader", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().leader = Some(s);
            }),
            ("classic member id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].member_id = s;
            }),
            ("classic instance id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].group_instance_id = Some(s);
            }),
            ("classic client id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].client_id = s;
            }),
            ("classic client host", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].client_host = s;
            }),
        ];
        let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
        for (name, set) in fields {
            for (length, encodes) in [(limit, true), (limit + 1, false)] {
                let mut pending = every_record();
                set(&mut pending, "a".repeat(length));

                let outcome = pending.to_batch("g", 0);

                assert!(outcome.is_ok() == encodes, "{name} of {length} bytes");
                assert!(
                    encodes || matches!(outcome, Err(BrokerError::Protocol(_))),
                    "{name} of {length} bytes is a protocol error"
                );
            }
        }
        for (length, encodes) in [(limit, true), (limit + 1, false)] {
            let outcome = every_record().to_batch(&"g".repeat(length), 0);

            assert!(outcome.is_ok() == encodes, "group id of {length} bytes");
        }
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

        let mut m = subscribed_member(
            "m1",
            &["t"],
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        m.member_epoch = 7;
        m.previous_member_epoch = 6;
        m.assigned_partitions.insert(topic, vec![0, 1]);
        m.assignment_epochs.insert(topic, [(0, 4), (1, 7)].into());
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
            .insert("m1".to_string(), maplit::hashmap! {topic => vec![0, 1, 2]});
        state.add_or_update_member(m);

        let mut pending = snapshot_pending_after_change(&state, &["m1".to_string()], true);
        let resolution = p::RegularExpressionValue {
            topics: vec!["t".to_string()],
            version: 9,
            timestamp_ms: 1_000,
        };
        pending.resolved_regexes = vec![("t.*".to_string(), Some(resolution.clone()))];
        let (coordinator, _) = make_coordinator();
        pending.apply_to_cache(&coordinator, "g");
        let seed = coordinator.cached_seed("g").expect("cached seed");

        let expected = crate::coordinator::unified::GroupSeed {
            has_subscription_metadata_record: false,
            group_epoch: 7,
            metadata_hash: 0,
            target_epoch: 6,
            assignment_timestamp_ms: 0,
            members: maplit::hashmap! {"m1".to_string() => p::MemberMetadataValue {
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
                }),
            }},
            target_per_member: maplit::hashmap! {"m1".to_string() => p::TargetAssignmentMemberValue {
                topic_partitions: vec![p::AssignedTopicPartitions {
                    topic_id: topic,
                    partitions: vec![0, 1, 2],
                }],
            }},
            current_per_member: maplit::hashmap! {"m1".to_string() => p::CurrentMemberAssignmentValue {
                member_epoch: 7,
                previous_member_epoch: 6,
                state: MemberAssignmentState::Stable,
                assigned_partitions: vec![p::CurrentTopicPartitions {
                    topic_id: topic,
                    partitions: vec![0, 1],
                    assignment_epochs: Some(vec![4, 7]),
                }],
                partitions_pending_revocation: vec![],
            }},
            resolved_regexes: maplit::hashmap! {"t.*".to_string() => resolution},
        };
        assert!(seed == expected);
    }

    /// A resolved regular expression is written under the key of Kafka's
    /// `ConsumerGroupRegularExpressionKey`, and a tombstone removes it from
    /// the respawn cache.
    #[test]
    fn a_resolved_regex_is_written_and_tombstoned() {
        use crate::coordinator::unified::persistence_next_gen as p;

        let value = p::RegularExpressionValue {
            topics: vec!["t".to_string()],
            version: 9,
            timestamp_ms: 1_000,
        };
        let write = PendingRecords {
            resolved_regexes: vec![("t.*".to_string(), Some(value.clone()))],
            ..Default::default()
        };
        let batch = write.to_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(
            batch.records[0].key.as_deref()
                == Some(
                    &p::encode_key(&p::NextGenKey::RegularExpression {
                        group_id: "g".into(),
                        regex: "t.*".into(),
                    })
                    .unwrap()[..]
                )
        );
        assert!(batch.records[0].value.as_deref() == Some(&value.encode()[..]));

        let (coordinator, _) = make_coordinator();
        write.apply_to_cache(&coordinator, "g");
        assert!(coordinator.cached_seed("g").unwrap().resolved_regexes.len() == 1);

        let tombstone = PendingRecords {
            resolved_regexes: vec![("t.*".to_string(), None)],
            ..Default::default()
        };
        assert!(
            tombstone.to_batch("g", 0).unwrap().records[0]
                .value
                .is_none()
        );
        tombstone.apply_to_cache(&coordinator, "g");
        assert!(
            coordinator
                .cached_seed("g")
                .unwrap()
                .resolved_regexes
                .is_empty()
        );
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
