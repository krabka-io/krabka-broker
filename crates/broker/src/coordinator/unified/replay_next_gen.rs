//! Bootstrap replay of the KIP-848 consumer group records, as Kafka 4.3.1's
//! `GroupMetadataManager.replay` applies them.
//!
//! Each method applies one decoded record to the bootstrap seed and to the
//! last-known-good cache, so a group that finishes replay is ready to hydrate
//! an actor and to survive a later actor respawn. A record with a value
//! creates the group that it names, and a member record or a current
//! assignment record creates the member. A tombstone of a group or a member
//! that the replay does not hold is ignored, and a tombstone whose
//! preconditions the records before it did not establish fails the load, as
//! the `IllegalStateException` that Kafka throws does. See
//! [`replay_policy`](super::replay_policy).

use {
    super::{
        group_coordinator::GroupCoordinator,
        persistence_next_gen::{self, CurrentMemberAssignmentValue, MemberAssignmentState},
        replay_policy::{
            ExistingGroup, LEAVE_GROUP_MEMBER_EPOCH, ModernGroupType, group_tombstone,
            persisted_group_with_create, target_metadata, target_metadata_tombstone,
        },
        seeds::{GroupSeed, ReplaySeed},
    },
    crate::error::BrokerError,
};

/// The fields of a member that Kafka's replay creates from a current
/// assignment record alone: `ConsumerGroupMember.Builder`'s defaults.
fn uninitialized_member() -> persistence_next_gen::MemberMetadataValue {
    persistence_next_gen::MemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        subscribed_topic_names: Vec::new(),
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout_ms: -1,
        classic: None,
    }
}

impl GroupCoordinator {
    /// The modern group that the replay holds under `group_id`, if any.
    pub(crate) fn replayed_modern_group(&self, group_id: &str) -> Option<ExistingGroup> {
        if self.seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Consumer))
        } else if self.share_seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Share))
        } else if self.streams_seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Streams))
        } else {
            None
        }
    }

    /// Drops the consumer, share or streams group that the replay holds under
    /// `group_id`, as Kafka does when a classic `GroupMetadata` value puts a
    /// classic group in its place or a classic tombstone removes the group.
    pub(crate) fn forget_replayed_group(&self, group_id: &str) {
        if self.replayed_modern_group(group_id).is_none() {
            return;
        }
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.share_seeds.remove(group_id);
        self.share_seeds_cache.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.group_types.remove(group_id);
    }

    /// Kafka's `getOrMaybeCreatePersistedConsumerGroup` among the modern
    /// groups: whether the group exists once the lookup is done. The
    /// bootstrap replay settles a classic group under the id before it calls
    /// this.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a share or streams group.
    pub(crate) fn persisted_consumer_group(
        &self,
        group_id: &str,
        create_if_not_exists: bool,
    ) -> Result<bool, BrokerError> {
        persisted_group_with_create(
            group_id,
            ModernGroupType::Consumer,
            self.replayed_modern_group(group_id),
            create_if_not_exists,
            || {
                self.seeds.insert(group_id.into(), GroupSeed::new_group());
                self.seeds_cache
                    .insert(group_id.into(), GroupSeed::new_group());
            },
        )
    }

    /// Applies `update` to the replayed group and to its cached copy.
    fn update_consumer_seed(&self, group_id: &str, update: impl Fn(&mut GroupSeed)) {
        if let Some(mut seed) = self.seeds.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.seeds_cache
                .entry(group_id.into())
                .or_insert_with(GroupSeed::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    fn update_consumer_seed_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut GroupSeed, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.seeds.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.seeds_cache
                .entry(group_id.into())
                .or_insert_with(GroupSeed::new_group)
                .value_mut(),
            cached,
        );
    }

    /// Reads the replayed group, which must exist.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group with no replayed
    /// state.
    fn consumer_seed<T>(
        &self,
        group_id: &str,
        read: impl FnOnce(&GroupSeed) -> T,
    ) -> Result<T, BrokerError> {
        self.seeds
            .get(group_id)
            .map(|seed| read(seed.value()))
            .ok_or_else(|| BrokerError::Startup(format!("Group {group_id} has no replayed state.")))
    }

    /// `ConsumerGroupMetadataValue`: the group epoch and the metadata hash.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_group_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::GroupMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.group_epoch = v.epoch;
            seed.metadata_hash = v.metadata_hash;
        });
        Ok(())
    }

    /// A deprecated `ConsumerGroupPartitionMetadata` value (key v4): the group
    /// holds the record, so that its next metadata update tombstones it. The
    /// topics in the value are not kept: the metadata hash replaced them.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_partition_metadata(&self, group_id: &str) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.has_subscription_metadata_record = true;
        });
        Ok(())
    }

    /// `ConsumerGroupMemberMetadataValue`: the member's subscription, which
    /// creates the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.members.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ConsumerGroupTargetAssignmentMetadataValue`: the assignment epoch and
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`, or Kafka's
    /// `IllegalArgumentException` for an epoch below -1 or a negative time.
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        target_metadata(v.assignment_epoch, v.assignment_timestamp_ms)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.target_epoch = v.assignment_epoch;
            seed.assignment_timestamp_ms = v.assignment_timestamp_ms;
        });
        Ok(())
    }

    /// `ConsumerGroupTargetAssignmentMemberValue`: one member's target, which
    /// Kafka keeps whether or not the group holds the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.target_per_member.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ConsumerGroupCurrentMemberAssignmentValue`: one member's epochs and
    /// partitions. It creates a member that the group does not hold yet,
    /// with the defaults of Kafka's `ConsumerGroupMember.Builder`.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: CurrentMemberAssignmentValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.install_current_member(member_id, v, uninitialized_member);
        });
        Ok(())
    }

    /// `ConsumerGroupRegularExpressionValue`: what `regex` resolved to in the
    /// group, which is how a coordinator failover restores the topics of
    /// every regex subscription.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_regular_expression(
        &self,
        group_id: &str,
        regex: &str,
        v: persistence_next_gen::RegularExpressionValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.resolved_regexes.insert(regex.into(), v);
        });
        Ok(())
    }

    /// Applies the tombstone of a consumer group record, as Kafka's replay of
    /// a null value does. A tombstone of a group that the replay does not
    /// hold, or of a member that the group does not hold, changes nothing.
    ///
    /// - Group metadata (k3): removes the group, once its members, its
    ///   target assignment and its target assignment metadata are gone.
    /// - Deprecated subscription metadata (k4): the group no longer holds it.
    /// - Member subscription (k5): removes the member, once its current
    ///   assignment was tombstoned and its target assignment is gone.
    /// - Target assignment metadata (k6): the assignment epoch becomes -1 and
    ///   the time 0, once every member's target assignment is gone.
    /// - Target assignment (k7): removes the member's target.
    /// - Current assignment (k8): the member keeps its subscription and state
    ///   at epoch -1 with no partitions.
    /// - Regular expression (k16): removes the resolution.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group of another type
    /// and for a tombstone whose preconditions do not hold.
    pub fn replay_next_gen_tombstone(
        &self,
        key: &persistence_next_gen::NextGenKey,
    ) -> Result<(), BrokerError> {
        use persistence_next_gen::NextGenKey as K;
        let group_id = key.group_id();
        if !self.persisted_consumer_group(group_id, false)? {
            return Ok(());
        }
        match key {
            K::GroupMetadata { .. } => {
                self.consumer_seed(group_id, |seed| {
                    group_tombstone(
                        ModernGroupType::Consumer,
                        group_id,
                        seed.members.len(),
                        seed.target_per_member.len(),
                        seed.target_epoch,
                    )
                })??;
                self.seeds.remove(group_id);
                self.seeds_cache.remove(group_id);
                self.group_types.remove(group_id);
            }
            K::PartitionMetadata { .. } => self.update_consumer_seed(group_id, |seed| {
                seed.has_subscription_metadata_record = false;
            }),
            K::MemberMetadata { member_id, .. } => {
                let remove =
                    self.consumer_seed(group_id, |seed| seed.member_tombstone(member_id))??;
                if remove {
                    self.update_consumer_seed(group_id, |seed| {
                        seed.remove_replayed_member(member_id);
                    });
                }
            }
            K::TargetAssignmentMetadata { .. } => {
                self.consumer_seed(group_id, |seed| {
                    target_metadata_tombstone(group_id, seed.target_per_member.len())
                })??;
                self.update_consumer_seed(group_id, |seed| {
                    seed.target_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                });
            }
            K::TargetAssignmentMember { member_id, .. } => {
                self.update_consumer_seed(group_id, |seed| {
                    seed.target_per_member.remove(member_id);
                });
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                if self.consumer_seed(group_id, |seed| seed.members.contains_key(member_id))? {
                    self.update_consumer_seed(group_id, |seed| {
                        let state = seed
                            .current_per_member
                            .get(member_id)
                            .map_or(MemberAssignmentState::Stable, |current| current.state);
                        seed.current_per_member.insert(
                            member_id.clone(),
                            CurrentMemberAssignmentValue {
                                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                state,
                                assigned_partitions: Vec::new(),
                                partitions_pending_revocation: Vec::new(),
                            },
                        );
                    });
                }
            }
            K::RegularExpression { regex, .. } => self.update_consumer_seed(group_id, |seed| {
                seed.resolved_regexes.remove(regex);
            }),
        }
        Ok(())
    }
}
#[cfg(test)]
mod persisted_group_behavior {
    use {
        super::*,
        crate::coordinator::{
            bootstrap::test_support::replay_compacted_group_log,
            unified::{
                persistence::encode_string_key,
                seeds::{ShareGroupSeed, StreamsGroupSeed},
                test_support::{make_coord, persisted_snapshots},
            },
        },
        assert2::check,
        bytes::Bytes,
        rstest_bdd::{Slot, datatable::Rows},
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        serde_json::{Value, json},
        std::{mem, sync::Arc},
    };
    enum GroupProtocol {
        Consumer,
        Share,
        Streams,
    }
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum RecoveredSeed {
        Consumer(GroupSeed),
        Share(ShareGroupSeed),
        Streams(StreamsGroupSeed),
    }
    impl RecoveredSeed {
        fn snapshot(&self) -> Value {
            match self {
                Self::Consumer(seed) => persisted_snapshots::consumer_seed(seed),
                Self::Share(seed) => persisted_snapshots::share_seed(seed),
                Self::Streams(seed) => persisted_snapshots::streams_seed(seed),
            }
        }
    }
    #[derive(Default)]
    struct CompactedGroupRecovery {
        coordinator: Option<Arc<GroupCoordinator>>,
        protocol: Option<GroupProtocol>,
        records: Vec<(Bytes, Option<Bytes>)>,
        recovered: Option<Result<Option<RecoveredSeed>, String>>,
        cache: Slot<Option<RecoveredSeed>>,
    }
    #[rstest::fixture]
    fn compacted_recovery() -> CompactedGroupRecovery {
        // Every recovery starts with an unread log and no observed bootstrap result.
        CompactedGroupRecovery::default()
    }
    #[derive(DataTableRow)]
    struct CompactedGroupRecord {
        family: String,
        member_or_regex: String,
        value_hex: String,
    }
    #[given(
        "a fresh coordinator will recover {protocol} group g from these compacted offsets records"
    )]
    fn persisted_group_log(
        compacted_recovery: &mut CompactedGroupRecovery,
        protocol: String,
        #[datatable] rows: Rows<CompactedGroupRecord>,
    ) {
        compacted_recovery.coordinator = Some(make_coord());
        compacted_recovery.protocol = Some(match protocol.as_str() {
            "consumer" => GroupProtocol::Consumer,
            "share" => GroupProtocol::Share,
            "streams" => GroupProtocol::Streams,
            _ => panic!("unknown group protocol"),
        });
        compacted_recovery.records = rows
            .into_iter()
            .map(|row| {
                let version = match row.family.as_str() {
                    "consumer-group-metadata" => 3,
                    "consumer-subscription-metadata" => 4,
                    "consumer-member" => 5,
                    "consumer-target-epoch" => 6,
                    "consumer-target" => 7,
                    "consumer-current" => 8,
                    "share-member" => 10,
                    "share-group-metadata" => 11,
                    "share-target-epoch" => 12,
                    "share-target" => 13,
                    "share-current" => 14,
                    "share-state-partitions" => 15,
                    "consumer-regex" => 16,
                    "streams-group-metadata" => 17,
                    "streams-member" => 19,
                    "streams-target-epoch" => 20,
                    "streams-target" => 21,
                    "streams-current" => 22,
                    "streams-topology" => 23,
                    _ => panic!("unknown persisted group record family"),
                };
                let parts = if row.member_or_regex.is_empty() {
                    vec!["g"]
                } else {
                    vec!["g", row.member_or_regex.as_str()]
                };
                let key = encode_string_key(version, &parts).expect("literal compacted record key");
                let value = if row.value_hex == "tombstone" {
                    None
                } else {
                    Some(Bytes::from(
                        hex::decode(&row.value_hex).expect("complete literal Kafka value"),
                    ))
                };
                (key, value)
            })
            .collect();
    }
    #[when("bootstrap reads the persisted group records in order")]
    fn recover_compacted_group(compacted_recovery: &mut CompactedGroupRecovery) {
        let coordinator = compacted_recovery
            .coordinator
            .as_ref()
            .expect("fresh coordinator prepared");
        let loaded =
            replay_compacted_group_log(coordinator, mem::take(&mut compacted_recovery.records));
        let (seed, cached) = match compacted_recovery
            .protocol
            .as_ref()
            .expect("group protocol")
        {
            GroupProtocol::Consumer => (
                coordinator
                    .seeds
                    .get("g")
                    .map(|seed| RecoveredSeed::Consumer(seed.value().clone())),
                coordinator.cached_seed("g").map(RecoveredSeed::Consumer),
            ),
            GroupProtocol::Share => (
                coordinator
                    .share_seeds
                    .get("g")
                    .map(|seed| RecoveredSeed::Share(seed.value().clone())),
                coordinator.cached_share_seed("g").map(RecoveredSeed::Share),
            ),
            GroupProtocol::Streams => (
                coordinator
                    .streams_seeds
                    .get("g")
                    .map(|seed| RecoveredSeed::Streams(seed.value().clone())),
                coordinator
                    .cached_streams_seed("g")
                    .map(RecoveredSeed::Streams),
            ),
        };
        compacted_recovery.recovered = Some(loaded.map(|()| seed).map_err(|error| match error {
            BrokerError::Startup(message) => message,
            other => other.to_string(),
        }));
        compacted_recovery.cache.set(cached);
    }
    #[then(
        "the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed"
    )]
    fn recovered_group_state(compacted_recovery: &CompactedGroupRecovery, docstring: String) {
        let expected: Value =
            serde_json::from_str(&docstring).expect("literal independent full seed or exact error");
        let recovered = compacted_recovery
            .recovered
            .as_ref()
            .expect("bootstrap replay completed");
        let observed = match recovered {
            Ok(seed) => json!({"seed":seed.as_ref().map(RecoveredSeed::snapshot)}),
            Err(error) => json!({"error":error}),
        };
        check!(observed == expected);
        if let Ok(seed) = recovered {
            check!(compacted_recovery.cache.get().as_ref() == Some(seed));
        }
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/replay.feature",fixtures=[compacted_recovery:CompactedGroupRecovery]);
}
