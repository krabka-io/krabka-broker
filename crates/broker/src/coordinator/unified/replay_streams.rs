//! Bootstrap replay of the KIP-1071 streams group records, as Kafka 4.3.1's
//! `GroupMetadataManager.replay` applies them.
//!
//! Each method applies one decoded record to the bootstrap seed and to the
//! last-known-good cache. The rules for creating a group, ignoring a
//! tombstone, and failing the load are
//! [`replay_policy`](super::replay_policy)'s.

use {
    super::{
        group_coordinator::GroupCoordinator,
        replay_policy::{
            LEAVE_GROUP_MEMBER_EPOCH, ModernGroupType, group_tombstone,
            persisted_group_with_create, target_metadata, target_metadata_tombstone,
        },
        seeds::{ReplaySeed, StreamsGroupSeed},
        streams,
    },
    crate::error::BrokerError,
    std::collections::BTreeMap,
};

/// The fields of a member that Kafka's replay creates from a current
/// assignment record alone. Kafka's `getOrCreateUninitializedMember` leaves
/// them null; these are the values the broker gives a member before its
/// first heartbeat.
fn uninitialized_member() -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        process_id: String::new(),
        user_endpoint: None,
        client_tags: Vec::new(),
        rebalance_timeout_ms: -1,
        topology_epoch: -1,
    }
}

impl GroupCoordinator {
    /// Kafka's `getOrMaybeCreatePersistedStreamsGroup` among the modern
    /// groups: whether the group exists once the lookup is done. The
    /// bootstrap replay settles a classic group under the id before it calls
    /// this.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a consumer or share group.
    pub(crate) fn persisted_streams_group(
        &self,
        group_id: &str,
        create_if_not_exists: bool,
    ) -> Result<bool, BrokerError> {
        persisted_group_with_create(
            group_id,
            ModernGroupType::Streams,
            self.replayed_modern_group(group_id),
            create_if_not_exists,
            || {
                self.streams_seeds
                    .insert(group_id.into(), StreamsGroupSeed::new_group());
                self.streams_seeds_cache
                    .insert(group_id.into(), StreamsGroupSeed::new_group());
            },
        )
    }

    /// Applies `update` to the replayed group and to its cached copy.
    fn update_streams_seed(&self, group_id: &str, update: impl Fn(&mut StreamsGroupSeed)) {
        if let Some(mut seed) = self.streams_seeds.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.streams_seeds_cache
                .entry(group_id.into())
                .or_insert_with(StreamsGroupSeed::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    fn update_streams_seed_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut StreamsGroupSeed, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.streams_seeds.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.streams_seeds_cache
                .entry(group_id.into())
                .or_insert_with(StreamsGroupSeed::new_group)
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
    fn streams_seed<T>(
        &self,
        group_id: &str,
        read: impl FnOnce(&StreamsGroupSeed) -> T,
    ) -> Result<T, BrokerError> {
        self.streams_seeds
            .get(group_id)
            .map(|seed| read(seed.value()))
            .ok_or_else(|| BrokerError::Startup(format!("Group {group_id} has no replayed state.")))
    }

    /// Applies a `StreamsGroupMetadataValue`, as Kafka's
    /// `GroupMetadataManager.replay` of the record does: the group epoch, the
    /// metadata hash, the validated topology epoch, and the last assignment
    /// configuration, empty for a null list. It also keeps Kafka trunk's
    /// KIP-1331 description epochs, which 4.3.1 keeps as unknown tags, in
    /// either write mode.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`, or
    /// [`BrokerError::Startup`] when the configuration list names a key twice:
    /// Kafka's `Collectors.toMap` throws `IllegalStateException` for it, which
    /// fails the load.
    pub fn replay_streams_group_metadata(
        &self,
        group_id: &str,
        value: streams::persistence::StreamsGroupMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        let mut last_assignment_configs = BTreeMap::new();
        for config in value.last_assignment_configs.into_iter().flatten() {
            if let Some(first) = last_assignment_configs.get(&config.key) {
                return Err(BrokerError::Startup(format!(
                    "Duplicate key {} (attempted merging values {first} and {})",
                    config.key, config.value
                )));
            }
            last_assignment_configs.insert(config.key, config.value);
        }
        self.update_streams_seed(group_id, |seed| {
            seed.group_epoch = value.epoch;
            seed.metadata_hash = value.metadata_hash;
            seed.validated_topology_epoch = value.validated_topology_epoch;
            seed.last_assignment_configs
                .clone_from(&last_assignment_configs);
            seed.description_epochs = value.description;
        });
        Ok(())
    }

    /// `StreamsGroupMemberMetadataValue`: the member's metadata, which
    /// creates the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupMemberMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.members.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `StreamsGroupTopologyValue`: the group's topology.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_topology(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupTopologyValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.topology = Some(v);
        });
        Ok(())
    }

    /// `StreamsGroupTargetAssignmentMetadataValue`: the assignment epoch and
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`, or Kafka's
    /// `IllegalArgumentException` for an epoch below -1 or a negative time.
    pub fn replay_streams_target_assignment_metadata(
        &self,
        group_id: &str,
        value: streams::persistence::StreamsGroupTargetAssignmentMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        target_metadata(value.assignment_epoch, value.assignment_timestamp_ms)?;
        self.update_streams_seed(group_id, |seed| {
            seed.assignment_epoch = value.assignment_epoch;
            seed.assignment_timestamp_ms = value.assignment_timestamp_ms;
        });
        Ok(())
    }

    /// `StreamsGroupTargetAssignmentMemberValue`: one member's target, which
    /// Kafka keeps whether or not the group holds the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.target_per_member.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `StreamsGroupCurrentMemberAssignmentValue`: one member's epochs and
    /// tasks. It creates a member that the group does not hold yet, as
    /// Kafka's `getOrCreateUninitializedMember` does.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.install_current_member(member_id, v, uninitialized_member);
        });
        Ok(())
    }

    /// Applies the tombstone of a streams group record, as Kafka's replay of
    /// a null value does. A tombstone of a group that the replay does not
    /// hold, or of a member that the group does not hold, changes nothing.
    ///
    /// - Group metadata (k17): removes the group, once its members are gone
    ///   and its target assignment metadata was tombstoned. Kafka's streams
    ///   check does not look at the members' targets.
    /// - Member metadata (k18): removes the member, once its current
    ///   assignment was tombstoned and its target assignment is gone.
    /// - Topology (k19): the group has no topology.
    /// - Target assignment metadata (k20): the assignment epoch becomes -1
    ///   and the time 0, once every member's target assignment is gone.
    /// - Target assignment (k21): removes the member's target.
    /// - Current assignment (k22): the member keeps its metadata and state at
    ///   epoch -1 with no tasks.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group of another type
    /// and for a tombstone whose preconditions do not hold.
    pub fn replay_streams_tombstone(
        &self,
        key: &streams::persistence::StreamsGroupKey,
    ) -> Result<(), BrokerError> {
        use streams::persistence::StreamsGroupKey as K;
        let group_id = key.group_id();
        if !self.persisted_streams_group(group_id, false)? {
            return Ok(());
        }
        match key {
            K::GroupMetadata { .. } => {
                self.streams_seed(group_id, |seed| {
                    group_tombstone(
                        ModernGroupType::Streams,
                        group_id,
                        seed.members.len(),
                        seed.target_per_member.len(),
                        seed.assignment_epoch,
                    )
                })??;
                self.streams_seeds.remove(group_id);
                self.streams_seeds_cache.remove(group_id);
                self.group_types.remove(group_id);
            }
            K::MemberMetadata { member_id, .. } => {
                let remove =
                    self.streams_seed(group_id, |seed| seed.member_tombstone(member_id))??;
                if remove {
                    self.update_streams_seed(group_id, |seed| {
                        seed.remove_replayed_member(member_id);
                    });
                }
            }
            K::Topology { .. } => self.update_streams_seed(group_id, |seed| seed.topology = None),
            K::TargetAssignmentMetadata { .. } => {
                self.streams_seed(group_id, |seed| {
                    target_metadata_tombstone(group_id, seed.target_per_member.len())
                })??;
                self.update_streams_seed(group_id, |seed| {
                    seed.assignment_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                });
            }
            K::TargetAssignmentMember { member_id, .. } => {
                self.update_streams_seed(group_id, |seed| {
                    seed.target_per_member.remove(member_id);
                });
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                if self.streams_seed(group_id, |seed| seed.members.contains_key(member_id))? {
                    self.update_streams_seed(group_id, |seed| {
                        let state = seed.current_per_member.get(member_id).map_or(
                            streams::persistence::StreamsMemberWireState::Stable,
                            |current| current.state,
                        );
                        seed.current_per_member.insert(
                            member_id.clone(),
                            streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
                                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                state,
                                ..Default::default()
                            },
                        );
                    });
                }
            }
        }
        Ok(())
    }
}
