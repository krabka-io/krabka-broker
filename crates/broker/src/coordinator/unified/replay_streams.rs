//! Bootstrap replay of the KIP-1071 streams-group records.
//!
//! Each method applies one decoded record to both the bootstrap seed and the
//! last-known-good cache. The k15 group-metadata tombstone is the downgrade
//! marker, so it also drops the group's type lock.

use super::{
    group_coordinator::GroupCoordinator,
    replay_policy::{ReplayRecordKind, replay_epoch_is_admissible, replay_write_is_admissible},
    seeds::StreamsGroupSeed,
    streams,
};

/// The seed of a group that its first replayed record creates. Kafka replays
/// the records into a new `StreamsGroup`, whose target assignment epoch is
/// the initial one until a target assignment metadata record sets it.
fn new_seed() -> StreamsGroupSeed {
    StreamsGroupSeed {
        assignment_epoch: streams::state::INITIAL_EPOCH,
        ..StreamsGroupSeed::default()
    }
}

impl GroupCoordinator {
    pub fn replay_streams_group_metadata(
        &self,
        group_id: &str,
        value: streams::persistence::StreamsGroupMetadataValue,
    ) {
        let epoch = value.epoch;
        if !replay_write_is_admissible(
            ReplayRecordKind::GroupMetadata,
            self.streams_seeds.contains_key(group_id)
                || self.streams_seeds_cache.contains_key(group_id),
            false,
        ) || epoch < 0
        {
            return;
        }
        {
            let mut seed = self
                .streams_seeds
                .entry(group_id.into())
                .or_insert_with(new_seed);
            if replay_epoch_is_admissible(seed.group_epoch, epoch) {
                seed.group_epoch = epoch;
                seed.metadata_hash = value.metadata_hash;
                seed.description_epochs = value.description;
            }
        }
        {
            let mut cached = self
                .streams_seeds_cache
                .entry(group_id.into())
                .or_insert_with(new_seed);
            if replay_epoch_is_admissible(cached.group_epoch, epoch) {
                cached.group_epoch = epoch;
                cached.metadata_hash = value.metadata_hash;
                cached.description_epochs = value.description;
            }
        }
    }
    pub fn replay_streams_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupMemberMetadataValue,
    ) {
        super::seeds::update_replayed_member!((self, streams_seeds, streams_seeds_cache, group_id, member_id, v); metadata);
    }
    pub fn replay_streams_topology(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupTopologyValue,
    ) {
        super::seeds::update_replayed_seeds!(self, streams_seeds, streams_seeds_cache, group_id; (v.clone(), v);
            |seed| replay_write_is_admissible(ReplayRecordKind::Topology, true, false) => |value| {
                seed.topology = Some(value);
            }
        );
    }
    pub fn replay_streams_partition_metadata(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupPartitionMetadataValue,
    ) {
        super::seeds::update_replayed_seeds!(self, streams_seeds, streams_seeds_cache, group_id; (v.clone(), v);
            |seed| replay_write_is_admissible(ReplayRecordKind::PartitionMetadata, true, false) => |value| {
                seed.partition_metadata = Some(value);
            }
        );
    }
    pub fn replay_streams_target_assignment_metadata(&self, group_id: &str, assignment_epoch: i32) {
        if assignment_epoch < 0 {
            return;
        }
        super::seeds::update_replayed_seeds!(self, streams_seeds, streams_seeds_cache, group_id; (assignment_epoch, assignment_epoch);
            |seed| replay_write_is_admissible(
                ReplayRecordKind::TargetAssignmentMetadata,
                true,
                false,
            )
            && replay_epoch_is_admissible(seed.assignment_epoch, assignment_epoch) => |value| {
                seed.assignment_epoch = value;
            }
        );
    }
    pub fn replay_streams_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    ) {
        super::seeds::update_replayed_member!((self, streams_seeds, streams_seeds_cache, group_id, member_id, v); target);
    }
    pub fn replay_streams_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    ) {
        super::seeds::update_replayed_member!((self, streams_seeds, streams_seeds_cache, group_id, member_id, v); current);
    }

    /// Apply a tombstone for a streams-group key.
    ///
    /// The method removes the matching entry from both `streams_seeds` and
    /// `streams_seeds_cache`.
    ///
    /// A `GroupMetadata` k15 tombstone is the load-bearing downgrade tombstone
    /// of KIP-1071. It removes the whole seed, so `finalize_bootstrap` does
    /// not respawn the group as streams. It also removes the `Streams` type
    /// lock, so a classic `GroupMetadata` k2 write that comes later can lock
    /// the group again as `Classic`.
    pub fn replay_streams_tombstone(&self, key: &streams::persistence::StreamsGroupKey) {
        use streams::persistence::StreamsGroupKey as K;
        let group_id = key.group_id();
        // k15 GroupMetadata tombstone: purge the whole seed so finalize_bootstrap
        // does not respawn this group as streams; also drop the Streams type lock
        // so a later classic join can re-lock it as Classic.
        if matches!(key, K::GroupMetadata { .. }) {
            super::seeds::remove_replayed_group(
                &self.streams_seeds,
                &self.streams_seeds_cache,
                &self.group_types,
                group_id,
            );
            return;
        }
        let scrub = |seed: &mut StreamsGroupSeed| {
            super::seeds::scrub_seed_assignments!(seed, key, K, assignment_epoch;
                K::GroupMetadata { .. } => unreachable!("handled above"),
            K::Topology { .. } => seed.topology = None,
            K::PartitionMetadata { .. } => seed.partition_metadata = None,
            );
        };

        super::seeds::scrub_replayed_seeds(
            &self.streams_seeds,
            &self.streams_seeds_cache,
            group_id,
            scrub,
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{StreamsGroupSeed, streams};
    use crate::coordinator::unified::test_support::{make_coord, real_uuid, streams_member};

    #[test]
    fn streams_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = streams_member("streams-member");
        let topology = streams::persistence::StreamsGroupTopologyValue {
            epoch: 31,
            subtopologies: vec![streams::persistence::StoredSubtopology {
                subtopology_id: "subtopology-a".into(),
                source_topics: vec!["input".into()],
                source_topic_regex: vec!["input-.*".into()],
                repartition_sink_topics: vec!["sink".into()],
                state_changelog_topics: vec![streams::persistence::StoredTopicInfo {
                    name: "store-changelog".into(),
                    partitions: 2,
                    replication_factor: 1,
                    topic_configs: vec![("cleanup.policy".into(), "compact".into())],
                }],
                repartition_source_topics: vec![],
                copartition_groups: vec![],
            }],
        };
        let partition_metadata = streams::persistence::StreamsGroupPartitionMetadataValue {
            topics: vec![streams::persistence::StreamsTopicMeta {
                topic_name: "input".into(),
                topic_id: real_uuid(5),
                num_partitions: 2,
            }],
        };
        let mut active = std::collections::BTreeMap::new();
        active.insert("subtopology-a".into(), vec![0, 1]);
        let target = streams::persistence::StreamsGroupTargetAssignmentMemberValue {
            active: active.clone(),
            ..Default::default()
        };
        let current = streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 7,
            previous_member_epoch: 6,
            state: streams::persistence::StreamsMemberWireState::UnrevokedTasks,
            active,
            ..Default::default()
        };

        coord.replay_streams_group_metadata(
            "st",
            streams::persistence::StreamsGroupMetadataValue {
                epoch: 30,
                metadata_hash: 44,
                description: streams::persistence::DescriptionEpochs {
                    stored: 3,
                    failed: -1,
                },
            },
        );
        coord.replay_streams_member_metadata("st", "streams-member", member.clone());
        coord.replay_streams_topology("st", topology.clone());
        coord.replay_streams_partition_metadata("st", partition_metadata.clone());
        coord.replay_streams_target_assignment_metadata("st", 32);
        coord.replay_streams_target_assignment_member("st", "streams-member", target.clone());
        coord.replay_streams_current_member_assignment("st", "streams-member", current.clone());

        let expected = StreamsGroupSeed {
            group_epoch: 30,
            metadata_hash: 44,
            description_epochs: streams::persistence::DescriptionEpochs {
                stored: 3,
                failed: -1,
            },
            assignment_epoch: 32,
            topology: Some(topology),
            partition_metadata: Some(partition_metadata),
            members: maplit::hashmap! {"streams-member".to_string() => member},
            target_per_member: maplit::hashmap! {"streams-member".to_string() => target},
            current_per_member: maplit::hashmap! {"streams-member".to_string() => current},
        };
        assert!(*coord.streams_seeds.get("st").unwrap() == expected);
        assert!(coord.cached_streams_seed("st") == Some(expected));
    }

    /// Kafka replays the records into a new `StreamsGroup`, so a group with no
    /// target assignment metadata record keeps the initial assignment epoch 1,
    /// as a group does while its initial rebalance delay holds the first
    /// assignment back.
    #[test]
    fn a_group_without_target_metadata_replays_at_the_initial_assignment_epoch() {
        let coord = make_coord();
        coord.replay_streams_group_metadata(
            "st",
            streams::persistence::StreamsGroupMetadataValue {
                epoch: 2,
                metadata_hash: 7,
                description: streams::persistence::DescriptionEpochs::default(),
            },
        );

        let expected = StreamsGroupSeed {
            group_epoch: 2,
            metadata_hash: 7,
            assignment_epoch: 1,
            ..StreamsGroupSeed::default()
        };
        check!(*coord.streams_seeds.get("st").unwrap() == expected);
        check!(coord.cached_streams_seed("st") == Some(expected));
    }

    #[test]
    fn streams_group_tombstone_blocks_orphan_topology_and_member() {
        let coord = make_coord();
        coord.mark_streams("st");
        coord.replay_streams_group_metadata(
            "st",
            streams::persistence::StreamsGroupMetadataValue {
                epoch: 2,
                metadata_hash: 0,
                description: streams::persistence::DescriptionEpochs::default(),
            },
        );
        coord.replay_streams_member_metadata("st", "m", streams_member("m"));

        coord.replay_streams_tombstone(&streams::persistence::StreamsGroupKey::GroupMetadata {
            group_id: "st".into(),
        });
        coord.replay_streams_member_metadata("st", "m", streams_member("m"));
        coord.replay_streams_topology(
            "st",
            streams::persistence::StreamsGroupTopologyValue::default(),
        );

        check!(coord.cached_streams_seed("st").is_none());
        assert!(coord.group_type("st").is_none());
    }
}
