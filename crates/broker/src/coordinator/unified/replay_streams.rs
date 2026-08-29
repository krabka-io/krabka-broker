//! Bootstrap replay of the KIP-1071 streams-group records.
//!
//! Each method applies one decoded record to both the bootstrap seed and the
//! last-known-good cache. The k15 group-metadata tombstone is the downgrade
//! marker, so it also drops the group's type lock.

use super::{group_coordinator::GroupCoordinator, seeds::StreamsGroupSeed, streams};

impl GroupCoordinator {
    pub fn replay_streams_group_metadata(&self, group_id: &str, epoch: i32) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.group_epoch = epoch;
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = epoch;
    }
    pub fn replay_streams_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupMemberMetadataValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_streams_topology(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupTopologyValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.topology = Some(v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.topology = Some(v);
    }
    pub fn replay_streams_partition_metadata(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupPartitionMetadataValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.partition_metadata = Some(v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.partition_metadata = Some(v);
    }
    pub fn replay_streams_target_assignment_metadata(&self, group_id: &str, assignment_epoch: i32) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.assignment_epoch = assignment_epoch;
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.assignment_epoch = assignment_epoch;
    }
    pub fn replay_streams_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_streams_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
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
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::Topology { group_id }
            | K::PartitionMetadata { group_id }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. } => group_id.as_str(),
        };
        // k15 GroupMetadata tombstone: purge the whole seed so finalize_bootstrap
        // does not respawn this group as streams; also drop the Streams type lock
        // so a later classic join can re-lock it as Classic.
        if matches!(key, K::GroupMetadata { .. }) {
            self.streams_seeds.remove(group_id);
            self.streams_seeds_cache.remove(group_id);
            self.group_types.remove(group_id);
            return;
        }
        let scrub = |seed: &mut StreamsGroupSeed| match key {
            K::GroupMetadata { .. } => unreachable!("handled above"),
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::Topology { .. } => seed.topology = None,
            K::PartitionMetadata { .. } => seed.partition_metadata = None,
            K::TargetAssignmentMetadata { .. } => seed.assignment_epoch = 0,
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
        };
        {
            if let Some(mut s) = self.streams_seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.streams_seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
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
            state: 1,
            active,
            ..Default::default()
        };

        coord.replay_streams_group_metadata("st", 30);
        coord.replay_streams_member_metadata("st", "streams-member", member.clone());
        coord.replay_streams_topology("st", topology.clone());
        coord.replay_streams_partition_metadata("st", partition_metadata.clone());
        coord.replay_streams_target_assignment_metadata("st", 32);
        coord.replay_streams_target_assignment_member("st", "streams-member", target.clone());
        coord.replay_streams_current_member_assignment("st", "streams-member", current.clone());

        let expected = StreamsGroupSeed {
            group_epoch: 30,
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
}
