//! Bootstrap replay of the KIP-932 share-group records.
//!
//! Each method applies one decoded record to both the bootstrap seed and the
//! last-known-good cache. The share-state partition metadata is read back from
//! that cache by the admin share-offset RPCs, so its accessor lives here too.

use super::{group_coordinator::GroupCoordinator, seeds::ShareGroupSeed, share};

impl GroupCoordinator {
    pub fn replay_share_group_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.group_epoch = v.epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = v.epoch;
    }
    pub fn replay_share_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupMemberMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_share_target_assignment_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_epoch = v.assignment_epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_epoch = v.assignment_epoch;
    }
    pub fn replay_share_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_share_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupCurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Replay a KIP-932 `ShareGroupStatePartitionMetadata` record, key v14.
    ///
    /// The method records which `(topic_id, partition)` share-states the group
    /// has initialized. The lifecycle hook can then skip a re-initialization
    /// after a restart.
    pub fn replay_share_state_partition_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupStatePartitionMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.state_partition_metadata = v.clone();
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.state_partition_metadata = v;
    }

    /// Read the cached `ShareGroupStatePartitionMetadata` for `group_id`.
    ///
    /// The value records which `(topic_id, partition)` share-states the group
    /// has initialized. The method returns `None` for an unknown group. It
    /// drives the admin offset RPCs Describe/Alter/Delete `ShareGroupOffsets`.
    /// Those RPCs list the initialized partitions when the request omits an
    /// explicit list.
    #[must_use]
    pub fn share_state_partition_metadata(
        &self,
        group_id: &str,
    ) -> Option<share::persistence::ShareGroupStatePartitionMetadataValue> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().state_partition_metadata.clone())
    }

    /// Apply a tombstone for a share-group key.
    ///
    /// The method removes the matching entry from both `share_seeds` and
    /// `share_seeds_cache`.
    pub fn replay_share_tombstone(&self, key: &share::persistence::ShareGroupKey) {
        use share::persistence::ShareGroupKey as K;
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. }
            | K::StatePartitionMetadata { group_id } => group_id.as_str(),
        };
        let scrub = |seed: &mut ShareGroupSeed| match key {
            K::GroupMetadata { .. } => seed.group_epoch = 0,
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => seed.target_epoch = 0,
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
            K::StatePartitionMetadata { .. } => {
                seed.state_partition_metadata =
                    share::persistence::ShareGroupStatePartitionMetadataValue::default();
            }
        };
        {
            if let Some(mut s) = self.share_seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.share_seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::test_support::{make_coord, proto_uuid, share_member};

    #[test]
    fn share_state_partition_metadata_none_then_some() {
        let coord = make_coord();
        // Unknown group → None.
        assert!(coord.share_state_partition_metadata("sg").is_none());

        let tid = uuid::Uuid::from_u128(1);
        let v = share::persistence::ShareGroupStatePartitionMetadataValue {
            initialized: vec![(tid, vec![0, 1])],
            deleting: vec![],
        };
        coord.replay_share_state_partition_metadata("sg", v.clone());
        // Some after a replay, with the same contents.
        assert!(coord.share_state_partition_metadata("sg") == Some(v));
    }

    #[test]
    fn share_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = share_member("share-member");
        let target = share::persistence::ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(proto_uuid(4), vec![0, 3])],
        };
        let current = share::persistence::ShareGroupCurrentMemberAssignmentValue {
            member_epoch: 6,
            assigned_partitions: vec![(proto_uuid(4), vec![1])],
        };

        coord.replay_share_group_metadata(
            "sg",
            share::persistence::ShareGroupMetadataValue { epoch: 21 },
        );
        coord.replay_share_member_metadata("sg", "share-member", member.clone());
        coord.replay_share_target_assignment_metadata(
            "sg",
            share::persistence::ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: 22,
            },
        );
        coord.replay_share_target_assignment_member("sg", "share-member", target.clone());
        coord.replay_share_current_member_assignment("sg", "share-member", current.clone());

        let expected = ShareGroupSeed {
            group_epoch: 21,
            target_epoch: 22,
            members: std::collections::HashMap::from([("share-member".to_string(), member)]),
            target_per_member: std::collections::HashMap::from([(
                "share-member".to_string(),
                target,
            )]),
            current_per_member: std::collections::HashMap::from([(
                "share-member".to_string(),
                current,
            )]),
            state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue {
                initialized: vec![],
                deleting: vec![],
            },
        };
        assert!(*coord.share_seeds.get("sg").unwrap() == expected);
        assert!(coord.cached_share_seed("sg") == Some(expected));
    }
}
