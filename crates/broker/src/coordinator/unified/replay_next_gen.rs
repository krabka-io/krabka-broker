//! Bootstrap replay of the KIP-848 next-gen consumer-group records.
//!
//! Each method applies one decoded record to both the bootstrap seed and the
//! last-known-good cache, so a group that finishes replay is ready to hydrate
//! an actor and to survive a later actor respawn.

use super::{
    group_coordinator::GroupCoordinator,
    persistence_next_gen,
    replay_policy::{
        ReplayMutation, ReplayRecordKind, replay_epoch_is_admissible, replay_mutation,
    },
    seeds::GroupSeed,
};

impl GroupCoordinator {
    pub fn replay_group_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::GroupMetadataValue,
    ) {
        if replay_mutation(
            ReplayRecordKind::GroupMetadata,
            Some(ReplayRecordKind::GroupMetadata),
            self.seeds.contains_key(group_id) || self.seeds_cache.contains_key(group_id),
            false,
        ) != ReplayMutation::Apply
            || v.epoch < 0
        {
            return;
        }
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            if replay_epoch_is_admissible(seed.group_epoch, v.epoch) {
                seed.group_epoch = v.epoch;
            }
        }
        {
            let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
            if replay_epoch_is_admissible(cached.group_epoch, v.epoch) {
                cached.group_epoch = v.epoch;
            }
        }
    }
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) {
        {
            if let Some(mut seed) = self.seeds.get_mut(group_id)
                && replay_mutation(
                    ReplayRecordKind::MemberMetadata,
                    Some(ReplayRecordKind::MemberMetadata),
                    true,
                    seed.members.contains_key(member_id),
                ) == ReplayMutation::Apply
            {
                seed.members.insert(member_id.into(), v.clone());
            }
        }
        if let Some(mut cached) = self.seeds_cache.get_mut(group_id)
            && replay_mutation(
                ReplayRecordKind::MemberMetadata,
                Some(ReplayRecordKind::MemberMetadata),
                true,
                cached.members.contains_key(member_id),
            ) == ReplayMutation::Apply
        {
            cached.members.insert(member_id.into(), v);
        }
    }
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) {
        if v.assignment_epoch < 0 {
            return;
        }
        {
            if let Some(mut seed) = self.seeds.get_mut(group_id)
                && replay_mutation(
                    ReplayRecordKind::TargetAssignmentMetadata,
                    Some(ReplayRecordKind::TargetAssignmentMetadata),
                    true,
                    false,
                ) == ReplayMutation::Apply
                && replay_epoch_is_admissible(seed.target_epoch, v.assignment_epoch)
            {
                seed.target_epoch = v.assignment_epoch;
            }
        }
        if let Some(mut cached) = self.seeds_cache.get_mut(group_id)
            && replay_mutation(
                ReplayRecordKind::TargetAssignmentMetadata,
                Some(ReplayRecordKind::TargetAssignmentMetadata),
                true,
                false,
            ) == ReplayMutation::Apply
            && replay_epoch_is_admissible(cached.target_epoch, v.assignment_epoch)
        {
            cached.target_epoch = v.assignment_epoch;
        }
    }
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) {
        {
            if let Some(mut seed) = self.seeds.get_mut(group_id)
                && replay_mutation(
                    ReplayRecordKind::TargetAssignmentMember,
                    Some(ReplayRecordKind::TargetAssignmentMember),
                    true,
                    seed.members.contains_key(member_id),
                ) == ReplayMutation::Apply
            {
                seed.target_per_member.insert(member_id.into(), v.clone());
            }
        }
        if let Some(mut cached) = self.seeds_cache.get_mut(group_id)
            && replay_mutation(
                ReplayRecordKind::TargetAssignmentMember,
                Some(ReplayRecordKind::TargetAssignmentMember),
                true,
                cached.members.contains_key(member_id),
            ) == ReplayMutation::Apply
        {
            cached.target_per_member.insert(member_id.into(), v);
        }
    }
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::CurrentMemberAssignmentValue,
    ) {
        {
            if let Some(mut seed) = self.seeds.get_mut(group_id)
                && replay_mutation(
                    ReplayRecordKind::CurrentMemberAssignment,
                    Some(ReplayRecordKind::CurrentMemberAssignment),
                    true,
                    seed.members.contains_key(member_id),
                ) == ReplayMutation::Apply
                && seed
                    .current_per_member
                    .get(member_id)
                    .is_none_or(|current| {
                        replay_epoch_is_admissible(current.member_epoch, v.member_epoch)
                    })
            {
                seed.current_per_member.insert(member_id.into(), v.clone());
            }
        }
        if let Some(mut cached) = self.seeds_cache.get_mut(group_id)
            && replay_mutation(
                ReplayRecordKind::CurrentMemberAssignment,
                Some(ReplayRecordKind::CurrentMemberAssignment),
                true,
                cached.members.contains_key(member_id),
            ) == ReplayMutation::Apply
            && cached
                .current_per_member
                .get(member_id)
                .is_none_or(|current| {
                    replay_epoch_is_admissible(current.member_epoch, v.member_epoch)
                })
        {
            cached.current_per_member.insert(member_id.into(), v);
        }
    }

    /// Apply a tombstone for a next-gen key.
    ///
    /// The method removes the matching entry from both `seeds` and
    /// `seeds_cache`. Bootstrap replay calls it to honor records with
    /// `value = None`.
    ///
    /// A `GroupMetadata` tombstone is the migration DOWNGRADE marker. It drops
    /// the whole next-gen group. Replay must REMOVE the seed from both `seeds`
    /// and `seeds_cache`, so that the group disappears from the next-gen set
    /// that `finalize` derives. A later classic k2 `GroupMetadata` record can
    /// then rebuild it as a CLASSIC group, because log order wins. A change
    /// that only zeroed the epoch would leave the group classified as next-gen
    /// and would replay it back as an empty consumer group.
    pub fn replay_next_gen_tombstone(&self, key: &persistence_next_gen::NextGenKey) {
        use persistence_next_gen::NextGenKey as K;
        if let K::GroupMetadata { group_id } = key {
            assert2::debug_assert!(
                replay_mutation(ReplayRecordKind::GroupMetadata, None, true, false)
                    == ReplayMutation::RemoveGroup
            );
            self.seeds.remove(group_id);
            self.seeds_cache.remove(group_id);
            self.group_types.remove(group_id);
            return;
        }
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. } => group_id.as_str(),
        };
        let scrub = |seed: &mut GroupSeed| match key {
            // Unreachable: the `GroupMetadata` tombstone removes the whole seed
            // and returns above. Kept only for match exhaustiveness.
            K::GroupMetadata { .. } => {
                seed.group_epoch = 0;
            }
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
                seed.target_per_member.remove(member_id);
                seed.current_per_member.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => {
                seed.target_epoch = 0;
                seed.target_per_member.clear();
            }
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
        };
        {
            if let Some(mut s) = self.seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{GroupSeed, persistence_next_gen};
    use crate::coordinator::unified::test_support::{
        make_coord, next_current, next_member, proto_uuid,
    };

    #[test]
    fn next_gen_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = next_member("member-a");
        let target = persistence_next_gen::TargetAssignmentMemberValue {
            topic_partitions: vec![persistence_next_gen::AssignedTopicPartitions {
                topic_id: proto_uuid(3),
                partitions: vec![1, 2],
            }],
        };
        let current = next_current(5);

        coord.replay_group_metadata("g", persistence_next_gen::GroupMetadataValue { epoch: 11 });
        coord.replay_member_metadata("g", "member-a", member.clone());
        coord.replay_target_assignment_metadata(
            "g",
            persistence_next_gen::TargetAssignmentMetadataValue {
                assignment_epoch: 12,
            },
        );
        coord.replay_target_assignment_member("g", "member-a", target.clone());
        coord.replay_current_member_assignment("g", "member-a", current.clone());

        let expected = GroupSeed {
            group_epoch: 11,
            target_epoch: 12,
            members: maplit::hashmap! {"member-a".to_string() => member},
            target_per_member: maplit::hashmap! {"member-a".to_string() => target},
            current_per_member: maplit::hashmap! {"member-a".to_string() => current},
        };
        assert!(*coord.seeds.get("g").unwrap() == expected);
        assert!(coord.cached_seed("g") == Some(expected));
    }

    #[test]
    fn group_tombstone_blocks_orphans_and_epoch_regression() {
        let coord = make_coord();
        coord.mark_next_gen("g");
        coord.replay_group_metadata("g", persistence_next_gen::GroupMetadataValue { epoch: 4 });
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(4));

        coord.replay_next_gen_tombstone(&persistence_next_gen::NextGenKey::GroupMetadata {
            group_id: "g".into(),
        });
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(i32::MAX));
        check!(coord.cached_seed("g").is_none());
        check!(coord.group_type("g").is_none());

        coord.replay_group_metadata("g", persistence_next_gen::GroupMetadataValue { epoch: 0 });
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(i32::MAX));
        coord.replay_current_member_assignment("g", "m", next_current(3));
        coord.replay_group_metadata("g", persistence_next_gen::GroupMetadataValue { epoch: -1 });
        let seed = coord.cached_seed("g").unwrap();
        check!(seed.group_epoch == 0);
        assert!(seed.current_per_member["m"].member_epoch == i32::MAX);
    }
}
