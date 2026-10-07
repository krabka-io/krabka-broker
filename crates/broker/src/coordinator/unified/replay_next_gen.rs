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
                seed.metadata_hash = v.metadata_hash;
            }
        }
        {
            let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
            if replay_epoch_is_admissible(cached.group_epoch, v.epoch) {
                cached.group_epoch = v.epoch;
                cached.metadata_hash = v.metadata_hash;
            }
        }
    }
    /// Applies a deprecated `ConsumerGroupPartitionMetadata` value (key v4),
    /// as Kafka 4.3.1's `GroupMetadataManager.replay` of the record does: it
    /// creates the consumer group when the log has none yet, and marks it as
    /// holding the record, so that the group's next metadata update writes
    /// the record's tombstone. The topics in the value are not kept: the
    /// metadata hash replaced them.
    pub fn replay_partition_metadata(&self, group_id: &str) {
        self.seeds
            .entry(group_id.into())
            .or_default()
            .has_subscription_metadata_record = true;
        self.seeds_cache
            .entry(group_id.into())
            .or_default()
            .has_subscription_metadata_record = true;
    }
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) {
        super::seeds::update_replayed_member!((self, seeds, seeds_cache, group_id, member_id, v); metadata);
    }
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) {
        if v.assignment_epoch < 0 {
            return;
        }
        super::seeds::update_replayed_seeds!(self, seeds, seeds_cache, group_id; (v, v);
            |seed| replay_mutation(
                ReplayRecordKind::TargetAssignmentMetadata,
                Some(ReplayRecordKind::TargetAssignmentMetadata),
                true,
                false,
            ) == ReplayMutation::Apply
            && replay_epoch_is_admissible(seed.target_epoch, v.assignment_epoch) => |value| {
                seed.target_epoch = value.assignment_epoch;
                seed.assignment_timestamp_ms = value.assignment_timestamp_ms;
            }
        );
    }
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) {
        super::seeds::update_replayed_member!((self, seeds, seeds_cache, group_id, member_id, v); target);
    }
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::CurrentMemberAssignmentValue,
    ) {
        super::seeds::update_replayed_member!((self, seeds, seeds_cache, group_id, member_id, v); current);
    }

    /// Applies a `ConsumerGroupRegularExpression` record: what `regex`
    /// resolved to in the group. Kafka's `GroupMetadataManager.replay` of the
    /// record updates the resolved regular expressions of the group, which is
    /// how a coordinator failover restores the topics of every regex
    /// subscription.
    pub fn replay_regular_expression(
        &self,
        group_id: &str,
        regex: &str,
        v: persistence_next_gen::RegularExpressionValue,
    ) {
        super::seeds::update_replayed_seeds!(self, seeds, seeds_cache, group_id; (v.clone(), v);
            |seed| replay_mutation(
                ReplayRecordKind::RegularExpression,
                Some(ReplayRecordKind::RegularExpression),
                true,
                false,
            ) == ReplayMutation::Apply => |value| {
                seed.resolved_regexes.insert(regex.into(), value);
            }
        );
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
            super::seeds::remove_replayed_group(
                &self.seeds,
                &self.seeds_cache,
                &self.group_types,
                group_id,
            );
            return;
        }
        let group_id = key.group_id();
        let scrub = |seed: &mut GroupSeed| {
            super::seeds::scrub_seed_assignments!(seed, key, K, target_epoch;
                K::GroupMetadata { .. } => { seed.group_epoch = 0; },
            // Kafka ignores the tombstone of a group it does not hold, and
            // otherwise clears `hasSubscriptionMetadataRecord`.
            K::PartitionMetadata { .. } => { seed.has_subscription_metadata_record = false; },
            K::RegularExpression { regex, .. } => { seed.resolved_regexes.remove(regex); },
            );
        };

        super::seeds::scrub_replayed_seeds(&self.seeds, &self.seeds_cache, group_id, scrub);
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

        coord.replay_group_metadata(
            "g",
            persistence_next_gen::GroupMetadataValue {
                epoch: 11,
                metadata_hash: -556_879_919_459_959_918,
            },
        );
        coord.replay_member_metadata("g", "member-a", member.clone());
        coord.replay_target_assignment_metadata(
            "g",
            persistence_next_gen::TargetAssignmentMetadataValue {
                assignment_epoch: 12,
                assignment_timestamp_ms: 1_791_331_200_000,
            },
        );
        coord.replay_target_assignment_member("g", "member-a", target.clone());
        coord.replay_current_member_assignment("g", "member-a", current.clone());
        let resolved = persistence_next_gen::RegularExpressionValue {
            topics: vec!["topic-a".into()],
            version: 40,
            timestamp_ms: 1_000,
        };
        coord.replay_regular_expression("g", "topic-.*", resolved.clone());

        let expected = GroupSeed {
            has_subscription_metadata_record: false,
            group_epoch: 11,
            metadata_hash: -556_879_919_459_959_918,
            target_epoch: 12,
            assignment_timestamp_ms: 1_791_331_200_000,
            members: maplit::hashmap! {"member-a".to_string() => member},
            target_per_member: maplit::hashmap! {"member-a".to_string() => target},
            current_per_member: maplit::hashmap! {"member-a".to_string() => current},
            resolved_regexes: maplit::hashmap! {"topic-.*".to_string() => resolved},
        };
        assert!(*coord.seeds.get("g").unwrap() == expected);
        assert!(coord.cached_seed("g") == Some(expected));
    }

    /// A `ConsumerGroupRegularExpression` record needs its group, and its
    /// tombstone removes the entry from the seed and from the cache.
    #[test]
    fn a_regular_expression_record_follows_its_group_and_its_tombstone() {
        use persistence_next_gen::NextGenKey;

        let coord = make_coord();
        let resolved = |topics: &[&str]| persistence_next_gen::RegularExpressionValue {
            topics: topics.iter().map(|topic| (*topic).to_string()).collect(),
            version: 1,
            timestamp_ms: 2,
        };
        // No group yet: the record is ignored.
        coord.replay_regular_expression("g", "a.*", resolved(&["a"]));
        check!(coord.cached_seed("g").is_none());

        coord.replay_group_metadata(
            "g",
            persistence_next_gen::GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            },
        );
        coord.replay_regular_expression("g", "a.*", resolved(&["a"]));
        coord.replay_regular_expression("g", "b.*", resolved(&["b"]));
        coord.replay_regular_expression("g", "a.*", resolved(&["a", "a2"]));
        let seed = coord.cached_seed("g").unwrap();
        check!(
            seed.resolved_regexes
                == maplit::hashmap! {
                    "a.*".to_string() => resolved(&["a", "a2"]),
                    "b.*".to_string() => resolved(&["b"]),
                }
        );

        coord.replay_next_gen_tombstone(&NextGenKey::RegularExpression {
            group_id: "g".into(),
            regex: "a.*".into(),
        });
        check!(
            coord.cached_seed("g").unwrap().resolved_regexes
                == maplit::hashmap! {"b.*".to_string() => resolved(&["b"])}
        );
        check!(coord.seeds.get("g").unwrap().resolved_regexes.len() == 1);
    }

    #[test]
    fn group_tombstone_blocks_orphans_and_epoch_regression() {
        let coord = make_coord();
        coord.mark_next_gen("g");
        coord.replay_group_metadata(
            "g",
            persistence_next_gen::GroupMetadataValue {
                epoch: 4,
                metadata_hash: 0,
            },
        );
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(4));

        coord.replay_next_gen_tombstone(&persistence_next_gen::NextGenKey::GroupMetadata {
            group_id: "g".into(),
        });
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(i32::MAX));
        check!(coord.cached_seed("g").is_none());
        check!(coord.group_type("g").is_none());

        coord.replay_group_metadata(
            "g",
            persistence_next_gen::GroupMetadataValue {
                epoch: 0,
                metadata_hash: 0,
            },
        );
        coord.replay_member_metadata("g", "m", next_member("m"));
        coord.replay_current_member_assignment("g", "m", next_current(i32::MAX));
        coord.replay_current_member_assignment("g", "m", next_current(3));
        coord.replay_group_metadata(
            "g",
            persistence_next_gen::GroupMetadataValue {
                epoch: -1,
                metadata_hash: 0,
            },
        );
        let seed = coord.cached_seed("g").unwrap();
        check!(seed.group_epoch == 0);
        assert!(seed.current_per_member["m"].member_epoch == i32::MAX);
    }
}
