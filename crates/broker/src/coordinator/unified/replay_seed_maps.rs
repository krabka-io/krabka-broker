//! The replayed and last-known-good seed maps of one group protocol, viewed
//! together.
//!
//! The bootstrap replay applies every record to both maps, so that a group
//! that finishes replay is ready to hydrate an actor and to survive a later
//! actor respawn. The consumer, share and streams replays differ only in the
//! seed type and the pair of maps, which [`SeedMaps`] takes as a parameter.

use {
    super::{
        group_coordinator::GroupCoordinator,
        seeds::{GroupSeed, ReplaySeed, ShareGroupSeed},
    },
    crate::error::BrokerError,
    dashmap::DashMap,
};

/// One protocol's replayed seeds and their cached copies.
pub(super) struct SeedMaps<'a, S> {
    live: &'a DashMap<String, S>,
    cache: &'a DashMap<String, S>,
}

impl<S: ReplaySeed> SeedMaps<'_, S> {
    /// Puts a new group under `group_id` in both maps, as Kafka's replay
    /// creates one for the first record of a group id.
    pub(super) fn create(&self, group_id: &str) {
        self.live.insert(group_id.into(), S::new_group());
        self.cache.insert(group_id.into(), S::new_group());
    }

    /// Applies `update` to the replayed group and to its cached copy.
    pub(super) fn update(&self, group_id: &str, update: impl Fn(&mut S)) {
        if let Some(mut seed) = self.live.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.cache
                .entry(group_id.into())
                .or_insert_with(S::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    pub(super) fn update_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut S, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.live.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.cache
                .entry(group_id.into())
                .or_insert_with(S::new_group)
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
    pub(super) fn read<T>(
        &self,
        group_id: &str,
        read: impl FnOnce(&S) -> T,
    ) -> Result<T, BrokerError> {
        self.live
            .get(group_id)
            .map(|seed| read(seed.value()))
            .ok_or_else(|| BrokerError::Startup(format!("Group {group_id} has no replayed state.")))
    }
}

impl GroupCoordinator {
    /// The consumer groups' replayed seeds and cached copies.
    pub(super) fn consumer_replay(&self) -> SeedMaps<'_, GroupSeed> {
        SeedMaps {
            live: &self.seeds,
            cache: &self.seeds_cache,
        }
    }

    /// The share groups' replayed seeds and cached copies.
    pub(super) fn share_replay(&self) -> SeedMaps<'_, ShareGroupSeed> {
        SeedMaps {
            live: &self.share_seeds,
            cache: &self.share_seeds_cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::super::{
            group_coordinator::GroupCoordinator,
            persistence_next_gen::{
                GroupMetadataValue, NextGenKey, TargetAssignmentMemberValue,
                TargetAssignmentMetadataValue,
            },
            seeds::{GroupSeed, ReplaySeed, ShareGroupSeed, StreamsGroupSeed},
            share::persistence::{
                ShareGroupCurrentMemberAssignmentValue, ShareGroupKey, ShareGroupMetadataValue,
                ShareGroupTargetAssignmentMemberValue, ShareGroupTargetAssignmentMetadataValue,
            },
            streams::persistence::{
                DescriptionEpochs, StreamsGroupCurrentMemberAssignmentValue, StreamsGroupKey,
                StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMemberValue,
                StreamsGroupTargetAssignmentMetadataValue,
            },
            test_support::{make_coord, next_current, next_member, share_member, streams_member},
        },
        crate::error::BrokerError,
        assert2::{assert, check},
        dashmap::DashMap,
    };

    /// What a replay left behind: its result, with a startup error as its
    /// exact message, and the group in the replayed and the cached map.
    #[derive(Debug, PartialEq)]
    struct Outcome<S> {
        result: Result<(), String>,
        live: Option<S>,
        cache: Option<S>,
    }

    impl<S> Outcome<S> {
        fn ok(seed: Option<S>) -> Self
        where
            S: Clone,
        {
            Self {
                result: Ok(()),
                live: seed.clone(),
                cache: seed,
            }
        }

        fn failed(message: &str, seed: S) -> Self
        where
            S: Clone,
        {
            Self {
                result: Err(message.into()),
                live: Some(seed.clone()),
                cache: Some(seed),
            }
        }
    }

    struct Case<S> {
        name: &'static str,
        replay: fn(&GroupCoordinator) -> Result<(), BrokerError>,
        expected: Outcome<S>,
    }

    fn check_cases<S: Clone + PartialEq + std::fmt::Debug>(
        cases: Vec<Case<S>>,
        maps: impl Fn(&GroupCoordinator) -> (&DashMap<String, S>, &DashMap<String, S>),
    ) {
        for case in cases {
            let coordinator = make_coord();
            let result = (case.replay)(&coordinator).map_err(|error| match error {
                BrokerError::Startup(message) => message,
                other => other.to_string(),
            });
            let (live, cache) = maps(&coordinator);
            let observed = Outcome {
                result,
                live: live.get("g").map(|seed| seed.value().clone()),
                cache: cache.get("g").map(|seed| seed.value().clone()),
            };
            check!(observed == case.expected, "{}", case.name);
        }
    }

    fn consumer(edit: impl FnOnce(&mut GroupSeed)) -> GroupSeed {
        let mut seed = GroupSeed::new_group();
        edit(&mut seed);
        seed
    }

    fn share(edit: impl FnOnce(&mut ShareGroupSeed)) -> ShareGroupSeed {
        let mut seed = ShareGroupSeed::new_group();
        edit(&mut seed);
        seed
    }

    fn streams(edit: impl FnOnce(&mut StreamsGroupSeed)) -> StreamsGroupSeed {
        let mut seed = StreamsGroupSeed::new_group();
        edit(&mut seed);
        seed
    }

    fn consumer_key(build: fn(String) -> NextGenKey) -> NextGenKey { build("g".into()) }

    fn consumer_member_key(build: fn(String, String) -> NextGenKey) -> NextGenKey {
        build("g".into(), "m".into())
    }

    #[test]
    fn consumer_value_then_tombstone_sequences_leave_identical_live_and_cached_seeds() {
        let cases = vec![
            Case {
                name: "group metadata after its target metadata tombstone removes the group",
                replay: |c| {
                    c.replay_group_metadata("g", GroupMetadataValue {
                        epoch: 5,
                        metadata_hash: 9,
                    })?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::TargetAssignmentMetadata { group_id }
                    }))?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::GroupMetadata { group_id }
                    }))
                },
                expected: Outcome::ok(None),
            },
            Case {
                name: "group metadata tombstone with the target metadata still live fails",
                replay: |c| {
                    c.replay_group_metadata("g", GroupMetadataValue {
                        epoch: 5,
                        metadata_hash: 9,
                    })?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::GroupMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete group g but did not receive \
                     ConsumerGroupTargetAssignmentMetadataValue tombstone.",
                    consumer(|seed| {
                        seed.group_epoch = 5;
                        seed.metadata_hash = 9;
                    }),
                ),
            },
            Case {
                name: "group metadata tombstone with a member fails",
                replay: |c| {
                    c.replay_member_metadata("g", "m", next_member("client"))?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::GroupMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete group g but the group still has 1 \
                     members.",
                    consumer(|seed| {
                        seed.members.insert("m".into(), next_member("client"));
                    }),
                ),
            },
            Case {
                name: "target metadata tombstone clears the epoch and time",
                replay: |c| {
                    c.replay_target_assignment_metadata("g", TargetAssignmentMetadataValue {
                        assignment_epoch: 4,
                        assignment_timestamp_ms: 77,
                    })?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::ok(Some(consumer(|seed| {
                    seed.target_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                }))),
            },
            Case {
                name: "target metadata tombstone with a member target fails",
                replay: |c| {
                    c.replay_target_assignment_member(
                        "g",
                        "m",
                        TargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_next_gen_tombstone(&consumer_key(|group_id| {
                        NextGenKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete target assignment of g but the \
                     assignment still has 1 members.",
                    consumer(|seed| {
                        seed.target_per_member
                            .insert("m".into(), TargetAssignmentMemberValue::default());
                    }),
                ),
            },
            Case {
                name: "target member tombstone removes the target",
                replay: |c| {
                    c.replay_target_assignment_member(
                        "g",
                        "m",
                        TargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::TargetAssignmentMember {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(GroupSeed::new_group())),
            },
            Case {
                name: "member tombstone after its current tombstone removes the member",
                replay: |c| {
                    c.replay_member_metadata("g", "m", next_member("client"))?;
                    c.replay_current_member_assignment("g", "m", next_current(3))?;
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::CurrentMemberAssignment {
                            group_id,
                            member_id,
                        }
                    }))?;
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(GroupSeed::new_group())),
            },
            Case {
                name: "member tombstone before its current tombstone fails",
                replay: |c| {
                    c.replay_member_metadata("g", "m", next_member("client"))?;
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m but did not receive \
                     ConsumerGroupCurrentMemberAssignmentValue tombstone.",
                    consumer(|seed| {
                        seed.members.insert("m".into(), next_member("client"));
                    }),
                ),
            },
            Case {
                name: "member tombstone of a member with a target fails",
                replay: |c| {
                    c.replay_member_metadata("g", "m", next_member("client"))?;
                    c.replay_current_member_assignment("g", "m", next_current(-1))?;
                    c.replay_target_assignment_member(
                        "g",
                        "m",
                        TargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m but did not receive \
                     ConsumerGroupTargetAssignmentMetadataValue tombstone.",
                    consumer(|seed| {
                        seed.members.insert("m".into(), next_member("client"));
                        seed.current_per_member.insert("m".into(), next_current(-1));
                        seed.target_per_member
                            .insert("m".into(), TargetAssignmentMemberValue::default());
                    }),
                ),
            },
            Case {
                name: "tombstone of a group the replay does not hold changes nothing",
                replay: |c| {
                    c.replay_next_gen_tombstone(&consumer_member_key(|group_id, member_id| {
                        NextGenKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(None),
            },
        ];
        check_cases(cases, |c| (&*c.seeds, &*c.seeds_cache));
    }

    fn share_key(build: fn(String) -> ShareGroupKey) -> ShareGroupKey { build("g".into()) }

    fn share_member_key(build: fn(String, String) -> ShareGroupKey) -> ShareGroupKey {
        build("g".into(), "m".into())
    }

    fn share_current(epoch: i32) -> ShareGroupCurrentMemberAssignmentValue {
        ShareGroupCurrentMemberAssignmentValue {
            member_epoch: epoch,
            previous_member_epoch: epoch - 1,
            assigned_partitions: Vec::new(),
        }
    }

    #[test]
    fn share_value_then_tombstone_sequences_leave_identical_live_and_cached_seeds() {
        let cases = vec![
            Case {
                name: "group metadata after its target metadata tombstone removes the group",
                replay: |c| {
                    c.replay_share_group_metadata("g", ShareGroupMetadataValue {
                        epoch: 5,
                        metadata_hash: 9,
                    })?;
                    c.replay_share_tombstone(&share_key(|group_id| {
                        ShareGroupKey::TargetAssignmentMetadata { group_id }
                    }))?;
                    c.replay_share_tombstone(&share_key(|group_id| ShareGroupKey::GroupMetadata {
                        group_id,
                    }))
                },
                expected: Outcome::ok(None),
            },
            Case {
                name: "group metadata tombstone with the target metadata still live fails",
                replay: |c| {
                    c.replay_share_group_metadata("g", ShareGroupMetadataValue {
                        epoch: 5,
                        metadata_hash: 9,
                    })?;
                    c.replay_share_tombstone(&share_key(|group_id| ShareGroupKey::GroupMetadata {
                        group_id,
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete group g but target assignment epoch \
                     in invalid.",
                    share(|seed| {
                        seed.group_epoch = 5;
                        seed.metadata_hash = 9;
                    }),
                ),
            },
            Case {
                name: "group metadata tombstone with a member target fails",
                replay: |c| {
                    c.replay_share_target_assignment_member(
                        "g",
                        "m",
                        ShareGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_share_tombstone(&share_key(|group_id| ShareGroupKey::GroupMetadata {
                        group_id,
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete group g but the target assignment \
                     still has 1 members.",
                    share(|seed| {
                        seed.target_per_member
                            .insert("m".into(), ShareGroupTargetAssignmentMemberValue::default());
                    }),
                ),
            },
            Case {
                name: "target metadata tombstone clears the epoch and time",
                replay: |c| {
                    c.replay_share_target_assignment_metadata(
                        "g",
                        ShareGroupTargetAssignmentMetadataValue {
                            assignment_epoch: 4,
                            assignment_timestamp_ms: 77,
                        },
                    )?;
                    c.replay_share_tombstone(&share_key(|group_id| {
                        ShareGroupKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::ok(Some(share(|seed| {
                    seed.target_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                }))),
            },
            Case {
                name: "target metadata tombstone with a member target fails",
                replay: |c| {
                    c.replay_share_target_assignment_member(
                        "g",
                        "m",
                        ShareGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_share_tombstone(&share_key(|group_id| {
                        ShareGroupKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete target assignment of g but the \
                     assignment still has 1 members.",
                    share(|seed| {
                        seed.target_per_member
                            .insert("m".into(), ShareGroupTargetAssignmentMemberValue::default());
                    }),
                ),
            },
            Case {
                name: "target member tombstone removes the target",
                replay: |c| {
                    c.replay_share_target_assignment_member(
                        "g",
                        "m",
                        ShareGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::TargetAssignmentMember {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(ShareGroupSeed::new_group())),
            },
            Case {
                name: "member tombstone after its current tombstone removes the member",
                replay: |c| {
                    c.replay_share_member_metadata("g", "m", share_member("client"))?;
                    c.replay_share_current_member_assignment("g", "m", share_current(3))?;
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::CurrentMemberAssignment {
                            group_id,
                            member_id,
                        }
                    }))?;
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(ShareGroupSeed::new_group())),
            },
            Case {
                name: "member tombstone before its current tombstone fails",
                replay: |c| {
                    c.replay_share_member_metadata("g", "m", share_member("client"))?;
                    c.replay_share_current_member_assignment("g", "m", share_current(3))?;
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m with invalid leave group \
                     epoch.",
                    share(|seed| {
                        seed.members.insert("m".into(), share_member("client"));
                        seed.current_per_member.insert("m".into(), share_current(3));
                    }),
                ),
            },
            Case {
                name: "member tombstone of a member with a target fails",
                replay: |c| {
                    c.replay_share_member_metadata("g", "m", share_member("client"))?;
                    c.replay_share_current_member_assignment("g", "m", share_current(-1))?;
                    c.replay_share_target_assignment_member(
                        "g",
                        "m",
                        ShareGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m but member exists in target \
                     assignment.",
                    share(|seed| {
                        seed.members.insert("m".into(), share_member("client"));
                        seed.current_per_member
                            .insert("m".into(), share_current(-1));
                        seed.target_per_member
                            .insert("m".into(), ShareGroupTargetAssignmentMemberValue::default());
                    }),
                ),
            },
            Case {
                name: "tombstone of a group the replay does not hold changes nothing",
                replay: |c| {
                    c.replay_share_tombstone(&share_member_key(|group_id, member_id| {
                        ShareGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(None),
            },
        ];
        check_cases(cases, |c| (&*c.share_seeds, &*c.share_seeds_cache));
    }

    fn streams_key(build: fn(String) -> StreamsGroupKey) -> StreamsGroupKey { build("g".into()) }

    fn streams_member_key(build: fn(String, String) -> StreamsGroupKey) -> StreamsGroupKey {
        build("g".into(), "m".into())
    }

    fn streams_metadata() -> StreamsGroupMetadataValue {
        StreamsGroupMetadataValue {
            epoch: 5,
            metadata_hash: 9,
            validated_topology_epoch: 2,
            last_assignment_configs: None,
            description: DescriptionEpochs::default(),
        }
    }

    fn streams_current(epoch: i32) -> StreamsGroupCurrentMemberAssignmentValue {
        StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: epoch,
            previous_member_epoch: epoch - 1,
            ..Default::default()
        }
    }

    #[test]
    fn streams_value_then_tombstone_sequences_leave_identical_live_and_cached_seeds() {
        let cases = vec![
            Case {
                name: "group metadata after its target metadata tombstone removes the group",
                replay: |c| {
                    c.replay_streams_group_metadata("g", streams_metadata())?;
                    c.replay_streams_tombstone(&streams_key(|group_id| {
                        StreamsGroupKey::TargetAssignmentMetadata { group_id }
                    }))?;
                    c.replay_streams_tombstone(&streams_key(|group_id| {
                        StreamsGroupKey::GroupMetadata { group_id }
                    }))
                },
                expected: Outcome::ok(None),
            },
            Case {
                name: "group metadata tombstone with the target metadata still live fails",
                replay: |c| {
                    c.replay_streams_group_metadata("g", streams_metadata())?;
                    c.replay_streams_tombstone(&streams_key(|group_id| {
                        StreamsGroupKey::GroupMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete group g but did not receive \
                     StreamsGroupTargetAssignmentMetadataValue tombstone.",
                    streams(|seed| {
                        seed.group_epoch = 5;
                        seed.metadata_hash = 9;
                        seed.validated_topology_epoch = 2;
                    }),
                ),
            },
            Case {
                name: "target metadata tombstone clears the epoch and time",
                replay: |c| {
                    c.replay_streams_target_assignment_metadata(
                        "g",
                        StreamsGroupTargetAssignmentMetadataValue {
                            assignment_epoch: 4,
                            assignment_timestamp_ms: 77,
                        },
                    )?;
                    c.replay_streams_tombstone(&streams_key(|group_id| {
                        StreamsGroupKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::ok(Some(streams(|seed| {
                    seed.assignment_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                }))),
            },
            Case {
                name: "target metadata tombstone with a member target fails",
                replay: |c| {
                    c.replay_streams_target_assignment_member(
                        "g",
                        "m",
                        StreamsGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_streams_tombstone(&streams_key(|group_id| {
                        StreamsGroupKey::TargetAssignmentMetadata { group_id }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete target assignment of g but the \
                     assignment still has 1 members.",
                    streams(|seed| {
                        seed.target_per_member.insert(
                            "m".into(),
                            StreamsGroupTargetAssignmentMemberValue::default(),
                        );
                    }),
                ),
            },
            Case {
                name: "target member tombstone removes the target",
                replay: |c| {
                    c.replay_streams_target_assignment_member(
                        "g",
                        "m",
                        StreamsGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::TargetAssignmentMember {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(StreamsGroupSeed::new_group())),
            },
            Case {
                name: "member tombstone after its current tombstone removes the member",
                replay: |c| {
                    c.replay_streams_member_metadata("g", "m", streams_member("client"))?;
                    c.replay_streams_current_member_assignment("g", "m", streams_current(3))?;
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::CurrentMemberAssignment {
                            group_id,
                            member_id,
                        }
                    }))?;
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(Some(StreamsGroupSeed::new_group())),
            },
            Case {
                name: "member tombstone before its current tombstone fails",
                replay: |c| {
                    c.replay_streams_member_metadata("g", "m", streams_member("client"))?;
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m but did not receive \
                     StreamsGroupCurrentMemberAssignmentValue tombstone.",
                    streams(|seed| {
                        seed.members.insert("m".into(), streams_member("client"));
                    }),
                ),
            },
            Case {
                name: "member tombstone of a member with a target fails",
                replay: |c| {
                    c.replay_streams_member_metadata("g", "m", streams_member("client"))?;
                    c.replay_streams_current_member_assignment("g", "m", streams_current(-1))?;
                    c.replay_streams_target_assignment_member(
                        "g",
                        "m",
                        StreamsGroupTargetAssignmentMemberValue::default(),
                    )?;
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::failed(
                    "Received a tombstone record to delete member m but did not receive \
                     StreamsGroupTargetAssignmentMetadataValue tombstone.",
                    streams(|seed| {
                        seed.members.insert("m".into(), streams_member("client"));
                        seed.current_per_member
                            .insert("m".into(), streams_current(-1));
                        seed.target_per_member.insert(
                            "m".into(),
                            StreamsGroupTargetAssignmentMemberValue::default(),
                        );
                    }),
                ),
            },
            Case {
                name: "tombstone of a group the replay does not hold changes nothing",
                replay: |c| {
                    c.replay_streams_tombstone(&streams_member_key(|group_id, member_id| {
                        StreamsGroupKey::MemberMetadata {
                            group_id,
                            member_id,
                        }
                    }))
                },
                expected: Outcome::ok(None),
            },
        ];
        check_cases(cases, |c| (&*c.streams_seeds, &*c.streams_seeds_cache));
    }

    #[test]
    fn an_update_of_a_group_the_replay_does_not_hold_creates_only_its_cached_copy() {
        let c = make_coord();
        c.consumer_replay().update("g", |seed| seed.group_epoch = 7);
        c.share_replay()
            .update_with("g", 8, |seed, epoch| seed.group_epoch = epoch);
        check!(
            (c.seeds.get("g").is_none(), c.cached_seed("g"))
                == (true, Some(consumer(|seed| seed.group_epoch = 7)))
        );
        check!(
            (c.share_seeds.get("g").is_none(), c.cached_share_seed("g"))
                == (true, Some(share(|seed| seed.group_epoch = 8)))
        );
        let read = c.consumer_replay().read("g", |seed| seed.group_epoch);
        assert!(let Err(BrokerError::Startup(message)) = read);
        check!(message == "Group g has no replayed state.");
    }
}
