//! The KIP-227 partition-set diff: it drops forgotten partitions from a
//! session's cached set and merges the requested topics back in.
//!
//! `apply_incremental` is the pure core of an incremental fetch, with no lock
//! and no cache state around it, so the property test below and the stateright
//! model in `fetch_session_model` can drive it directly.

use {
    super::state::{FetchSessionKey, SessionPartitions},
    krabka_protocol::{
        owned::fetch_request::{FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid as WireUuid,
    },
    std::collections::{HashMap, HashSet},
};

/// Pure core of the incremental-fetch session update (KIP-227). It drops
/// forgotten partitions, then merges the requested topics into a session's
/// partition map.
///
/// - **forget**: a `ForgottenTopic` matches a cached key by either
///   `topic_name` (Fetch v 12 and below) **or** `topic_id` (v 13 and above),
///   plus the partition.
/// - **merge**: for each requested partition, this function finds a cached key
///   by either identity half plus the partition, and updates its desired state
///   in place. It inserts a new default-state key only when the partition is
///   truly not cached. That rule stops a partial-identity copy from shadowing
///   a fully resolved key. A new key goes to the end of the session's order,
///   as Kafka's `FetchSession.update` adds it, and an updated key keeps its
///   place.
///
/// `fetch_session_model` exercises the asymmetry between the OR-match forget
/// and the either-half-match merge exhaustively.
pub(crate) fn apply_incremental(
    partitions: &mut SessionPartitions,
    forgotten: &[ForgottenTopic],
    topics: &[FetchTopic],
) {
    let mut forgotten_names = HashSet::new();
    let mut forgotten_ids = HashSet::new();
    for ft in forgotten {
        for &partition in &ft.partitions {
            if !ft.topic.is_empty() {
                forgotten_names.insert((ft.topic.clone(), partition));
            }
            if ft.topic_id != WireUuid::ZERO {
                forgotten_ids.insert((ft.topic_id, partition));
            }
        }
    }
    if !forgotten_names.is_empty() || !forgotten_ids.is_empty() {
        partitions.retain(|k, _| {
            !forgotten_names.contains(&(k.topic_name.clone(), k.partition))
                && !forgotten_ids.contains(&(k.topic_id, k.partition))
        });
    }
    if topics.iter().all(|topic| topic.partitions.is_empty()) {
        return;
    }

    // Keep the first position for each identity, matching the old ordered
    // search even if malformed input has made the name and id aliases point at
    // different cached keys.
    let mut by_name = HashMap::with_capacity(partitions.len());
    let mut by_id = HashMap::with_capacity(partitions.len());
    for (index, key) in partitions.keys().enumerate() {
        if !key.topic_name.is_empty() {
            by_name
                .entry((key.topic_name.clone(), key.partition))
                .or_insert(index);
        }
        if key.topic_id != WireUuid::ZERO {
            by_id.entry((key.topic_id, key.partition)).or_insert(index);
        }
    }

    for t in topics {
        for fp in &t.partitions {
            let name_index = (!t.topic.is_empty())
                .then(|| by_name.get(&(t.topic.clone(), fp.partition)).copied())
                .flatten();
            let id_index = (t.topic_id != WireUuid::ZERO)
                .then(|| by_id.get(&(t.topic_id, fp.partition)).copied())
                .flatten();
            let existing_index = match (name_index, id_index) {
                (Some(name), Some(id)) => Some(name.min(id)),
                (name, id) => name.or(id),
            };
            let inserted = existing_index.is_none();
            let key = existing_index
                .and_then(|index| partitions.get_index(index).map(|(key, _)| key.clone()))
                .unwrap_or_else(|| FetchSessionKey {
                    topic_name: t.topic.clone(),
                    topic_id: t.topic_id,
                    partition: fp.partition,
                });
            let index = partitions.get_index_of(&key).unwrap_or(partitions.len());
            let entry = partitions.entry(key).or_default();
            entry.fetch_offset = fp.fetch_offset;
            entry.max_bytes = fp.partition_max_bytes;
            entry.current_leader_epoch = fp.current_leader_epoch;
            entry.last_fetched_epoch = fp.last_fetched_epoch;
            entry.log_start_offset = fp.log_start_offset;

            if inserted {
                if !t.topic.is_empty() {
                    by_name.insert((t.topic.clone(), fp.partition), index);
                }
                if t.topic_id != WireUuid::ZERO {
                    by_id.insert((t.topic_id, fp.partition), index);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::fetch_session::{CachedPartitionState, test_support::forgotten_topic},
        krabka_protocol::owned::fetch_request::FetchPartition,
    };

    fn fetch_topic(topic: &str, topic_id: WireUuid, partition: i32, max_bytes: i32) -> FetchTopic {
        FetchTopic {
            topic: topic.into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition,
                partition_max_bytes: max_bytes,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[yare::parameterized(
        topic_name = { WireUuid::ZERO, WireUuid::ZERO, false },
        topic_id = { WireUuid([1u8; 16]), WireUuid([2u8; 16]), true },
    )]
    fn forgotten_topic_drops_only_matching_topic_partition(
        topic_id: WireUuid,
        other_topic_id: WireUuid,
        by_id: bool,
    ) {
        let target = FetchSessionKey {
            topic_name: "t".into(),
            topic_id,
            partition: 0,
        };
        let other = FetchSessionKey {
            topic_name: "u".into(),
            topic_id: other_topic_id,
            partition: 0,
        };
        let mut partitions = indexmap::indexmap! {
            target.clone() => CachedPartitionState::default(),
            other.clone() => CachedPartitionState::default(),
        };
        let forgotten = vec![if by_id {
            forgotten_topic()
                .topic(String::new())
                .topic_id(topic_id)
                .call()
        } else {
            forgotten_topic().call()
        }];

        apply_incremental(&mut partitions, &forgotten, &[]);

        assert2::assert!(!partitions.contains_key(&target));
        assert2::assert!(partitions.contains_key(&other));
    }

    #[test]
    fn forget_precedes_merge_and_duplicate_updates_keep_order() {
        let id = WireUuid([1; 16]);
        let first = FetchSessionKey {
            topic_name: "first".into(),
            topic_id: id,
            partition: 0,
        };
        let retained = FetchSessionKey {
            topic_name: "retained".into(),
            topic_id: WireUuid([2; 16]),
            partition: 0,
        };
        let mut partitions = indexmap::indexmap! {
            first.clone() => CachedPartitionState::default(),
            retained.clone() => CachedPartitionState::default(),
        };
        let forgotten = vec![
            forgotten_topic()
                .topic("first".into())
                .topic_id(WireUuid::ZERO)
                .call(),
        ];
        let topics = vec![
            fetch_topic("first", id, 0, 10),
            fetch_topic("first", id, 0, 20),
        ];

        apply_incremental(&mut partitions, &forgotten, &topics);

        let expected = vec![retained, first];
        assert2::assert!(partitions.keys().cloned().collect::<Vec<_>>() == expected);
        assert2::assert!(partitions[&expected[1]].max_bytes == 20);
    }

    #[test]
    fn merge_uses_earliest_key_when_name_and_id_aliases_disagree() {
        let requested_id = WireUuid([1; 16]);
        let by_name = FetchSessionKey {
            topic_name: "topic".into(),
            topic_id: WireUuid([2; 16]),
            partition: 0,
        };
        let by_id = FetchSessionKey {
            topic_name: "other".into(),
            topic_id: requested_id,
            partition: 0,
        };
        for keys in [[by_name.clone(), by_id.clone()], [
            by_id.clone(),
            by_name.clone(),
        ]] {
            let mut partitions: SessionPartitions = keys
                .iter()
                .cloned()
                .map(|key| (key, CachedPartitionState::default()))
                .collect();
            apply_incremental(&mut partitions, &[], &[fetch_topic(
                "topic",
                requested_id,
                0,
                42,
            )]);
            assert2::assert!(partitions[&keys[0]].max_bytes == 42);
            assert2::assert!(partitions[&keys[1]].max_bytes == 0);
            assert2::assert!(partitions.keys().cloned().collect::<Vec<_>>() == keys);
        }
    }
}

/// Large-N random fuzzing of `apply_incremental`, the KIP-227 forget and merge
/// path. It complements the exhaustive `fetch_session_model`. Random sequences
/// of incremental fetches over a small topic, id, and partition universe, with
/// random identity halves, must keep no-shadow, subscription fidelity, and
/// no-orphan-default after every step.
#[cfg(test)]
mod fuzz {
    use {
        super::{SessionPartitions, apply_incremental},
        crate::fetch_session::test_support::forgotten_topic,
        krabka_protocol::{
            owned::fetch_request::{FetchPartition, FetchTopic},
            primitives::uuid::Uuid as WireUuid,
        },
        proptest::prelude::*,
    };

    // name index 0 = empty (id-only wire form), 1 = "A", 2 = "B".
    fn name_of(i: u8) -> String { ["", "A", "B"][i as usize].to_string() }
    // id index 0 = ZERO (name-only wire form), 1 = U, 2 = V.
    fn id_of(i: u8) -> WireUuid {
        [WireUuid::ZERO, WireUuid([1; 16]), WireUuid([2; 16])][i as usize]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn forget_merge_invariants(
            ops in proptest::collection::vec(
                // (forget name, forget id, forget partition,
                //  do-subscribe, sub name, sub id, sub partition, sub max_bytes)
                (0u8..3, 0u8..3, 0i32..2, any::<bool>(), 0u8..3, 0u8..3, 0i32..2, 1i32..4),
                0..200,
            )
        ) {
            let mut partitions = SessionPartitions::new();
            for (fname, fid, fp, do_sub, sname, sid, sp, mb) in ops {
                // A forget with an all-empty identity matches nothing — skip it
                // (the wire never carries a topic with neither name nor id).
                let forgotten = if fname == 0 && fid == 0 {
                    vec![]
                } else {
                    vec![forgotten_topic()
                        .topic(name_of(fname))
                        .topic_id(id_of(fid))
                        .partitions(vec![krabka_ids::PartitionIndex(fp)])
                        .call()]
                };
                let subscribe = do_sub && !(sname == 0 && sid == 0);
                let topics = if subscribe {
                    vec![FetchTopic {
                        topic: name_of(sname),
                        topic_id: id_of(sid),
                        partitions: vec![FetchPartition {
                            partition: sp,
                            partition_max_bytes: mb,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }]
                } else {
                    vec![]
                };

                apply_incremental(&mut partitions, &forgotten, &topics);

                // No shadow: no two keys share a logical partition.
                let keys: Vec<_> = partitions.keys().cloned().collect();
                for (i, a) in keys.iter().enumerate() {
                    for b in &keys[i + 1..] {
                        let shadow = a.partition == b.partition
                            && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                                || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id));
                        prop_assert!(!shadow, "shadow: {a:?} vs {b:?}");
                    }
                }

                // No orphan default: every cached entry carries a subscribed
                // max_bytes (merge always sets it; we only ever request >= 1).
                prop_assert!(partitions.values().all(|v| v.max_bytes != 0));

                // Subscription fidelity: a subscribed partition is reflected with
                // the requested max_bytes by some key matching the request.
                if subscribe {
                    let name = name_of(sname);
                    let id = id_of(sid);
                    let present = partitions.iter().any(|(k, st)| {
                        k.partition == sp
                            && ((!name.is_empty() && k.topic_name == name)
                                || (id != WireUuid::ZERO && k.topic_id == id))
                            && st.max_bytes == mb
                    });
                    prop_assert!(present, "subscription not reflected");
                }
            }
        }
    }
}
