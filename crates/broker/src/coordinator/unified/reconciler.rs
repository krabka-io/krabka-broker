//! Trigger-driven reconciler. It runs at the next heartbeat after a dirty
//! signal: a subscription change, a member add or leave, a metadata change that
//! [`refresh_metadata`] found, or an assignor selection change.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
};

use krabka_protocol::primitives::uuid::Uuid;

use super::assignor::{
    Assignor, GroupSpec, MemberSubscription, SubscriptionShape, TopicMetadata, subscription_type,
};
use crate::coordinator::unified::consumer_state::{GroupState, MemberState};

#[derive(Debug, Clone, Default)]
pub struct ReconcileInput {
    pub topic_id_by_name: HashMap<String, Uuid>,
    pub partitions_per_topic: HashMap<Uuid, i32>,
    /// Per-`(topic_id, partition_index)` set of replica racks.
    /// An empty or missing entry means there is no rack data for that
    /// partition. The built-in assignors do not read it, as in Kafka.
    pub partition_racks: HashMap<(Uuid, i32), Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOutcome {
    NoChange,
    Recomputed,
    EpochExhausted,
}

pub fn reconcile_if_dirty(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor: &dyn Assignor,
) -> ReconcileOutcome {
    if !group.dirty {
        return ReconcileOutcome::NoChange;
    }
    let subscriptions: Vec<MemberSubscription> = group
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(m, &input.topic_id_by_name),
            assigned_partitions: group
                .target
                .per_member
                .get(&m.member_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let shapes: Vec<SubscriptionShape<'_>> = group
        .members
        .values()
        .map(|m| SubscriptionShape {
            topic_names: &m.subscribed_topic_names,
            topic_regex: m.subscribed_topic_regex.as_deref(),
            regex_topic_names: regex_topic_names(m, &input.topic_id_by_name),
        })
        .collect();
    let spec = GroupSpec {
        members: subscriptions,
        subscription_type: subscription_type(&shapes),
    };
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
        partition_racks: input.partition_racks.clone(),
    };
    let assignment = assignor.assign(&spec, &topics);
    if !group.bump_epoch() {
        return ReconcileOutcome::EpochExhausted;
    }
    group.install_target(assignment);
    group.record_metadata_hash(metadata_hash(group, input));
    group.dirty = false;
    ReconcileOutcome::Recomputed
}

/// Kafka's `ModernGroup.computeMetadataHash`: one number over the metadata of
/// every topic that the group subscribes to and `input` holds.
///
/// A member subscribes to its topic names and to the topics that its regex
/// resolved to. Kafka's `Utils.computeTopicHash` hashes the id, the name, the
/// partition count and the partition racks of a topic, and
/// `Utils.computeGroupHash` combines the topic hashes in name order, or gives
/// `0` when no subscribed topic exists. This hash reads the same fields from
/// the reconcile snapshot, in the same order, and it is also `0` for no topic.
/// The snapshot names each rack of a partition once, where Kafka lists the
/// rack of each replica.
///
/// The hash stays in memory, so it does not need Kafka's bytes: the group
/// only compares it with the hash that its current target was computed from.
#[must_use]
pub fn metadata_hash(group: &GroupState, input: &ReconcileInput) -> u64 {
    let mut topics: BTreeSet<&str> = BTreeSet::new();
    for member in group.members.values() {
        topics.extend(member.subscribed_topic_names.iter().map(String::as_str));
        topics.extend(regex_topic_names(member, &input.topic_id_by_name));
    }
    let mut hasher = DefaultHasher::new();
    let mut any_topic = false;
    for name in topics {
        let Some(topic_id) = input.topic_id_by_name.get(name) else {
            continue;
        };
        any_topic = true;
        let partitions = input
            .partitions_per_topic
            .get(topic_id)
            .copied()
            .unwrap_or(0);
        (name, topic_id, partitions).hash(&mut hasher);
        for partition in 0..partitions {
            input
                .partition_racks
                .get(&(*topic_id, partition))
                .hash(&mut hasher);
        }
    }
    if any_topic { hasher.finish() } else { 0 }
}

/// Computes the metadata hash again for a group whose metadata expired, as
/// Kafka's `consumerGroupHeartbeat` and `classicGroupJoinToConsumerGroup` do
/// when `hasMetadataExpired` holds.
///
/// A new hash marks the group dirty. The reconcile that follows then bumps the
/// group epoch, computes a new target and records the hash, as Kafka's
/// `updateSubscriptionMetadata` does. An unchanged hash only ends the refresh:
/// Kafka bumps the group epoch for a new hash only, so a change that leaves the
/// assignor's input as it was, such as a new partition leader, does not
/// rebalance the group.
pub fn refresh_metadata(group: &mut GroupState, input: &ReconcileInput) {
    let hash = metadata_hash(group, input);
    if hash == group.metadata_hash() {
        group.record_metadata_hash(hash);
    } else {
        group.dirty = true;
    }
}

/// Insert into `out` every topic-id that a member subscribes to, both by exact
/// name and through its cached compiled regex. This is the single source of
/// truth for what a member subscribes to, and both `reconcile_if_dirty`
/// and `membership_topic_ids` use it.
///
/// At KIP-848 v1+ the client supplies the regex as a Java `Pattern`-syntax
/// string. Krabka compiles it with Rust's RE2-based `regex` crate, which
/// differs only in extended constructs such as lookaround. Operator-facing
/// subscription patterns do not use those. Compilation happens once, when the
/// pattern changes, and `MemberState` caches the result. An invalid pattern
/// compiles to `None`, with one warning at set time, so a bad regex never
/// poisons the rest of the assignment and never recompiles on the hot path.
fn collect_subscribed_topic_ids(
    member: &MemberState,
    topic_id_by_name: &HashMap<String, Uuid>,
    out: &mut HashSet<Uuid>,
) {
    for name in &member.subscribed_topic_names {
        if let Some(id) = topic_id_by_name.get(name) {
            out.insert(*id);
        }
    }
    if let Some(re) = member.compiled_regex() {
        for (name, id) in topic_id_by_name {
            if re.is_match(name) && member.regex_authorized_topics.contains(name) {
                out.insert(*id);
            }
        }
    }
}

/// The existing topic names that a member's regex subscription resolves to,
/// for Kafka's subscription-type rule.
fn regex_topic_names<'a>(
    member: &MemberState,
    topic_id_by_name: &'a HashMap<String, Uuid>,
) -> Vec<&'a str> {
    member.compiled_regex().map_or_else(Vec::new, |re| {
        topic_id_by_name
            .keys()
            .filter(|name| re.is_match(name) && member.regex_authorized_topics.contains(*name))
            .map(String::as_str)
            .collect()
    })
}

/// Resolve a member's effective topic-id subscription as a vector.
fn resolve_subscribed_topic_ids(
    member: &MemberState,
    topic_id_by_name: &HashMap<String, Uuid>,
) -> Vec<Uuid> {
    let mut out = HashSet::new();
    collect_subscribed_topic_ids(member, topic_id_by_name, &mut out);
    out.into_iter().collect()
}

#[must_use]
pub fn membership_topic_ids(group: &GroupState, input: &ReconcileInput) -> HashSet<Uuid> {
    let mut out = HashSet::new();
    for m in group.members.values() {
        collect_subscribed_topic_ids(m, &input.topic_id_by_name, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::{assert, check};

    use super::{super::assignor::UniformAssignor, *};
    use crate::coordinator::unified::{
        consumer_state::MemberState, persistence_next_gen::MemberAssignmentState,
    };

    fn fresh_member(id: &str, topic: &str) -> MemberState {
        let mut sub = HashSet::new();
        sub.insert(topic.into());
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: sub,
            subscribed_topic_regex: None,
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            regex_authorized_topics: HashSet::new(),
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs: HashMap::new(),
            last_seen: Instant::now(),
            classic: None,
        }
    }

    fn input(topic_name: &str, partitions: i32) -> (ReconcileInput, Uuid) {
        let t = Uuid([1; 16]);
        (
            ReconcileInput {
                topic_id_by_name: [(topic_name.into(), t)].into(),
                partitions_per_topic: [(t, partitions)].into(),
                ..Default::default()
            },
            t,
        )
    }

    #[test]
    fn dirty_triggers_recompute() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 4);
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        check!(outcome == ReconcileOutcome::Recomputed);
        check!(g.target.per_member["m1"][&t] == vec![0, 1, 2, 3]);
        check!(!g.dirty);
    }

    #[test]
    fn clean_is_no_op() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        let (inp, _) = input("t", 4);
        assert!(reconcile_if_dirty(&mut g, &inp, &UniformAssignor) == ReconcileOutcome::NoChange);
    }

    #[test]
    fn dirty_group_rejects_epoch_exhaustion_without_installing_a_target() {
        let mut group = GroupState::new("g");
        group.group_epoch = i32::MAX;
        group.dirty = true;
        let input = ReconcileInput::default();

        let outcome = reconcile_if_dirty(&mut group, &input, &UniformAssignor);

        assert!(outcome == ReconcileOutcome::EpochExhausted);
        assert!(group.group_epoch == i32::MAX);
        assert!(group.dirty);
    }

    #[test]
    fn idempotent_under_repeated_calls() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let epoch1 = g.group_epoch;
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert!(outcome == ReconcileOutcome::NoChange);
        assert!(g.group_epoch == epoch1);
    }

    #[test]
    fn metadata_change_via_dirty_flag_recomputes() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp1, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp1, &UniformAssignor);
        let epoch_before = g.group_epoch;
        let (inp2, _) = input("t", 4);
        g.dirty = true;
        let outcome = reconcile_if_dirty(&mut g, &inp2, &UniformAssignor);
        assert!(outcome == ReconcileOutcome::Recomputed);
        assert!(g.group_epoch > epoch_before);
    }

    #[test]
    fn subscription_topic_ids_resolved() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 2);
        let ids = membership_topic_ids(&g, &inp);
        assert!(ids.contains(&t));
    }

    // ── subscribed_topic_regex resolution ───────────────────

    fn input_with_topics(topics: &[(&str, i32)]) -> ReconcileInput {
        let mut topic_id_by_name = HashMap::new();
        let mut partitions_per_topic = HashMap::new();
        for (i, (name, parts)) in topics.iter().enumerate() {
            let id = Uuid([u8::try_from(i + 1).unwrap_or(255); 16]);
            topic_id_by_name.insert((*name).to_string(), id);
            partitions_per_topic.insert(id, *parts);
        }
        ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            ..Default::default()
        }
    }

    fn member_with_regex(
        id: &str,
        names: &[&str],
        regex: Option<&str>,
        authorized: &[&str],
    ) -> MemberState {
        let mut sub = HashSet::new();
        for n in names {
            sub.insert((*n).to_string());
        }
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: sub,
            subscribed_topic_regex: regex.map(String::from),
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            regex_authorized_topics: authorized.iter().map(|n| (*n).to_string()).collect(),
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs: HashMap::new(),
            last_seen: Instant::now(),
            classic: None,
        }
    }

    #[test]
    fn regex_resolves_to_matching_topic_ids() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex(
            "m1",
            &[],
            Some("^orders-.*"),
            &["orders-eu", "orders-us"],
        ));
        let inp = input_with_topics(&[("orders-eu", 1), ("orders-us", 1), ("shipments", 1)]);
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        let orders_us = inp.topic_id_by_name["orders-us"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        // Both `orders-*` topics match; `shipments` must not.
        assert!(assigned == maplit::hashset! {orders_eu, orders_us});
    }

    /// The security-fix regression case: a regex matches two topics, but the
    /// member's principal may not `Describe` one of them. The reconciler must
    /// drop that topic from the assignment even though it matches the
    /// pattern, matching Kafka's `filterTopicDescribeAuthorizedTopics`.
    #[test]
    fn regex_match_excludes_describe_denied_topics() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex(
            "m1",
            &[],
            Some("^orders-.*"),
            &["orders-eu"],
        ));
        let inp = input_with_topics(&[("orders-eu", 1), ("orders-us", 1), ("shipments", 1)]);
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        // `orders-us` matches the pattern but is not (yet) Describe-authorized,
        // so only `orders-eu` is assigned.
        assert!(assigned == maplit::hashset! {orders_eu});
    }

    #[test]
    fn regex_unions_with_names() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex(
            "m1",
            &["audit"],
            Some("^orders-.*"),
            &["orders-eu"],
        ));
        let inp = input_with_topics(&[("orders-eu", 1), ("audit", 1), ("shipments", 1)]);
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        let audit = inp.topic_id_by_name["audit"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        // Union of the regex match (`orders-eu`) and the explicit name
        // (`audit`); `shipments` must not appear.
        assert!(assigned == maplit::hashset! {orders_eu, audit});
    }

    #[test]
    fn invalid_regex_is_silently_dropped_and_names_still_apply() {
        let mut g = GroupState::new("g");
        // `*` at the start is an invalid Rust regex (and invalid in JVM
        // too — `PatternSyntaxException`). We must not panic; just fall
        // back to the names-only subscription.
        g.add_or_update_member(member_with_regex("m1", &["audit"], Some("*invalid"), &[]));
        let inp = input_with_topics(&[("audit", 1), ("orders-eu", 1)]);
        let audit = inp.topic_id_by_name["audit"];
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert!(
            assigned.contains(&audit),
            "names-only subscription still applied"
        );
        assert!(
            !assigned.contains(&orders_eu),
            "invalid regex must not silently match every topic"
        );
    }

    #[test]
    fn empty_regex_matches_everything() {
        // Empty pattern is a valid regex that matches every string; this
        // covers the operator-intended "subscribe to all topics" case
        // without forcing the client to enumerate them.
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some(""), &["a", "b", "c"]));
        let inp = input_with_topics(&[("a", 1), ("b", 1), ("c", 1)]);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert!(
            assigned.len() == 3,
            "empty regex matches every topic; got {assigned:?}"
        );
    }

    #[test]
    fn regex_change_marks_group_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("^a"), &["a1"]));
        let inp = input_with_topics(&[("a1", 1), ("b1", 1)]);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert!(!g.dirty, "fresh recompute clears dirty");
        let epoch_before = g.group_epoch;

        // Change the regex pattern → must dirty the group so the next
        // reconcile re-runs.
        g.add_or_update_member(member_with_regex("m1", &[], Some("^b"), &["b1"]));
        assert!(g.dirty, "regex change must mark group dirty");
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert!(outcome == ReconcileOutcome::Recomputed);
        assert!(g.group_epoch > epoch_before);
    }

    /// Kafka's metadata hash covers the id, the name, the partition count and
    /// the partition racks of every topic that the group subscribes to, by
    /// name or through a resolved regex, and nothing else.
    #[test]
    fn the_metadata_hash_moves_with_the_subscribed_topics_only() {
        let snapshot = |topics: &[(&str, u8, i32)], racks: &[(u8, i32, &str)]| ReconcileInput {
            topic_id_by_name: topics
                .iter()
                .map(|(name, id, _)| ((*name).to_string(), Uuid([*id; 16])))
                .collect(),
            partitions_per_topic: topics
                .iter()
                .map(|(_, id, partitions)| (Uuid([*id; 16]), *partitions))
                .collect(),
            partition_racks: racks
                .iter()
                .map(|(id, partition, rack)| ((Uuid([*id; 16]), *partition), vec![(*rack).into()]))
                .collect(),
        };
        let base: &[(&str, u8, i32)] = &[
            ("orders", 1, 2),
            ("payments", 2, 1),
            ("payouts", 3, 1),
            ("audit", 4, 1),
        ];
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "orders"));
        g.add_or_update_member(member_with_regex("m2", &[], Some("^pay.*"), &["payments"]));
        let before = metadata_hash(&g, &snapshot(base, &[(1, 0, "rack-a")]));

        // (name, the snapshot after, whether the hash moves)
        let rows = [
            (
                "nothing changes",
                snapshot(base, &[(1, 0, "rack-a")]),
                false,
            ),
            (
                "an unsubscribed topic is created",
                snapshot(
                    &[base, &[("refunds", 5, 1)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "an unsubscribed topic grows",
                snapshot(
                    &[&base[..3], &[("audit", 4, 2)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "a topic that the regex matches without authorization grows",
                snapshot(
                    &[&base[..2], &[("payouts", 3, 2), ("audit", 4, 1)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "a subscribed topic grows",
                snapshot(
                    &[&[("orders", 1, 3)][..], &base[1..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                true,
            ),
            (
                "a subscribed topic is deleted",
                snapshot(&base[1..], &[]),
                true,
            ),
            (
                "a subscribed topic is created again with a new id",
                snapshot(
                    &[&[("orders", 9, 2)][..], &base[1..]].concat(),
                    &[(9, 0, "rack-a")],
                ),
                true,
            ),
            (
                "a partition of a subscribed topic moves to another rack",
                snapshot(base, &[(1, 0, "rack-b")]),
                true,
            ),
            (
                "a topic that the regex resolved to grows",
                snapshot(
                    &[&base[..1], &[("payments", 2, 2)][..], &base[2..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                true,
            ),
        ];
        let mut found = Vec::new();
        let mut expected = Vec::new();
        for (name, after, moves) in rows {
            found.push((name, metadata_hash(&g, &after) != before));
            expected.push((name, moves));
        }
        check!(found == expected);
        check!(metadata_hash(&g, &ReconcileInput::default()) == 0);
    }

    #[test]
    fn membership_topic_ids_includes_regex_matches() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex(
            "m1",
            &[],
            Some("^orders-"),
            &["orders-eu"],
        ));
        let inp = input_with_topics(&[("orders-eu", 1), ("shipments", 1)]);
        let orders = inp.topic_id_by_name["orders-eu"];
        let ids = membership_topic_ids(&g, &inp);
        assert!(
            ids.contains(&orders),
            "regex match flows into membership set"
        );
    }
}
