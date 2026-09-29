//! Unit tests for the regular-expression subscriptions of the KIP-848
//! heartbeat: the resolution a heartbeat makes for its group, the records it
//! writes, and the refreshes that a heartbeat without a pattern makes.

use std::{collections::HashSet, time::Instant};

use assert2::{assert, check};
use krabka_protocol::primitives::uuid::Uuid;

use super::*;
use crate::coordinator::unified::{
    actor::test_support::StaticMetadata, persistence_next_gen::RegularExpressionValue,
    reconciler::ReconcileInput, regex_resolver::FixedRegexResolver,
};

const A1: Uuid = Uuid([1; 16]);
const A2: Uuid = Uuid([2; 16]);
const B1: Uuid = Uuid([3; 16]);

/// Topics `a1` and `b1`, and `a2` when `with_a2`.
fn metadata(with_a2: bool) -> StaticMetadata {
    let mut topics = vec![("a1", A1), ("b1", B1)];
    if with_a2 {
        topics.push(("a2", A2));
    }
    StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: topics
                .iter()
                .map(|(name, id)| ((*name).to_owned(), *id))
                .collect(),
            partitions_per_topic: topics.iter().map(|(_, id)| (*id, 2)).collect(),
            ..Default::default()
        },
    }
}

fn request(
    member_id: &str,
    member_epoch: i32,
    regex: Option<&str>,
) -> ConsumerGroupHeartbeatRequest {
    let joining = member_epoch == 0;
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: if joining { 60_000 } else { -1 },
        topic_partitions: joining.then(Vec::new),
        subscribed_topic_regex: regex.map(str::to_owned),
        ..Default::default()
    }
}

fn heartbeat(
    state: &mut GroupState,
    metadata: &StaticMetadata,
    request: &ConsumerGroupHeartbeatRequest,
    regexes: &RegexResolution<'_>,
) -> HeartbeatStep {
    step_heartbeat(
        state,
        &NextGenConfig::assigning_at_once(),
        metadata,
        request,
        ClientIdentity {
            id: "client",
            host: "host",
        },
        Instant::now(),
        regexes,
    )
}

/// The topics that the target assigns to `member_id`.
fn target_topics(state: &GroupState, member_id: &str) -> HashSet<Uuid> {
    state
        .target
        .per_member
        .get(member_id)
        .map(|topics| topics.keys().copied().collect())
        .unwrap_or_default()
}

fn resolved(topics: &[&str]) -> RegularExpressionValue {
    RegularExpressionValue {
        topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
        version: 100,
        timestamp_ms: RegexResolution::TEST_NOW_MS,
    }
}

/// A member that joins with a regex has it resolved for the group at once:
/// its target holds the topics of the resolution, and the heartbeat writes the
/// resolution as a `ConsumerGroupRegularExpression` record.
#[test]
fn a_member_that_joins_with_a_regex_gets_the_resolved_topics_at_once() {
    let resolver = FixedRegexResolver::new(&[("a.*", &["a1", "a2"])]);
    let mut state = GroupState::new("g");

    let step = heartbeat(
        &mut state,
        &metadata(true),
        &request("m1", 0, Some("a.*")),
        &RegexResolution::with(&resolver),
    );

    check!(step.response.error_code == 0);
    check!(
        step.pending.resolved_regexes == vec![("a.*".to_owned(), Some(resolved(&["a1", "a2"])))]
    );
    check!(target_topics(&state, "m1") == HashSet::from([A1, A2]));
    let held: HashSet<Uuid> = state.members["m1"]
        .assigned_partitions
        .keys()
        .copied()
        .collect();
    assert!(held == HashSet::from([A1, A2]));
}

/// Members that subscribe to the same regex share the group's resolution: the
/// second one triggers no resolution and writes no record.
#[test]
fn members_share_the_resolution_of_the_same_regex() {
    let resolver = FixedRegexResolver::new(&[("a.*", &["a1"])]);
    let mut state = GroupState::new("g");
    let metadata = metadata(false);
    heartbeat(
        &mut state,
        &metadata,
        &request("m1", 0, Some("a.*")),
        &RegexResolution::with(&resolver),
    );

    let step = heartbeat(
        &mut state,
        &metadata,
        &request("m2", 0, Some("a.*")),
        &RegexResolution::with(&resolver),
    );

    check!(resolver.calls() == 1);
    check!(step.pending.resolved_regexes.is_empty());
    check!(target_topics(&state, "m2") == HashSet::from([A1]));
}

/// A heartbeat that carries no pattern still refreshes the resolutions of its
/// group once the metadata has changed since the last one, so a topic created
/// after the members joined is assigned without a member sending its pattern
/// again, and a `SubscribedTopicRegex` that a Java client never resends does
/// not leave the topic unassigned.
#[test]
fn a_heartbeat_without_the_pattern_refreshes_the_resolution_when_the_metadata_changed() {
    let first = FixedRegexResolver::new(&[("a.*", &["a1"])]);
    let mut state = GroupState::new("g");
    heartbeat(
        &mut state,
        &metadata(false),
        &request("m1", 0, Some("a.*")),
        &RegexResolution::with(&first),
    );
    check!(target_topics(&state, "m1") == HashSet::from([A1]));
    let epoch = state.members["m1"].member_epoch;
    let second = FixedRegexResolver::new(&[("a.*", &["a1", "a2"])]);

    // (label, the version of the latest relevant image, whether it refreshes)
    let rows = [
        ("no image changed since the resolution", 100, false),
        ("a topic was created since the resolution", 101, true),
    ];
    let mut refreshed = Vec::new();
    for (label, refresh_version, _) in rows {
        let step = heartbeat(
            &mut state,
            &metadata(true),
            &request("m1", epoch, None),
            &RegexResolution {
                refresh_version,
                now_ms: RegexResolution::TEST_NOW_MS + 60_000,
                ..RegexResolution::with(&second)
            },
        );
        refreshed.push((label, !step.pending.resolved_regexes.is_empty()));
    }
    check!(
        refreshed
            == rows
                .iter()
                .map(|(label, _, refresh)| (*label, *refresh))
                .collect::<Vec<_>>()
    );
    check!(target_topics(&state, "m1") == HashSet::from([A1, A2]));
    check!(
        state
            .resolved_regex("a.*")
            .map(|resolution| resolution.topics.len())
            == Some(2)
    );
}

/// The resolution is made with the principal of the heartbeat that finds it
/// stale, so a regex that the principal may no longer describe loses the
/// topic: the group's target drops it at the same heartbeat.
#[test]
fn a_refresh_that_no_longer_selects_a_topic_takes_it_from_the_target() {
    let first = FixedRegexResolver::new(&[("a.*", &["a1", "a2"])]);
    let mut state = GroupState::new("g");
    let metadata = metadata(true);
    heartbeat(
        &mut state,
        &metadata,
        &request("m1", 0, Some("a.*")),
        &RegexResolution::with(&first),
    );
    check!(target_topics(&state, "m1") == HashSet::from([A1, A2]));
    let epoch = state.members["m1"].member_epoch;
    let second = FixedRegexResolver::new(&[("a.*", &["a1"])]);

    let step = heartbeat(
        &mut state,
        &metadata,
        &request("m1", epoch, None),
        &RegexResolution {
            refresh_version: 101,
            now_ms: RegexResolution::TEST_NOW_MS + 60_000,
            ..RegexResolution::with(&second)
        },
    );

    check!(step.pending.resolved_regexes == vec![("a.*".to_owned(), Some(resolved(&["a1"])))]);
    check!(target_topics(&state, "m1") == HashSet::from([A1]));
}

/// A coordinator failover restores the group from its records, the resolution
/// of every regex included. The first heartbeat after it carries no pattern,
/// as a Java client sends it only when it changes, and the member keeps the
/// topics that the regex resolved to: the group asks no resolver, and its
/// target holds them.
#[test]
fn a_failover_keeps_the_topics_of_a_regex_without_a_heartbeat_that_carries_the_pattern() {
    use crate::coordinator::unified::{
        GroupSeed,
        persistence_next_gen::{
            AssignedTopicPartitions, CurrentMemberAssignmentValue, CurrentTopicPartitions,
            MemberAssignmentState, MemberMetadataValue, TargetAssignmentMemberValue,
        },
    };

    let both = vec![
        AssignedTopicPartitions {
            topic_id: A1,
            partitions: vec![0, 1],
        },
        AssignedTopicPartitions {
            topic_id: A2,
            partitions: vec![0, 1],
        },
    ];
    let seed = GroupSeed {
        group_epoch: 5,
        target_epoch: 5,
        members: [(
            "m1".to_string(),
            MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "client".into(),
                client_host: "host".into(),
                subscribed_topic_names: vec![],
                subscribed_topic_regex: Some("a.*".into()),
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: None,
            },
        )]
        .into(),
        target_per_member: [(
            "m1".to_string(),
            TargetAssignmentMemberValue {
                topic_partitions: both.clone(),
            },
        )]
        .into(),
        current_per_member: [(
            "m1".to_string(),
            CurrentMemberAssignmentValue {
                member_epoch: 5,
                previous_member_epoch: 4,
                state: MemberAssignmentState::Stable,
                assigned_partitions: both
                    .iter()
                    .map(|topic| CurrentTopicPartitions {
                        topic_id: topic.topic_id,
                        partitions: topic.partitions.clone(),
                        assignment_epochs: None,
                    })
                    .collect(),
                partitions_pending_revocation: vec![],
            },
        )]
        .into(),
        resolved_regexes: [(
            "a.*".to_string(),
            RegularExpressionValue {
                topics: vec!["a1".into(), "a2".into()],
                version: 100,
                timestamp_ms: RegexResolution::TEST_NOW_MS,
            },
        )]
        .into(),
    };
    let metadata = metadata(true);
    let mut state = GroupState::new("g");
    crate::coordinator::unified::actor::seed::apply_seed(&mut state, seed, &metadata.input);
    // Were the resolver asked, it would find nothing.
    let resolver = FixedRegexResolver::new(&[]);

    let step = heartbeat(
        &mut state,
        &metadata,
        &request("m1", 5, None),
        &RegexResolution::with(&resolver),
    );

    check!(step.response.error_code == 0);
    check!(resolver.calls() == 0);
    check!(target_topics(&state, "m1") == HashSet::from([A1, A2]));
    let held: HashSet<Uuid> = state.members["m1"]
        .assigned_partitions
        .keys()
        .copied()
        .collect();
    assert!(held == HashSet::from([A1, A2]));
}

/// A member that leaves takes the resolution of the regex only it used, and
/// the group keeps one that another member still uses.
#[test]
fn a_leaving_member_tombstones_the_resolution_only_it_used() {
    let resolver = FixedRegexResolver::new(&[("a.*", &["a1"])]);
    let mut state = GroupState::new("g");
    let metadata = metadata(false);
    for member in ["m1", "m2"] {
        heartbeat(
            &mut state,
            &metadata,
            &request(member, 0, Some("a.*")),
            &RegexResolution::with(&resolver),
        );
    }

    let first = heartbeat(
        &mut state,
        &metadata,
        &request("m1", -1, None),
        &RegexResolution::with(&resolver),
    );
    check!(first.pending.resolved_regexes.is_empty());
    check!(state.resolved_regex("a.*").is_some());

    let last = heartbeat(
        &mut state,
        &metadata,
        &request("m2", -1, None),
        &RegexResolution::with(&resolver),
    );
    check!(last.pending.resolved_regexes == vec![("a.*".to_owned(), None)]);
    check!(state.resolved_regex("a.*").is_none());
}
