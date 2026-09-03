//! Unit tests for `build_topic_responses`, and for the `Nodes` block that is
//! built from the same `QuorumState` snapshot: the metadata-partition row,
//! the voter and directory-id sentinels a follower reports, the
//! `INVALID_TOPIC_EXCEPTION` rows, and the `i32` saturation guards.

use std::collections::BTreeMap;

use assert2::assert;
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        describe_quorum_request::{PartitionData as ReqPartitionData, TopicData as ReqTopicData},
        describe_quorum_response::Listener,
    },
};

use super::*;
use crate::handlers::describe_quorum::nodes::build_nodes;

/// A fully specified expected voter row, with no struct-update syntax.
fn expected_voter(replica_id: i32, log_end_offset: i64) -> ReplicaState {
    ReplicaState {
        replica_id,
        replica_directory_id: Uuid::ZERO,
        log_end_offset,
        last_fetch_timestamp: -1,
        last_caught_up_timestamp: -1,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

fn req_for(topic: &str, partition: i32) -> Vec<ReqTopicData> {
    vec![ReqTopicData {
        topic_name: topic.into(),
        partitions: vec![ReqPartitionData {
            partition_index: partition,
            ..Default::default()
        }],
        ..Default::default()
    }]
}

/// Helper that builds a `QuorumState` for a test.
fn quorum_state(
    leader: Option<u64>,
    term: u64,
    applied: u64,
    voters: &[u64],
    matched: &[(u64, u64)],
) -> QuorumState {
    QuorumState {
        current_term: term,
        last_applied_index: applied,
        current_leader: leader.map(krabka_raft::NodeId),
        voters: voters.iter().copied().map(krabka_raft::NodeId).collect(),
        voter_nodes: BTreeMap::new(),
        per_voter_matched_index: matched
            .iter()
            .map(|&(v, m)| (krabka_raft::NodeId(v), m))
            .collect::<BTreeMap<_, _>>(),
        per_replica_last_fetch_ms: BTreeMap::new(),
        per_replica_last_caught_up_ms: BTreeMap::new(),
        observer_directory_ids: BTreeMap::new(),
        is_leader: true,
    }
}

#[test]
fn metadata_topic_partition_zero_returns_voter_list_with_leader() {
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(
        Some(2),
        /*term=*/ 7,
        /*applied=*/ 42,
        &[1, 2, 3],
        &[(1, 40), (2, 42), (3, 38)],
    );
    let out = build_topic_responses(&req, &q);
    let expected = vec![TopicData {
        topic_name: CLUSTER_METADATA_TOPIC.to_string(),
        partitions: vec![PartitionData {
            partition_index: 0,
            error_code: codes::NONE,
            error_message: None,
            leader_id: 2,
            // current_term surfaces as leader_epoch.
            leader_epoch: 7,
            // last_applied_index surfaces as HW.
            high_watermark: 42,
            // Each voter's `log_end_offset` comes from the per-voter map.
            current_voters: vec![
                expected_voter(1, 40),
                expected_voter(2, 42),
                expected_voter(3, 38),
            ],
            // No observers in Krabka yet.
            observers: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }];
    assert!(out == expected);
}

#[test]
fn voters_missing_from_replication_map_get_unknown_sentinel() {
    // Follower case: replication map is empty (only the leader knows
    // peers' progress). Every voter's log_end_offset should be -1.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(
        Some(1),
        /*term=*/ 3,
        /*applied=*/ 10,
        &[1, 2, 3],
        &[],
    );
    let out = build_topic_responses(&req, &q);
    let pd = &out[0].partitions[0];
    for v in &pd.current_voters {
        assert!(
            v.log_end_offset == UNKNOWN_LOG_END_OFFSET,
            "follower replication map empty → voter LEOs all -1"
        );
    }
}

#[test]
fn voter_with_partial_replication_map_uses_per_voter_value_where_available() {
    // Mixed: leader knows progress for voter 1 only.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(Some(1), 4, 50, &[1, 2, 3], &[(1, 50)]);
    let out = build_topic_responses(&req, &q);
    let pd = &out[0].partitions[0];
    let by_id: BTreeMap<i32, i64> = pd
        .current_voters
        .iter()
        .map(|v| (v.replica_id, v.log_end_offset))
        .collect();
    // Voter 1 gets its matched index; voters missing from the
    // replication map fall back to the -1 sentinel.
    let expected: BTreeMap<i32, i64> = [
        (1, 50),
        (2, UNKNOWN_LOG_END_OFFSET),
        (3, UNKNOWN_LOG_END_OFFSET),
    ]
    .into_iter()
    .collect();
    assert!(by_id == expected);
}

#[test]
fn unknown_topic_returns_invalid_topic_exception() {
    let req = req_for("__consumer_offsets", 0);
    let q = quorum_state(Some(1), 1, 0, &[1], &[]);
    let out = build_topic_responses(&req, &q);
    let pd = &out[0].partitions[0];
    let expected = PartitionData {
        partition_index: 0,
        error_code: codes::INVALID_TOPIC_EXCEPTION,
        // The message names the only supported topic.
        error_message: Some("DescribeQuorum supports only `__cluster_metadata`".to_string()),
        leader_id: -1,
        leader_epoch: -1,
        high_watermark: -1,
        current_voters: Vec::new(),
        observers: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(*pd == expected);
}

#[test]
fn metadata_topic_partition_nonzero_returns_invalid_topic_exception() {
    // KRaft cluster-metadata topic has exactly one partition (id 0).
    let req = req_for(CLUSTER_METADATA_TOPIC, 7);
    let q = quorum_state(Some(1), 1, 0, &[1], &[]);
    let out = build_topic_responses(&req, &q);
    let pd = &out[0].partitions[0];
    assert!(
        pd.error_code == codes::INVALID_TOPIC_EXCEPTION,
        "partition != 0 is not the metadata partition; reject"
    );
    assert!(pd.partition_index == 7, "echo the requested index back");
}

#[test]
fn unknown_leader_emits_minus_one() {
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(/*leader=*/ None, 0, 0, &[1, 2], &[]);
    let out = build_topic_responses(&req, &q);
    let pd = &out[0].partitions[0];
    assert!(pd.leader_id == -1, "leader unknown surfaces as -1 sentinel");
    // Voter list still populated even when leader is unknown.
    assert!(pd.current_voters.len() == 2);
}

#[test]
fn empty_request_returns_no_topics() {
    let q = quorum_state(Some(1), 1, 0, &[1], &[]);
    let out = build_topic_responses(&[], &q);
    assert!(out.is_empty());
}

#[test]
fn multiple_topics_each_get_their_own_row() {
    let req = vec![
        ReqTopicData {
            topic_name: CLUSTER_METADATA_TOPIC.into(),
            partitions: vec![ReqPartitionData {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        },
        ReqTopicData {
            topic_name: "other".into(),
            partitions: vec![ReqPartitionData {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        },
    ];
    let q = quorum_state(Some(1), 1, 0, &[1], &[]);
    let out = build_topic_responses(&req, &q);
    let codes_by_topic: Vec<(&str, i16)> = out
        .iter()
        .map(|t| (t.topic_name.as_str(), t.partitions[0].error_code))
        .collect();
    assert!(
        codes_by_topic
            == vec![
                (CLUSTER_METADATA_TOPIC, codes::NONE),
                ("other", codes::INVALID_TOPIC_EXCEPTION),
            ]
    );
}

#[test]
fn v2_replica_directory_id_and_nodes_come_from_voter_nodes() {
    use krabka_metadata::VoterEndpoint;
    use krabka_raft::Node;

    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let dir1 = uuid::Uuid::from_u128(1);
    let dir2 = uuid::Uuid::from_u128(2);
    let mut voter_nodes = BTreeMap::new();
    voter_nodes.insert(
        krabka_audit::NodeId(1u64),
        Node {
            directory_id: dir1,
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "10.0.0.1".into(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        },
    );
    voter_nodes.insert(
        krabka_audit::NodeId(2u64),
        Node {
            directory_id: dir2,
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "10.0.0.2".into(),
                port: 9094,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        },
    );
    let q = QuorumState {
        current_term: 1,
        last_applied_index: 5,
        current_leader: Some(krabka_audit::NodeId(1)),
        voters: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
        voter_nodes,
        per_voter_matched_index: BTreeMap::new(),
        per_replica_last_fetch_ms: BTreeMap::new(),
        per_replica_last_caught_up_ms: BTreeMap::new(),
        observer_directory_ids: BTreeMap::new(),
        is_leader: true,
    };

    // Per-voter replica_directory_id is sourced from voter_nodes.
    let topics = build_topic_responses(&req, &q);
    let voters = &topics[0].partitions[0].current_voters;
    let dir_by_id: BTreeMap<i32, Uuid> = voters
        .iter()
        .map(|v| (v.replica_id, v.replica_directory_id))
        .collect();
    assert!(dir_by_id[&1] == Uuid(*dir1.as_bytes()));
    assert!(dir_by_id[&2] == Uuid(*dir2.as_bytes()));

    // Top-level v2 Nodes block names each voter with its listeners.
    let nodes = build_nodes(&q);
    assert!(nodes.len() == 2);
    let first_voter = nodes.iter().find(|n| n.node_id == 1).unwrap();
    let expected_listener = Listener {
        name: "CONTROLLER".to_string(),
        host: "10.0.0.1".to_string(),
        port: 9093,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(first_voter.listeners == vec![expected_listener]);
}

#[test]
fn unknown_voter_directory_id_falls_back_to_zero() {
    // Follower with an empty voter_nodes map (only the leader fully
    // knows membership endpoints) → each replica_directory_id is ZERO
    // and the Nodes block is empty.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(Some(1), 1, 0, &[1, 2], &[]);
    let topics = build_topic_responses(&req, &q);
    for v in &topics[0].partitions[0].current_voters {
        assert!(v.replica_directory_id == Uuid::ZERO);
    }
    assert!(build_nodes(&q).is_empty());
}

#[test]
fn leader_id_above_i32_max_falls_back_to_minus_one() {
    // A raft NodeId is u64; the wire replica/leader id is i32. A leader
    // node id beyond i32::MAX must surface as the -1 "unknown" sentinel
    // (via `try_from(..).unwrap_or(-1)`), never wrap into a positive id.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let huge = u64::from(u32::MAX) + 1; // > i32::MAX, try_from fails
    let q = quorum_state(Some(huge), 1, 0, &[1], &[]);
    let out = build_topic_responses(&req, &q);
    assert!(
        out[0].partitions[0].leader_id == -1,
        "leader node id > i32::MAX must fall back to -1, not a positive id"
    );
}

#[test]
fn voter_replica_id_above_i32_max_falls_back_to_minus_one() {
    // Same guard on the per-voter replica_id: a voter node id beyond
    // i32::MAX surfaces as -1, not a wrapped positive value.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let huge = u64::from(u32::MAX) + 1; // > i32::MAX
    let q = quorum_state(Some(1), 1, 0, &[huge], &[]);
    let out = build_topic_responses(&req, &q);
    let voters = &out[0].partitions[0].current_voters;
    assert!(voters.len() == 1);
    assert!(
        voters[0].replica_id == -1,
        "voter node id > i32::MAX must fall back to -1, not a positive id"
    );
}

#[test]
fn current_term_above_i32_max_saturates() {
    // Defensive: openraft's term is u64; KRaft wire is i32. A term
    // beyond i32::MAX (huge cluster history) saturates so we don't
    // wrap silently into a negative epoch.
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let q = quorum_state(Some(1), u64::MAX, 0, &[1], &[]);
    let out = build_topic_responses(&req, &q);
    assert!(out[0].partitions[0].leader_epoch == i32::MAX);
}

#[test]
fn voter_and_observer_timestamps_and_directory_ids_surfaced() {
    let req = req_for(CLUSTER_METADATA_TOPIC, 0);
    let mut q = quorum_state(Some(1), 1, 10, &[1], &[(1, 10), (2, 9)]);
    let obs_uuid = uuid::Uuid::new_v4();
    q.per_replica_last_fetch_ms
        .insert(krabka_raft::NodeId(1), 1700000001000);
    q.per_replica_last_caught_up_ms
        .insert(krabka_raft::NodeId(1), 1700000001000);
    q.per_replica_last_fetch_ms
        .insert(krabka_raft::NodeId(2), 1700000002000);
    q.per_replica_last_caught_up_ms
        .insert(krabka_raft::NodeId(2), 1700000001500);
    q.observer_directory_ids
        .insert(krabka_raft::NodeId(2), obs_uuid);

    let out = build_topic_responses(&req, &q);
    let partition = &out[0].partitions[0];
    assert!(partition.current_voters.len() == 1);
    assert!(partition.current_voters[0].last_fetch_timestamp == 1700000001000);
    assert!(partition.current_voters[0].last_caught_up_timestamp == 1700000001000);

    assert!(partition.observers.len() == 1);
    assert!(partition.observers[0].replica_id == 2);
    assert!(partition.observers[0].last_fetch_timestamp == 1700000002000);
    assert!(partition.observers[0].last_caught_up_timestamp == 1700000001500);
    assert!(partition.observers[0].replica_directory_id == Uuid(*obs_uuid.as_bytes()));
}
