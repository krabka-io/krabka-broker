//! Kafka's request checks on an inbound `Vote`, `BeginQuorumEpoch`,
//! `EndQuorumEpoch` and `FetchSnapshot`: one table per API, each row a request and the whole
//! decoded response it must get.

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
        begin_quorum_epoch_response::{self as bqe_resp, BeginQuorumEpochResponse},
        end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
        end_quorum_epoch_response::{self as eqe_resp, EndQuorumEpochResponse},
        fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
        fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse},
        vote_request::{self as vote_req, VoteRequest},
        vote_response::{self as vote_resp, VoteResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::*;
use crate::kraft::{
    controller::test_support::{build_engine_only, voter_set},
    transport::wire::{FETCH_SNAPSHOT_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION},
};

const METADATA_TOPIC: &str = "__cluster_metadata";

/// The Kafka base64 form of a cluster id that is not the engines' nil id.
fn foreign_cluster_id() -> String {
    URL_SAFE_NO_PAD.encode(Uuid::from_u128(7).as_bytes())
}

/// Delivers one inbound request to `engine` and returns its answer, or `None`
/// when the engine dropped the reply, which closes the connection.
fn deliver(
    engine: &mut Engine,
    inbound: impl FnOnce(bytes::Bytes, oneshot::Sender<bytes::Bytes>) -> Inbound,
    body: bytes::Bytes,
) -> Option<bytes::Bytes> {
    let (reply, mut answer) = oneshot::channel();
    engine.on_inbound(inbound(body, reply));
    answer.try_recv().ok()
}

fn vote(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::Vote { req, reply }
}

fn begin(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::BeginQuorumEpoch { req, reply }
}

fn end(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::EndQuorumEpoch { req, reply }
}

fn fetch_snapshot(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::FetchSnapshot { req, reply }
}

fn encode<M: Encode>(message: &M, version: i16) -> bytes::Bytes {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.freeze()
}

/// Node 1 of voters 1, 2 and 3, following node 2 in epoch 5.
fn follower_of_2_in_epoch_5() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });
    (engine, dir)
}

fn vote_request(edit: impl FnOnce(&mut VoteRequest)) -> VoteRequest {
    let mut request = VoteRequest {
        voter_id: 1,
        topics: vec![vote_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![vote_req::PartitionData {
                partition_index: 0,
                replica_epoch: 6,
                replica_id: 3,
                last_offset_epoch: 0,
                last_offset: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn vote_partition(request: &mut VoteRequest) -> &mut vote_req::PartitionData {
    &mut request.topics[0].partitions[0]
}

/// A response that names the metadata partition and node 2 at epoch 5.
fn vote_answer(error_code: i16) -> VoteResponse {
    VoteResponse {
        topics: vec![vote_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![vote_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id: 2,
                leader_epoch: 5,
                vote_granted: false,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: vec![vote_resp::NodeEndpoint {
            node_id: 2,
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn vote_runs_kafka_request_checks() {
    let top_level = |error_code| VoteResponse {
        error_code,
        ..Default::default()
    };
    let rows: Vec<(&str, VoteRequest, VoteResponse)> = vec![
        (
            "foreign cluster id",
            vote_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            top_level(104),
        ),
        (
            "cluster id that does not parse",
            vote_request(|r| r.cluster_id = Some("not-a-cluster".into())),
            top_level(104),
        ),
        (
            "another topic",
            vote_request(|r| r.topics[0].topic_name = "other".into()),
            top_level(42),
        ),
        (
            "partition 1",
            vote_request(|r| vote_partition(r).partition_index = 1),
            top_level(42),
        ),
        (
            "two partitions",
            vote_request(|r| {
                let second = r.topics[0].partitions[0].clone();
                r.topics[0].partitions.push(second);
            }),
            top_level(42),
        ),
        (
            "two topics",
            vote_request(|r| {
                let second = r.topics[0].clone();
                r.topics.push(second);
            }),
            top_level(42),
        ),
        (
            "negative last offset",
            vote_request(|r| vote_partition(r).last_offset = -1),
            vote_answer(42),
        ),
        (
            "negative last epoch",
            vote_request(|r| vote_partition(r).last_offset_epoch = -1),
            vote_answer(42),
        ),
        (
            "standard vote whose last epoch is the replica epoch",
            vote_request(|r| vote_partition(r).last_offset_epoch = 6),
            vote_answer(42),
        ),
        (
            "pre-vote whose last epoch is above the replica epoch",
            vote_request(|r| {
                vote_partition(r).pre_vote = true;
                vote_partition(r).last_offset_epoch = 7;
            }),
            vote_answer(42),
        ),
        (
            "replica epoch below the local epoch",
            vote_request(|r| {
                vote_partition(r).replica_epoch = 4;
                vote_partition(r).pre_vote = true;
            }),
            vote_answer(74),
        ),
        (
            "negative replica id",
            vote_request(|r| vote_partition(r).replica_id = -1),
            vote_answer(42),
        ),
        (
            "voter key names another replica",
            vote_request(|r| r.voter_id = 2),
            vote_answer(125),
        ),
        (
            "pre-vote while a leader is known",
            vote_request(|r| {
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            vote_answer(0),
        ),
        (
            "pre-vote with no voter key",
            vote_request(|r| {
                r.voter_id = -1;
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            vote_answer(0),
        ),
    ];
    for (label, request, expected) in rows {
        let (mut engine, _dir) = follower_of_2_in_epoch_5();
        let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");
        let response = VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode");
        check!(response == expected, "{label}");
    }
}

/// A granted standard vote moves the voter to the candidate's epoch and names
/// no leader.
#[tokio::test]
async fn vote_grant_names_the_new_epoch_and_no_leader() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let request = vote_request(|r| vote_partition(r).replica_epoch = 1);

    let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");

    check!(
        VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode")
            == VoteResponse {
                topics: vec![vote_resp::TopicData {
                    topic_name: METADATA_TOPIC.into(),
                    partitions: vec![vote_resp::PartitionData {
                        partition_index: 0,
                        leader_id: -1,
                        leader_epoch: 1,
                        vote_granted: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}

/// Kafka's `isValidVoterKey` compares the directory id when both sides have
/// one.
#[tokio::test]
async fn vote_refuses_a_voter_key_with_another_directory_id() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let mut voters = voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
    let mut local = voters.get(NodeId(1)).expect("local voter").clone();
    local.directory_id = Uuid::from_u128(11);
    voters = VoterSet::from_voters(
        voters
            .ids()
            .into_iter()
            .filter(|id| *id != NodeId(1))
            .filter_map(|id| voters.get(id).cloned())
            .chain([local]),
    );
    engine.core = QuorumStateMachine::new(
        NodeId(1),
        crate::kraft::types::QuorumState::bootstrap(Uuid::nil(), voters),
        engine.election_timeout,
    );

    for (label, directory_id, want_error) in [
        ("another directory", Uuid::from_u128(12), 125),
        ("the local directory", Uuid::from_u128(11), 0),
        ("no directory", Uuid::nil(), 0),
    ] {
        let request = vote_request(|r| {
            vote_partition(r).voter_directory_id = WireUuid(*directory_id.as_bytes());
            vote_partition(r).pre_vote = true;
            vote_partition(r).replica_epoch = 0;
        });
        let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");
        let response = VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode");
        check!(
            response.topics[0].partitions[0].error_code == want_error,
            "{label}"
        );
    }
}

#[tokio::test]
async fn a_vote_body_that_does_not_decode_gets_no_answer() {
    let (mut engine, _dir) = follower_of_2_in_epoch_5();
    let body = encode(&vote_request(|_| {}), VOTE_VERSION);
    check!(deliver(&mut engine, vote, body.slice(..body.len() - 1)).is_none());
}

fn begin_request(edit: impl FnOnce(&mut BeginQuorumEpochRequest)) -> BeginQuorumEpochRequest {
    let mut request = BeginQuorumEpochRequest {
        voter_id: -1,
        topics: vec![bqe_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![bqe_req::PartitionData {
                partition_index: 0,
                leader_id: 3,
                leader_epoch: 6,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn begin_answer(error_code: i16, leader_id: i32, leader_epoch: i32) -> BeginQuorumEpochResponse {
    BeginQuorumEpochResponse {
        topics: vec![bqe_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![bqe_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id,
                leader_epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: vec![bqe_resp::NodeEndpoint {
            node_id: leader_id,
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn begin_quorum_epoch_runs_kafka_request_checks() {
    let top_level = |error_code| BeginQuorumEpochResponse {
        error_code,
        ..Default::default()
    };
    let rows: Vec<(&str, BeginQuorumEpochRequest, BeginQuorumEpochResponse)> = vec![
        (
            "foreign cluster id",
            begin_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            top_level(104),
        ),
        (
            "another topic",
            begin_request(|r| r.topics[0].topic_name = "other".into()),
            top_level(42),
        ),
        (
            "two partitions",
            begin_request(|r| {
                let second = r.topics[0].partitions[0].clone();
                r.topics[0].partitions.push(second);
            }),
            top_level(42),
        ),
        (
            "leader epoch below the local epoch",
            begin_request(|r| r.topics[0].partitions[0].leader_epoch = 4),
            begin_answer(74, 2, 5),
        ),
        (
            "negative leader id",
            begin_request(|r| r.topics[0].partitions[0].leader_id = -1),
            begin_answer(42, 2, 5),
        ),
        (
            "voter key names another replica, after the transition",
            begin_request(|r| r.voter_id = 2),
            begin_answer(125, 3, 6),
        ),
        ("a new leader", begin_request(|_| {}), begin_answer(0, 3, 6)),
    ];
    for (label, request, expected) in rows {
        let (mut engine, _dir) = follower_of_2_in_epoch_5();
        let body =
            deliver(&mut engine, begin, encode(&request, QUORUM_EPOCH_VERSION)).expect("an answer");
        let response =
            BeginQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
        check!(response == expected, "{label}");
    }
}

#[tokio::test]
async fn a_begin_quorum_epoch_body_that_does_not_decode_gets_no_answer() {
    let (mut engine, _dir) = follower_of_2_in_epoch_5();
    check!(deliver(&mut engine, begin, bytes::Bytes::from_static(&[0xff])).is_none());
}

fn end_request(edit: impl FnOnce(&mut EndQuorumEpochRequest)) -> EndQuorumEpochRequest {
    let mut request = EndQuorumEpochRequest {
        topics: vec![eqe_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![eqe_req::PartitionData {
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 5,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn candidates(ids: &[i32]) -> Vec<eqe_req::ReplicaInfo> {
    ids.iter()
        .map(|&candidate_id| eqe_req::ReplicaInfo {
            candidate_id,
            ..Default::default()
        })
        .collect()
}

#[tokio::test]
async fn end_quorum_epoch_runs_kafka_request_checks() {
    let top_level = |error_code| EndQuorumEpochResponse {
        error_code,
        ..Default::default()
    };
    let answer = |error_code, leader_id: i32| EndQuorumEpochResponse {
        topics: vec![eqe_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![eqe_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id,
                leader_epoch: 5,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: (leader_id >= 0)
            .then(|| eqe_resp::NodeEndpoint {
                node_id: leader_id,
                host: "127.0.0.1".into(),
                port: 9_093,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    };
    // (label, request, response, the role name after the request)
    let rows: Vec<(&str, EndQuorumEpochRequest, EndQuorumEpochResponse, &str)> = vec![
        (
            "foreign cluster id",
            end_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            top_level(104),
            "Follower",
        ),
        (
            "partition 1",
            end_request(|r| r.topics[0].partitions[0].partition_index = 1),
            top_level(42),
            "Follower",
        ),
        (
            "leader epoch below the local epoch",
            end_request(|r| r.topics[0].partitions[0].leader_epoch = 4),
            answer(74, 2),
            "Follower",
        ),
        (
            "negative leader id",
            end_request(|r| r.topics[0].partitions[0].leader_id = -1),
            answer(42, 2),
            "Follower",
        ),
        (
            "first preferred candidate elects at once",
            end_request(|r| r.topics[0].partitions[0].preferred_candidates = candidates(&[1, 3])),
            answer(0, -1),
            "Prospective",
        ),
        (
            "no preferred candidates elects at once",
            end_request(|_| {}),
            answer(0, -1),
            "Prospective",
        ),
        (
            "second preferred candidate waits",
            end_request(|r| r.topics[0].partitions[0].preferred_candidates = candidates(&[3, 1])),
            answer(0, -1),
            "Unattached",
        ),
        (
            "a replica the list does not name waits",
            end_request(|r| r.topics[0].partitions[0].preferred_candidates = candidates(&[3])),
            answer(0, -1),
            "Unattached",
        ),
    ];
    for (label, request, expected, role) in rows {
        let (mut engine, _dir) = follower_of_2_in_epoch_5();
        let body =
            deliver(&mut engine, end, encode(&request, QUORUM_EPOCH_VERSION)).expect("an answer");
        let response =
            EndQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
        check!(response == expected, "{label}");
        check!(engine.core.role().name() == role, "{label}");
    }
}

/// A resigning leader names the other voters most caught up first.
#[tokio::test]
async fn preferred_candidates_follow_replication_progress() {
    let (mut engine, _dir) =
        build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);
    engine.replica_fetch_offsets.insert(NodeId(2), 5);
    engine.replica_fetch_offsets.insert(NodeId(3), 9);

    check!(
        engine.preferred_candidates()
            == vec![
                (NodeId(3), Uuid::nil()),
                (NodeId(2), Uuid::nil()),
                (NodeId(4), Uuid::nil()),
            ]
    );
}

fn fetch_snapshot_request(
    edit: impl FnOnce(&mut FetchSnapshotRequest, &mut fs_req::PartitionSnapshot),
) -> FetchSnapshotRequest {
    let mut partition = fs_req::PartitionSnapshot {
        partition: 0,
        current_leader_epoch: 1,
        snapshot_id: fs_req::SnapshotId {
            end_offset: 10,
            epoch: 1,
            ..Default::default()
        },
        position: 0,
        ..Default::default()
    };
    let mut request = FetchSnapshotRequest {
        replica_id: 2,
        max_bytes: 4,
        ..Default::default()
    };
    edit(&mut request, &mut partition);
    if request.topics.is_empty() {
        request.topics = vec![fs_req::TopicSnapshot {
            name: METADATA_TOPIC.into(),
            partitions: vec![partition],
            ..Default::default()
        }];
    }
    request
}

/// An answer that names `topic` and `index`, with the leader view of `leader`
/// (a leader id and epoch) when it is `Some`, and node 1's endpoint.
fn fetch_snapshot_answer(
    (topic, index): (&str, i32),
    error_code: i16,
    leader: Option<(i32, i32)>,
    chunk: Option<(i64, i64, &'static [u8])>,
) -> FetchSnapshotResponse {
    let mut partition = fs_resp::PartitionSnapshot {
        index,
        error_code,
        ..Default::default()
    };
    if let Some((leader_id, leader_epoch)) = leader {
        partition.current_leader = fs_resp::LeaderIdAndEpoch {
            leader_id,
            leader_epoch,
            ..Default::default()
        };
    }
    if let Some((size, position, bytes)) = chunk {
        partition.snapshot_id = fs_resp::SnapshotId {
            end_offset: 10,
            epoch: 1,
            ..Default::default()
        };
        partition.size = size;
        partition.position = position;
        partition.unaligned_records =
            krabka_protocol::records::RecordsPayload::Raw(bytes::Bytes::from_static(bytes));
    }
    FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: topic.into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        node_endpoints: vec![fs_resp::NodeEndpoint {
            node_id: leader.map_or(1, |(leader_id, _)| leader_id),
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Node 1, the only voter, leading epoch 1, with checkpoints (10, 1) and the
/// bootstrap id (0, 0) on disk.
fn single_voter_leader_with_checkpoints() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    engine.on_event(Event::ElectionTimeout);
    assert2::assert!(
        (
            engine.core.role().is_leader(),
            engine.core.quorum_state().leader_epoch
        ) == (true, 1)
    );
    let checkpoints = checkpoint_dir(&engine.data_dir);
    checkpoint::write_checkpoint(&checkpoints, 10, 1, b"0123456789").expect("checkpoint 10/1");
    checkpoint::write_checkpoint(&checkpoints, 0, 0, b"bootstrap").expect("checkpoint 0/0");
    (engine, dir)
}

#[tokio::test]
async fn fetch_snapshot_runs_kafka_request_checks() {
    let top_level = |error_code| FetchSnapshotResponse {
        error_code,
        ..Default::default()
    };
    let metadata = (METADATA_TOPIC, 0);
    let leader = Some((1, 1));
    let rows: Vec<(&str, FetchSnapshotRequest, FetchSnapshotResponse)> = vec![
        (
            "foreign cluster id",
            fetch_snapshot_request(|r, _| r.cluster_id = Some(foreign_cluster_id())),
            top_level(104),
        ),
        (
            "two partitions",
            fetch_snapshot_request(|r, p| {
                r.topics = vec![fs_req::TopicSnapshot {
                    name: METADATA_TOPIC.into(),
                    partitions: vec![p.clone(), p.clone()],
                    ..Default::default()
                }];
            }),
            top_level(42),
        ),
        (
            "another topic",
            fetch_snapshot_request(|r, p| {
                r.topics = vec![fs_req::TopicSnapshot {
                    name: "other".into(),
                    partitions: vec![p.clone()],
                    ..Default::default()
                }];
            }),
            fetch_snapshot_answer(("other", 0), 3, None, None),
        ),
        (
            "partition 1",
            fetch_snapshot_request(|_, p| p.partition = 1),
            fetch_snapshot_answer((METADATA_TOPIC, 1), 3, None, None),
        ),
        (
            "leader epoch below the local epoch",
            fetch_snapshot_request(|_, p| p.current_leader_epoch = 0),
            fetch_snapshot_answer(metadata, 74, leader, None),
        ),
        (
            "leader epoch above the local epoch",
            fetch_snapshot_request(|_, p| p.current_leader_epoch = 2),
            fetch_snapshot_answer(metadata, 75, leader, None),
        ),
        (
            "unknown snapshot id",
            fetch_snapshot_request(|_, p| p.snapshot_id.end_offset = 11),
            fetch_snapshot_answer(metadata, 98, leader, None),
        ),
        (
            "bootstrap snapshot id",
            fetch_snapshot_request(|_, p| {
                p.snapshot_id.end_offset = 0;
                p.snapshot_id.epoch = 0;
            }),
            fetch_snapshot_answer(metadata, 98, leader, None),
        ),
        (
            "negative position",
            fetch_snapshot_request(|_, p| p.position = -1),
            fetch_snapshot_answer(metadata, 99, leader, None),
        ),
        (
            "position at the size",
            fetch_snapshot_request(|_, p| p.position = 10),
            fetch_snapshot_answer(metadata, 99, leader, None),
        ),
        (
            "last byte",
            fetch_snapshot_request(|_, p| p.position = 9),
            fetch_snapshot_answer(metadata, 0, leader, Some((10, 9, b"9"))),
        ),
        (
            "first chunk",
            fetch_snapshot_request(|_, _| {}),
            fetch_snapshot_answer(metadata, 0, leader, Some((10, 0, b"0123"))),
        ),
    ];
    for (label, request, expected) in rows {
        let (mut engine, _dir) = single_voter_leader_with_checkpoints();
        let body = deliver(
            &mut engine,
            fetch_snapshot,
            encode(&request, FETCH_SNAPSHOT_VERSION),
        )
        .expect("an answer");
        check!(body == encode(&expected, FETCH_SNAPSHOT_VERSION), "{label}");
    }
}

/// A follower refuses `FetchSnapshot` at its own epoch and names the leader.
#[tokio::test]
async fn fetch_snapshot_to_a_follower_names_the_leader() {
    let (mut engine, _dir) = follower_of_2_in_epoch_5();
    let request = fetch_snapshot_request(|_, p| p.current_leader_epoch = 5);

    let body = deliver(
        &mut engine,
        fetch_snapshot,
        encode(&request, FETCH_SNAPSHOT_VERSION),
    )
    .expect("an answer");

    check!(
        body == encode(
            &fetch_snapshot_answer((METADATA_TOPIC, 0), 6, Some((2, 5)), None),
            FETCH_SNAPSHOT_VERSION
        )
    );
}
