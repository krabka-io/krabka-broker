//! Kafka's request checks on an inbound `Vote`, `BeginQuorumEpoch`,
//! `EndQuorumEpoch` and `FetchSnapshot`: one table per API, each row a request and the whole
//! decoded response it must get.

use {
    super::*,
    crate::{
        kraft::{
            controller::test_support::{EngineInDir, build_engine_only, voter_set},
            transport::wire::{FETCH_SNAPSHOT_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION},
            types::{QuorumState, SimInstant},
        },
        test_support::encode,
    },
    assert2::check,
    krabka_protocol::{
        Decode,
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
        records::RecordsPayload,
    },
};

const METADATA_TOPIC: &str = "__cluster_metadata";

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
    Inbound::Vote {
        req,
        version: VOTE_VERSION,
        reply,
    }
}

fn begin(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::BeginQuorumEpoch {
        req,
        version: QUORUM_EPOCH_VERSION,
        reply,
    }
}

fn end(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::EndQuorumEpoch {
        req,
        version: QUORUM_EPOCH_VERSION,
        reply,
    }
}

fn fetch_snapshot(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::FetchSnapshot {
        req,
        version: FETCH_SNAPSHOT_VERSION,
        reply,
    }
}

/// Node 1 of voters 1, 2 and 3, following node 2 in epoch 5.
fn follower_of_2_in_epoch_5() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().call();
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });
    EngineInDir { engine, dir }
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

fn engine_with_local_directory() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().call();
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
        QuorumState::bootstrap(Uuid::nil(), voters),
        engine.timeouts,
    );
    EngineInDir { engine, dir }
}

/// A granted standard vote moves the voter to the candidate's epoch and names
/// no leader.
#[tokio::test]
async fn vote_grant_names_the_new_epoch_and_no_leader() {
    let (mut engine, _dir) = build_engine_only().call();
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
#[yare::parameterized(
    another_directory = { Uuid::from_u128(12), 125 },
    local_directory = { Uuid::from_u128(11), 0 },
    no_directory = { Uuid::nil(), 0 },
)]
#[test_macro(tokio::test)]
async fn vote_refuses_a_voter_key_with_another_directory_id(directory_id: Uuid, want_error: i16) {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = engine_with_local_directory();

    let request = vote_request(|r| {
        vote_partition(r).voter_directory_id = WireUuid(*directory_id.as_bytes());
        vote_partition(r).pre_vote = true;
        vote_partition(r).replica_epoch = 0;
    });
    let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");
    let response = VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode");
    check!(response.topics[0].partitions[0].error_code == want_error);
}

#[tokio::test]
async fn a_vote_body_that_does_not_decode_gets_no_answer() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
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
async fn a_begin_quorum_epoch_body_that_does_not_decode_gets_no_answer() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
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

/// The topic and partition index that an answer names.
#[derive(Clone, Copy)]
struct AnsweredPartition<'a> {
    topic: &'a str,
    index: i32,
}

/// The leader id and epoch that an answer names.
#[derive(Clone, Copy)]
struct AnsweredLeader {
    id: i32,
    epoch: i32,
}

/// The chunk that an answer carries: the snapshot's size, the chunk's
/// position and its bytes.
#[derive(Clone, Copy)]
struct AnsweredChunk {
    size: i64,
    position: i64,
    bytes: &'static [u8],
}

/// An answer that names `partition`, with the leader view of `leader` when it
/// is `Some`, and node 1's endpoint.
fn fetch_snapshot_answer(
    AnsweredPartition { topic, index }: AnsweredPartition<'_>,
    error_code: i16,
    leader: Option<AnsweredLeader>,
    chunk: Option<AnsweredChunk>,
) -> FetchSnapshotResponse {
    let mut partition = fs_resp::PartitionSnapshot {
        index,
        error_code,
        ..Default::default()
    };
    if let Some(AnsweredLeader {
        id: leader_id,
        epoch: leader_epoch,
    }) = leader
    {
        partition.current_leader = fs_resp::LeaderIdAndEpoch {
            leader_id,
            leader_epoch,
            ..Default::default()
        };
    }
    if let Some(AnsweredChunk {
        size,
        position,
        bytes,
    }) = chunk
    {
        partition.snapshot_id = fs_resp::SnapshotId {
            end_offset: 10,
            epoch: 1,
            ..Default::default()
        };
        partition.size = size;
        partition.position = position;
        partition.unaligned_records = RecordsPayload::Raw(bytes::Bytes::from_static(bytes));
    }
    FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: topic.into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        node_endpoints: vec![fs_resp::NodeEndpoint {
            node_id: leader.map_or(1, |leader| leader.id),
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Node 1, the only voter, leading epoch 1, with checkpoints (10, 1) and the
/// bootstrap id (0, 0) on disk.
fn single_voter_leader_with_checkpoints() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    engine.on_event(Event::ElectionTimeout);
    assert2::assert!(
        (
            engine.core.role().is_leader(),
            engine.core.quorum_state().leader_epoch
        ) == (true, 1)
    );
    let checkpoints = engine.data_dir.clone();
    checkpoint::write_checkpoint(&checkpoints, 10, 1, b"0123456789").expect("checkpoint 10/1");
    checkpoint::write_checkpoint(&checkpoints, 0, 0, b"bootstrap").expect("checkpoint 0/0");
    EngineInDir { engine, dir }
}

/// A follower refuses `FetchSnapshot` at its own epoch and names the leader.
#[tokio::test]
async fn fetch_snapshot_to_a_follower_names_the_leader() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
    let request = fetch_snapshot_request(|_, p| p.current_leader_epoch = 5);

    let body = deliver(
        &mut engine,
        fetch_snapshot,
        encode(&request, FETCH_SNAPSHOT_VERSION),
    )
    .expect("an answer");

    check!(
        body == encode(
            &fetch_snapshot_answer(
                AnsweredPartition {
                    topic: METADATA_TOPIC,
                    index: 0,
                },
                6,
                Some(AnsweredLeader { id: 2, epoch: 5 }),
                None,
            ),
            FETCH_SNAPSHOT_VERSION
        )
    );
}

/// A leader that only the adjacent voter set of an uncommitted KIP-853 change
/// names is followed, and its endpoint still goes into `NodeEndpoints`.
#[tokio::test]
async fn a_leader_from_the_adjacent_voter_set_keeps_its_endpoint() {
    let (mut engine, _dir) = build_engine_only().call();
    let _ = engine
        .core
        .apply_voter_set(voter_set(&[NodeId(1), NodeId(2)]), SimInstant(0));

    let body = deliver(
        &mut engine,
        begin,
        encode(&begin_request(|_| {}), QUORUM_EPOCH_VERSION),
    )
    .expect("an answer");

    check!(
        BeginQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode")
            == begin_answer(0, 3, 6)
    );
}

#[tokio::test]
async fn end_quorum_epoch_matches_candidate_directory_id() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = engine_with_local_directory();
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });

    // Candidate directory matches local directory -> ranks 0, elects at once (kills `!=`)
    let req = end_request(|r| {
        r.topics[0].partitions[0].preferred_candidates = vec![eqe_req::ReplicaInfo {
            candidate_id: 1,
            candidate_directory_id: WireUuid(*Uuid::from_u128(11).as_bytes()),
            ..Default::default()
        }];
    });
    let body = deliver(&mut engine, end, encode(&req, QUORUM_EPOCH_VERSION)).expect("an answer");
    let response =
        EndQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
    check!(response.error_code == 0);
    check!(engine.core.role().name() == "Prospective");

    // Reset back to follower
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });

    // Candidate directory is nil WireUuid -> ranks 0, elects at once (kills `&&`)
    let req2 = end_request(|r| {
        r.topics[0].partitions[0].preferred_candidates = vec![eqe_req::ReplicaInfo {
            candidate_id: 1,
            candidate_directory_id: WireUuid(*Uuid::nil().as_bytes()),
            ..Default::default()
        }];
    });
    let body2 = deliver(&mut engine, end, encode(&req2, QUORUM_EPOCH_VERSION)).expect("an answer");
    let response2 =
        EndQuorumEpochResponse::decode(&mut &body2[..], QUORUM_EPOCH_VERSION).expect("decode");
    check!(response2.error_code == 0);
    check!(engine.core.role().name() == "Prospective");
}

use rstest_bdd_macros::{given, then, when};

#[derive(Default)]
struct VoteAdmission {
    leader: Option<EngineInDir>,
    request: Option<VoteRequest>,
    body: Option<bytes::Bytes>,
}
#[rstest::fixture]
fn vote_admission() -> VoteAdmission {
    // Each request owns fresh quorum state and its on-disk directory.
    VoteAdmission::default()
}
#[given("a Vote request with {request}")]
fn vote_input(vote_admission: &mut VoteAdmission, request: String) {
    vote_admission.leader = Some(follower_of_2_in_epoch_5());
    let input: VoteInput = serde_json::from_str(&request).expect("native Vote request fields");
    vote_admission.request = Some(input.native());
}
#[when("the quorum controller handles the Vote request")]
fn vote_operation(vote_admission: &mut VoteAdmission) {
    let request = vote_admission.request.as_ref().expect("request prepared");
    let engine = &mut vote_admission
        .leader
        .as_mut()
        .expect("quorum configured")
        .engine;
    vote_admission.body =
        Some(deliver(engine, vote, encode(request, VOTE_VERSION)).expect("controller answered"));
}
#[then("the Vote response matches every expected Kafka field {expected}")]
fn vote_response(vote_admission: &VoteAdmission, expected: String) {
    let expected: VoteExpected =
        serde_json::from_str(&expected).expect("independent complete expected response");
    let body = vote_admission.body.as_ref().expect("request handled");
    let response =
        VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("complete Kafka response");
    check!(response == expected.native());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/kraft/controller/tests_quorum_requests.feature",
    name = "Vote validates the incoming request and returns the full Kafka response"
)]
#[tokio::test]
async fn vote_runs_kafka_request_checks(
    vote_admission: VoteAdmission,
    request: String,
    expected: String,
) {
    let _ = (vote_admission, request, expected);
}

#[derive(Default)]
struct BeginQuorumEpochAdmission {
    leader: Option<EngineInDir>,
    request: Option<BeginQuorumEpochRequest>,
    body: Option<bytes::Bytes>,
}
#[rstest::fixture]
fn begin_quorum_epoch_admission() -> BeginQuorumEpochAdmission {
    // Each request owns fresh quorum state and its on-disk directory.
    BeginQuorumEpochAdmission::default()
}
#[given("a BeginQuorumEpoch request with {request}")]
fn begin_quorum_epoch_input(
    begin_quorum_epoch_admission: &mut BeginQuorumEpochAdmission,
    request: String,
) {
    begin_quorum_epoch_admission.leader = Some(follower_of_2_in_epoch_5());
    let input: BeginInput =
        serde_json::from_str(&request).expect("native BeginQuorumEpoch request fields");
    begin_quorum_epoch_admission.request = Some(input.native());
}
#[when("the quorum controller handles the BeginQuorumEpoch request")]
fn begin_quorum_epoch_operation(begin_quorum_epoch_admission: &mut BeginQuorumEpochAdmission) {
    let request = begin_quorum_epoch_admission
        .request
        .as_ref()
        .expect("request prepared");
    let engine = &mut begin_quorum_epoch_admission
        .leader
        .as_mut()
        .expect("quorum configured")
        .engine;
    begin_quorum_epoch_admission.body = Some(
        deliver(engine, begin, encode(request, QUORUM_EPOCH_VERSION)).expect("controller answered"),
    );
}
#[then("the BeginQuorumEpoch response matches every expected Kafka field {expected}")]
fn begin_quorum_epoch_response(
    begin_quorum_epoch_admission: &BeginQuorumEpochAdmission,
    expected: String,
) {
    let expected: EpochExpected =
        serde_json::from_str(&expected).expect("independent complete expected response");
    let body = begin_quorum_epoch_admission
        .body
        .as_ref()
        .expect("request handled");
    let response = BeginQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION)
        .expect("complete Kafka response");
    check!(response == expected.begin());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/kraft/controller/tests_quorum_requests.feature",
    name = "BeginQuorumEpoch validates the incoming request and returns the full Kafka response"
)]
#[tokio::test]
async fn begin_quorum_epoch_runs_kafka_request_checks(
    begin_quorum_epoch_admission: BeginQuorumEpochAdmission,
    request: String,
    expected: String,
) {
    let _ = (begin_quorum_epoch_admission, request, expected);
}

#[derive(Default)]
struct EndQuorumEpochAdmission {
    leader: Option<EngineInDir>,
    request: Option<EndQuorumEpochRequest>,
    body: Option<bytes::Bytes>,
}
#[rstest::fixture]
fn end_quorum_epoch_admission() -> EndQuorumEpochAdmission {
    // Each request owns fresh quorum state and its on-disk directory.
    EndQuorumEpochAdmission::default()
}
#[given("a EndQuorumEpoch request with {request}")]
fn end_quorum_epoch_input(
    end_quorum_epoch_admission: &mut EndQuorumEpochAdmission,
    request: String,
) {
    end_quorum_epoch_admission.leader = Some(follower_of_2_in_epoch_5());
    let input: EndInput =
        serde_json::from_str(&request).expect("native EndQuorumEpoch request fields");
    end_quorum_epoch_admission.request = Some(input.native());
}
#[when("the quorum controller handles the EndQuorumEpoch request")]
fn end_quorum_epoch_operation(end_quorum_epoch_admission: &mut EndQuorumEpochAdmission) {
    let request = end_quorum_epoch_admission
        .request
        .as_ref()
        .expect("request prepared");
    let engine = &mut end_quorum_epoch_admission
        .leader
        .as_mut()
        .expect("quorum configured")
        .engine;
    end_quorum_epoch_admission.body = Some(
        deliver(engine, end, encode(request, QUORUM_EPOCH_VERSION)).expect("controller answered"),
    );
}
#[then("the EndQuorumEpoch response matches every expected Kafka field {expected} and role {role}")]
fn end_quorum_epoch_response(
    end_quorum_epoch_admission: &EndQuorumEpochAdmission,
    expected: String,
    role: String,
) {
    let expected: EpochExpected =
        serde_json::from_str(&expected).expect("independent complete expected response");
    let body = end_quorum_epoch_admission
        .body
        .as_ref()
        .expect("request handled");
    let response = EndQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION)
        .expect("complete Kafka response");
    check!(response == expected.end());
    check!(
        end_quorum_epoch_admission
            .leader
            .as_ref()
            .expect("quorum configured")
            .engine
            .core
            .role()
            .name()
            == role
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/kraft/controller/tests_quorum_requests.feature",
    name = "EndQuorumEpoch validates the incoming request and returns the full Kafka response"
)]
#[tokio::test]
async fn end_quorum_epoch_runs_kafka_request_checks(
    end_quorum_epoch_admission: EndQuorumEpochAdmission,
    request: String,
    expected: String,
    role: String,
) {
    let _ = (end_quorum_epoch_admission, request, expected, role);
}

#[derive(Default)]
struct FetchSnapshotAdmission {
    leader: Option<EngineInDir>,
    request: Option<FetchSnapshotRequest>,
    body: Option<bytes::Bytes>,
}
#[rstest::fixture]
fn fetch_snapshot_admission() -> FetchSnapshotAdmission {
    // Each request owns fresh quorum state and its on-disk directory.
    FetchSnapshotAdmission::default()
}
#[given("a FetchSnapshot request with {request}")]
fn fetch_snapshot_input(fetch_snapshot_admission: &mut FetchSnapshotAdmission, request: String) {
    fetch_snapshot_admission.leader = Some(single_voter_leader_with_checkpoints());
    let input: SnapshotInput =
        serde_json::from_str(&request).expect("native FetchSnapshot request fields");
    fetch_snapshot_admission.request = Some(input.native());
}
#[when("the quorum controller handles the FetchSnapshot request")]
fn fetch_snapshot_operation(fetch_snapshot_admission: &mut FetchSnapshotAdmission) {
    let request = fetch_snapshot_admission
        .request
        .as_ref()
        .expect("request prepared");
    let engine = &mut fetch_snapshot_admission
        .leader
        .as_mut()
        .expect("quorum configured")
        .engine;
    fetch_snapshot_admission.body = Some(
        deliver(
            engine,
            fetch_snapshot,
            encode(request, FETCH_SNAPSHOT_VERSION),
        )
        .expect("controller answered"),
    );
}
#[then("the FetchSnapshot response matches every expected Kafka field {expected}")]
fn fetch_snapshot_response(fetch_snapshot_admission: &FetchSnapshotAdmission, expected: String) {
    let expected: SnapshotExpected =
        serde_json::from_str(&expected).expect("independent complete expected response");
    let body = fetch_snapshot_admission
        .body
        .as_ref()
        .expect("request handled");
    check!(*body == encode(&expected.native(), FETCH_SNAPSHOT_VERSION));
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/kraft/controller/tests_quorum_requests.feature",
    name = "FetchSnapshot validates the incoming request and returns the full Kafka response"
)]
#[tokio::test]
async fn fetch_snapshot_runs_kafka_request_checks(
    fetch_snapshot_admission: FetchSnapshotAdmission,
    request: String,
    expected: String,
) {
    let _ = (fetch_snapshot_admission, request, expected);
}

type QuorumTopics<P> = Vec<(String, Vec<P>)>;
type VotePartitionInput = (i32, i32, i32, i32, i64, bool);
type BeginPartitionInput = (i32, i32, i32);
type EndPartitionInput = (i32, i32, i32, Vec<i32>);
type VotePartitionAnswer = (i32, i16, i32, i32, bool);
type EpochPartitionAnswer = (i32, i16, i32, i32);
type SnapshotPartitionInput = (i32, i32, i64, i32, i64);

#[derive(serde::Deserialize)]
struct VoteInput {
    cluster: Option<String>,
    voter: i32,
    topics: QuorumTopics<VotePartitionInput>,
}
impl VoteInput {
    fn native(self) -> VoteRequest {
        let topics = self
            .topics
            .into_iter()
            .map(|(topic_name, partitions)| vote_req::TopicData {
                topic_name,
                partitions: partitions
                    .into_iter()
                    .map(
                        |(
                            partition_index,
                            replica_epoch,
                            replica_id,
                            last_offset_epoch,
                            last_offset,
                            pre_vote,
                        )| vote_req::PartitionData {
                            partition_index,
                            replica_epoch,
                            replica_id,
                            last_offset_epoch,
                            last_offset,
                            pre_vote,
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            })
            .collect();
        VoteRequest {
            cluster_id: self.cluster,
            voter_id: self.voter,
            topics,
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct BeginInput {
    cluster: Option<String>,
    voter: i32,
    topics: QuorumTopics<BeginPartitionInput>,
}
impl BeginInput {
    fn native(self) -> BeginQuorumEpochRequest {
        BeginQuorumEpochRequest {
            cluster_id: self.cluster,
            voter_id: self.voter,
            topics: self
                .topics
                .into_iter()
                .map(|(topic_name, partitions)| bqe_req::TopicData {
                    topic_name,
                    partitions: partitions
                        .into_iter()
                        .map(
                            |(partition_index, leader_id, leader_epoch)| bqe_req::PartitionData {
                                partition_index,
                                leader_id,
                                leader_epoch,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct EndInput {
    cluster: Option<String>,
    topics: QuorumTopics<EndPartitionInput>,
}
impl EndInput {
    fn native(self) -> EndQuorumEpochRequest {
        EndQuorumEpochRequest {
            cluster_id: self.cluster,
            topics: self
                .topics
                .into_iter()
                .map(|(topic_name, partitions)| eqe_req::TopicData {
                    topic_name,
                    partitions: partitions
                        .into_iter()
                        .map(|(partition_index, leader_id, leader_epoch, preferred)| {
                            eqe_req::PartitionData {
                                partition_index,
                                leader_id,
                                leader_epoch,
                                preferred_candidates: candidates(&preferred),
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct VoteExpected {
    code: i16,
    topics: QuorumTopics<VotePartitionAnswer>,
    endpoints: Vec<(i32, String, u16)>,
}
impl VoteExpected {
    fn native(self) -> VoteResponse {
        VoteResponse {
            error_code: self.code,
            topics: self
                .topics
                .into_iter()
                .map(|(topic_name, partitions)| vote_resp::TopicData {
                    topic_name,
                    partitions: partitions
                        .into_iter()
                        .map(
                            |(
                                partition_index,
                                error_code,
                                leader_id,
                                leader_epoch,
                                vote_granted,
                            )| vote_resp::PartitionData {
                                partition_index,
                                error_code,
                                leader_id,
                                leader_epoch,
                                vote_granted,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            node_endpoints: self
                .endpoints
                .into_iter()
                .map(|(node_id, host, port)| vote_resp::NodeEndpoint {
                    node_id,
                    host,
                    port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct EpochExpected {
    code: i16,
    topics: QuorumTopics<EpochPartitionAnswer>,
    endpoints: Vec<(i32, String, u16)>,
}
fn begin_epoch_partition(
    (partition_index, error_code, leader_id, leader_epoch): (i32, i16, i32, i32),
) -> bqe_resp::PartitionData {
    bqe_resp::PartitionData {
        partition_index,
        error_code,
        leader_id,
        leader_epoch,
        ..Default::default()
    }
}
fn end_epoch_partition(
    (partition_index, error_code, leader_id, leader_epoch): (i32, i16, i32, i32),
) -> eqe_resp::PartitionData {
    eqe_resp::PartitionData {
        partition_index,
        error_code,
        leader_id,
        leader_epoch,
        ..Default::default()
    }
}

impl EpochExpected {
    fn begin(self) -> BeginQuorumEpochResponse {
        BeginQuorumEpochResponse {
            error_code: self.code,
            topics: self
                .topics
                .into_iter()
                .map(|(topic_name, partitions)| bqe_resp::TopicData {
                    topic_name,
                    partitions: partitions.into_iter().map(begin_epoch_partition).collect(),
                    ..Default::default()
                })
                .collect(),
            node_endpoints: self
                .endpoints
                .into_iter()
                .map(|(node_id, host, port)| bqe_resp::NodeEndpoint {
                    node_id,
                    host,
                    port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn end(self) -> EndQuorumEpochResponse {
        EndQuorumEpochResponse {
            error_code: self.code,
            topics: self
                .topics
                .into_iter()
                .map(|(topic_name, partitions)| eqe_resp::TopicData {
                    topic_name,
                    partitions: partitions.into_iter().map(end_epoch_partition).collect(),
                    ..Default::default()
                })
                .collect(),
            node_endpoints: self
                .endpoints
                .into_iter()
                .map(|(node_id, host, port)| eqe_resp::NodeEndpoint {
                    node_id,
                    host,
                    port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct SnapshotInput {
    cluster: Option<String>,
    replica: i32,
    max_bytes: i32,
    topics: QuorumTopics<SnapshotPartitionInput>,
}
impl SnapshotInput {
    fn native(self) -> FetchSnapshotRequest {
        FetchSnapshotRequest {
            cluster_id: self.cluster,
            replica_id: self.replica,
            max_bytes: self.max_bytes,
            topics: self
                .topics
                .into_iter()
                .map(|(name, partitions)| fs_req::TopicSnapshot {
                    name,
                    partitions: partitions
                        .into_iter()
                        .map(
                            |(partition, current_leader_epoch, end_offset, epoch, position)| {
                                fs_req::PartitionSnapshot {
                                    partition,
                                    current_leader_epoch,
                                    position,
                                    snapshot_id: fs_req::SnapshotId {
                                        end_offset,
                                        epoch,
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                }
                            },
                        )
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
#[derive(serde::Deserialize)]
struct ExpectedSnapshotPartition {
    index: i32,
    code: i16,
    leader: Option<(i32, i32)>,
    chunk: Option<(i64, i32, i64, i64, String)>,
}
#[derive(serde::Deserialize)]
struct SnapshotExpected {
    code: i16,
    topics: Vec<(String, Vec<ExpectedSnapshotPartition>)>,
    endpoints: Vec<(i32, String, u16)>,
}
impl SnapshotExpected {
    fn native(self) -> FetchSnapshotResponse {
        FetchSnapshotResponse {
            error_code: self.code,
            topics: self
                .topics
                .into_iter()
                .map(|(name, partitions)| fs_resp::TopicSnapshot {
                    name,
                    partitions: partitions
                        .into_iter()
                        .map(|row| {
                            let mut result = fs_resp::PartitionSnapshot {
                                index: row.index,
                                error_code: row.code,
                                ..Default::default()
                            };
                            if let Some((leader_id, leader_epoch)) = row.leader {
                                result.current_leader = fs_resp::LeaderIdAndEpoch {
                                    leader_id,
                                    leader_epoch,
                                    ..Default::default()
                                };
                            }
                            if let Some((end_offset, epoch, size, position, chunk)) = row.chunk {
                                result.snapshot_id = fs_resp::SnapshotId {
                                    end_offset,
                                    epoch,
                                    ..Default::default()
                                };
                                result.size = size;
                                result.position = position;
                                result.unaligned_records =
                                    RecordsPayload::Raw(bytes::Bytes::from(chunk));
                            }
                            result
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            node_endpoints: self
                .endpoints
                .into_iter()
                .map(|(node_id, host, port)| fs_resp::NodeEndpoint {
                    node_id,
                    host,
                    port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}
