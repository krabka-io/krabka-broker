use assert2::{assert, check};
use base64::Engine as _;

use super::*;

fn raw_vote_request() -> VoteRequest {
    VoteRequest {
        voter_id: 1,
        topics: vec![vote_req::TopicData {
            topic_name: METADATA_TOPIC.to_string(),
            partitions: vec![vote_req::PartitionData {
                partition_index: METADATA_PARTITION,
                replica_epoch: 3,
                replica_id: 2,
                last_offset_epoch: 2,
                last_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn raw_vote_body(request: &VoteRequest) -> Bytes {
    encode_body(request, VOTE_VERSION)
}

#[test]
fn vote_request_round_trips() {
    let req = PeerRequest::Vote {
        cluster_id: Some(uuid::Uuid::from_u128(1)),
        voter_id: NodeId(9),
        voter_directory_id: uuid::Uuid::from_u128(2),
        candidate_epoch: 3,
        candidate: NodeId(7),
        candidate_directory_id: uuid::Uuid::from_u128(3),
        last_epoch: 2,
        last_offset: 42,
        pre_vote: true,
    };
    assert2::assert!(decode_vote(&req.encode()) == Some(req));
}

#[test]
fn vote_request_preserves_legitimate_node_zero() {
    let req = PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(0),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch: 0,
        candidate: NodeId(0),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch: 0,
        last_offset: 0,
        pre_vote: false,
    };
    assert2::assert!(decode_vote(&req.encode()) == Some(req));
}

#[test]
fn vote_decode_accepts_kafka_base64_cluster_id() {
    let cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    let mut request = raw_vote_request();
    request.cluster_id =
        Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()));

    let Some(PeerRequest::Vote {
        cluster_id: decoded,
        ..
    }) = decode_vote(&raw_vote_body(&request))
    else {
        panic!("valid Kafka cluster id must decode");
    };
    assert2::assert!(decoded == Some(cluster_id));
}

#[test]
fn vote_encode_rejects_values_above_signed_wire_maximum() {
    let max_id = i32::MAX as u64;
    let max_epoch = i32::MAX as u32;
    let request = |voter_id, candidate, candidate_epoch, last_epoch| PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(voter_id),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch,
        candidate: NodeId(candidate),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch,
        last_offset: 0,
        pre_vote: false,
    };

    assert2::assert!(
        request(max_id, max_id, max_epoch, max_epoch)
            .try_encode()
            .is_some()
    );
    assert2::assert!(request(max_id + 1, 0, 0, 0).try_encode().is_none());
    assert2::assert!(request(0, max_id + 1, 0, 0).try_encode().is_none());
    assert2::assert!(request(0, 0, max_epoch + 1, 0).try_encode().is_none());
    assert2::assert!(request(0, 0, 0, max_epoch + 1).try_encode().is_none());
}

#[test]
fn vote_decode_rejects_negative_ids() {
    let mut request = raw_vote_request();
    request.voter_id = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].replica_id = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_negative_epochs() {
    let mut request = raw_vote_request();
    request.topics[0].partitions[0].replica_epoch = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].last_offset_epoch = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_wrong_topic_or_partition() {
    let mut request = raw_vote_request();
    request.topics[0].topic_name = "other".to_string();
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].partition_index = 1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_trailing_bytes() {
    let mut body = raw_vote_body(&raw_vote_request()).to_vec();
    body.push(0);
    assert2::assert!(decode_vote(&body).is_none());
}

#[test]
fn generic_request_decode_accepts_vote_request() {
    let req = PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(9),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch: 3,
        candidate: NodeId(7),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch: 2,
        last_offset: 42,
        pre_vote: true,
    };
    assert2::assert!(PeerRequest::decode(&req.encode()) == Some(req));
}

#[test]
fn encoded_vote_request_carries_target_voter_and_empty_cluster_id() {
    use krabka_protocol::Decode;

    let req = PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(9),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch: 3,
        candidate: NodeId(7),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch: 2,
        last_offset: 42,
        pre_vote: true,
    };
    let mut cur = &req.encode()[..];
    let raw = VoteRequest::decode(&mut cur, VOTE_VERSION).expect("decode vote request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.cluster_id.as_ref(),
            raw.voter_id,
            partition.replica_epoch,
            partition.replica_id,
            partition.last_offset_epoch,
            partition.last_offset,
            partition.pre_vote,
        ) == (None, 9, 3, 7, 2, 42, true)
    );
}

#[test]
fn begin_end_round_trip() {
    let begin = PeerRequest::BeginQuorumEpoch {
        leader_id: NodeId(5),
        leader_epoch: 9,
    };
    assert2::assert!(decode_begin(&begin.encode()) == Some(begin));
    let end = PeerRequest::EndQuorumEpoch {
        leader_id: NodeId(1),
        leader_epoch: 4,
    };
    assert2::assert!(decode_end(&end.encode()) == Some(end));
}

#[test]
fn encoded_begin_and_end_requests_carry_quorum_defaults_and_leader() {
    use krabka_protocol::Decode;

    let begin = PeerRequest::BeginQuorumEpoch {
        leader_id: NodeId(5),
        leader_epoch: 9,
    };
    let mut begin_cur = &begin.encode()[..];
    let raw_begin = BeginQuorumEpochRequest::decode(&mut begin_cur, QUORUM_EPOCH_VERSION)
        .expect("decode begin request");
    let begin_partition = &raw_begin.topics[0].partitions[0];
    assert2::assert!(raw_begin.cluster_id.as_ref() == None);
    assert2::assert!(raw_begin.voter_id == -1);
    assert2::assert!(begin_partition.leader_id == 5);
    assert2::assert!(begin_partition.leader_epoch == 9);

    let end = PeerRequest::EndQuorumEpoch {
        leader_id: NodeId(1),
        leader_epoch: 4,
    };
    let mut end_cur = &end.encode()[..];
    let raw_end = EndQuorumEpochRequest::decode(&mut end_cur, QUORUM_EPOCH_VERSION)
        .expect("decode end request");
    let end_partition = &raw_end.topics[0].partitions[0];
    assert2::assert!(raw_end.cluster_id.as_ref() == None);
    assert2::assert!(end_partition.leader_id == 1);
    assert2::assert!(end_partition.leader_epoch == 4);
}

#[test]
fn fetch_request_round_trips() {
    let req = PeerRequest::Fetch {
        from: NodeId(2),
        fetch_epoch: 1,
        fetch_offset: 11,
    };
    assert2::assert!(decode_fetch(&req.encode()) == Some(req));
}

#[test]
fn encoded_fetch_request_carries_replica_state_epoch_sentinel() {
    use krabka_protocol::{Decode, owned::fetch_request::FetchRequest};

    let req = PeerRequest::Fetch {
        from: NodeId(2),
        fetch_epoch: 1,
        fetch_offset: 11,
    };
    let mut cur = &req.encode()[..];
    let raw = FetchRequest::decode(&mut cur, FETCH_VERSION).expect("decode fetch request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.replica_state.replica_id,
            raw.replica_state.replica_epoch,
            partition.current_leader_epoch,
            partition.last_fetched_epoch,
            partition.fetch_offset,
        ) == (2, -1, 1, 1, 11)
    );
}

#[test]
fn fetch_snapshot_request_round_trips() {
    let req = PeerRequest::FetchSnapshot {
        from: NodeId(2),
        snapshot_id: (42, 3),
        position: 128,
        max_bytes: 4096,
    };
    assert2::assert!(decode_fetch_snapshot(&req.encode()) == Some(req));
}

#[test]
fn encoded_fetch_snapshot_request_carries_empty_cluster_id() {
    use krabka_protocol::Decode;

    let req = PeerRequest::FetchSnapshot {
        from: NodeId(2),
        snapshot_id: (42, 3),
        position: 128,
        max_bytes: 4096,
    };
    let mut cur = &req.encode()[..];
    let raw = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION)
        .expect("decode fetch snapshot request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.cluster_id.as_ref(),
            raw.replica_id,
            raw.max_bytes,
            partition.current_leader_epoch,
            partition.snapshot_id.end_offset,
            partition.snapshot_id.epoch,
            partition.position,
        ) == (None, 2, 4096, 3, 42, 3, 128)
    );
}
