use assert2::{assert, check};

use super::*;

#[test]
fn vote_request_round_trips() {
    let req = PeerRequest::Vote {
        voter_id: NodeId(9),
        candidate_epoch: 3,
        candidate: NodeId(7),
        last_epoch: 2,
        last_offset: 42,
        pre_vote: true,
    };
    assert2::assert!(decode_vote(&req.encode()) == Some(req));
}

#[test]
fn generic_request_decode_accepts_vote_request() {
    let req = PeerRequest::Vote {
        voter_id: NodeId(9),
        candidate_epoch: 3,
        candidate: NodeId(7),
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
        voter_id: NodeId(9),
        candidate_epoch: 3,
        candidate: NodeId(7),
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
