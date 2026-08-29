use assert2::{assert, check};
use bytes::BytesMut;

use super::*;
use crate::kraft::transport::wire::NOT_LEADER_OR_FOLLOWER;

#[test]
fn vote_response_round_trips() {
    let resp = PeerResponse::Vote {
        epoch: 3,
        granted: true,
    };
    assert2::assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
}

#[test]
fn encoded_vote_response_carries_success_error_codes() {
    use krabka_protocol::Decode;

    let resp = PeerResponse::Vote {
        epoch: 3,
        granted: true,
    };
    let mut cur = &resp.encode()[..];
    let raw = VoteResponse::decode(&mut cur, VOTE_VERSION).expect("decode vote response");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.error_code,
            partition.partition_index,
            partition.error_code,
            partition.leader_epoch,
            partition.vote_granted,
        ) == (0, METADATA_PARTITION, 0, 3, true)
    );
}

#[test]
fn decodes_jvm_style_response_without_echo_tag() {
    // A real JVM `VoteResponse` is byte-faithful Kafka v2 with no Krabka
    // echo tag. Build one straight from the generated protocol type
    // (bypassing `PeerResponse::Vote::encode`) and confirm `decode_vote`
    // tolerates it — the regression guard for the removed
    // `PRE_VOTE_ECHO_TAG`.
    let resp = VoteResponse {
        error_code: 0,
        topics: vec![vote_resp::TopicData {
            topic_name: METADATA_TOPIC.to_string(),
            partitions: vec![vote_resp::PartitionData {
                partition_index: METADATA_PARTITION,
                error_code: 0,
                leader_id: -1,
                leader_epoch: epoch_to_wire(7),
                vote_granted: true,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let bytes = encode_body(&resp, VOTE_VERSION);
    let decoded = PeerResponse::decode_vote(&bytes).unwrap();
    assert2::assert!(
        decoded
            == PeerResponse::Vote {
                epoch: 7,
                granted: true
            }
    );
}

#[test]
fn ack_round_trips() {
    let resp = PeerResponse::Ack { epoch: 8 };
    assert2::assert!(PeerResponse::decode_ack(&resp.encode()) == Some(resp));
}

#[test]
fn encoded_ack_response_carries_success_error_codes() {
    use krabka_protocol::Decode;

    let resp = PeerResponse::Ack { epoch: 8 };
    let mut cur = &resp.encode()[..];
    let raw = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).expect("decode ack");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.error_code,
            partition.partition_index,
            partition.error_code,
            partition.leader_id,
            partition.leader_epoch,
        ) == (0, METADATA_PARTITION, 0, -1, 8)
    );
}

#[test]
fn fetch_response_carries_snapshot_id() {
    let resp = PeerResponse::Fetch {
        leader_id: NodeId(1),
        leader_epoch: 4,
        diverging: None,
        snapshot_id: Some((42, 3)),
        hwm: 0,
        records: Bytes::new(),
    };
    assert2::assert!(PeerResponse::decode_fetch(&resp.encode()) == Some(resp));
}

#[test]
fn fetch_snapshot_response_round_trips() {
    let resp = PeerResponse::FetchSnapshot {
        snapshot_id: (42, 3),
        size: 9,
        position: 0,
        bytes: Bytes::from_static(b"snapshotX"),
        error_code: 0,
    };
    assert2::assert!(PeerResponse::decode_fetch_snapshot(&resp.encode()) == Some(resp));
}

#[test]
fn fetch_response_round_trips() {
    let with_records = PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 5,
        diverging: None,
        snapshot_id: None,
        hwm: 7,
        records: Bytes::from_static(b"\x01\x02\x03"),
    };
    assert2::assert!(PeerResponse::decode_fetch(&with_records.encode()) == Some(with_records));

    let diverged = PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 5,
        diverging: Some(LogOffsetMetadata {
            offset: 5,
            epoch: 1,
        }),
        snapshot_id: None,
        hwm: 0,
        records: Bytes::new(),
    };
    assert2::assert!(PeerResponse::decode_fetch(&diverged.encode()) == Some(diverged));
}

#[test]
fn fetch_error_round_trips_with_unknown_leader() {
    use krabka_protocol::{Decode, owned::fetch_response::FetchResponse};

    let resp = PeerResponse::FetchError {
        leader_epoch: 5,
        error_code: NOT_LEADER_OR_FOLLOWER,
    };
    let encoded = resp.encode();
    assert2::assert!(PeerResponse::decode_fetch(&encoded) == Some(resp));

    let mut cur = &encoded[..];
    let raw = FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode Fetch error");
    let partition = &raw.responses[0].partitions[0];
    check!(
        (
            raw.error_code,
            partition.error_code,
            partition.high_watermark,
            partition.current_leader.leader_id,
            partition.current_leader.leader_epoch,
        ) == (0, NOT_LEADER_OR_FOLLOWER, -1, -1, 5)
    );
}

#[test]
fn fetch_error_with_zero_leader_preserves_redirect() {
    use krabka_protocol::{Decode, Encode, owned::fetch_response::FetchResponse};

    let success = PeerResponse::Fetch {
        leader_id: NodeId(0),
        leader_epoch: 5,
        diverging: None,
        snapshot_id: None,
        hwm: -1,
        records: Bytes::new(),
    }
    .encode();
    let mut cur = &success[..];
    let mut raw = FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode Fetch");
    raw.responses[0].partitions[0].error_code = NOT_LEADER_OR_FOLLOWER;
    let mut encoded = BytesMut::new();
    raw.encode(&mut encoded, FETCH_VERSION)
        .expect("encode Fetch error");

    assert2::assert!(matches!(
        PeerResponse::decode_fetch(&encoded),
        Some(PeerResponse::Fetch {
            leader_id: NodeId(0),
            leader_epoch: 5,
            ..
        })
    ));
}

#[test]
fn encoded_fetch_response_carries_partition_success_fields() {
    use krabka_protocol::{Decode, owned::fetch_response::FetchResponse};

    let resp = PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 5,
        diverging: None,
        snapshot_id: None,
        hwm: 7,
        records: Bytes::new(),
    };
    let mut cur = &resp.encode()[..];
    let raw = FetchResponse::decode(&mut cur, FETCH_VERSION).expect("decode fetch response");
    let partition = &raw.responses[0].partitions[0];
    check!(
        (
            partition.partition_index,
            partition.error_code,
            partition.high_watermark,
            partition.current_leader.leader_id,
            partition.current_leader.leader_epoch,
        ) == (METADATA_PARTITION, 0, 7, 2, 5)
    );
}
