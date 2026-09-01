//! Wire drivers for the requests the client-quota tests throttle: `Produce`
//! and a consumer `Fetch`, each on its own authenticated SASL/PLAIN
//! connection, plus an `AddOffsetsToTxn` driver that reuses one such
//! connection.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        add_offsets_to_txn_response::AddOffsetsToTxnResponse,
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        allocate_producer_ids_response::AllocateProducerIdsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    records::{Record, RecordBatch},
};
use tokio::net::TcpStream;

use super::wire::{round_trip, round_trip_split_header, sasl_plain_authenticate};

/// The `AddOffsetsToTxn` version the driver below speaks: the newest the
/// broker advertises, and flexible (v3+), so the response header carries the
/// empty tagged-fields byte the leading-throttle patch has to skip over.
const ADD_OFFSETS_TO_TXN_VERSION: i16 = 4;

/// Drives one `AddOffsetsToTxn` request for a transactional id this broker
/// does not coordinate, on an already-authenticated `stream`, and returns the
/// whole decoded response.
///
/// `AddOffsetsToTxn` is picked because it is one of the few APIs that both
/// reaches `maybe_apply_request_quota` -- its dispatch entry carries
/// `RequestQuotaPolicy::ApplyFallbackAccounting` -- and puts `ThrottleTimeMs`
/// first in its response, so the request-quota delay is reported by patching
/// that leading int32 into the already-encoded body rather than by a handler
/// filling the field in. The request is expected to fail: the point is the
/// framing around the error, not the error.
///
/// The caller supplies `corr_id` so several requests can share one connection;
/// reusing the connection keeps the SASL handshake out of the quota bucket.
pub async fn drive_add_offsets_to_txn(
    stream: &mut TcpStream,
    corr_id: i32,
) -> AddOffsetsToTxnResponse {
    let req = AddOffsetsToTxnRequest {
        transactional_id: "krabka-quota-test-no-such-txn".to_string(),
        group_id: "krabka-quota-test-group".to_string(),
        ..Default::default()
    };

    let mut body = BytesMut::new();
    req.encode(&mut body, ADD_OFFSETS_TO_TXN_VERSION)
        .expect("encode AddOffsetsToTxn");
    let resp_bytes = round_trip(stream, 25, ADD_OFFSETS_TO_TXN_VERSION, corr_id, true, &body)
        .await
        .expect("AddOffsetsToTxn round-trip");
    let mut cur: &[u8] = &resp_bytes;
    AddOffsetsToTxnResponse::decode(&mut cur, ADD_OFFSETS_TO_TXN_VERSION)
        .expect("decode AddOffsetsToTxnResponse")
}

/// `AllocateProducerIds` exists at v0 only, and is flexible from v0. A
/// request below that minimum is answered by the dispatch loop's
/// unsupported-version path rather than by the handler.
const ALLOCATE_PRODUCER_IDS_ADVERTISED_VERSION: i16 = 0;

/// Below `AllocateProducerIds`' minimum version, so the request header is
/// parsed as non-flexible while the reply is encoded at v0 with a flexible
/// response header. The two disagree, which is the point.
const ALLOCATE_PRODUCER_IDS_UNSUPPORTED_VERSION: i16 = -1;

/// Drives one `AllocateProducerIds` request at a version the broker does not
/// support, on an already-authenticated `stream`, and returns the whole
/// decoded `UNSUPPORTED_VERSION` reply.
///
/// The dispatch loop rejects the version before decoding the body, so the body
/// bytes are a well-formed v0 request only so that the frame is a realistic
/// one; the broker never looks at them.
///
/// This is the one path on which the request's version and the response's
/// differ, and `AllocateProducerIds` is the API where that difference is
/// visible on the wire: its response leads with `ThrottleTimeMs`, so the
/// request-quota delay is reported by patching the leading int32, and the
/// offset that patch writes at depends on which of the two headers is
/// consulted.
pub async fn drive_unsupported_allocate_producer_ids(
    stream: &mut TcpStream,
    corr_id: i32,
) -> AllocateProducerIdsResponse {
    let mut body = BytesMut::new();
    AllocateProducerIdsRequest::default()
        .encode(&mut body, ALLOCATE_PRODUCER_IDS_ADVERTISED_VERSION)
        .expect("encode AllocateProducerIds");
    let resp_bytes = round_trip_split_header(
        stream,
        67,
        ALLOCATE_PRODUCER_IDS_UNSUPPORTED_VERSION,
        corr_id,
        false,
        true,
        &body,
    )
    .await
    .expect("AllocateProducerIds round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let response =
        AllocateProducerIdsResponse::decode(&mut cur, ALLOCATE_PRODUCER_IDS_ADVERTISED_VERSION)
            .expect("decode AllocateProducerIdsResponse");
    assert!(
        cur.is_empty(),
        "{} trailing bytes after AllocateProducerIdsResponse",
        cur.len()
    );
    response
}

/// Drives a `Produce` request over an already-authenticated SASL stream.
///
/// Returns the full `ProduceResponse`.
pub async fn drive_produce_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    topic: &str,
    record_bytes: usize,
    count: usize,
) -> ProduceResponse {
    let version: i16 = 11; // flexible, supports throttle_time_ms

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).unwrap(),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, version, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, version).expect("decode ProduceResponse")
}

/// Drives a consumer `Fetch` request with `replica_id=-1` over SASL.
///
/// Returns the full `FetchResponse`.
pub async fn drive_fetch_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    topic: &str,
) -> FetchResponse {
    let version: i16 = 12; // flexible, supports throttle_time_ms

    let req = FetchRequest {
        replica_id: -1, // consumer fetch (not inter-broker)
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Fetch");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Fetch");
    let resp_bytes = round_trip(&mut stream, 1, version, 1, true, &body)
        .await
        .expect("Fetch round-trip");
    let mut cur: &[u8] = &resp_bytes;
    FetchResponse::decode(&mut cur, version).expect("decode FetchResponse")
}
