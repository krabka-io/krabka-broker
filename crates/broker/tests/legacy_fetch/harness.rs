//! The raw-socket plumbing the legacy Fetch tests need: a non-flexible request
//! and response exchange, a modern flexible `ProduceRequest` v9 that seeds the
//! partition with a v2 batch, and the legacy `FetchRequest` encoder that asks
//! for the down-converted `MessageSet`.
//!
//! The suite talks to the broker over a `TcpStream` rather than through the
//! Rust client because the client always negotiates the highest Fetch version,
//! and these tests need to pin v0 and v3 exactly.

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    kafka_3_6_2::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{RecordBatch, RecordsPayload},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// ── Wire helpers ──────────────────────────────────────────────────────────────

/// Sends a non-flexible Kafka request frame, v0 to v11, and returns the
/// response body bytes with the `correlation_id` already stripped. Neither
/// direction carries tagged-fields bytes, because v3 is non-flexible.
pub async fn round_trip_nonflexible(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    body: &[u8],
) -> Vec<u8> {
    let client_id = "legacy-fetch-test";
    let mut frame = BytesMut::with_capacity(12 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(client_id.len()).expect("fits in i16"));
    frame.put_slice(client_id.as_bytes());
    // non-flexible: NO trailing tagged-fields byte in request header
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await
        .expect("write frame length");
    stream.write_all(&frame).await.expect("write frame body");
    stream.flush().await.expect("flush");

    let resp_len = stream.read_u32().await.expect("read resp length");
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.expect("read resp body");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32(); // strip correlation_id
    // non-flexible response header: just the 4-byte correlation_id, nothing more
    cur.to_vec()
}

// ── Topic helpers ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
pub async fn topic_id_for(client: &krabka_client_core::Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Creates a single-partition topic with the modern client and asserts that it
/// succeeds.
pub async fn create_topic(client: &krabka_client_core::Client, name: &str) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        cr.topics[0].error_code == 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );
}

/// Produces a single v2 batch to (topic, partition=0) with a modern flexible
/// `ProduceRequest`, version 9. Returns `Ok(())` on success.
pub async fn produce_batch(addr: std::net::SocketAddr, topic: &str, batch: RecordBatch) {
    const PRODUCE_VERSION: i16 = 9;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![batch])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .expect("encode ProduceRequest v9");

    let mut stream = TcpStream::connect(addr).await.expect("connect for produce");
    stream.set_nodelay(true).ok();
    // ProduceRequest v9 is flexible (FLEXIBLE_MIN = 9).
    let client_id = "legacy-fetch-produce";
    let mut frame = BytesMut::new();
    frame.put_i16(0); // api_key = Produce
    frame.put_i16(PRODUCE_VERSION);
    frame.put_i32(99); // correlation_id
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible request header: empty tagged fields
    frame.put_slice(&body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .expect("write produce frame length");
    stream.write_all(&frame).await.expect("write produce frame");
    stream.flush().await.expect("flush produce");

    let resp_len = stream.read_u32().await.expect("read produce resp len");
    let mut resp = vec![0u8; resp_len as usize];
    stream
        .read_exact(&mut resp)
        .await
        .expect("read produce resp");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _tagged = cur.get_u8(); // flexible response header tagged fields
    let produce_resp =
        ProduceResponse::decode(&mut cur, PRODUCE_VERSION).expect("decode ProduceResponse v9");
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part_resp.error_code == 0,
        "produce error: {}",
        part_resp.error_code
    );
}

/// Sends a Fetch request at the given legacy `version` for (topic,
/// partition=0) from offset 0, and returns the raw response body bytes with
/// the `correlation_id` stripped.
///
/// Encoding at a low version drops the fields that version does not have, for
/// example `max_bytes`, which is v3+. One struct therefore works for v0 to
/// v3.
pub async fn fetch_legacy_raw(addr: std::net::SocketAddr, topic: &str, version: i16) -> Vec<u8> {
    fetch_legacy_raw_at(addr, topic, version, 0).await
}

pub async fn fetch_legacy_raw_at(
    addr: std::net::SocketAddr,
    topic: &str,
    version: i16,
    fetch_offset: i64,
) -> Vec<u8> {
    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode legacy FetchRequest");

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect for legacy fetch");
    stream.set_nodelay(true).ok();
    round_trip_nonflexible(&mut stream, 1, version, 42, &body).await
}
