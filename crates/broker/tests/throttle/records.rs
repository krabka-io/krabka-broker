//! Wire drivers for the data path the throttle actually caps: a PLAINTEXT
//! Produce that fills the partition, and a replica Fetch that measures how
//! many bytes come back.
//!
//! The Fetch driver frames its own request instead of reusing
//! `crate::wire::round_trip`, because the assertion under test is the *size* of
//! the raw response and that has to be captured before decoding.

use std::net::SocketAddr;

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use krabka_protocol::{Decode, Encode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::wire::round_trip;

/// Produce `count` records of `record_bytes` bytes each to `(topic, 0)` over
/// a PLAINTEXT connection. Asserts `error_code=0` on the partition row.
pub async fn produce_plaintext(addr: SocketAddr, topic: &str, record_bytes: usize, count: usize) {
    const VERSION: i16 = 9; // flexible, pre-KIP-516 (no topic_id needed)

    use krabka_protocol::{
        owned::{
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::ProduceResponse,
        },
        records::{Record, RecordBatch},
    };

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    let req = ProduceRequest {
        acks: 1, // leader ack only (rf=1 topic)
        timeout_ms: 5_000,
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

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, VERSION, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse");
    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "Produce must succeed: error_code={}",
        part.error_code
    );
}

/// Issue a single Fetch request with `replica_id` over a PLAINTEXT
/// connection. A value `>= 0` means an inter-broker replica fetch, which the
/// leader-side throttle applies to. Returns the raw response payload byte
/// length.
pub async fn fetch_plaintext_replica(addr: SocketAddr, topic: &str, replica_id: i32) -> usize {
    const VERSION: i16 = 12; // flexible

    use krabka_protocol::owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    };

    let req = FetchRequest {
        replica_id,
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

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Fetch");

    // Send raw frame and capture the full raw response (before decode) so we
    // can measure response bytes.
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(1i16); // api_key
    frame.put_i16(VERSION);
    frame.put_i32(1i32); // corr_id
    let client_id = "krabka-throttle-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible header tagged-fields
    frame.put_slice(&body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let resp_len = stream.read_u32().await.unwrap();
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.unwrap();

    // Decode to assert no transport error.
    let mut cur: &[u8] = &resp[4..]; // skip corr_id
    let _tagged = cur.get_u8(); // v1 header tagged-fields
    let _decoded = FetchResponse::decode(&mut cur, VERSION).expect("decode FetchResponse");

    resp.len()
}
