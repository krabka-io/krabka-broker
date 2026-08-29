//! The pieces both backlog cases share: the cluster lock that serialises them,
//! the HTTP scrape of the broker's `/metrics` endpoint, and the `CreateTopics`
//! and `Produce` drivers that put records behind a share group.
//!
//! The scrape is written against a raw `TcpStream` rather than an HTTP client
//! because the assertion is on the exposition text itself, one
//! `krabka_broker_share_group_backlog` line with its labels.

use std::{net::SocketAddr, sync::OnceLock};

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    sync::Mutex,
};

pub const TOPIC: &str = "backlog-itest";

pub fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub async fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    let response = String::from_utf8(bytes).unwrap();
    let body = response.find("\r\n\r\n").map_or(0, |at| at + 4);
    response[body..].to_owned()
}

pub async fn create_topic(client: &Client, partitions: i32, replication_factor: i16) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: partitions,
                replication_factor,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0, "{response:?}");
}

pub async fn produce_five(client: &Client, topic_id: uuid::Uuid) {
    let records = (0..5)
        .map(|offset| Record {
            offset_delta: offset,
            value: Some(bytes::Bytes::from_static(b"work")),
            ..Default::default()
        })
        .collect();
    let response = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(
                        RecordBatch {
                            last_offset_delta: 4,
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
        })
        .await
        .unwrap();
    assert!(
        response.responses[0].partition_responses[0].error_code == 0,
        "{response:?}"
    );
}
