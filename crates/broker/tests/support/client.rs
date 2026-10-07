//! Client connections and topic creation shared by coordinator suites.
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use assert2::assert;
use krabka_broker::{BrokerConfig, BrokerHandle, SslPrincipalMapper, config::ListenerSpec};
use krabka_client_core::{Client, Connection, ConnectionOptions};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use krabka_raft::{KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse};
use krabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
pub async fn connect(bootstrap: &str, client_id: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .build()
            .await
            .unwrap(),
    )
}
pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    create_topic_with(client, topic, partitions, 1, 5_000).await;
}

pub async fn scrape_metrics(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let s = String::from_utf8(buf).unwrap();
    // Strip the HTTP head, keep the body so we can grep metric names.
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

pub async fn create_topic_with(
    client: &Client,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
    timeout_ms: i32,
) -> WireUuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor,
                ..Default::default()
            }],
            timeout_ms,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
    resp.topics[0].topic_id
}

/// One vN record per offset, starting at zero within the batch.
pub fn value_batch(n: i32) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: (n - 1).max(0),
        records: (0..n)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Produce one batch, preserving the caller's acknowledgement and deadline.
pub async fn produce_batch(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
    acks: i16,
    timeout_ms: i32,
) -> PartitionProduceResponse {
    let response = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.to_owned(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

/// Read one rendered Prometheus series; a missing series has value zero.
pub async fn metric_value(handle: &BrokerHandle, series: &str) -> f64 {
    let mut rendered = String::new();
    {
        let registry = handle.metrics().registry.lock().await;
        prometheus_client::encoding::text::encode(&mut rendered, &registry)
            .expect("encode registry");
    }
    rendered
        .lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

pub fn metrics_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![ListenerSpec {
        name: "PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Plaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "PLAINTEXT".into();
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    cfg
}

pub async fn metadata_fetch(
    controller: SocketAddr,
    client_id: &str,
    from: i64,
) -> KrabkaMetadataFetchResponse {
    let connection = Connection::connect(
        controller,
        ConnectionOptions {
            client_id: client_id.to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("dial the controller listener");
    let mut body = Vec::new();
    KrabkaMetadataFetchRequest {
        fetch_offset: from,
        max_bytes: 4 << 20,
        replica_id: -1,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .encode_v0(&mut body);
    let raw = connection
        .raw_request(
            krabka_raft::API_KEY_METADATA_FETCH,
            0,
            bytes::Bytes::from(body),
        )
        .await
        .expect("metadata fetch");
    connection.close();
    let mut cursor: &[u8] = &raw;
    let response = KrabkaMetadataFetchResponse::decode_v0(&mut cursor)
        .expect("decode the metadata fetch response");
    assert!(response.error_code == 0, "the controller served the fetch");
    response
}
