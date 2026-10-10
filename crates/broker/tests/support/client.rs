//! Client connections and topic creation shared by coordinator suites.
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use assert2::assert;
use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_client_core::{Client, Connection, ConnectionOptions};
use krabka_protocol::{
    owned::produce_response::PartitionProduceResponse, primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_raft::{KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse};
use krabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::support::{
    produce::single_partition_produce,
    records::{batch_from_records, value_record},
    topics::{creatable_topic, create_topic_request},
};
pub async fn connect(bootstrap: &str, client_id: &str) -> Arc<Client> {
    Arc::new(connect_client(bootstrap, Some(client_id)).await)
}

/// Connect using an explicit ID, or keep the builder's default ID.
///
/// # Panics
/// Panics if the client cannot connect to the bootstrap broker.
pub async fn connect_client(bootstrap: impl AsRef<str>, client_id: Option<&str>) -> Client {
    build_client(bootstrap.as_ref(), client_id).await.unwrap()
}

async fn build_client(
    bootstrap: &str,
    client_id: Option<&str>,
) -> Result<Client, krabka_client_core::ClientError> {
    let builder = Client::builder().bootstrap(bootstrap);
    match client_id {
        Some(client_id) => builder.client_id(client_id).build().await,
        None => builder.build().await,
    }
}

/// Connect while retaining default-ID behavior and the fixture's diagnostic.
///
/// # Panics
/// Panics with `context` if connecting to the bootstrap broker fails.
pub async fn connect_with_context(
    bootstrap: impl AsRef<str>,
    client_id: Option<&str>,
    context: &str,
) -> Client {
    build_client(bootstrap.as_ref(), client_id)
        .await
        .expect(context)
}

/// Connect a named client while retaining the fixture's diagnostic context.
///
/// # Panics
/// Panics with `context` if connecting to the bootstrap broker fails.
pub async fn connect_owned(bootstrap: impl AsRef<str>, client_id: &str, context: &str) -> Client {
    connect_with_context(bootstrap, Some(client_id), context).await
}
pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    create_topic_with(client, topic, partitions, 1, 5_000).await;
}

/// Create a topic and verify that its first partition becomes local.
///
/// # Panics
/// Panics if creation fails or the partition never becomes local.
pub async fn create_led_topic(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    partitions: i32,
) {
    create_topic(client, topic, partitions).await;
    broker.wait_until_partition_present(topic, 0).await;
    assert!(broker.has_partition(topic, 0), "partition never led");
}

pub async fn scrape_metrics(addr: SocketAddr) -> String {
    let s = http_get(addr, "/metrics", true).await;
    // Strip the HTTP head, keep the body so we can inspect metric names.
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

pub async fn http_get(addr: SocketAddr, path: &str, flush: bool) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    if flush {
        stream.flush().await.unwrap();
    }
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    String::from_utf8(buf).unwrap()
}

pub async fn create_topic_with(
    client: &Client,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
    timeout_ms: i32,
) -> WireUuid {
    let resp = client
        .send(create_topic_request(
            creatable_topic(topic, partitions, replication_factor),
            timeout_ms,
        ))
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
        ..batch_from_records(
            (0..n)
                .map(|i| value_record(i, Some(bytes::Bytes::from(format!("v{i}")))))
                .collect(),
        )
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
        .send(single_partition_produce(
            topic.to_owned(),
            topic_id,
            0,
            Some(batch.into()),
            (acks, timeout_ms),
        ))
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

krabka_macros::metric_registry_fixture!(render_registry);

/// Read one rendered Prometheus series; a missing series has value zero.
pub async fn metric_value(handle: &BrokerHandle, series: &str) -> f64 {
    let rendered = {
        render_registry!(handle.metrics(), rendered, registry; expect("encode registry"));
        rendered
    };
    rendered
        .lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

pub fn metrics_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![crate::support::listeners::loopback_listener(
        "PLAINTEXT",
        ListenerProtocol::Plaintext,
    )];
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

/// Create one configured topic and check the original single-result success oracle.
///
/// # Panics
/// Panics if the request fails, its topic row is missing, or creation is rejected.
pub async fn create_configured_topic(
    client: &Client,
    topic: &str,
    configs: &[(&str, &str)],
    partitions: i32,
    replication_factor: i16,
    timeout_ms: i32,
) {
    let response = client
        .send(crate::support::topics::configured_topic_request(
            topic,
            configs,
            partitions,
            replication_factor,
            timeout_ms,
        ))
        .await
        .expect("CreateTopics");
    let created = response.topics.first().expect("one topic result");
    assert!(
        created.error_code == 0,
        "create {topic}: {:?}",
        created.error_message
    );
}

/// The share and streams protocol suites use the same short client identity.
///
/// # Panics
/// Panics if the connection cannot be built.
pub async fn connect_c1(bootstrap: &str) -> Arc<Client> {
    connect(bootstrap, "c1").await
}

/// Start a configured broker and connect the fixture's explicitly named client.
/// The caller retains its original directory guard and readiness policy.
///
/// # Panics
/// Panics if startup or client construction fails, with each caller's diagnostic.
pub async fn start_broker_client(
    config: BrokerConfig,
    client_id: &str,
    broker_context: &str,
    client_context: &str,
) -> (BrokerHandle, Client) {
    let broker = configured_broker(config, Some(broker_context)).await;
    let client = connect_owned(broker.listen_addr().to_string(), client_id, client_context).await;
    (broker, client)
}

/// Start a configured broker and connect a default or explicitly named client.
/// The returned bootstrap string retains the restart fixtures' original binding.
///
/// # Panics
/// Panics if broker startup or client construction fails, with their original unwrap policy.
pub async fn start_client(
    config: BrokerConfig,
    client_id: Option<&str>,
) -> (BrokerHandle, String, Client) {
    let broker = configured_broker(config, None).await;
    let bootstrap = broker.listen_addr().to_string();
    let client = connect_client(&bootstrap, client_id).await;
    (broker, bootstrap, client)
}

async fn configured_broker(config: BrokerConfig, context: Option<&str>) -> BrokerHandle {
    // Keep startup off the fixture's stack as LogConfig grows.
    let result = Box::pin(krabka_broker::Broker::start(config)).await;
    match context {
        Some(context) => result.expect(context),
        None => result.unwrap(),
    }
}
