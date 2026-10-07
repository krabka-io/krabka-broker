//! The raw-wire drivers this suite speaks to the broker with: one
//! length-prefixed request and response exchange, and the four typed calls
//! built on it (`CreateTopics`, `AlterReplicaLogDirs`, `DescribeLogDirs`).
//!
//! Every API used here is flexible, so the request header always carries its
//! tagged-fields byte and the response header always has one to strip. The
//! protocol versions the suite pins live here too, next to the encoders that
//! read them.

use std::{io, net::SocketAddr};

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
        },
        alter_replica_log_dirs_response::AlterReplicaLogDirsResponse,
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
};
use tokio::net::TcpStream;

use crate::kafka_wire;

const CLIENT_ID: &str = "krabka-arld-test";
const ALTER_VERSION: i16 = 2;
const DESCRIBE_VERSION: i16 = 4;

/// One length-prefixed request/response exchange on correlation id 1, with
/// flexible headers because every API this suite sends is flexible; see
/// [`kafka_wire::round_trip`].
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    kafka_wire::round_trip(stream, api_key, api_version, 1, CLIENT_ID, true, body).await
}

pub(crate) async fn create_topic(addr: SocketAddr, topic: &str, partitions: i32) {
    kafka_wire::create_topic_plaintext(addr, CLIENT_ID, kafka_wire::topic(topic, partitions, 1))
        .await;
}

pub(crate) async fn alter_replica_log_dirs(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    partitions: Vec<i32>,
) -> AlterReplicaLogDirsResponse {
    let req = AlterReplicaLogDirsRequest {
        dirs: vec![AlterReplicaLogDir {
            path: target_dir.to_string_lossy().to_string(),
            topics: vec![AlterReplicaLogDirTopic {
                name: topic.to_string(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, ALTER_VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 34, ALTER_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    AlterReplicaLogDirsResponse::decode(&mut cur, ALTER_VERSION).unwrap()
}

pub(crate) async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    describe_log_dirs_at(addr, DESCRIBE_VERSION).await
}

/// `DescribeLogDirs` for every partition, at `version`.
pub(crate) async fn describe_log_dirs_at(
    addr: SocketAddr,
    version: i16,
) -> DescribeLogDirsResponse {
    let req = DescribeLogDirsRequest {
        topics: None,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let resp_bytes = round_trip(&mut stream, 35, version, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    DescribeLogDirsResponse::decode(&mut cur, version).unwrap()
}

/// `IncrementalAlterConfigs` v1 that SETs one key on broker `broker`, and the
/// per-resource `(error_code, error_message)` it answers.
pub(crate) async fn set_broker_config(
    addr: SocketAddr,
    broker: i32,
    name: &str,
    value: &str,
) -> (i16, Option<String>) {
    use krabka_protocol::owned::{
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    };
    let version: i16 = 1;
    let req = IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: 4,
            resource_name: broker.to_string(),
            configs: vec![AlterableConfig {
                name: name.to_string(),
                config_operation: 0,
                value: Some(value.to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let resp_bytes = round_trip(&mut stream, 44, version, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = IncrementalAlterConfigsResponse::decode(&mut cur, version).unwrap();
    (
        resp.responses[0].error_code,
        resp.responses[0].error_message.clone(),
    )
}
