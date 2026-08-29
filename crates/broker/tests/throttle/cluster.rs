//! Cluster fixtures: bringing up the single broker each test runs against, in
//! either its `SASL_PLAINTEXT` or its PLAINTEXT flavour, plus topic creation
//! and the wait that absorbs raft commit latency.
//!
//! The suite needs both listener flavours — the config tests want a named
//! principal, the fetch tests only want the shortest path to a leader — so the
//! two starters and their two matching `CreateTopics` drivers are collected
//! here rather than duplicated per test module.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use krabka_protocol::{Decode, Encode};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::net::TcpStream;

use crate::wire::{round_trip, sasl_plain_authenticate};

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
pub fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = std::iter::once(super_user.to_string()).collect();

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

/// Start a single-broker PLAINTEXT cluster (no SASL).
/// Returns `(handle, _dir, addr)`.
pub async fn start_single_broker_plaintext() -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Create a topic through SASL/PLAIN as the given admin user.
/// Copied from `partition_reassignment.rs`.
pub async fn create_topic_as_admin(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, "admin", b"admin-secret")
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Create a topic through PLAINTEXT. There is no SASL, and the compat shim
/// allows everything.
pub async fn create_topic_plaintext(addr: SocketAddr, topic: &str, partitions: i32, rf: i16) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Await until `handle` sees `(topic, partition)` present in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}
