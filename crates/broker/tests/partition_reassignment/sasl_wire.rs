//! SASL/PLAIN wire helpers for the authorization test.
//!
//! The deny test needs a principal that is not a super-user, so it runs against
//! a single-broker `SASL_PLAINTEXT` listener with `SimpleAclAuthorizer`
//! installed. This module holds the handshake, that cluster boot, and the
//! authenticated `CreateTopics` and `AlterPartitionReassignments` drivers.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerHandle, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use krabka_protocol::{self, Decode, Encode};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::net::TcpStream;

use crate::{
    kafka_wire,
    plaintext_wire::{CLIENT_ID, round_trip},
};

/// Opens a TCP stream to `addr` and authenticates it with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password).await
}

/// Starts a single-broker SASL/PLAINTEXT cluster and returns
/// `(handle, _dir, addr)`. `super_user` becomes the only super-user. `users`
/// is a slice of `(username, password)` pairs that this function adds to
/// `plain_credentials`.
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
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = std::iter::once(super_user.to_string()).collect();
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; default is `AllowAllAuthorizer`.
    // Clients reach this broker over SASL, but its own heartbeat reaches the
    // PLAINTEXT controller listener as ANONYMOUS; like a Kafka inter-broker
    // principal it needs `ClusterAction`, or the broker never unfences.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(
        cfg.super_users
            .iter()
            .cloned()
            .chain(std::iter::once("ANONYMOUS".to_string()))
            .collect(),
    ));

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

/// Creates a topic over SASL/PLAIN as the given admin user. Copied from
/// `create_topic_sasl_plain` in `elect_leaders.rs`.
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

/// Drives `AlterPartitionReassignments` over a SASL/PLAIN authenticated
/// connection. It returns `(topic_name, [(partition_index, error_code)])`
/// rows.
pub async fn drive_alter_reassignments_sasl_plain(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    use krabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    };

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<ReassignablePartition>> =
        std::collections::BTreeMap::new();
    for (topic, partition, target_opt) in rows {
        by_topic
            .entry(topic.to_string())
            .or_default()
            .push(ReassignablePartition {
                partition_index: partition,
                replicas: target_opt,
                ..Default::default()
            });
    }
    let topics: Vec<ReassignableTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopic {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let req = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: true,
        topics,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterPartitionReassignments");
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .expect("encode AlterPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 45, 1, 1, true, &body)
        .await
        .expect("AlterPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterPartitionReassignmentsResponse::decode(&mut cur, 1)
        .expect("decode AlterPartitionReassignmentsResponse");

    resp.responses
        .into_iter()
        .map(|r| {
            (
                r.name,
                r.partitions
                    .into_iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect()
}
