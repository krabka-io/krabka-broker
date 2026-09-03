//! Cluster setup and metadata seeding for the tuple-quota test: the
//! single-broker SASL/PLAINTEXT boot, the admin `CreateTopics` call, and the
//! two ACL records the authorizer needs before alice may produce.
//!
//! The seeding is here rather than in the test body because one of the two ACLs
//! exists only to disable the compatibility shim, which allows every operation
//! while the image holds no ACL at all.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::tuple_quota_wire::{round_trip, sasl_plain_authenticate};

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers (copied from client_quotas.rs)
// ─────────────────────────────────────────────────────────────────────────────

/// Starts a single-broker SASL/PLAINTEXT cluster. Returns
/// `(handle, _dir, addr)`.
pub(crate) fn start_single_broker_sasl_plaintext_with_users(
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

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

/// Creates a topic over SASL/PLAIN as admin, and asserts that it succeeds.
pub(crate) async fn create_topic_as_admin(
    addr: SocketAddr,
    password: &[u8],
    topic: &str,
    partitions: i32,
    replication_factor: i16,
) {
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
    let mut stream = sasl_plain_authenticate(addr, "admin", password)
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

/// Waits until `handle` sees `(topic, partition)` in its image.
pub(crate) async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: seed a dummy ACL to disable the compat shim (allow-all when no ACLs)
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) async fn seed_compat_shim_disable_acl(handle: &BrokerHandle) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "__compat_shim_disable__".to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed dummy ACL to disable compat shim");
    // Small pause to absorb raft commit-then-apply gap.
    // real-time wait (not a progress poll): raft commit-then-apply settle, no local condition to poll
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Seeds an ACL that allows alice to Write to the topic `topic`.
pub(crate) async fn seed_alice_write_acl(handle: &BrokerHandle, topic: &str) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: topic.to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Write,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed alice Write ACL");
    // intentional: absorb raft commit-then-apply gap; ACL propagation to the
    // request handler's image snapshot has no awaiter/metric to poll.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
