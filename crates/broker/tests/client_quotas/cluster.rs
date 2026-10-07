//! Fixtures that stand up the cluster the client-quota tests run against.
//!
//! The module starts a single-broker SASL/PLAINTEXT cluster with a
//! `SimpleAclAuthorizer`, creates topics as the super user, and seeds the ACL
//! records a test needs before the authorizer lets alice produce or fetch.

use std::net::SocketAddr;

use krabka_broker::BrokerHandle;

pub use crate::support::sasl::start_sasl_plaintext_with_acl_users as start_single_broker_sasl_plaintext_with_users;
use crate::{CLIENT_ID, kafka_wire};

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Creates a topic with SASL/PLAIN as admin. Asserts success.
pub async fn create_topic_as_admin(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
) {
    kafka_wire::create_topic_sasl(
        addr,
        CLIENT_ID,
        ("admin", b"admin-secret"),
        kafka_wire::topic(topic, partitions, replication_factor),
    )
    .await;
}

/// Waits until `handle` sees `(topic, partition)` in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: seed a dummy ACL to disable the compat shim (allow-all when no ACLs)
// ─────────────────────────────────────────────────────────────────────────────

pub async fn seed_compat_shim_disable_acl(handle: &BrokerHandle) {
    crate::support::acl::seed_topic_acl(
        handle,
        "__compat_shim_disable__",
        "User:admin",
        krabka_metadata::AclOperation::Read,
    )
    .await;
}

/// Seeds an ACL that allows alice to Write topic `topic`.
pub async fn seed_alice_write_acl(handle: &BrokerHandle, topic: &str) {
    crate::support::acl::seed_topic_acl(
        handle,
        topic,
        "User:alice",
        krabka_metadata::AclOperation::Write,
    )
    .await;
}

/// Seeds an ACL that allows alice to Read topic `topic`.
pub async fn seed_alice_read_acl(handle: &BrokerHandle, topic: &str) {
    crate::support::acl::seed_topic_acl(
        handle,
        topic,
        "User:alice",
        krabka_metadata::AclOperation::Read,
    )
    .await;
}
