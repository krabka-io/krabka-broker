//! ACL fixtures whose commit/application settling behavior matches quota suites.

use krabka_broker::BrokerHandle;
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};

/// Install the simple authorizer using exactly the broker's current super-user set.
pub fn use_simple_acl_authorizer(config: &mut krabka_broker::BrokerConfig) {
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
}

pub async fn seed_topic_acl(
    broker: &BrokerHandle,
    topic: &str,
    principal: &str,
    operation: AclOperation,
) {
    broker
        .submit_metadata_record_for_test(topic_acl_record(topic, principal, operation))
        .await
        .expect("seed topic ACL");
    // Preserve the fixtures' commit/application settle before their RPC retry checks.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

/// A literal Allow ACL on one topic and principal, without committing or waiting.
pub fn topic_acl_record(topic: &str, principal: &str, operation: AclOperation) -> MetadataRecord {
    literal_allow_acl(ResourceType::Topic, topic, principal, "*", operation)
}

/// A literal cluster Allow ACL, including the host the original request must match.
pub fn cluster_acl_record(principal: &str, host: &str, operation: AclOperation) -> MetadataRecord {
    literal_allow_acl(
        ResourceType::Cluster,
        "kafka-cluster",
        principal,
        host,
        operation,
    )
}

fn literal_allow_acl(
    resource_type: ResourceType,
    resource_name: &str,
    principal: &str,
    host: &str,
    operation: AclOperation,
) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type,
        resource_name: resource_name.into(),
        pattern_type: PatternType::Literal,
        principal: principal.into(),
        host: host.into(),
        operation,
        permission_type: PermissionType::Allow,
    })
}

// Literal expected Kafka bit positions, independent of production ACL helpers.
pub const BIT_READ: i32 = 1 << 3;
pub const BIT_WRITE: i32 = 1 << 4;
pub const BIT_CREATE: i32 = 1 << 5;
pub const BIT_DELETE: i32 = 1 << 6;
pub const BIT_ALTER: i32 = 1 << 7;
pub const BIT_DESCRIBE: i32 = 1 << 8;
pub const BIT_CLUSTER_ACTION: i32 = 1 << 9;
pub const BIT_DESCRIBE_CONFIGS: i32 = 1 << 10;
pub const BIT_ALTER_CONFIGS: i32 = 1 << 11;
pub const BIT_IDEMPOTENT_WRITE: i32 = 1 << 12;

pub const TOPIC_FULL_MASK: i32 = BIT_READ
    | BIT_WRITE
    | BIT_CREATE
    | BIT_DELETE
    | BIT_ALTER
    | BIT_DESCRIBE
    | BIT_DESCRIBE_CONFIGS
    | BIT_ALTER_CONFIGS;

/// A literal `Allow` binding on any resource, with the original wildcard host.
pub fn resource_allow_acl(
    resource_type: ResourceType,
    resource_name: &str,
    principal: &str,
    operation: AclOperation,
) -> MetadataRecord {
    literal_allow_acl(resource_type, resource_name, principal, "*", operation)
}

pub async fn seed_compat_shim_disable_acl(handle: &BrokerHandle) {
    seed_topic_acl(
        handle,
        "__compat_shim_disable__",
        "User:admin",
        AclOperation::Read,
    )
    .await;
}

pub async fn seed_alice_write_acl(handle: &BrokerHandle, topic: &str) {
    seed_topic_acl(handle, topic, "User:alice", AclOperation::Write).await;
}
