//! ACL fixtures whose commit/application settling behavior matches quota suites.

use krabka_broker::BrokerHandle;
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};

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
    MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: topic.into(),
        pattern_type: PatternType::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: PermissionType::Allow,
    })
}
