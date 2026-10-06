//! The fixtures that the `ConsumerGroupHeartbeat` and `ShareGroupHeartbeat`
//! handler tests share: ACLs for `User:alice`, topic records, and the
//! principal itself.

use krabka_metadata::MetadataRecord;

/// `Describe` on `Topic(name)`, allowed to `User:alice` from any host.
pub(super) fn describe_acl(name: &str) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(krabka_metadata::AclEntry {
        resource_type: krabka_metadata::ResourceType::Topic,
        resource_name: name.into(),
        pattern_type: krabka_metadata::PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: krabka_metadata::AclOperation::Describe,
        permission_type: krabka_metadata::PermissionType::Allow,
    })
}

/// `Read` on `Group(name)`, allowed to `User:alice` from any host.
pub(super) fn group_read_acl(name: &str) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(krabka_metadata::AclEntry {
        resource_type: krabka_metadata::ResourceType::Group,
        resource_name: name.into(),
        pattern_type: krabka_metadata::PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: krabka_metadata::AclOperation::Read,
        permission_type: krabka_metadata::PermissionType::Allow,
    })
}

/// A bare `V1Topic` record of `partitions` partitions.
fn topic_record(name: &str, topic_id: uuid::Uuid, partitions: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id,
        partitions,
        replication_factor: 1,
    })
}

/// A `V1Topic` record plus one `V1Partition` per index, assigned to
/// `node`. The KIP-631 wire framing does not carry `TopicRecord.partitions`
/// -- a decoded `V1Topic` round-trips back at `partitions == 0`, and the
/// real count comes from the `V1Partition` records that follow it -- so a
/// topic meant to be assignable needs both, unlike [`topic_record`] alone
/// (used only where a test never reaches the assignor).
pub(super) fn topic_with_partitions(
    name: &str,
    topic_id: uuid::Uuid,
    partitions: i32,
    node: krabka_raft::NodeId,
) -> Vec<MetadataRecord> {
    let replicas = vec![node];
    let mut records = vec![topic_record(name, topic_id, partitions)];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: name.into(),
            partition,
            leader: node,
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    }));
    records
}

/// The SASL/PLAIN principal `User:alice` that the ACLs above name.
pub(super) fn alice() -> krabka_security::Principal {
    krabka_security::Principal {
        name: "alice".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    }
}
