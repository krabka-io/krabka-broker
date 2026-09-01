//! The generated topic-config reference table, one row for each whitelisted
//! key.

use super::{
    CLEANUP_POLICY, COMPRESSION_TYPE, DELETE_RETENTION_MS, LOCAL_RETENTION_BYTES,
    LOCAL_RETENTION_MS, MAX_MESSAGE_BYTES, MIN_INSYNC_REPLICAS, REMOTE_STORAGE_ENABLE,
    RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES,
    delivery::{
        DELIVERY_MAX_DELAY_MS, DELIVERY_MODE, DELIVERY_MODE_IMMEDIATE, DELIVERY_SCHEDULE_MONOTONIC,
    },
    diskless::DISKLESS,
    qos::{DEFAULT_QOS_TIER, QOS_TIER},
    recovery::{UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
    schema::{
        SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_ID,
        SCHEMA_VALIDATION_VALUE,
    },
};

/// One whitelisted topic-config key, for the generated reference page.
#[derive(Debug, Clone, Copy)]
pub struct TopicConfigDoc {
    pub key: &'static str,
    pub value_type: &'static str,
    pub default: Option<&'static str>,
    pub kip: Option<&'static str>,
    pub description: &'static str,
}

const TOPIC_CONFIG_DOCS: &[TopicConfigDoc] = &[
    TopicConfigDoc {
        key: RETENTION_MS,
        value_type: "long (ms)",
        default: None,
        kip: None,
        description: "Retention time before log segments become eligible for deletion.",
    },
    TopicConfigDoc {
        key: RETENTION_BYTES,
        value_type: "long (bytes)",
        default: None,
        kip: None,
        description: "Maximum partition size before old segments are deleted.",
    },
    TopicConfigDoc {
        key: SEGMENT_BYTES,
        value_type: "int (bytes)",
        default: None,
        kip: None,
        description: "Target size of a single log segment file.",
    },
    TopicConfigDoc {
        key: CLEANUP_POLICY,
        value_type: "string",
        default: Some("delete"),
        kip: None,
        description: "`delete`, `compact`, or `compact,delete`.",
    },
    TopicConfigDoc {
        key: COMPRESSION_TYPE,
        value_type: "string",
        default: Some("producer"),
        kip: None,
        description: "Broker-side compression codec for the topic.",
    },
    TopicConfigDoc {
        key: MIN_INSYNC_REPLICAS,
        value_type: "int (>=1)",
        default: Some("1"),
        kip: None,
        description: "With acks=all, the minimum in-sync replicas required to accept a write; otherwise NOT_ENOUGH_REPLICAS (19).",
    },
    TopicConfigDoc {
        key: MAX_MESSAGE_BYTES,
        value_type: "int (bytes, >=0)",
        default: Some("1048588"),
        kip: None,
        description: "Largest record batch accepted for this topic, measured over the batch's whole wire encoding; a larger one is refused with MESSAGE_TOO_LARGE (10). Unset topics inherit the broker's message.max.bytes.",
    },
    TopicConfigDoc {
        key: UNCLEAN_LEADER_ELECTION_ENABLE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KIP-841"),
        description: "Allow electing an out-of-ISR replica as leader on ISR-empty failover (possible data loss).",
    },
    TopicConfigDoc {
        key: UNCLEAN_RECOVERY_STRATEGY,
        value_type: "string",
        default: Some("None"),
        kip: Some("KIP-966"),
        description: "Offset-aware unclean recovery: `None`, `Balanced`, or `Aggressive`. Supersedes unclean.leader.election.enable.",
    },
    TopicConfigDoc {
        key: REMOTE_STORAGE_ENABLE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KIP-405"),
        description: "Opt this topic into tiered (remote) storage.",
    },
    TopicConfigDoc {
        key: LOCAL_RETENTION_MS,
        value_type: "long (ms)",
        default: None,
        kip: Some("KIP-405"),
        description: "Local-tier retention time for tiered partitions.",
    },
    TopicConfigDoc {
        key: LOCAL_RETENTION_BYTES,
        value_type: "long (bytes)",
        default: None,
        kip: Some("KIP-405"),
        description: "Local-tier retention size budget for tiered partitions.",
    },
    TopicConfigDoc {
        key: DELETE_RETENTION_MS,
        value_type: "long (ms)",
        default: Some("86400000"),
        kip: Some("KIP-534"),
        description: "How long tombstones and transaction markers are retained after becoming compaction-eligible.",
    },
    TopicConfigDoc {
        key: QOS_TIER,
        value_type: "string",
        default: Some(DEFAULT_QOS_TIER),
        kip: None,
        description: "Krabka QoS tier used to partition producer quota buckets.",
    },
    TopicConfigDoc {
        key: DISKLESS,
        value_type: "boolean",
        default: Some("false"),
        kip: None,
        description: "Route this topic through the diskless WAL data path instead of the local log. Fixed when the topic is created, and exclusive with both remote.storage.enable and delivery.mode=scheduled.",
    },
    TopicConfigDoc {
        key: DELIVERY_MODE,
        value_type: "string",
        default: Some(DELIVERY_MODE_IMMEDIATE),
        kip: Some("KFC-1"),
        description: "`immediate` or `scheduled`. Under `scheduled` a batch stays invisible to consumers until its own timestamp comes due.",
    },
    TopicConfigDoc {
        key: DELIVERY_MAX_DELAY_MS,
        value_type: "long (ms)",
        default: Some("604800000"),
        kip: Some("KFC-1"),
        description: "Largest delivery delay accepted at produce time, measured forward from produce time; -1 removes the bound.",
    },
    TopicConfigDoc {
        key: DELIVERY_SCHEDULE_MONOTONIC,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-1"),
        description: "Reject a batch whose delivery time precedes the largest delivery time already in the partition.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_KEY,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-7"),
        description: "Validate the schema of every record key produced to this topic.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_VALUE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-7"),
        description: "Validate the schema of every record value produced to this topic.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_MODE,
        value_type: "string",
        default: Some(SCHEMA_VALIDATION_MODE_ID),
        kip: Some("KFC-7"),
        description: "`id` checks the Confluent header alone; `full` also decodes the record body against the schema the header names.",
    },
    TopicConfigDoc {
        key: crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
        value_type: "string",
        default: None,
        kip: Some("KIP-73"),
        description: "Replica list throttled on the leader side during reassignment.",
    },
    TopicConfigDoc {
        key: crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
        value_type: "string",
        default: None,
        kip: Some("KIP-73"),
        description: "Replica list throttled on the follower side during reassignment.",
    },
];

/// The full whitelist documented on the topic-configs reference page.
#[must_use]
pub fn topic_config_docs() -> Vec<TopicConfigDoc> {
    TOPIC_CONFIG_DOCS.to_vec()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{super::validation::is_recognized, *};

    #[test]
    fn topic_config_docs_cover_known_keys() {
        use std::collections::HashSet;
        let docs = topic_config_docs();
        let doc_keys: HashSet<&str> = docs.iter().map(|d| d.key).collect();
        // No duplicate keys in the doc table.
        assert!(
            doc_keys.len() == docs.len(),
            "duplicate key in topic_config_docs"
        );
        // Every documented key is recognized by the validator.
        for k in &doc_keys {
            assert!(
                is_recognized(k),
                "documented key `{k}` not recognized by validator"
            );
        }
        // Every recognized key is documented.
        for k in [
            RETENTION_MS,
            RETENTION_BYTES,
            SEGMENT_BYTES,
            CLEANUP_POLICY,
            COMPRESSION_TYPE,
            MIN_INSYNC_REPLICAS,
            MAX_MESSAGE_BYTES,
            UNCLEAN_LEADER_ELECTION_ENABLE,
            UNCLEAN_RECOVERY_STRATEGY,
            REMOTE_STORAGE_ENABLE,
            LOCAL_RETENTION_MS,
            LOCAL_RETENTION_BYTES,
            DELETE_RETENTION_MS,
            QOS_TIER,
            DISKLESS,
            DELIVERY_MODE,
            DELIVERY_MAX_DELAY_MS,
            DELIVERY_SCHEDULE_MONOTONIC,
            SCHEMA_VALIDATION_KEY,
            SCHEMA_VALIDATION_VALUE,
            SCHEMA_VALIDATION_MODE,
            crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
            crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
        ] {
            assert!(
                doc_keys.contains(k),
                "recognized key `{k}` missing from topic_config_docs"
            );
        }
        assert!(docs.iter().all(|d| !d.description.is_empty()));
    }
}
