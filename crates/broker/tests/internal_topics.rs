//! The `is_internal` flag on the topics the broker owns.
//!
//! The flag is what hides a topic from `AdminClient.listTopics()`, from
//! `kafka-topics --list --exclude-internal`, and from a pattern subscription
//! that leaves `exclude.internal.topics` at its default. A broker-owned topic
//! that answers `false` is one a `.*` mirroring or audit sink starts consuming
//! without anyone asking it to, so every such topic is pinned here.
//!
//! `Metadata` and `DescribeTopicPartitions` are both covered, because a client
//! must get the same answer whichever RPC it asks with.
//!
//! # The one name the `__` convention gets wrong
//!
//! `__remote_log_metadata` carries broker-owned tiered-storage state and Kafka
//! still reports it as an ordinary topic, so the flag has to read a set of
//! names rather than the `__` prefix. That was settled against the pinned
//! images, two ways that agree: the `INTERNAL_TOPICS` set in
//! `org.apache.kafka.common.internals.Topic` inside each image's
//! `kafka-clients` jar, and creating every name below on a running broker and
//! diffing `kafka-topics --list` against `kafka-topics --list
//! --exclude-internal`. `apache/kafka:4.3.1` and `apache/kafka:4.0.0` hide
//! `__consumer_offsets`, `__transaction_state` and `__share_group_state` and
//! nothing else; `confluentinc/cp-kafka:7.5.0` predates KIP-932 and hides the
//! first two. Krabka has a share coordinator, so it follows the 4.x answer.

use assert2::{assert, check};
mod support;

use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        describe_topic_partitions_request::DescribeTopicPartitionsRequest,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponseTopic,
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's error code for a topic that already exists.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// KIP-430 bits for every topic operation Krabka supports, which is what the
/// allow-all authorizer behind `support::start` grants.
///
/// `DescribeTopicPartitions` has no opt-in flag for the bitfield on its v0
/// schema, so every row carries it and the whole-row comparison below has to
/// name it. The `describe_topic_partitions` suite pins the mask itself.
const TOPIC_FULL_MASK: i32 = (1 << 3)  // Read
    | (1 << 4)  // Write
    | (1 << 5)  // Create
    | (1 << 6)  // Delete
    | (1 << 7)  // Alter
    | (1 << 8)  // Describe
    | (1 << 10) // DescribeConfigs
    | (1 << 11); // AlterConfigs

/// Every topic the broker owns, plus the three names that prove the flag reads
/// a set of names rather than the `__` prefix.
const TOPICS: [(&str, bool); 9] = [
    ("__consumer_offsets", true),
    ("__transaction_state", true),
    ("__share_group_state", true),
    ("__krabka_audit", true),
    ("__barrier_state", true),
    ("__diskless_wal_index", true),
    ("__remote_log_metadata", false),
    ("__user_topic", false),
    ("orders", false),
];

/// Create every name in [`TOPICS`], so one metadata sweep sees them all.
///
/// A broker-owned topic is normally created by the subsystem that owns it, on
/// the first request that needs it. Driving `CreateTopics` instead puts all of
/// them in the image without booting six subsystems, and the flag is a
/// property of the name rather than of who wrote the record.
async fn create_every_topic(p: &support::InProcess) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: TOPICS
                .iter()
                .map(|(name, _)| CreatableTopic {
                    name: (*name).into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    for row in &resp.topics {
        // `__consumer_offsets` is bootstrapped with 50 partitions before the
        // first client request, so its row comes back as already-existing.
        check!(
            row.error_code == 0 || row.error_code == TOPIC_ALREADY_EXISTS,
            "create {:?}: {row:?}",
            row.name,
        );
    }
}

/// The row as it arrives minus its partitions, so one comparison covers every
/// other field at once.
///
/// Partition counts differ per topic — `__consumer_offsets` is bootstrapped
/// with 50 — and the partition shape is pinned by the `Metadata` and
/// `DescribeTopicPartitions` suites already.
fn without_partitions(row: &MetadataResponseTopic) -> MetadataResponseTopic {
    MetadataResponseTopic {
        partitions: Vec::new(),
        ..row.clone()
    }
}

#[tokio::test]
async fn metadata_marks_every_broker_owned_topic_internal() {
    let p = support::start().await;
    create_every_topic(&p).await;

    let resp = p
        .client
        .send(MetadataRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .expect("Metadata");

    for (name, is_internal) in TOPICS {
        assert!(
            let Some(row) = resp.topics.iter().find(|t| t.name.as_deref() == Some(name)),
            "no metadata row for {name}"
        );
        // A default topic id would make the whole-row comparison below pass on
        // a row the broker never filled in.
        check!(
            row.topic_id != WireUuid::default(),
            "{name} has no topic id"
        );
        check!(
            without_partitions(row)
                == MetadataResponseTopic {
                    error_code: 0,
                    name: Some(name.to_string()),
                    topic_id: row.topic_id,
                    is_internal,
                    partitions: Vec::new(),
                    // The request did not opt in to KIP-430, so the bitfield
                    // keeps its sentinel.
                    topic_authorized_operations: i32::MIN,
                    ..Default::default()
                },
            "{name}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn describe_topic_partitions_agrees_with_metadata() {
    let p = support::start().await;
    create_every_topic(&p).await;

    let metadata = p
        .client
        .send(MetadataRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .expect("Metadata");
    let described = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: Vec::new(),
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    for (name, is_internal) in TOPICS {
        assert!(
            let Some(row) = described
                .topics
                .iter()
                .find(|t| t.name.as_deref() == Some(name)),
            "no describe row for {name}"
        );
        assert!(
            let Some(metadata_row) = metadata
                .topics
                .iter()
                .find(|t| t.name.as_deref() == Some(name)),
            "no metadata row for {name}"
        );
        check!(
            DescribeTopicPartitionsResponseTopic {
                partitions: Vec::new(),
                ..row.clone()
            } == DescribeTopicPartitionsResponseTopic {
                error_code: 0,
                name: Some(name.to_string()),
                topic_id: metadata_row.topic_id,
                is_internal,
                partitions: Vec::new(),
                topic_authorized_operations: TOPIC_FULL_MASK,
                ..Default::default()
            },
            "{name}"
        );
    }

    p.broker.shutdown().await;
}
