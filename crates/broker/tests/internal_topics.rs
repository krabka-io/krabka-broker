//! The `is_internal` flag on the topics the broker owns.
//!
//! The flag is what hides a topic from `AdminClient.listTopics()`, from
//! `kafka-topics --list --exclude-internal`, and from a pattern subscription
//! that leaves `exclude.internal.topics` at its default. A broker-owned topic
//! that answers `false` is one a `.*` mirroring or audit sink starts consuming
//! without anyone asking it to, so every such topic is pinned here.
//!
//! `Metadata` and `DescribeTopicPartitions` are both covered, because a client
//! must get the same answer whichever RPC it asks with. The audit log is
//! covered twice over, because `krabka.audit.topic` renames it: the flag has to
//! land on the name the broker is auditing to, and a name outside the `__`
//! convention has to be refused at startup rather than left internal on one
//! rule and freezable on the other.
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
#![cfg(test)]

use {
    crate::support::{
        client::connect_owned, discovery::topic_metadata_request, topics::creatable_topic,
    },
    assert2::{assert, check},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
    tempfile::{TempDir, tempdir},
};
mod support;

use {
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_protocol::{
        owned::{
            create_topics_request::CreateTopicsRequest,
            describe_topic_partitions_request::DescribeTopicPartitionsRequest,
            describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
            metadata_response::MetadataResponseTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    },
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

struct TopicClassification {
    process: Option<InProcess>,
    configured: Option<BrokerConfig>,
    custom_broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    audit_topic: String,
    metadata: Vec<MetadataResponseTopic>,
    described: Vec<DescribeTopicPartitionsResponseTopic>,
    startup_error: Option<String>,
}

#[fixture]
fn classification() -> TopicClassification {
    // Start this scenario with fresh inputs and observations.

    TopicClassification {
        process: None,
        configured: None,
        custom_broker: None,
        directory: None,
        audit_topic: String::new(),
        metadata: Vec::new(),
        described: Vec::new(),
        startup_error: None,
    }
}

#[given("a broker hosts these topic names")]
async fn hosted_topics(classification: &mut TopicClassification, datatable: Vec<Vec<String>>) {
    let process = support::start().await;
    let topics = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [name]: [String; 1] = row.try_into().expect("one topic name");
            creatable_topic().name(name).call()
        })
        .collect();
    let response = process
        .client
        .send(CreateTopicsRequest {
            topics,
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    for row in &response.topics {
        check!(
            row.error_code == 0 || row.error_code == TOPIC_ALREADY_EXISTS,
            "create {:?}: {row:?}",
            row.name
        );
    }
    classification.process = Some(process);
}

#[given("a broker configuration names its audit topic {name}")]
fn configured_audit(classification: &mut TopicClassification, name: String) {
    let directory = tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(directory.path().to_path_buf());
    config.audit_topic.clone_from(&name);
    classification.audit_topic = name;
    classification.configured = Some(config);
    classification.directory = Some(directory);
}

#[when("the configured audit broker starts")]
async fn start_audit_broker(classification: &mut TopicClassification) {
    let config = classification
        .configured
        .take()
        .expect("an audit configuration was supplied");
    match Broker::start(config).await {
        Ok(broker) => classification.custom_broker = Some(broker),
        Err(error) => classification.startup_error = Some(error.to_string()),
    }
}

#[when("all topic metadata is requested")]
async fn request_metadata(classification: &mut TopicClassification) {
    let client = if let Some(process) = &classification.process {
        process.client.clone()
    } else {
        let broker = classification
            .custom_broker
            .as_ref()
            .expect("the audit broker started");
        broker
            .wait_until_partition_present(&classification.audit_topic, 0)
            .await;
        connect_owned(
            broker.listen_addr().to_string(),
            "krabka-broker-test-renamed-audit",
            "client build",
        )
        .await
    };
    classification.metadata = client
        .send(topic_metadata_request(None))
        .await
        .expect("Metadata")
        .topics;
}

#[when("all topic metadata and partition descriptions are requested")]
async fn request_both_descriptions(classification: &mut TopicClassification) {
    request_metadata(classification).await;
    let process = classification
        .process
        .as_ref()
        .expect("the topic fixture was started");
    classification.described = process
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: Vec::new(),
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions")
        .topics;
}

fn expected_internal_flags(rows: Vec<Vec<String>>) -> Vec<(String, bool)> {
    rows.into_iter()
        .skip(1)
        .map(|row| {
            let [name, internal]: [String; 2] =
                row.try_into().expect("topic name and internal flag");
            (name, internal.parse().expect("internal boolean"))
        })
        .collect()
}

#[then("metadata reports these topic classifications")]
fn metadata_flags(classification: &TopicClassification, datatable: Vec<Vec<String>>) {
    for (name, is_internal) in expected_internal_flags(datatable) {
        assert!(let Some(row) = classification.metadata.iter().find(|topic| topic.name.as_deref() == Some(name.as_str())), "no metadata row for {name}");
        check!(
            row.topic_id != WireUuid::default(),
            "{name} has no topic id"
        );
        check!(
            without_partitions(row)
                == MetadataResponseTopic {
                    error_code: 0,
                    name: Some(name.clone()),
                    topic_id: row.topic_id,
                    is_internal,
                    partitions: Vec::new(),
                    topic_authorized_operations: i32::MIN,
                    ..Default::default()
                },
            "{name}"
        );
    }
}

#[then("partition descriptions agree with metadata on these classifications")]
fn partition_flags(classification: &TopicClassification, datatable: Vec<Vec<String>>) {
    for (name, is_internal) in expected_internal_flags(datatable) {
        assert!(let Some(row) = classification.described.iter().find(|topic| topic.name.as_deref() == Some(name.as_str())), "no describe row for {name}");
        assert!(let Some(metadata) = classification.metadata.iter().find(|topic| topic.name.as_deref() == Some(name.as_str())), "no metadata row for {name}");
        check!(
            DescribeTopicPartitionsResponseTopic {
                partitions: Vec::new(),
                ..row.clone()
            } == DescribeTopicPartitionsResponseTopic {
                error_code: 0,
                name: Some(name.clone()),
                topic_id: metadata.topic_id,
                is_internal,
                partitions: Vec::new(),
                topic_authorized_operations: TOPIC_FULL_MASK,
                ..Default::default()
            },
            "{name}"
        );
    }
}

#[then("the renamed audit topic is internal and the default audit name is not")]
fn renamed_audit_flag(classification: &TopicClassification) {
    let internal = classification
        .metadata
        .iter()
        .filter(|topic| topic.is_internal)
        .filter_map(|topic| topic.name.as_deref())
        .collect::<Vec<_>>();
    check!(
        internal.contains(&classification.audit_topic.as_str()),
        "{internal:?}"
    );
    check!(
        !internal.contains(&"__krabka_audit"),
        "the default name is not the audit log on this broker: {internal:?}"
    );
}

#[then("startup refuses the invalid audit_topic configuration")]
fn audit_startup_refusal(classification: &TopicClassification) {
    assert!(let Some(error) = &classification.startup_error);
    check!(error.contains("audit_topic"), "{error}");
}

async fn stop_classification(classification: TopicClassification) {
    if let Some(process) = classification.process {
        process.broker.shutdown().await;
    }
    if let Some(broker) = classification.custom_broker {
        broker.shutdown().await;
    }
}

#[scenario(
    path = "tests/features/migrated/tests/internal_topics.feature",
    name = "Metadata classifies broker-owned topics without relying on their prefix alone"
)]
#[tokio::test]
async fn metadata_marks_every_broker_owned_topic_internal(classification: TopicClassification) {
    stop_classification(classification).await;
}

#[scenario(
    path = "tests/features/migrated/tests/internal_topics.feature",
    name = "Partition descriptions preserve the same topic classification and identity"
)]
#[tokio::test]
async fn describe_topic_partitions_agrees_with_metadata(classification: TopicClassification) {
    stop_classification(classification).await;
}

#[scenario(
    path = "tests/features/migrated/tests/internal_topics.feature",
    name = "Renaming the audit topic moves its internal classification to the configured name"
)]
#[tokio::test]
async fn a_renamed_audit_topic_is_the_internal_one(classification: TopicClassification) {
    stop_classification(classification).await;
}

#[scenario(
    path = "tests/features/migrated/tests/internal_topics.feature",
    name = "An audit topic outside the internal naming convention prevents startup"
)]
#[tokio::test]
async fn a_broker_refuses_an_audit_topic_outside_the_convention(
    classification: TopicClassification,
) {
    stop_classification(classification).await;
}
