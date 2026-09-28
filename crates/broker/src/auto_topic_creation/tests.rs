use std::path::PathBuf;

use assert2::{assert, check};
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreatableTopicConfig};

use super::*;

fn configs(pairs: &[(&str, &str)]) -> Vec<CreatableTopicConfig> {
    pairs
        .iter()
        .map(|(name, value)| CreatableTopicConfig {
            name: (*name).to_owned(),
            value: Some((*value).to_owned()),
            ..Default::default()
        })
        .collect()
}

fn topic(
    name: &str,
    num_partitions: i32,
    replication_factor: i16,
    configs: Vec<CreatableTopicConfig>,
) -> CreatableTopic {
    CreatableTopic {
        name: name.to_owned(),
        num_partitions,
        replication_factor,
        configs,
        ..Default::default()
    }
}

/// Kafka's `DefaultAutoTopicCreationManager.creatableTopic` gives each
/// coordinator topic its configured partition count, replication factor and
/// topic configs. The values below differ per topic, so a mix-up of two
/// settings shows.
#[test]
fn coordinator_topic_uses_each_topics_configured_shape() {
    let mut config = BrokerConfig::for_tests(PathBuf::from("/tmp"));
    config.offsets_topic_num_partitions = 11;
    config.offsets_topic_replication_factor = 3;
    config.transaction_state_num_partitions = 12;
    config.transaction_state_replication_factor = 2;
    config.transaction_state_min_isr = 2;
    config.share_coordinator.state_topic_num_partitions = 13;
    config.share_coordinator.state_topic_replication_factor = 4;
    config.share_coordinator.state_topic_min_isr = 3;
    config.barrier_state_num_partitions = 14;
    config.barrier_state_replication_factor = 5;

    let cases = [
        (
            crate::coordinator::bootstrap::OFFSETS_TOPIC,
            Some(topic(
                crate::coordinator::bootstrap::OFFSETS_TOPIC,
                11,
                3,
                configs(&[
                    ("cleanup.policy", "compact"),
                    ("compression.type", "producer"),
                    ("segment.bytes", "104857600"),
                ]),
            )),
        ),
        (
            crate::txn::bootstrap::TOPIC,
            Some(topic(
                crate::txn::bootstrap::TOPIC,
                12,
                2,
                configs(&[
                    ("cleanup.policy", "compact"),
                    ("compression.type", "uncompressed"),
                    ("min.insync.replicas", "2"),
                    ("segment.bytes", "104857600"),
                    ("unclean.leader.election.enable", "false"),
                ]),
            )),
        ),
        (
            crate::share_coordinator::bootstrap::TOPIC,
            Some(topic(
                crate::share_coordinator::bootstrap::TOPIC,
                13,
                4,
                configs(&[
                    ("cleanup.policy", "delete"),
                    ("compression.type", "producer"),
                    ("min.insync.replicas", "3"),
                    ("retention.ms", "-1"),
                    ("segment.bytes", "104857600"),
                ]),
            )),
        ),
        (
            crate::barrier::STATE_TOPIC,
            Some(topic(
                crate::barrier::STATE_TOPIC,
                14,
                5,
                configs(&[("cleanup.policy", "compact")]),
            )),
        ),
        ("orders", None),
        ("__remote_log_metadata", None),
    ];
    for (name, expected) in cases {
        check!(coordinator_topic(&config, name) == expected, "{name}");
    }
}

/// A name that a creation holds is skipped until that creation ends, as
/// Kafka's `filterCreatableTopics` skips a name in `inflightTopics`.
#[test]
fn a_name_in_flight_is_skipped_until_its_creation_ends() {
    let creation = AutoTopicCreation::default();
    assert!(creation.begin("__consumer_offsets"));
    check!(!creation.begin("__consumer_offsets"));
    check!(creation.begin("__transaction_state"));
    check!(creation.is_in_flight("__consumer_offsets"));
    creation.end("__consumer_offsets");
    check!(!creation.is_in_flight("__consumer_offsets"));
    check!(creation.begin("__consumer_offsets"));
}

/// Before the broker binds itself, a request creates nothing, and its caller
/// retries as it does for any other `COORDINATOR_NOT_AVAILABLE`.
#[tokio::test]
async fn an_unbound_request_creates_nothing() {
    let creation = Arc::new(AutoTopicCreation::default());
    creation.request(crate::coordinator::bootstrap::OFFSETS_TOPIC);
    check!(creation.started() == 0);
    check!(!creation.is_in_flight(crate::coordinator::bootstrap::OFFSETS_TOPIC));
}

/// A bound broker refuses a name that is not a coordinator topic: an
/// ordinary topic is not the component's to create.
#[tokio::test]
async fn a_request_for_an_ordinary_topic_creates_nothing() {
    let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
    })
    .await;
    let broker = handle.broker_arc_for_test();
    broker.auto_topic_creation.request("orders");
    check!(broker.auto_topic_creation.started() == 0);
    check!(broker.controller.current_image().topic("orders").is_none());
    handle.shutdown().await;
}
