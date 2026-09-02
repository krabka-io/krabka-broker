//! The `kafka-configs` tool: an `--alter --add-config` and `--describe`
//! round-trip over a topic configuration.

use assert2::assert;

use crate::jvm_acceptance::{
    broker0_advertised, docker_run_kafka_tool, nc_check_connectivity, start_host_broker,
};

/// `kafka-configs --alter --add-config retention.ms=60000 --topic t` then
/// `--describe` round-trips through `V1TopicConfig` and the supervisor
/// reconcile push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trip() {
    const TOPIC: &str = "krabka-cfg-alter-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "retention.ms=60000",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("retention.ms=60000"),
        "describe output missing retention.ms=60000: {s}"
    );
}

/// `kafka-configs --describe --all` renders the effective configuration and
/// the provenance of every value in it (KIP-226).
///
/// This is the surface an operator uses to tell a dynamic override from an
/// inherited default, and it only works if the broker types each key and
/// returns its synonym chain. The JVM tool prints
/// `name=value sensitive=… synonyms={SOURCE:name=value, …}`, so the three
/// assertions below are the three cases the chain has: a topic override, a
/// value inherited from the cluster-wide default, and a key nobody has set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_describe_all_shows_effective_values_and_their_sources() {
    const TOPIC: &str = "krabka-cfg-describe-all-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "retention.ms=60000",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    // The cluster-wide default the topic will inherit.
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-default",
        "--add-config",
        "unclean.leader.election.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--all",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let rendered = String::from_utf8_lossy(&out.stdout);

    for (label, expected) in [
        (
            "the topic override sits at the head of its chain",
            "retention.ms=60000 sensitive=false \
             synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=60000}",
        ),
        (
            "an inherited value names the cluster default above the built-in one",
            "unclean.leader.election.enable=true sensitive=false \
             synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:unclean.leader.election.enable=true, \
             DEFAULT_CONFIG:unclean.leader.election.enable=false}",
        ),
        (
            "a key nobody set still reports its effective value",
            "cleanup.policy=delete sensitive=false synonyms={}",
        ),
    ] {
        assert!(
            rendered.contains(expected),
            "{label}: expected {expected:?} in {rendered}"
        );
    }
}
