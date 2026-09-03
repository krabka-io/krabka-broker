//! The `kafka-configs` tool: an `--alter --add-config` and `--describe`
//! round-trip over a topic configuration.

use assert2::assert;

use crate::jvm_acceptance::{
    broker0_advertised, docker_run_kafka_tool, docker_run_kafka_tool_allowing_failure,
    nc_check_connectivity, start_host_broker, tool_output,
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

/// `kafka-configs --alter --add-config` over every Kafka `TopicConfig` key
/// krabka registers, then `--describe` back.
///
/// The JVM tool parses each value with the type the broker reports for the
/// key, so a key krabka typed differently from Kafka fails here rather than
/// in a client months later. One `--alter` carries the whole set, as an
/// operator's own command would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trips_every_registered_topic_key() {
    const TOPIC: &str = "krabka-cfg-every-key-itest";
    /// Each key with a value Kafka's own `ConfigDef` accepts for it.
    const SETTINGS: &[(&str, &str)] = &[
        ("segment.ms", "60000"),
        ("segment.index.bytes", "10485760"),
        ("segment.jitter.ms", "0"),
        ("min.compaction.lag.ms", "0"),
        ("max.compaction.lag.ms", "86400000"),
        ("min.cleanable.dirty.ratio", "0.25"),
        ("file.delete.delay.ms", "60000"),
        ("flush.messages", "10000"),
        ("flush.ms", "1000"),
        ("index.interval.bytes", "8192"),
        ("preallocate", "false"),
        ("message.timestamp.type", "CreateTime"),
        ("message.timestamp.after.max.ms", "3600000"),
        ("message.timestamp.before.max.ms", "3600000"),
    ];

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

    let added = SETTINGS
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        &added,
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
    let rendered = String::from_utf8_lossy(&out.stdout);
    for (key, value) in SETTINGS {
        assert!(
            rendered.contains(&format!("{key}={value}")),
            "describe output missing {key}={value}: {rendered}"
        );
    }
}

/// `kafka-topics --create` with the override set Kafka Streams'
/// `RepartitionTopicConfig` sends on every internal repartition topic, and
/// the `WindowedChangelogTopicConfig` set whose `cleanup.policy` is exactly
/// `compact,delete`. A broker that refuses either cannot host a Streams
/// application.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_create_accepts_the_streams_internal_topic_configs() {
    /// (topic, the `--config` arguments Streams sends, the `--describe`
    /// substring that proves the broker stored them).
    const CASES: &[(&str, &[&str], &str)] = &[
        (
            "krabka-streams-repartition-itest",
            &[
                "cleanup.policy=delete",
                "segment.bytes=52428800",
                "retention.ms=-1",
                "message.timestamp.type=CreateTime",
            ],
            "cleanup.policy=delete",
        ),
        (
            "krabka-streams-windowed-changelog-itest",
            &[
                "cleanup.policy=compact,delete",
                "retention.ms=86400000",
                "min.compaction.lag.ms=0",
                "message.timestamp.type=CreateTime",
            ],
            "cleanup.policy=compact,delete",
        ),
    ];

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    for (topic, configs, expected) in CASES {
        let mut create = vec![
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
        ];
        for config in *configs {
            create.push("--config");
            create.push(config);
        }
        docker_run_kafka_tool(&create);

        let out = docker_run_kafka_tool(&[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
        ]);
        let rendered = String::from_utf8_lossy(&out.stdout);
        assert!(
            rendered.contains(expected),
            "{topic}: describe output missing {expected}: {rendered}"
        );
    }
}

/// Kafka's `LogConfig.validate` refuses tiered storage on a compacted topic,
/// and the JVM tool surfaces that as a `ConfigException` on the alter. The
/// refusal covers `compact,delete` as well, because Kafka tests the policy
/// list for membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_refuses_tiered_storage_on_a_compacted_topic() {
    const TOPIC: &str = "krabka-cfg-compact-tiered-itest";

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
        "--config",
        "cleanup.policy=compact,delete",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // The alter is the case, so the tool must be allowed to fail: it exits
    // non-zero and prints the broker's `InvalidConfigurationException`, which
    // is exactly the evidence this test wants.
    let out = docker_run_kafka_tool_allowing_failure(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "remote.storage.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let rendered = tool_output(&out);
    assert!(
        !out.status.success(),
        "the alter must be refused, not applied: {rendered}"
    );
    assert!(
        rendered.contains("Tiered storage is not supported for compacted topics"),
        "alter should be refused with Kafka's message: {rendered}"
    );
}
