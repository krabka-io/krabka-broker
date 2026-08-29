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
