//! Scenario 4: the KIP-1155 mixed-version downgrade safety gate.
//!
//! Kafka 4.0 predates KIP-1155 and advertises no downgrade capability, so both
//! safe and unsafe online `metadata.version` downgrades must be rejected while
//! that node is registered. The scenario drives `kafka-features.sh` rather than
//! the replication path, and it needs its own registration waits, so it does
//! not share a file with the truncation scenarios.

use std::time::{Duration, Instant};

use krabka_broker::BrokerHandle;

use crate::{
    docker::{KAFKA_IMAGE_FEATURES, docker_run_kafka_tool_with_image},
    mixed_cluster::{MixedCluster, start_mixed_cluster},
    support,
    topic_admin::create_mixed_topic,
};

fn run_features(bootstrap: &str, command: &[&str]) -> std::process::Output {
    let mut args = vec![
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap,
    ];
    args.extend_from_slice(command);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_FEATURES, &args)
}

/// Default text a JVM broker returns for `UNKNOWN_SERVER_ERROR`. A
/// broker-only JVM node answers with it when its controller forward fails.
const JVM_FORWARD_FAILURE: &str = "The server experienced an unexpected error";

/// Run `kafka-features.sh` until a Krabka broker serves the request.
///
/// The `AdminClient` fetches metadata from a random bootstrap node and sends
/// `UpdateFeatures` to the node that the response names as controller. Krabka
/// names the raft leader, which is a Krabka broker. A `KRaft` JVM broker names
/// a random live broker, so it can name itself. `bootstrap` names the JVM
/// broker too. A broker-only JVM node forwards admin writes to the controller
/// with the KIP-590 `Envelope` RPC. Krabka does not implement `Envelope` (see
/// `docs/KIP_MATRIX.md`). The JVM node then answers `UNKNOWN_SERVER_ERROR`
/// with its default text, and that answer says nothing about Krabka's
/// validation. Retry on that text until a Krabka broker serves the request.
/// Any other outcome returns at once.
async fn run_features_on_krabka(bootstrap: &str, command: &[&str]) -> std::process::Output {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = run_features(bootstrap, command);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !text.contains(JVM_FORWARD_FAILURE) {
            return output;
        }
        eprintln!(
            "KRABKA[kip320] kafka-features {command:?} landed on the JVM broker \
             (Envelope forward failed); retrying"
        );
        assert2::assert!(
            Instant::now() <= deadline,
            "kafka-features {command:?} never reached a Krabka broker: {text}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Block until every Krabka broker sees the JVM broker (id 3) advertise
/// `expected` as its `metadata.version` maximum. The `AdminClient` can route
/// `UpdateFeatures` to either Krabka broker, so both images must hold the
/// registration before the test sends a downgrade.
async fn wait_for_jvm_metadata_max(cluster: &MixedCluster, expected: i16) {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let observed = cluster
            .krabka
            .iter()
            .map(|(broker, _)| {
                broker
                    .controller_image_for_test()
                    .broker(krabka_broker::NodeId(3))
                    .and_then(|registration| {
                        registration
                            .features
                            .get(krabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
                            .map(|(_, max)| *max)
                    })
            })
            .collect::<Vec<_>>();
        if observed.iter().all(|max| *max == Some(expected)) {
            return;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "JVM broker did not advertise metadata.version max {expected} on every Krabka \
             broker; observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Block until every Krabka broker's image holds a controller registration
/// for every voter. `UpdateFeatures` rejects a `metadata.version` downgrade
/// with "Controller N has not registered" before it checks the downgrade
/// capability. The test must not send the downgrade before those
/// registrations land, or it asserts on the wrong rejection text.
async fn wait_for_voter_registrations(cluster: &MixedCluster) {
    for (broker, _) in &cluster.krabka {
        broker
            .wait_for_image(|image| {
                image
                    .voters()
                    .iter()
                    .all(|voter| image.controller(voter.id).is_some())
            })
            .await;
    }
}

/// KIP-1155 mixed-version safety: Kafka 4.0 predates the proposed online
/// downgrade capability. It must block both safe and unsafe downgrades; unsafe
/// permits record loss, but never permits a node that cannot perform the
/// immediate snapshot/reload protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + published controller/data ports; Linux-bound"]
async fn metadata_version_downgrade_rejects_pre_kip1155_jvm() {
    const EXISTING_TOPIC: &str = "krabka-mv-capability-existing";
    const UPPER_LEVEL: i16 = 25; // 4.0-IV3.

    let container = support::unique_container_name("krabka-mv-capability-jvm-broker");

    let cluster = start_mixed_cluster(&container, false).await;
    assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster"
    );
    wait_for_jvm_metadata_max(&cluster, UPPER_LEVEL).await;
    wait_for_voter_registrations(&cluster).await;
    create_mixed_topic(&cluster.bootstrap_all, EXISTING_TOPIC).await;
    let state = |broker: &BrokerHandle| {
        let image = broker.controller_image_for_test();
        (
            image.finalized_metadata_version(),
            image
                .brokers()
                .map(|registration| (registration.node_id, registration.log_dirs.clone()))
                .collect::<Vec<_>>(),
            image
                .partition(EXISTING_TOPIC, 0)
                .expect("existing mixed topic")
                .directories
                .clone(),
        )
    };
    let before = cluster
        .krabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    let image = cluster.krabka[0].0.controller_image_for_test();
    assert!(
        !image
            .broker(krabka_broker::NodeId(3))
            .expect("Kafka 4.0 registration")
            .features
            .contains_key(krabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_FEATURE),
        "pre-KIP-1155 JVM registration unexpectedly advertised downgrade capability"
    );

    for (kind, command) in [
        ("safe", vec!["downgrade", "--metadata", "3.7-IV1"]),
        (
            "unsafe",
            vec!["downgrade", "--metadata", "3.7-IV1", "--unsafe"],
        ),
    ] {
        let output = run_features_on_krabka(&cluster.bootstrap_all, &command).await;
        let error = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert2::assert!(
            !output.status.success()
                && error.contains("Broker 3")
                && error.contains("does not support online metadata.version downgrade"),
            "{kind} downgrade did not reject the pre-capability JVM node: {error}"
        );
    }

    let after = cluster
        .krabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    assert!(
        after == before,
        "rejected mixed-version downgrade changed finalized or directory metadata"
    );
    cluster.shutdown().await;
}
