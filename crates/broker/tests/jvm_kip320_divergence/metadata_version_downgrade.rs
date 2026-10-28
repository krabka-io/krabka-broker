//! Scenario 4: a lossy `metadata.version` downgrade in a mixed cluster.
//!
//! Kafka's `FeatureControlManager.updateMetadataVersion` refuses every
//! downgrade that crosses a level whose `didMetadataChange` is set, with
//! either downgrade type. The scenario drives `kafka-features.sh` rather than
//! the replication path, and it needs its own registration waits, so it does
//! not share a file with the truncation scenarios.

use {
    crate::{
        docker::{KAFKA_IMAGE_FEATURES, docker_run_kafka_tool_with_image},
        mixed_cluster::{MixedCluster, start_mixed_cluster},
        support,
        topic_admin::create_mixed_topic,
    },
    krabka_metadata::{MetadataImage, metadata_version},
    std::{
        process::Output,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time,
};

fn run_features(bootstrap: &str, command: &[&str]) -> Output {
    let mut args = vec![
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap,
    ];
    args.extend_from_slice(command);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_FEATURES, &args)
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
                            .get(metadata_version::METADATA_VERSION_FEATURE)
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
        time::sleep(Duration::from_millis(500)).await;
    }
}

/// Block until every Krabka broker's image holds a controller registration
/// for every voter. `UpdateFeatures` rejects any update with "controller N has
/// not registered" before it looks at the downgrade itself. The test must not
/// send the downgrade before those registrations land, or it asserts on the
/// wrong rejection text.
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

#[derive(Default)]
struct MetadataDowngrade {
    cluster: Option<MixedCluster>,
    topic: String,
    before: Vec<Arc<MetadataImage>>,
    output: Option<Output>,
}
#[rstest::fixture]
fn metadata_downgrade() -> MetadataDowngrade {
    // A mixed cluster and its metadata baseline belong to this downgrade sequence.
    MetadataDowngrade::default()
}
#[rstest_bdd_macros::given(
    "a three-broker mixed cluster at metadata capability {upper_level} with topic {topic}"
)]
async fn mixed_metadata_baseline(
    metadata_downgrade: &mut MetadataDowngrade,
    upper_level: i16,
    topic: String,
) {
    let container = support::unique_container_name("krabka-mv-capability-jvm-broker");

    let cluster = start_mixed_cluster(&container, false).await;
    assert2::assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster"
    );
    wait_for_jvm_metadata_max(&cluster, upper_level).await;
    wait_for_voter_registrations(&cluster).await;
    create_mixed_topic(&cluster.bootstrap_all, &topic).await;
    // The JVM reports its replica directory asynchronously after topic
    // creation. Wait for that report before capturing the metadata baseline;
    // other replicas of this empty topic may retain an unassigned slot.
    for (broker, _) in &cluster.krabka {
        broker
            .wait_for_image(|image| {
                image.partition(&topic, 0).is_some_and(|partition| {
                    partition
                        .replicas
                        .iter()
                        .position(|id| id.0 == 3)
                        .and_then(|slot| partition.directories.get(slot))
                        .is_some_and(|directory| {
                            !directory.is_nil()
                                && image.broker(krabka_broker::NodeId(3)).is_some_and(
                                    |registration| registration.log_dirs.contains(directory),
                                )
                        })
                })
            })
            .await;
    }

    metadata_downgrade.before = cluster
        .krabka
        .iter()
        .map(|(broker, _)| broker.controller_image_for_test())
        .collect();
    metadata_downgrade.cluster = Some(cluster);
    metadata_downgrade.topic = topic;
}
#[rstest_bdd_macros::when("Kafka features runs downgrade arguments {arguments}")]
fn request_metadata_downgrade(metadata_downgrade: &mut MetadataDowngrade, arguments: String) {
    let arguments: Vec<String> = serde_json::from_str(&arguments).expect("feature CLI arguments");
    let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    metadata_downgrade.output = Some(run_features(
        &metadata_downgrade
            .cluster
            .as_ref()
            .expect("mixed cluster")
            .bootstrap_all,
        &arguments,
    ));
}
#[rstest_bdd_macros::then("the metadata downgrade fails with message {message}")]
fn refused_metadata_downgrade(metadata_downgrade: &MetadataDowngrade, message: String) {
    let output = metadata_downgrade
        .output
        .as_ref()
        .expect("downgrade command executed");
    let error = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert2::assert!(
        !output.status.success() && error.contains(&message),
        "downgrade was not refused as lossy: {error}"
    );
}
#[rstest_bdd_macros::then(
    "finalized metadata versions, broker log directories and topic directories equal their baseline"
)]
fn unchanged_metadata_directory_state(metadata_downgrade: &MetadataDowngrade) {
    let cluster = metadata_downgrade.cluster.as_ref().expect("mixed cluster");
    for ((broker, _), before) in cluster.krabka.iter().zip(&metadata_downgrade.before) {
        let after = broker.controller_image_for_test();
        let topic = &metadata_downgrade.topic;
        assert2::assert!(after.finalized_metadata_version() == before.finalized_metadata_version());
        let previous = before
            .brokers()
            .map(|registration| (registration.node_id, registration.log_dirs.clone()))
            .collect::<Vec<_>>();
        let current = after
            .brokers()
            .map(|registration| (registration.node_id, registration.log_dirs.clone()))
            .collect::<Vec<_>>();
        assert2::assert!(current == previous);
        assert2::assert!(
            after
                .partition(topic, 0)
                .expect("existing topic")
                .directories
                == before
                    .partition(topic, 0)
                    .expect("baseline topic")
                    .directories
        );
    }
}
#[rstest_bdd_macros::then("the metadata downgrade cluster is shut down")]
async fn finish_metadata_downgrade(metadata_downgrade: &mut MetadataDowngrade) {
    metadata_downgrade
        .cluster
        .take()
        .expect("mixed cluster")
        .shutdown()
        .await;
}
#[ignore = "requires Docker + published controller/data ports; Linux-bound"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_kip320_divergence/metadata_version_downgrade.feature",
    name = "Reject safe and unsafe metadata loss without changing directory metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_version_downgrade_refuses_lossy_levels(metadata_downgrade: MetadataDowngrade) {
    let _ = metadata_downgrade;
}
