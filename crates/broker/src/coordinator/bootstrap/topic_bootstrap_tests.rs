//! Tests for the registration of `__consumer_offsets` in the metadata quorum
//! and for the local partition directories that [`super::bootstrap`] opens.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use tempfile::tempdir;

use super::{
    OFFSETS_PARTITION, OFFSETS_TOPIC, bootstrap,
    test_support::{controller_with_leader, test_coordinator},
};
use crate::{config::BrokerConfig, log_dir, partition_registry::PartitionRegistry};

#[tokio::test]
async fn bootstrap_creates_topic_dir() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let controller: Arc<dyn crate::metadata_source::MetadataSource> =
        controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
    let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
    let coordinator = test_coordinator(&controller, &partitions);
    let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());
    bootstrap(
        &config,
        &controller,
        &partitions,
        &coordinator,
        &log_dir_status,
        &Arc::new(crate::producer_state::ProducerState::new()),
    )
    .await
    .unwrap();
    let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
    check!(topic_dir.exists());
    check!(partitions.contains(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION)));
    check!(controller.current_image().topic(OFFSETS_TOPIC).is_some());
}

/// Regression for the bootstrap TOCTOU: a SECOND bootstrap against a
/// controller that already has `__consumer_offsets` must NOT submit a
/// second, conflicting `TopicRecord`.
///
/// The leader registered the topic on the first boot. The second bootstrap
/// must see the existing topic, skip the registration, succeed, and leave
/// EXACTLY ONE `__consumer_offsets` topic in the image. The test exercises
/// the "already exists => no-op" arm and the leader gate. Test node 1 is
/// the leader, so the first boot is the single writer. The second boot
/// finds the topic present and skips it.
#[tokio::test]
async fn second_bootstrap_does_not_duplicate_offsets_topic() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let controller: Arc<dyn crate::metadata_source::MetadataSource> =
        controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
    let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
    let coordinator = test_coordinator(&controller, &partitions);
    let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());

    // First boot: this node IS the leader, so it registers the topic.
    bootstrap(
        &config,
        &controller,
        &partitions,
        &coordinator,
        &log_dir_status,
        &Arc::new(crate::producer_state::ProducerState::new()),
    )
    .await
    .unwrap();
    let id_after_first = controller
        .current_image()
        .topic(OFFSETS_TOPIC)
        .expect("offsets topic registered on first boot")
        .topic_id;

    // Second boot (simulating a restart / a second broker reaching
    // bootstrap): topic already present => must be a no-op, no second
    // submit, and must succeed.
    bootstrap(
        &config,
        &controller,
        &partitions,
        &coordinator,
        &log_dir_status,
        &Arc::new(crate::producer_state::ProducerState::new()),
    )
    .await
    .unwrap();

    // Exactly one `__consumer_offsets` topic, and its id is unchanged
    // (no conflicting duplicate landed in the log).
    let image = controller.current_image();
    let count = image.topics().filter(|t| t.name == OFFSETS_TOPIC).count();
    assert!(
        count == 1,
        "expected exactly one __consumer_offsets, got {count}"
    );
    assert!(
        image.topic(OFFSETS_TOPIC).unwrap().topic_id == id_after_first,
        "topic_id changed across boots — a duplicate TopicRecord was submitted"
    );
}
