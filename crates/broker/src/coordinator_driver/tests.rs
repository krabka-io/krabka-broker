//! The readiness handoff gates replay, and shutdown interrupts a blocked load.

use {
    super::*,
    crate::{
        partition_registry::PartitionRegistry,
        share_coordinator::{
            bootstrap,
            config::ShareCoordinatorConfig,
            coordinator::{
                LoadStatus,
                test_support::{
                    ConfiguredCoordinator, await_load_status, configured_coordinator,
                    gated_state_partition, mark_append_uncertain,
                },
            },
        },
        test_support::metadata_partition_record,
    },
    assert2::assert,
    krabka_ids::PartitionIndex,
    krabka_metadata::{MetadataRecord, NodeId, TopicRecord},
    std::time::Duration,
};

fn ready_image() -> Arc<MetadataImage> {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.into(),
        topic_id: uuid::Uuid::from_bytes([17; 16]),
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(
        metadata_partition_record()
            .topic(bootstrap::TOPIC.into())
            .replica_ids(vec![1])
            .isr_ids(vec![1])
            .leader_epoch(0)
            .call(),
    ));
    Arc::new(image)
}

#[tokio::test]
async fn recovery_without_metadata_changes_releases_completed_replay_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let ConfiguredCoordinator { coord, .. } =
        configured_coordinator(dir.path(), ShareCoordinatorConfig {
            state_topic_num_partitions: 1,
            ..Default::default()
        });
    let coordinator = Arc::new(coord);
    let (_ready, receiver) = watch::channel(Some(ready_image()));
    let shutdown = CancellationToken::new();
    let driver = CoordinatorDriver::new(
        receiver,
        None,
        Some(Arc::clone(&coordinator)),
        shutdown.clone(),
    );
    let retained = Arc::clone(&driver.retained_loads);
    let task = tokio::spawn(driver.run());
    await_load_status(&coordinator, PartitionIndex(0), LoadStatus::Active).await;

    for _ in 0..16 {
        mark_append_uncertain(&coordinator, PartitionIndex(0));
        await_load_status(&coordinator, PartitionIndex(0), LoadStatus::Active).await;
        assert!(retained.load(Ordering::Relaxed) <= 1);
    }
    shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn waits_for_readiness_and_cancels_a_blocked_replay() {
    let dir = tempfile::tempdir().unwrap();
    let partitions = Arc::new(PartitionRegistry::new());
    let release_writer = Arc::new(tokio::sync::Notify::new());
    let partition = gated_state_partition(dir.path(), &release_writer);
    partition
        .current_leader
        .store(1, std::sync::atomic::Ordering::Release);
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );
    let coordinator = Arc::new(ShareCoordinator::new(
        NodeId(1),
        partitions,
        ShareCoordinatorConfig {
            state_topic_num_partitions: 1,
            ..Default::default()
        },
    ));
    let (ready, receiver) = watch::channel(None);
    let shutdown = CancellationToken::new();
    let driver = tokio::spawn(
        CoordinatorDriver::new(
            receiver,
            None,
            Some(Arc::clone(&coordinator)),
            shutdown.clone(),
        )
        .run(),
    );
    // The local log already exists and leads, but no completed replica pass
    // has published an image yet.
    tokio::task::yield_now().await;
    assert!(coordinator.load_status(PartitionIndex(0)).await.is_none());

    ready.send_replace(Some(ready_image()));
    // Replay waits behind the writer's FIFO barrier. Keep that writer
    // blocked and prove shutdown does not wait for its release.
    await_load_status(&coordinator, PartitionIndex(0), LoadStatus::Loading).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), driver)
        .await
        .expect("driver shutdown cancels replay")
        .unwrap();
    release_writer.notify_one();
    // A cancelled load must not publish after its barrier eventually replies.
    partition.writer_barrier().await.unwrap();
    assert!(coordinator.load_status(PartitionIndex(0)).await == Some(LoadStatus::Loading));
    let writer = partition.take_writer_handle().unwrap();
    writer.abort();
    let _ = writer.await;
}
