//! Unit tests for the `__share_group_state` replay: the recovery read bound,
//! the round trip from a written state back into memory, and the per-record
//! and inter-batch offset arithmetic that the replay cursor depends on.

use assert2::{assert, check};
use krabka_protocol::records::{Record, RecordBatch};
use tempfile::tempdir;

use super::*;
use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{
        config::ShareCoordinatorConfig,
        coordinator::test_support::{batch, coordinator, lead_all, open_state_partition},
        persistence::encode_state_key,
    },
};

#[tokio::test]
async fn recover_honors_nondefault_read_bound() {
    let dir = tempdir().unwrap();
    let registry = Arc::new(PartitionRegistry::new());
    open_state_partition(&registry, dir.path(), 0);
    let partition = registry
        .get(bootstrap::TOPIC, PartitionIndex(0))
        .expect("state partition open");
    let topic_id = uuid::Uuid::from_bytes([42; 16]);
    let key = ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "bounded".to_string(),
        topic_id,
        partition: 0,
    };
    let snapshot = ShareSnapshotValue {
        snapshot_epoch: 0,
        state_epoch: 3,
        leader_epoch: 4,
        start_offset: Offset(5),
        delivery_complete_count: 6,
        state_batches: vec![],
    };
    let mut batch = RecordBatch::default();
    batch.records.push(Record {
        key: Some(encode_state_key(&key)),
        value: Some(snapshot.encode()),
        ..Record::default()
    });
    batch.records.push(Record {
        value: Some(Bytes::from(vec![0; 2_048])),
        ..Record::default()
    });
    batch.last_offset_delta = 1;
    partition.produce_batch(batch).await.unwrap();

    let image = MetadataImage::from_records(
        uuid::Uuid::nil(),
        &[
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: bootstrap::TOPIC.to_string(),
                topic_id: uuid::Uuid::from_bytes([43; 16]),
                partitions: 1,
                replication_factor: 1,
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: bootstrap::TOPIC.to_string(),
                partition: 0,
                leader: krabka_metadata::NodeId(1),
                replicas: vec![krabka_metadata::NodeId(1)],
                isr: vec![krabka_metadata::NodeId(1)],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ],
    );
    let bounded = ShareCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&registry),
        ShareCoordinatorConfig {
            recovery_read_max: krabka_units::bytes(700),
            ..ShareCoordinatorConfig::default()
        },
    );
    bounded.recover(&image).await.unwrap();
    assert!(bounded.read("bounded", topic_id, 0).await.is_none());

    let unbounded = ShareCoordinator::new(
        krabka_audit::NodeId(1),
        registry,
        ShareCoordinatorConfig {
            recovery_read_max: krabka_units::kibibytes(4),
            ..ShareCoordinatorConfig::default()
        },
    );
    unbounded.recover(&image).await.unwrap();
    assert!(unbounded.read_summary("bounded", topic_id, 0).await == Some((3, 4, Offset(5), 6)));
}

#[tokio::test]
async fn write_persists_and_recovers() {
    let dir = tempdir().unwrap();
    let reg = Arc::new(PartitionRegistry::new());
    for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
        open_state_partition(&reg, dir.path(), p);
    }
    let tid = uuid::Uuid::from_bytes([10; 16]);
    {
        let coord = ShareCoordinator::new(
            krabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        );
        lead_all(&coord).await;
        coord.initialize("g", tid, 0, 2, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, (2, 3), (Offset(20), 4), vec![batch(20, 29)])
            .await
            .unwrap();
    }

    // New coordinator over the SAME registry (same open logs); recover
    // replays the records written above.
    let recovered = ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    );
    lead_all(&recovered).await;
    // `recover` re-derives leadership from a MetadataImage; here we seed
    // the leadership set directly (lead_all) and replay the open logs.
    recovered.replay_led_partitions().await;

    let st = recovered.read("g", tid, 0).await.expect("recovered");
    check!(st.state_epoch == 2);
    check!(st.leader_epoch == 3);
    check!(st.start_offset == 20);
    check!(st.delivery_complete_count == 4);
    check!(st.state_batches == vec![batch(20, 29)]);
}

// `replay_led_partitions` must derive each record's offset as
// `base_offset + offset_delta` and advance the inter-batch cursor as
// `base_offset + last_offset_delta + 1`. A hand-crafted TWO-record batch
// (an update at offset_delta 0, then a snapshot at offset_delta 1) pins
// both: after replay, the snapshot's recorded `last_snapshot_offset` must
// be `base_offset + 1`, and a second batch appended after it must also be
// replayed (only reachable when the cursor advances by
// `last_offset_delta + 1`).
#[tokio::test]
async fn replay_uses_per_record_and_inter_batch_offsets() {
    let dir = tempdir().unwrap();
    let (coord, reg) = coordinator(dir.path());
    lead_all(&coord).await;
    let tid = uuid::Uuid::from_bytes([11; 16]);
    let state_partition = coord.state_partition_for("g", &tid, 0);
    let part = reg
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");

    let snap_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });
    let upd_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_UPDATE,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });

    // Batch A (base_offset 0): an UPDATE at delta 0, then a SNAPSHOT at
    // delta 1 (last_offset_delta = 1). The snapshot's rec_offset is
    // `base_offset + 1 == 1`.
    let mut batch_a = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch_a.records.push(Record {
        offset_delta: 0,
        key: Some(upd_key.clone()),
        value: Some(
            ShareUpdateValue {
                snapshot_epoch: 0,
                leader_epoch: 1,
                start_offset: Offset(0),
                delivery_complete_count: 0,
                state_batches: vec![],
            }
            .encode(),
        ),
        ..Default::default()
    });
    batch_a.records.push(Record {
        offset_delta: 1,
        key: Some(snap_key.clone()),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 5,
                state_epoch: 2,
                leader_epoch: 3,
                start_offset: Offset(20),
                delivery_complete_count: 4,
                state_batches: vec![batch(20, 29)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_a).await.unwrap();

    // Batch B (base_offset 2): a later SNAPSHOT. Only reached if the cursor
    // advanced past batch A by `last_offset_delta + 1`.
    let mut batch_b = RecordBatch::default();
    batch_b.records.push(Record {
        offset_delta: 0,
        key: Some(snap_key.clone()),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 6,
                state_epoch: 2,
                leader_epoch: 9,
                start_offset: Offset(50),
                delivery_complete_count: 8,
                state_batches: vec![batch(50, 59)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_b).await.unwrap();

    coord.replay_led_partitions().await;

    let st = coord.read("g", tid, 0).await.expect("recovered");
    // Batch B is the final snapshot — proves the inter-batch cursor advanced
    // past batch A (base_offset + last_offset_delta + 1 == 2).
    check!(st.leader_epoch == 9);
    check!(st.start_offset == 50);
    check!(st.delivery_complete_count == 8);
    check!(st.state_batches == vec![batch(50, 59)]);
    // Batch B's snapshot sits at base_offset 2 (single record, delta 0).
    check!(st.last_snapshot_offset == 2);
}

/// A replay of ONLY batch A pins the per-record offset arithmetic.
///
/// The snapshot at `offset_delta 1` over `base_offset 0` records
/// `last_snapshot_offset == 1`, not `Offset(-1)`.
#[tokio::test]
async fn replay_snapshot_offset_is_base_plus_delta() {
    let dir = tempdir().unwrap();
    let (coord, reg) = coordinator(dir.path());
    lead_all(&coord).await;
    let tid = uuid::Uuid::from_bytes([12; 16]);
    let state_partition = coord.state_partition_for("g", &tid, 0);
    let part = reg
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");

    let snap_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });
    let upd_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_UPDATE,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });

    // Single batch, base_offset 0: an UPDATE at delta 0 then a SNAPSHOT at
    // delta 1. The snapshot's rec_offset is `0 + 1 == 1`.
    let mut batch_a = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch_a.records.push(Record {
        offset_delta: 0,
        key: Some(upd_key),
        value: Some(
            ShareUpdateValue {
                snapshot_epoch: 0,
                leader_epoch: 1,
                start_offset: Offset(0),
                delivery_complete_count: 0,
                state_batches: vec![],
            }
            .encode(),
        ),
        ..Default::default()
    });
    batch_a.records.push(Record {
        offset_delta: 1,
        key: Some(snap_key),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 5,
                state_epoch: 2,
                leader_epoch: 3,
                start_offset: Offset(20),
                delivery_complete_count: 4,
                state_batches: vec![batch(20, 29)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_a).await.unwrap();

    coord.replay_led_partitions().await;

    let st = coord.read("g", tid, 0).await.expect("recovered");
    check!(st.leader_epoch == 3);
    check!(st.start_offset == 20);
    // The snapshot record sits at base_offset(0) + offset_delta(1) == 1.
    check!(st.last_snapshot_offset == 1);
}
