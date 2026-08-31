//! Behaviour of the replica-lag sampler, driven through the gauge it
//! publishes rather than through its return value: what an operator scrapes is
//! the contract.

use std::{path::Path, sync::Arc, time::Instant};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use super::*;
use crate::partition::Partition;

const TOPIC: &str = "orders";
const LEADER: NodeId = NodeId(1);
const FOLLOWER: NodeId = NodeId(2);
const OTHER_BROKER: NodeId = NodeId(3);

/// A partition backed by a real log under `dir`, registered as led by
/// `LEADER` with `FOLLOWER` as its one in-sync follower.
fn led_partition(dir: &Path, topic: &str, partition: i32) -> Arc<Partition> {
    let partition_dir = crate::log_dir::partition_dir(dir, topic, partition);
    std::fs::create_dir_all(&partition_dir).expect("partition dir");
    let log = krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
        .expect("open partition log");
    let part = crate::broker::spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    part.current_leader.store(LEADER.0, Ordering::Release);
    part
}

/// Seed `partition`'s replica state with `LEADER` leading and `FOLLOWER` in
/// the ISR, both at offset zero.
async fn install_isr(partition: &Partition) {
    partition.replica_state.lock().await.install_isr(
        &[LEADER, FOLLOWER],
        &[LEADER, FOLLOWER],
        LEADER,
        Instant::now(),
    );
}

/// Append `count` records straight to the log, advancing the leader's log end
/// offset the way a produce does.
fn append_records(partition: &Partition, count: i32) {
    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta: count - 1,
        base_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..count)
            .map(|index| Record {
                attributes: 0,
                offset_delta: index,
                timestamp_delta: 0,
                key: None,
                value: Some(bytes::Bytes::from_static(b"v")),
                headers: vec![],
            })
            .collect(),
    };
    partition
        .log
        .lock()
        .expect("log mutex")
        .append(&mut batch)
        .expect("append");
}

/// The follower fetches everything the leader holds, as its replica fetcher
/// would.
async fn follower_fetches_everything(partition: &Partition) {
    let leader_log_end = partition.log_end_offset();
    partition.replica_state.lock().await.update_follower_leo(
        FOLLOWER,
        leader_log_end,
        leader_log_end,
        Instant::now(),
    );
}

/// Run one replica-lag pass and publish it, as the poller's tick does.
async fn sample_replica_lag(partitions: &PartitionRegistry, metrics: &BrokerMetrics) {
    metrics.publish_replica_lag(&replica_lag_samples(partitions, LEADER).await);
}

/// The published lag of `FOLLOWER` on `TOPIC`-`partition`, or `None` when the
/// family carries no such series.
fn published_replica_lag(metrics: &BrokerMetrics, partition: i32) -> Option<i64> {
    metrics
        .replica_lag
        .get(&ReplicaLagLabel {
            topic: TOPIC.into(),
            partition,
            replica: FOLLOWER.0,
        })
        .map(|gauge| gauge.get())
}

/// The acceptance case: while the follower keeps fetching its lag is zero, and
/// once its fetch is paused every leader append raises the gauge by exactly
/// the records it added.
#[tokio::test]
async fn a_paused_follower_fetch_makes_the_replica_lag_gauge_climb() {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = PartitionRegistry::new();
    let partition = led_partition(dir.path(), TOPIC, 0);
    install_isr(&partition).await;
    partitions.insert(TOPIC.into(), PartitionIndex(0), Arc::clone(&partition));
    let metrics = BrokerMetrics::new();

    append_records(&partition, 10);
    follower_fetches_everything(&partition).await;
    sample_replica_lag(&partitions, &metrics).await;
    assert!(published_replica_lag(&metrics, 0) == Some(0));
    check!(metrics.replica_lag_max.get() == 0);

    // The follower's fetch is paused from here on: nothing updates its
    // tracked offset while the leader keeps appending.
    for (appended, expected_lag) in [(5, 5), (5, 10), (20, 30)] {
        append_records(&partition, appended);
        sample_replica_lag(&partitions, &metrics).await;
        check!(published_replica_lag(&metrics, 0) == Some(expected_lag));
        check!(metrics.replica_lag_max.get() == expected_lag);
    }

    // Resuming the fetch takes the gauge back down, so it tracks the follower
    // rather than accumulating.
    follower_fetches_everything(&partition).await;
    sample_replica_lag(&partitions, &metrics).await;
    check!(published_replica_lag(&metrics, 0) == Some(0));
    check!(metrics.replica_lag_max.get() == 0);
}

/// The max rollup is the largest lag across partitions, not the last one
/// sampled.
#[tokio::test]
async fn the_max_rollup_reports_the_worst_follower_on_the_broker() {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = PartitionRegistry::new();
    for (index, records) in [(0, 3), (1, 40)] {
        let partition = led_partition(dir.path(), TOPIC, index);
        install_isr(&partition).await;
        append_records(&partition, records);
        partitions.insert(TOPIC.into(), PartitionIndex(index), partition);
    }
    let metrics = BrokerMetrics::new();

    sample_replica_lag(&partitions, &metrics).await;

    check!(published_replica_lag(&metrics, 0) == Some(3));
    check!(published_replica_lag(&metrics, 1) == Some(40));
    check!(metrics.replica_lag_max.get() == 40);
}

/// Leadership is what justifies a replica-lag series, so the pass that first
/// sees another broker leading the partition takes the series away.
#[tokio::test]
async fn losing_leadership_releases_the_partitions_replica_lag_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = PartitionRegistry::new();
    let partition = led_partition(dir.path(), TOPIC, 0);
    install_isr(&partition).await;
    append_records(&partition, 12);
    partitions.insert(TOPIC.into(), PartitionIndex(0), Arc::clone(&partition));
    let metrics = BrokerMetrics::new();
    sample_replica_lag(&partitions, &metrics).await;
    assert!(published_replica_lag(&metrics, 0) == Some(12));

    partition
        .current_leader
        .store(OTHER_BROKER.0, Ordering::Release);
    sample_replica_lag(&partitions, &metrics).await;

    check!(published_replica_lag(&metrics, 0) == None);
    check!(metrics.replica_lag_max.get() == 0);
}
