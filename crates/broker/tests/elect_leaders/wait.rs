//! Polling helpers that block until the committed metadata image of a
//! `BrokerHandle` reaches the partition state a test asserts on.
//!
//! Every test in this suite drives an election over the wire and then waits
//! for the result to appear in an image, so the waits are gathered here rather
//! than repeated next to each scenario.

use krabka_broker::BrokerHandle;
use krabka_metadata::{LeaderEpoch, PartitionRecord};

/// Waits until `handle` sees `(topic, partition)` in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

/// Waits until `handle` reports `leader` as the leader for `(topic, partition)`.
pub async fn wait_partition_leader(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    handle
        .wait_for_image(|img| img.partition(topic, partition).map(|p| p.leader.0) == Some(leader))
        .await;
}

/// Waits until the ISR for `(topic, partition)` contains `node`.
pub async fn wait_isr_contains(handle: &BrokerHandle, topic: &str, partition: i32, node: u64) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&krabka_broker::NodeId(node)))
        })
        .await;
}

/// Waits until the ISR for `(topic, partition)` is exactly `expected`.
pub async fn wait_partition_isr_only(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    expected: &[u64],
) {
    let expected_set: std::collections::HashSet<u64> = expected.iter().copied().collect();
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                let actual_set: std::collections::HashSet<u64> =
                    p.isr.iter().map(|n| n.0).collect();
                actual_set == expected_set
            })
        })
        .await;
}

/// Polls until the ISR of the partition contains `member`.
///
/// [`wait_partition_isr_only`] asserts an exact set. This function asserts
/// membership only. It thus accepts a live caught-up replica that the broker
/// admits or re-admits next to `member`.
pub async fn wait_partition_isr_contains(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    member: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&krabka_broker::NodeId(member)))
        })
        .await;
}

/// Polls until the metadata image of the handle shows the partition record.
///
/// The record is the one for `(topic, partition)`. This function returns a
/// clone of it.
pub async fn wait_partition_record_known(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
) -> PartitionRecord {
    handle.wait_until_partition_present(topic, partition).await;
    // A present partition record implies both accessors are populated.
    let isr = handle
        .partition_isr_for_test(topic, partition)
        .expect("partition present implies ISR known");
    let leader = handle
        .partition_leader_for_test(topic, partition)
        .expect("partition present implies leader known");
    // Reconstruct the record from the accessors we have.
    PartitionRecord {
        topic: topic.to_string(),
        partition,
        leader: krabka_broker::NodeId(leader),
        // We don't have a direct `replicas` accessor, but the
        // ISR is enough for our purposes (replicas=[1,2] is
        // well-known from the CreateTopics call with rf=2 on a
        // 3-broker cluster where the first two brokers are the
        // natural assignment).
        replicas: vec![krabka_broker::NodeId(1), krabka_broker::NodeId(2)],
        isr: isr.into_iter().map(krabka_broker::NodeId).collect(),
        leader_epoch: LeaderEpoch(0), // bumped by the forged record, not critical
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}
