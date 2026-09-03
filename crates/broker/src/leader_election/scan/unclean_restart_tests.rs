//! Tests for the scan that answers a broker re-registering under a new
//! incarnation id: the ISR removal, the leadership case that takes the
//! failover policy instead, and the ELR state the batch is left holding.
//!
//! The rules under test are Apache Kafka's `handleBrokerUncleanShutdown`, read
//! out of `kafka-metadata-4.3.1.jar` and quoted where each half is
//! implemented.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, TopicConfigRecord};

use super::*;
use crate::{
    config_keys::{ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS},
    leader_election::test_support::{img_with_partition, set_topic_config},
};

/// Liveness where each of `alive` heartbeated inside the current window.
async fn alive(alive: &[u64]) -> ControllerLivenessState {
    let liveness = ControllerLivenessState::new(krabka_units::secs(10));
    for &node in alive {
        liveness.record_heartbeat(node).await;
    }
    liveness
}

/// The batch a returning broker 3 writes against `image`.
async fn restart_batch(image: &MetadataImage, alive_nodes: &[u64]) -> FailoverPlan {
    compute_unclean_restart_changes(
        image,
        NodeId(3),
        &alive(alive_nodes).await,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await
}

/// The whole batch for the case the withdrawal alone could not close.
///
/// Broker 3 is in a healthy ISR and nothing about it is published, so the
/// `partitionsWithBrokerInElr` half has nothing to say. The
/// `partitionsWithBrokerInIsr` half is the batch: it takes broker 3 out of the
/// ISR, and because the ISR that is left is under `min.insync.replicas`, the
/// recompute that rides the same batch would hand broker 3 straight back as
/// eligible if it were not named as the unclean-shutdown replica. It is named,
/// so the published value puts it in the last-known column instead.
#[tokio::test]
async fn a_returning_broker_leaves_the_isr_and_does_not_re_enter_the_elr() {
    let mut image = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    crate::test_support::finalize_elr_version(&mut image);
    set_topic_config(&mut image, "t", MIN_INSYNC_REPLICAS, "3");

    let plan = restart_batch(&image, &[1, 2]).await;

    assert!(plan.recoveries.is_empty());
    assert!(plan.unavailable.is_empty());
    assert!(
        plan.changes
            == vec![
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: NodeId(1),
                    replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                    isr: vec![NodeId(1), NodeId(2)],
                    leader_epoch: LeaderEpoch(5),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 1,
                }),
                MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: "t".into(),
                    overrides: [
                        (MIN_INSYNC_REPLICAS.to_string(), "3".to_string()),
                        (ELIGIBLE_LEADER_REPLICAS.to_string(), "0::3".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                }),
            ]
    );
}

/// A partition broker 3 is only published as eligible for, and is not in the
/// ISR of, is the `partitionsWithBrokerInElr` half on its own: the membership
/// is withdrawn and no partition record is written, because there is no ISR
/// entry to remove.
#[tokio::test]
async fn a_published_membership_is_withdrawn_without_a_partition_change() {
    let mut image = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    crate::test_support::finalize_elr_version(&mut image);
    set_topic_config(&mut image, "t", ELIGIBLE_LEADER_REPLICAS, "0:3:");

    let plan = restart_batch(&image, &[1, 2]).await;

    assert!(
        plan.changes
            == vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: [(ELIGIBLE_LEADER_REPLICAS.to_string(), "0::3".to_string())]
                    .into_iter()
                    .collect(),
            })]
    );
}

/// A partition the returning broker is still recorded as leading takes the
/// failover policy, so leadership moves to a live ISR member and the leader
/// epoch advances with it. A bare ISR rewrite would leave broker 3 leading a
/// partition it is no longer in the ISR of.
#[tokio::test]
async fn a_partition_the_returning_broker_leads_is_re_elected() {
    let image = img_with_partition("t", 0, /*leader*/ 3, &[1, 2, 3], &[1, 2, 3]);

    let plan = restart_batch(&image, &[1, 2]).await;

    assert!(
        plan.changes
            == vec![MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: 0,
                leader: NodeId(1),
                replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                isr: vec![NodeId(1), NodeId(2)],
                leader_epoch: LeaderEpoch(6),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 1,
            })]
    );
}

/// A partition the returning broker is neither leading nor in the ISR of, and
/// that publishes nothing about it, costs nothing at all. Without this the
/// tests above would pass on a scan that rewrote every partition it walked.
#[tokio::test]
async fn a_partition_the_returning_broker_is_not_in_costs_nothing() {
    let image = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);

    let plan = restart_batch(&image, &[1, 2]).await;

    assert!(plan.changes.is_empty());
    assert!(plan.recoveries.is_empty());
    assert!(plan.unavailable.is_empty());
}
