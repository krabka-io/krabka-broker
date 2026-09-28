//! Test-only [`BrokerHandle`] helpers that bring up a coordinator topic the
//! way a client does, then wait until the coordinator serves it.
//!
//! No broker creates `__consumer_offsets`, `__transaction_state` or
//! `__share_group_state` when it starts. A client's first `FindCoordinator`
//! asks for the topic, and the lookup answers `COORDINATOR_NOT_AVAILABLE`
//! until the topic exists and has leaders. These helpers are the counterpart
//! of Kafka's `IntegrationTestHarness.createOffsetsTopic`: a test that drives
//! a coordinator directly, or that must not spend its own deadline on the
//! first lookup, calls one of them first.
//!
//! Each helper asks for the topic again on every poll while it is absent, as
//! a retrying client does, because a creation fails while fewer brokers are
//! registered than the configured replication factor. It then waits until
//! every partition has a leader and every partition that this broker leads
//! has finished its load. In a multi-node cluster, call it on each broker
//! whose coordinator the test uses.

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;

use crate::broker::{BrokerHandle, TEST_AWAITER_TIMEOUT};

/// The poll interval of the helpers below. The coordinators' load state has
/// no change-notification channel.
const POLL: std::time::Duration = std::time::Duration::from_millis(25);

/// The partitions of `topic` that `node` leads, or `None` while `topic` is
/// absent or one of its first `partitions` partitions has no leader.
fn led_partitions(
    image: &MetadataImage,
    topic: &str,
    partitions: i32,
    node: krabka_raft::NodeId,
) -> Option<Vec<i32>> {
    image.topic(topic)?;
    let mut led = Vec::new();
    for partition in 0..partitions {
        let record = image.partition(topic, partition)?;
        if record.leader == krabka_raft::NodeId(0) {
            return None;
        }
        if record.leader == node {
            led.push(partition);
        }
    }
    Some(led)
}

impl BrokerHandle {
    /// Test-only: create `__consumer_offsets` as a client's first group
    /// lookup does, and wait until every partition has a leader and this
    /// broker's group coordinator has loaded the partitions it leads.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_group_coordinator_ready(&self) {
        let topic = crate::coordinator::bootstrap::OFFSETS_TOPIC;
        let partitions = self.broker.config.offsets_topic_num_partitions;
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let image = self.broker.controller.current_image();
                if let Some(led) =
                    led_partitions(&image, topic, partitions, self.broker.config.node_id)
                {
                    let loading = led.into_iter().any(|partition| {
                        image.partition(topic, partition).is_none_or(|record| {
                            self.broker
                                .group_coordinator
                                .is_loading(partition, record.leader_epoch)
                        })
                    });
                    if !loading {
                        return;
                    }
                } else {
                    self.broker.auto_topic_creation.request(topic);
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "the group coordinator was not ready within {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: create `__transaction_state` as a client's first
    /// transaction lookup does, and wait until every partition has a leader
    /// and this broker's transaction coordinator has loaded the partitions
    /// it leads.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_transaction_coordinator_ready(&self) {
        let topic = crate::txn::bootstrap::TOPIC;
        let partitions = self.broker.config.transaction_state_num_partitions;
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let image = self.broker.controller.current_image();
                if let Some(led) =
                    led_partitions(&image, topic, partitions, self.broker.config.node_id)
                {
                    let mut loaded = true;
                    for partition in led {
                        loaded &= self
                            .broker
                            .txn_coordinator
                            .load_status(PartitionIndex(partition))
                            .await
                            == Some(crate::txn::coordinator::leadership::LoadStatus::Loaded);
                    }
                    if loaded {
                        return;
                    }
                } else {
                    self.broker.auto_topic_creation.request(topic);
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "the transaction coordinator was not ready within {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: create `__share_group_state` as the share persister's
    /// first lookup does, and wait until every partition has a leader and
    /// this broker's share coordinator has loaded the partitions it leads.
    ///
    /// A test must not seed the share coordinator's leadership by hand on a
    /// live broker: the metadata reconcile loop applies the image again at
    /// any time, and an image without the topic drops every led partition.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_coordinator_ready(&self) {
        let topic = crate::share_coordinator::bootstrap::TOPIC;
        let partitions = self
            .broker
            .config
            .share_coordinator
            .state_topic_num_partitions;
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let image = self.broker.controller.current_image();
                if let Some(led) =
                    led_partitions(&image, topic, partitions, self.broker.config.node_id)
                {
                    self.broker
                        .share_coordinator
                        .refresh_leader_partitions(&image)
                        .await
                        .finished()
                        .await;
                    let mut active = true;
                    for partition in led {
                        active &= self
                            .broker
                            .share_coordinator
                            .load_status(PartitionIndex(partition))
                            .await
                            == Some(crate::share_coordinator::coordinator::LoadStatus::Active);
                    }
                    if active {
                        return;
                    }
                } else {
                    self.broker.auto_topic_creation.request(topic);
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "the share coordinator was not ready within {TEST_AWAITER_TIMEOUT:?}"
        );
    }
}
