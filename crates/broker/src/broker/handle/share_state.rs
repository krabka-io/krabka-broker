//! Test-only [`BrokerHandle`] helpers for KIP-932 share state: the persisted
//! summary for one share partition and the awaiters that block until its
//! start offset, delivery-complete count, or acquired-batch count reaches an
//! expected value.

use crate::broker::{BrokerHandle, TEST_AWAITER_TIMEOUT};

impl BrokerHandle {
    /// Test-only: read the share-state summary
    /// `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`
    /// for `(group, topic_id, partition)` straight from this broker's
    /// internal `ShareCoordinator`.
    /// Returns `None` when the key has no initialized state. KIP-932 lifecycle
    /// tests use this to assert that the group coordinator initialized the
    /// per-partition share state. The persister RPCs stay off the wire.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<(i32, i32, i64, i32)> {
        // Unwrap the summary's `start_offset` -> `i64` at this test-helper
        // boundary: integration tests compare it against raw offset literals.
        self.broker
            .share_coordinator
            .read_summary(group, topic_id, partition)
            .await
            .map(|(state_epoch, leader_epoch, start_offset, count)| {
                (state_epoch, leader_epoch, start_offset.0, count)
            })
    }

    /// Test-only: await until the persisted share-state summary exists for
    /// `(group, topic_id, partition)` (share-state initialized / recovered).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_for_share_state_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    .is_some()
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "share-state summary for {group}:{topic_id}:{partition} not present within 30s"
        );
    }

    /// Test-only: await until the share-partition SPSO (start_offset) >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_spso(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i64,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some((_, _, spso, _)) = self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    && spso >= min
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "share SPSO for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the share-partition delivery-complete count >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_delivery_complete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some((_, _, _, dcc)) = self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    && dcc >= min
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "share dcc for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the live share-partition has exactly `n` Acquired
    /// in-flight batches. The count rises after a ShareFetch acquires, and it
    /// drops back after lock-timeout redelivery returns records to Available.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_acquired_count(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        n: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(cell) = self
                    .broker
                    .share_partition_leaders
                    .peek_for_test(group, topic_id, partition)
                {
                    let count = cell.lock().await.count_acquired_batches();
                    if count == n {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "share acquired-batch count for {group}:{topic_id}:{partition} did not reach {n} within 30s"
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use crate::{
        broker::{Broker, test_support::local_partition_with_records},
        config::BrokerConfig,
    };

    #[tokio::test]
    async fn single_broker_handle_share_and_raft_helpers_observe_real_state() {
        let dir = tempfile::tempdir().unwrap();
        let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let broker = handle.broker_arc_for_test();

        let share_group = "handle-share-summary-mutant-group";
        let share_topic_id = uuid::Uuid::from_u128(0xBEE5);
        let share_partition = 3;
        assert2::assert!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                .is_none()
        );
        let share_state_partition = broker.share_coordinator.state_partition_for(
            share_group,
            &share_topic_id,
            share_partition,
        );
        let share_state_part = local_partition_with_records(
            dir.path(),
            crate::share_coordinator::bootstrap::TOPIC,
            share_state_partition.0,
            &[],
        );
        broker.partitions.insert(
            crate::share_coordinator::bootstrap::TOPIC.into(),
            share_state_partition,
            share_state_part,
        );
        broker
            .share_coordinator
            .initialize(
                share_group,
                share_topic_id,
                share_partition,
                11,
                krabka_log::Offset(90),
            )
            .await
            .expect("initialize share state");
        broker
            .share_coordinator
            .write(
                share_group,
                share_topic_id,
                share_partition,
                (12, 2),
                (krabka_log::Offset(95), 7),
                vec![crate::share_coordinator::persistence::StateBatch {
                    first_offset: krabka_log::Offset(95),
                    last_offset: krabka_log::Offset(99),
                    delivery_state: 0,
                    delivery_count: 1,
                }],
            )
            .await
            .expect("write share state summary");
        check!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                == Some((12, 2, 95, 7))
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_spso(share_group, share_topic_id, share_partition, 95),
            )
            .await
            .is_ok()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_delivery_complete(
                    share_group,
                    share_topic_id,
                    share_partition,
                    7,
                ),
            )
            .await
            .is_ok()
        );

        let acquired_group = "handle-share-acquired-mutant-group";
        let acquired_topic_id = uuid::Uuid::from_u128(0xACCD);
        let acquired_cell = broker
            .share_partition_leaders
            .get_or_load(acquired_group, acquired_topic_id, 0)
            .await;
        assert2::assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_err()
        );
        {
            let mut state = acquired_cell.lock().await;
            state.materialize(krabka_log::Offset(3), 10);
            let acquired = state.acquire(
                "member-1",
                3,
                i32::MAX,
                std::time::Instant::now(),
                std::time::Duration::from_secs(30),
                i16::MAX,
            );
            assert2::assert!(!acquired.is_empty());
        }
        assert2::assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_ok()
        );

        let closed_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed learner port");
        let closed_addr = closed_listener.local_addr().expect("closed learner addr");
        drop(closed_listener);
        let add_learner = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handle.add_learner(krabka_raft::NodeId(handle.node_id() + 10), closed_addr),
        )
        .await
        .expect("add_learner returned before timeout");
        assert2::assert!(add_learner.is_ok());

        let own_directory = handle
            .voter_directory_id_for_test(krabka_raft::NodeId(handle.node_id()))
            .expect("own voter directory id");
        check!(own_directory != uuid::Uuid::nil());
        check!(
            handle.voter_directory_id_for_test(krabka_raft::NodeId(handle.node_id() + 10_000))
                == None
        );

        // Marking the same log dir offline twice: first succeeds, second is a
        // no-op.
        check!(handle.test_mark_log_dir_offline(dir.path()));
        check!(!handle.test_mark_log_dir_offline(dir.path()));
        handle.shutdown().await;
    }
}
