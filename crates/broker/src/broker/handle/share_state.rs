//! Test-only [`BrokerHandle`] helpers for KIP-932 share state: the persisted
//! summary for one share partition and the awaiters that block until its
//! start offset, delivery-complete count, or acquired-batch count reaches an
//! expected value.

use {
    crate::{
        broker::BrokerHandle,
        share_coordinator::{coordinator::ShareStateSummary, key::ShareKeyRef},
    },
    std::cell::Cell,
};

/// Unwraps the summary's `start_offset` to `i64` at this test-helper
/// boundary: integration tests compare it against raw offset literals.
fn raw_summary(summary: ShareStateSummary) -> (i32, i32, i64, i32) {
    (
        summary.state_epoch,
        summary.leader_epoch,
        summary.start_offset.0,
        summary.delivery_complete_count,
    )
}

impl BrokerHandle {
    /// Test-only: read the share-state summary
    /// `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`
    /// for `(group, topic_id, partition)` straight from this broker's
    /// internal `ShareCoordinator`, with no status or metadata check.
    /// Returns `None` when the key has no initialized state, also after its
    /// topic left the metadata image, so a test sees whether the share state
    /// itself was deleted. KIP-932 lifecycle
    /// tests use this to assert that the group coordinator initialized the
    /// per-partition share state. The persister RPCs stay off the wire.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<(i32, i32, i64, i32)> {
        self.broker
            .share_coordinator
            .stored_summary_for_test(ShareKeyRef {
                group,
                topic_id,
                partition,
            })
            .await
            .map(raw_summary)
    }

    /// Test-only: one read of the share-state summary of
    /// `(group, topic_id, partition)` as the active share coordinator holds
    /// it, or `None` when it holds no state for the key.
    ///
    /// A load of a `__share_group_state` partition drops its keys and
    /// replays them, so [`Self::share_state_summary_for_test`] can miss a key
    /// that the partition holds. This read takes the term guard the
    /// share-state operations take and does not wait. A caller that drives
    /// the partition itself, for example with heartbeats that reach the
    /// persister, retries it in its own loop.
    ///
    /// # Errors
    ///
    /// The share-state error code the share-state operations answer while
    /// the key's state partition is not active.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn try_active_share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<(i32, i32, i64, i32)>, i16> {
        self.broker
            .share_coordinator
            .active_summary_for_test(ShareKeyRef {
                group,
                topic_id,
                partition,
            })
            .await
            .map(|summary| summary.map(raw_summary))
    }

    /// Test-only: [`Self::try_active_share_state_summary_for_test`], retried
    /// for at most 30 seconds until the key's state partition is active. A
    /// key the active partition does not hold still reads `None` at once.
    ///
    /// # Panics
    ///
    /// Panics when the state partition is not active within 30 seconds.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn active_share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<(i32, i32, i64, i32)> {
        let read = Cell::new(None);
        let last_code = Cell::new(None);
        let active = super::await_until(|| {
            let (read, last_code) = (&read, &last_code);
            async move {
                match self
                    .try_active_share_state_summary_for_test(group, topic_id, partition)
                    .await
                {
                    Ok(summary) => read.set(Some(summary)),
                    Err(code) => last_code.set(Some(code)),
                }
                read.get().is_some()
            }
        })
        .await;
        assert2::assert!(
            active,
            "share-state partition of {group}:{topic_id}:{partition} not active within 30s, \
             last code {:?}",
            last_code.get()
        );
        read.get().flatten()
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
        let present = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some()
        })
        .await;
        assert2::assert!(
            present,
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
        let reached = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some_and(|(_, _, spso, _)| spso >= min)
        })
        .await;
        assert2::assert!(
            reached,
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
        let reached = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some_and(|(_, _, _, dcc)| dcc >= min)
        })
        .await;
        assert2::assert!(
            reached,
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
        let reached = super::await_until(|| async {
            match self
                .broker
                .share_partition_leaders
                .peek_for_test(ShareKeyRef::new(group, topic_id, partition))
            {
                Some(cell) => cell.lock().await.count_acquired_batches() == n,
                None => false,
            }
        })
        .await;
        assert2::assert!(
            reached,
            "share acquired-batch count for {group}:{topic_id}:{partition} did not reach {n} within 30s"
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            handlers::test_support::StartedBroker,
            share_coordinator::{
                coordinator::test_support::{image_with_topic, share_write},
                handlers::test_support::lead_share_state_partitions,
                key::ShareKeyRef,
                persistence::StateBatch,
            },
            share_partition::state::AcquisitionState,
            test_support::{start_broker_with, tolerate_starved_heartbeats},
        },
        assert2::check,
        std::time::{Duration, Instant},
        tokio::{net::TcpListener, time},
    };

    /// The active read waits while this broker leads no state partition, as
    /// it does before `__share_group_state` exists. Once the partitions are
    /// active it answers at once: `None` for a key with no state, and the
    /// summary for an initialized key.
    #[tokio::test]
    async fn active_share_state_summary_waits_for_an_active_state_partition() {
        let StartedBroker {
            broker: handle,
            dir: _dir,
        } = start_broker_with(tolerate_starved_heartbeats).await;
        let broker = handle.broker_arc_for_test();
        let group = "handle-active-summary-group";
        let topic_id = uuid::Uuid::from_u128(0xAC71);

        let before_leading = time::timeout(
            Duration::from_millis(75),
            handle.active_share_state_summary_for_test(group, topic_id, 0),
        )
        .await;
        lead_share_state_partitions(&broker).await;
        let no_state = time::timeout(
            Duration::from_secs(1),
            handle.active_share_state_summary_for_test(group, topic_id, 0),
        )
        .await;
        broker
            .share_coordinator
            .initialize(
                &image_with_topic(topic_id, 1),
                ShareKeyRef::new(group, topic_id, 0),
                4,
                krabka_log::Offset(-1),
            )
            .await
            .expect("initialize share state");
        let initialized = time::timeout(
            Duration::from_secs(1),
            handle.active_share_state_summary_for_test(group, topic_id, 0),
        )
        .await;

        check!(before_leading.is_err());
        check!(no_state == Ok(None));
        check!(initialized == Ok(Some((4, 0, -1, -1))));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn single_broker_handle_share_and_raft_helpers_observe_real_state() {
        let StartedBroker {
            broker: handle,
            dir,
        } = start_broker_with(tolerate_starved_heartbeats).await;
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
        lead_share_state_partitions(&broker).await;
        let share_image = image_with_topic(share_topic_id, share_partition + 1);
        broker
            .share_coordinator
            .initialize(
                &share_image,
                ShareKeyRef::new(share_group, share_topic_id, share_partition),
                11,
                krabka_log::Offset(90),
            )
            .await
            .expect("initialize share state");
        broker
            .share_coordinator
            .read(
                &share_image,
                ShareKeyRef::new(share_group, share_topic_id, share_partition),
                2,
            )
            .await
            .expect("raise the stored leader epoch");
        broker
            .share_coordinator
            .write(
                &share_image,
                ShareKeyRef::new(share_group, share_topic_id, share_partition),
                share_write((11, 2), (95, 7), vec![StateBatch {
                    first_offset: krabka_log::Offset(95),
                    last_offset: krabka_log::Offset(99),
                    delivery_state: 0,
                    delivery_count: 1,
                }]),
            )
            .await
            .expect("write share state summary");
        check!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                == Some((11, 2, 95, 7))
        );
        check!(
            time::timeout(
                Duration::from_secs(1),
                handle.wait_until_share_spso(share_group, share_topic_id, share_partition, 95),
            )
            .await
            .is_ok()
        );
        check!(
            time::timeout(
                Duration::from_secs(1),
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
        // The share coordinator refuses a read of a key with no state, so the
        // cell is cached directly. This part checks only the acquired count.
        let acquired_cell = broker.share_partition_leaders.insert_for_test(
            ShareKeyRef::new(acquired_group, acquired_topic_id, 0),
            AcquisitionState::new(krabka_log::Offset(0)),
        );
        assert2::assert!(
            time::timeout(
                Duration::from_millis(75),
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
                krabka_log::Offset(i64::MAX),
                Instant::now(),
                Duration::from_secs(30),
                i16::MAX,
            );
            drop(state);
            assert2::assert!(!acquired.is_empty());
        }
        assert2::assert!(
            time::timeout(
                Duration::from_secs(1),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_ok()
        );

        let closed_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed learner port");
        let closed_addr = closed_listener.local_addr().expect("closed learner addr");
        drop(closed_listener);
        let add_learner = time::timeout(
            Duration::from_secs(10),
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
