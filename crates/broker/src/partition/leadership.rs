//! The partition's leadership identity: the replication target that fences a
//! follower write, and the installation path that publishes a new leader and
//! epoch from the metadata image. That publication is a locking protocol of its
//! own, shared with produce admission, so it lives in its own module.

use std::sync::{Arc, atomic::Ordering};

use krabka_log::Log;

use crate::{error::BrokerError, partition::Partition};

/// Kafka's `RecordBatch.NO_PARTITION_LEADER_EPOCH`: the wire value a request
/// carries when it holds no leader epoch for the partition.
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// Immutable topic identity plus the leader generation allowed to mutate a
/// follower log. A read guard over this value linearizes replication writes
/// with [`Partition::install_leader_change`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicationTarget {
    pub(crate) topic_id: Option<uuid::Uuid>,
    pub(crate) leader_node_id: krabka_raft::NodeId,
    pub(crate) leader_epoch: krabka_metadata::LeaderEpoch,
}

pub(crate) fn initial_replication_target(
    topic_id: Option<uuid::Uuid>,
) -> Arc<tokio::sync::RwLock<ReplicationTarget>> {
    Arc::new(tokio::sync::RwLock::new(ReplicationTarget {
        topic_id,
        leader_node_id: krabka_raft::NodeId(0),
        leader_epoch: krabka_metadata::LeaderEpoch(0),
    }))
}

impl Partition {
    /// Hold the partition's metadata-transition barrier through one Produce.
    /// A diskless nominal-leader promotion takes the matching write lock while
    /// it hydrates the canonical log and rebuilds producer state, so no append
    /// can race that publication boundary.
    pub(crate) async fn lock_produce_transition(
        &self,
    ) -> tokio::sync::OwnedRwLockReadGuard<ReplicationTarget> {
        self.replication_target.clone().read_owned().await
    }

    /// Lock this partition for a mutation from `expected`, rejecting a stale
    /// task before it can enqueue work on the single writer.
    pub(crate) async fn lock_replication_target(
        &self,
        expected: ReplicationTarget,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<ReplicationTarget>, BrokerError> {
        let current = self.replication_target.clone().read_owned().await;
        if *current != expected {
            return Err(BrokerError::Replication(format!(
                "stale replication target: expected {expected:?}, current {:?}",
                *current
            )));
        }
        Ok(current)
    }

    /// Apply a leader change observed in the metadata image. This updates
    /// the cached `current_leader` and `current_leader_epoch`. If the
    /// leader or epoch changed, it clears the per-follower stats, which are
    /// stale under the new leader's view. On an idempotent re-install with
    /// the same leader and epoch, it keeps the per-follower progress. The
    /// supervisor calls this on every reconcile, and an unconditional clear
    /// would reset follower LEOs each time. That would drop HW back
    /// to 0 and block acks=-1 producers until followers re-fetch.
    /// The method fires `hw_advance_notify` so waiting Produce gates can
    /// re-check.
    pub async fn install_leader_change(&self, new_leader: u64, new_epoch: i32) {
        self.install_replication_target(None, new_leader, new_epoch)
            .await;
    }

    /// Install the complete metadata identity used to fence follower writes.
    /// `topic_id` is optional so callers that only process leader changes can
    /// preserve an identity installed when the partition was materialized.
    pub(crate) async fn install_replication_target(
        &self,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
    ) {
        // Reconciliation also runs for metadata-only changes such as an ISR
        // update. Do not queue for the exclusive transition barrier when the
        // target itself is unchanged: an acks=all Produce holds a read guard
        // while it waits for that ISR update to advance the HW.
        {
            let current = self.replication_target.read().await;
            if topic_id.is_none_or(|id| current.topic_id == Some(id))
                && current.leader_node_id == krabka_raft::NodeId(new_leader)
                && current.leader_epoch == krabka_metadata::LeaderEpoch(new_epoch)
            {
                return;
            }
        }
        // Wait for any accepted follower mutation to finish before making the
        // new local role visible. Conversely, a mutation that arrives after
        // this write lock observes the new tuple and is fenced.
        let target = self.replication_target.write().await;
        self.publish_replication_target(target, topic_id, new_leader, new_epoch)
            .await;
    }

    /// Serialize a local promotion with all follower mutations, prepare the
    /// canonical log, and only then publish the new leader role.
    ///
    /// Produce admission also checks `current_leader`, so it cannot observe
    /// the metadata promotion until `prepare` and producer-state recovery have
    /// completed successfully.
    pub(crate) async fn install_replication_target_after_log_prepare<T, F>(
        &self,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
        prepare: impl FnOnce(&mut Log) -> Result<T, BrokerError>,
        after_prepare: impl FnOnce(T) -> F,
    ) -> Result<(), BrokerError>
    where
        F: std::future::Future<Output = ()>,
    {
        let target = self.replication_target.write().await;
        let prepared = {
            let mut log = self
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prepare(&mut log)?
        };
        after_prepare(prepared).await;
        self.publish_replication_target(target, topic_id, new_leader, new_epoch)
            .await;
        Ok(())
    }

    async fn publish_replication_target(
        &self,
        mut target: tokio::sync::RwLockWriteGuard<'_, ReplicationTarget>,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
    ) {
        let prev_leader = self.current_leader.swap(new_leader, Ordering::AcqRel);
        let prev_epoch = self.current_leader_epoch.swap(new_epoch, Ordering::AcqRel);
        if let Some(topic_id) = topic_id {
            target.topic_id = Some(topic_id);
        }
        target.leader_node_id = krabka_raft::NodeId(new_leader);
        target.leader_epoch = krabka_metadata::LeaderEpoch(new_epoch);
        drop(target);
        let leader_changed = prev_leader != new_leader || prev_epoch != new_epoch;
        let mut st = self.replica_state.lock().await;
        if leader_changed {
            // Diagnostic: every broker hosting this partition logs the
            // leader/epoch transition it observes in committed metadata. Logged
            // on ALL replicas, so the full leadership sequence survives even
            // when the controller-leader pod that drove the change is killed —
            // used to trace failover leadership churn / flip-flop.
            tracing::info!(
                topic = %self.topic,
                partition = self.index.get(),
                prev_leader,
                new_leader,
                prev_epoch,
                new_epoch,
                "partition leadership changed (observed in committed metadata)"
            );
            st.per_follower.clear();
        }
        st.current_leader_epoch = krabka_ids::LeaderEpoch(new_epoch);
        drop(st);
        self.hw_advance_notify.notify_waiters();
    }

    /// KIP-320's leader-epoch comparison, Kafka's
    /// `Partition.checkCurrentLeaderEpoch`. An asserted epoch below this
    /// partition's live epoch belongs to a leader generation that has already
    /// been superseded and takes `FENCED_LEADER_EPOCH`; one above it names a
    /// generation this broker has not observed yet and takes
    /// `UNKNOWN_LEADER_EPOCH`.
    ///
    /// Returns the error code together with the live epoch, which a response
    /// shape that reports the current leader back to the client needs.
    ///
    /// Which request epochs count as *asserted* is not spelled the same way in
    /// every API, so each caller decides that before it gets here. See
    /// [`Self::fetch_leader_epoch_fence`] and
    /// [`Self::list_offsets_leader_epoch_fence`].
    fn check_leader_epoch(&self, request_epoch: i32) -> Option<(i16, i32)> {
        let current = self.current_leader_epoch.load(Ordering::Acquire);
        if request_epoch == current {
            return None;
        }
        let code = if request_epoch < current {
            crate::codes::FENCED_LEADER_EPOCH
        } else {
            crate::codes::UNKNOWN_LEADER_EPOCH
        };
        Some((code, current))
    }

    /// [`Self::check_leader_epoch`] as Fetch reaches it.
    /// `FetchRequest.optionalEpoch` maps *any* negative epoch to
    /// `Optional.empty`, so a Fetch that carries one asserts nothing and
    /// passes unfenced.
    pub(crate) fn fetch_leader_epoch_fence(&self, request_epoch: i32) -> Option<(i16, i32)> {
        if request_epoch < 0 {
            return None;
        }
        self.check_leader_epoch(request_epoch)
    }

    /// [`Self::check_leader_epoch`] as `ListOffsets` reaches it.
    /// `RequestUtils.getLeaderEpoch` maps only `NO_PARTITION_LEADER_EPOCH`
    /// (`-1`) to `Optional.empty`, so every *other* negative epoch is an
    /// assertion and is fenced. The two APIs really do differ: on
    /// apache/kafka:4.3.1 with a live epoch of 0, `ListOffsets` v4 answers
    /// `current_leader_epoch = -2` with `FENCED_LEADER_EPOCH` while Fetch v11
    /// serves that same request its records.
    pub(crate) fn list_offsets_leader_epoch_fence(&self, request_epoch: i32) -> Option<(i16, i32)> {
        if request_epoch == NO_PARTITION_LEADER_EPOCH {
            return None;
        }
        self.check_leader_epoch(request_epoch)
    }

    /// Test-only: directly set the partition's `current_leader_epoch`
    /// and do not use the supervisor's metadata-image-driven path.
    /// `tests/leader_epoch.rs` uses this to simulate split-brain with a
    /// forced epoch bump mid-Produce.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_leader_epoch(&self, epoch: i32) {
        self.current_leader_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;
    use tokio::sync::Notify;

    use super::*;
    use crate::partition::test_support::test_partition;

    #[tokio::test]
    async fn replication_target_guard_fences_stale_generation_and_topic_identity() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let partition = Arc::new(partition);
        let topic_id = uuid::Uuid::new_v4();
        let current = ReplicationTarget {
            topic_id: Some(topic_id),
            leader_node_id: krabka_raft::NodeId(1),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
        };
        partition
            .install_replication_target(current.topic_id, 1, 7)
            .await;

        for stale in [
            ReplicationTarget {
                leader_node_id: krabka_raft::NodeId(2),
                ..current
            },
            ReplicationTarget {
                leader_epoch: krabka_metadata::LeaderEpoch(6),
                ..current
            },
            ReplicationTarget {
                topic_id: Some(uuid::Uuid::new_v4()),
                ..current
            },
        ] {
            assert!(partition.lock_replication_target(stale).await.is_err());
        }

        let guard = partition
            .lock_replication_target(current)
            .await
            .expect("current target");
        let update = tokio::spawn({
            let partition = partition.clone();
            async move { partition.install_replication_target(None, 2, 8).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !update.is_finished(),
            "leader install waits for mutation guard"
        );
        drop(guard);
        update.await.expect("leader install");
        assert!(partition.lock_replication_target(current).await.is_err());
    }

    #[tokio::test]
    async fn idempotent_replication_target_install_does_not_wait_for_produce_guard() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let topic_id = uuid::Uuid::new_v4();
        partition
            .install_replication_target(Some(topic_id), 1, 7)
            .await;

        let produce_guard = partition.lock_produce_transition().await;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            partition.install_replication_target(Some(topic_id), 1, 7),
        )
        .await
        .expect("unchanged target should not wait for the Produce read guard");
        drop(produce_guard);
    }

    #[tokio::test]
    async fn install_leader_change_clears_followers() {
        // (new_leader, new_epoch, seeded_follower_leo):
        // first case changes only the leader, second only the epoch —
        // either change alone must clear follower state.
        let cases = [(2u64, 0i32, 11i64), (0, 9, 17)];
        for (leader, epoch, seeded_leo) in cases {
            let (p, _td) = test_partition(Arc::new(Notify::new()));
            p.install_isr(
                &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                krabka_audit::NodeId(1),
            )
            .await;
            {
                let mut st = p.replica_state.lock().await;
                st.per_follower
                    .get_mut(&krabka_audit::NodeId(2))
                    .expect("follower")
                    .leo = Offset(seeded_leo);
            }

            p.install_leader_change(leader, epoch).await;

            assert!(
                p.current_leader.load(Ordering::Acquire) == leader,
                "case ({leader}, {epoch})"
            );
            assert!(
                p.current_leader_epoch.load(Ordering::Acquire) == epoch,
                "case ({leader}, {epoch})"
            );
            let st = p.replica_state.lock().await;
            assert!(st.per_follower.is_empty(), "case ({leader}, {epoch})");
            assert!(
                st.current_leader_epoch == krabka_ids::LeaderEpoch(epoch),
                "case ({leader}, {epoch})"
            );
        }
    }

    #[tokio::test]
    async fn each_api_spells_the_no_epoch_sentinel_the_way_kafka_spells_it() {
        // Live epoch 3. Kafka fences a `ListOffsets` row for every request
        // epoch but `-1` and the live one, while Fetch also lets every other
        // negative epoch through. Verified on apache/kafka:4.3.1: with a live
        // epoch of 0, `ListOffsets` v4 answers `current_leader_epoch = -2`
        // with FENCED_LEADER_EPOCH (74) and Fetch v11 serves it records.
        const LIVE: i32 = 3;
        let (p, _td) = test_partition(Arc::new(Notify::new()));
        p.test_set_leader_epoch(LIVE);

        let fenced = Some((crate::codes::FENCED_LEADER_EPOCH, LIVE));
        let unknown = Some((crate::codes::UNKNOWN_LEADER_EPOCH, LIVE));
        // (request epoch, Fetch verdict, ListOffsets verdict)
        let cases = [
            (LIVE - 1, fenced, fenced),
            (LIVE, None, None),
            (LIVE + 1, unknown, unknown),
            (-1, None, None),
            (-2, None, fenced),
            (i32::MIN, None, fenced),
        ];
        for (request_epoch, fetch, list_offsets) in cases {
            assert!(
                p.fetch_leader_epoch_fence(request_epoch) == fetch,
                "{request_epoch}"
            );
            assert!(
                p.list_offsets_leader_epoch_fence(request_epoch) == list_offsets,
                "{request_epoch}"
            );
        }
    }

    #[tokio::test]
    async fn test_set_leader_epoch_updates_cached_epoch() {
        let (p, _td) = test_partition(Arc::new(Notify::new()));

        p.test_set_leader_epoch(6);

        assert!(p.current_leader_epoch.load(Ordering::Acquire) == 6);
    }
}
