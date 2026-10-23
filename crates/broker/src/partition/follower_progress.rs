//! Fenced follower observations and the partition's durability authority.

use {
    super::{Partition, ReplicationTarget},
    krabka_log::Offset,
    krabka_metadata::NodeId,
    std::time::Instant,
};

/// The follower state that a follower fetch reports for one partition, kept
/// until the first read of that partition runs.
///
/// Kafka's `Partition.fetchRecords` reads the follower's records first and
/// only then calls `updateFollowerFetchState`. So a follower fetch answered by
/// that read carries the high watermark from before its own fetch offset
/// counted toward it, and the follower learns the watermark its fetch moved
/// from its next response. Only a fetch that parks reads again after the
/// update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FollowerPosition {
    pub(crate) follower_id: i32,
    /// The broker epoch the follower's Fetch carried. KIP-841 compares it
    /// with the broker's registered epoch before the follower may rejoin the
    /// ISR.
    pub(crate) broker_epoch: i64,
    /// The follower's own log start offset, which a `DeleteRecords` waits for.
    pub(crate) log_start_offset: i64,
    /// The replication target that the leader checks of this fetch passed
    /// under. The leader records the position only while this target is
    /// still installed.
    pub(crate) target: ReplicationTarget,
}

impl Partition {
    /// Kafka's `Partition.updateFollowerFetchState`: record that `position`'s
    /// follower holds every record below `fetch_offset`, and recompute the high
    /// watermark. `leader_log_end` is the leader's log end offset when the read
    /// started, as Kafka passes `LogReadInfo.logEndOffset`. Returns whether the
    /// high watermark advanced.
    ///
    /// The caller runs this after the follower's read succeeded. Kafka records
    /// nothing for a read that failed or found a diverging epoch, because
    /// `fetchRecords` throws or returns before the update.
    ///
    /// A leadership change between the leader checks and this update installs a
    /// new replication target and clears the follower state. Kafka's
    /// `maybeIncrementLeaderHW` runs under `leaderIsrUpdateLock` only while this
    /// broker still leads. This method also records nothing after the target
    /// changed.
    pub(crate) async fn record_follower_position(
        &self,
        position: FollowerPosition,
        fetch_offset: i64,
        leader_log_end: Offset,
    ) -> bool {
        let transition = self.lock_produce_transition().await;
        if *transition != position.target {
            return false;
        }
        let follower = NodeId(u64::try_from(position.follower_id).unwrap_or(0));
        let advanced = {
            let mut state = self.replica_state.lock().await;
            let previous = state.hw;
            // Kafka's `Replica.updateFetchStateOrThrow` records the follower's log
            // start offset with its fetch offset. A `DeleteRecords` waits for it.
            state.record_follower_log_start(follower, Offset(position.log_start_offset));
            // KIP-841: the ISR-expansion eligibility check compares this with the
            // broker's registered epoch before `update_follower_leo` reads it.
            state.record_follower_broker_epoch(follower, position.broker_epoch);
            let now = Instant::now();
            if self.diskless {
                // Canonical replica progress must neither replace nor rewind the WAL watermark.
                state.record_follower_fetch(follower, Offset(fetch_offset), leader_log_end, now);
                false
            } else {
                state.update_follower_leo(follower, Offset(fetch_offset), leader_log_end, now)
                    > previous
            }
        };
        drop(transition);
        if advanced {
            self.hw_advance_notify.notify_waiters();
        }
        advanced
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{StorageMode, spawn_standalone_partition},
        assert2::assert,
        krabka_log::{Log, LogConfig},
        std::{path::Path, sync::Arc},
    };

    fn observation_partition(dir: &Path, storage: StorageMode) -> Arc<Partition> {
        spawn_standalone_partition()
            .log_dir(dir)
            .log(Log::open(dir, LogConfig::default()).expect("open partition log"))
            .topic("progress")
            .storage(storage)
            .call()
    }

    async fn install_two_replica_leader(partition: &Partition) {
        use krabka_raft::NodeId;

        partition.install_replication_target(None, 1, 0).await;
        partition
            .install_isr(&[NodeId(1), NodeId(2)], &[NodeId(1), NodeId(2)], NodeId(1))
            .await;
    }

    #[tokio::test]
    async fn a_follower_position_is_recorded_only_under_its_own_target() {
        use krabka_raft::NodeId;

        #[derive(Debug, PartialEq)]
        struct Recorded {
            advanced: bool,
            high_watermark: Offset,
            follower_log_end: Offset,
        }

        let cases = [
            ("the target is still installed", false, Recorded {
                advanced: true,
                high_watermark: Offset(2),
                follower_log_end: Offset(2),
            }),
            (
                "a new leader epoch was installed after the checks",
                true,
                Recorded {
                    advanced: false,
                    high_watermark: Offset(0),
                    follower_log_end: Offset(0),
                },
            ),
        ];

        for (name, epoch_moved, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let partition = observation_partition(dir.path(), StorageMode::Local);
            install_two_replica_leader(&partition).await;
            let position = FollowerPosition {
                follower_id: 2,
                broker_epoch: 7,
                log_start_offset: 1,
                target: *partition.replication_target.read().await,
            };
            if epoch_moved {
                partition.install_replication_target(None, 1, 1).await;
            }

            let before = partition.replica_state.lock().await.clone();
            let changed = partition.hw_advance_notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            let advanced = partition
                .record_follower_position(position, 2, Offset(2))
                .await;
            assert!(futures_util::poll!(changed.as_mut()).is_ready() == !epoch_moved);
            if epoch_moved {
                assert!(*partition.replica_state.lock().await == before);
            } else {
                let state = partition.replica_state.lock().await;
                assert!(state.eligibility(NodeId(2)).fetch_broker_epoch == Some(7));
                assert!(state.follower_can_serve(NodeId(2), Offset(1)));
                assert!(!state.follower_can_serve(NodeId(2), Offset(0)));
            }

            let recorded = Recorded {
                advanced,
                high_watermark: partition.high_watermark().await,
                follower_log_end: partition
                    .replica_state
                    .lock()
                    .await
                    .follower_progress(NodeId(2))
                    .0,
            };
            assert!(recorded == want, "{name}");
        }
    }

    #[yare::parameterized(
        stale_fetch_cannot_rewind_wal_durability = { StorageMode::Diskless, 2, 1, 2, false },
        replica_progress_cannot_replace_wal_durability = { StorageMode::Diskless, 1, 2, 1, false },
        classic_replication_still_advances_the_watermark = { StorageMode::Local, 1, 2, 2, true },
    )]
    #[test_macro(tokio::test)]
    async fn follower_fetches_preserve_the_storage_durability_authority(
        storage: StorageMode,
        durable: i64,
        read_log_end: i64,
        expected_watermark: i64,
        expected_advance: bool,
    ) {
        use {assert2::assert, krabka_raft::NodeId};

        let dir = tempfile::tempdir().unwrap();
        let partition = observation_partition(dir.path(), storage);
        install_two_replica_leader(&partition).await;
        partition
            .replica_state
            .lock()
            .await
            .recompute_hw_for_wal_durable(Offset(durable));
        let position = FollowerPosition {
            follower_id: 2,
            broker_epoch: 7,
            log_start_offset: 0,
            target: *partition.replication_target.read().await,
        };
        let changed = partition.hw_advance_notify.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();

        let advanced = partition
            .record_follower_position(position, read_log_end, Offset(read_log_end))
            .await;

        assert!(advanced == expected_advance);
        assert!(partition.high_watermark().await == Offset(expected_watermark));
        assert!(futures_util::poll!(changed.as_mut()).is_ready() == expected_advance);
        let state = partition.replica_state.lock().await;
        let (log_end, caught_up) = state.follower_progress(NodeId(2));
        assert!(log_end == Offset(read_log_end));
        assert!(caught_up.is_some());
    }
}
