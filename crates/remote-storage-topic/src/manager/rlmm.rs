//! The [`RemoteLogMetadataManager`] implementation for the topic-backed
//! manager, with the publish-and-wait bridge its mutation methods use.
//!
//! Every mutation serializes an event, publishes it to the metadata log, and
//! blocks until the consumer pump returns the result of applying the published
//! offset. The bridge lives in the sibling `mutations` module. Each gated read
//! consults [`ReadGate`] before it delegates to the inner cache.

use {
    super::{TopicBasedRemoteLogMetadataManager, assignment::ReadGate},
    crate::{error::MetadataLogError, partitioning::metadata_partition_for, serde::MetadataEvent},
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
        RemoteStorageError, TopicIdPartition,
    },
};

impl TopicBasedRemoteLogMetadataManager {
    /// Apply the assignment/readiness gate before consulting cached metadata.
    fn read_if_ready<T: Default>(
        &self,
        partition: &TopicIdPartition,
        read: impl FnOnce(&InmemoryRemoteLogMetadataManager) -> Result<T, RemoteStorageError>,
    ) -> Result<T, RemoteStorageError> {
        let mp = metadata_partition_for(partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // An unassigned partition is a genuine miss, even if the cache
            // retains entries consumed under an earlier assignment.
            ReadGate::Unassigned => Ok(T::default()),
            // Catching up is retryable and must remain distinct from a miss.
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => read(&self.inner),
        }
    }

    /// Whether metadata partition `mp` has applied everything below the
    /// log's current high-water mark. The caller has already passed the
    /// read gate, so the assignment-time target is reached; this check asks
    /// the log again so it also covers events committed after assignment.
    fn applied_through_current_hwm(&self, mp: i32) -> Result<bool, RemoteStorageError> {
        let hwms = self.runtime.block_on(async {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => Err(MetadataLogError::Closed),
                hwms = self.log.high_water_marks() => hwms,
            }
        });
        let hwms = hwms.map_err(MetadataLogError::into_storage)?;
        let idx = usize::try_from(mp).ok();
        let Some(hwm) = idx.and_then(|i| hwms.get(i).copied()) else {
            return Err(MetadataLogError::Other(format!(
                "no high-water mark for metadata partition {mp}"
            ))
            .into_storage());
        };
        let applied = self.applied.lock().expect("applied mutex poisoned");
        Ok(idx
            .and_then(|i| applied.get(i).copied())
            .is_some_and(|applied| applied >= hwm - 1))
    }
}

impl RemoteLogMetadataManager for TopicBasedRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        // Mirror the in-memory manager's eager precondition: fail
        // fast before paying a round trip through Kafka.
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id: metadata.remote_log_segment_id().clone(),
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let event = MetadataEvent::AddSegment(metadata).encode();
        self.publish_and_wait(&tp, event)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let tp = update.remote_log_segment_id.topic_id_partition.clone();
        let event = MetadataEvent::UpdateSegment(update).encode();
        self.publish_and_wait(&tp, event)
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
        })
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.highest_offset_for_epoch(topic_id_partition, leader_epoch)
        })
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.list_remote_log_segments(topic_id_partition)
        })
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.list_remote_log_segments_by_epoch(topic_id_partition, leader_epoch)
        })
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.topic_id_partition.clone();
        let event = MetadataEvent::PartitionDelete(metadata).encode();
        self.publish_and_wait(&tp, event)
    }

    // Like every sync method here, this blocks on the manager's runtime, so it
    // is called only from `spawn_blocking` threads.
    fn caught_up(&self, topic_id_partition: &TopicIdPartition) -> Result<bool, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // This broker does not consume `mp`, so it cannot vouch for it.
            ReadGate::Unassigned => Ok(false),
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => self.applied_through_current_hwm(mp),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            log::{InProcessMetadataEventLog, MetadataEventLog},
            manager::test_support::{
                HwmFlakyLog, finish, on_blocking, seed, seed_finished, start_manager,
                start_manager_all, started, tp,
            },
        },
        assert2::{assert, check},
        krabka_remote_storage::RemotePartitionDeleteState,
        std::sync::Arc,
    };

    krabka_macros::remote_segment_check!(check_finished_segment);

    #[tokio::test(flavor = "multi_thread")]
    async fn add_finish_query_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = start_manager_all(log).await;
        seed_finished(&m, &[seed(10, 0, 99)]).await;

        check_finished_segment(m.as_ref(), &tp());
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_with_wrong_state_is_rejected_eagerly() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log.clone());
        // Force a non-Started state via the lifecycle helper.
        let bad = started(10, 0, 9).with_update(&finish(10)).unwrap();
        let m2 = m.clone();
        let err = on_blocking(move || m2.add_remote_log_segment_metadata(bad).unwrap_err()).await;
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
        // Eager rejection means nothing was published.
        assert!(log.high_water_marks().await.unwrap() == vec![0; 2]);
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partition_delete_lifecycle_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager_all(log).await;
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            let m2 = m.clone();
            on_blocking(move || {
                m2.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                    topic_id_partition: tp(),
                    state,
                    event_timestamp_ms: 500,
                    broker_id: 1,
                })
                .unwrap();
            })
            .await;
        }
        m.shutdown();
    }

    /// How a `caught_up` call ended, in a form a test can compare whole.
    #[derive(Debug, PartialEq, Eq)]
    enum CaughtUp {
        Answer(bool),
        NotReady(i32),
        Io,
    }

    impl From<Result<bool, RemoteStorageError>> for CaughtUp {
        fn from(result: Result<bool, RemoteStorageError>) -> Self {
            match result {
                Ok(answer) => Self::Answer(answer),
                Err(RemoteStorageError::NotReady { partition }) => Self::NotReady(partition),
                Err(RemoteStorageError::Io(_)) => Self::Io,
                Err(other) => panic!("unexpected caught_up error: {other:?}"),
            }
        }
    }

    /// The state the manager is in when the test asks `caught_up`.
    #[derive(Clone, Copy)]
    enum Scenario {
        /// Assigned, and every published event applied.
        Seeded,
        /// Assigned and past the read gate, but another writer committed an
        /// event that the pump has not applied yet.
        Behind,
        /// The manager does not consume the metadata partition.
        Unassigned,
        /// Assigned, but the assignment-time HWM is still unknown.
        NotReady,
        /// Assigned and ready, but the current HWM cannot be fetched.
        HwmFailure,
        /// The manager has shut down.
        Shutdown,
    }

    async fn ask(m: &Arc<TopicBasedRemoteLogMetadataManager>) -> CaughtUp {
        let m = m.clone();
        on_blocking(move || m.caught_up(&tp())).await.into()
    }

    #[yare::parameterized(
        seeded = { Scenario::Seeded, |_| CaughtUp::Answer(true) },
        behind = { Scenario::Behind, |_| CaughtUp::Answer(false) },
        unassigned = { Scenario::Unassigned, |_| CaughtUp::Answer(false) },
        not_ready = { Scenario::NotReady, CaughtUp::NotReady },
        hwm_failure = { Scenario::HwmFailure, |_| CaughtUp::Io },
        shutdown = { Scenario::Shutdown, |_| CaughtUp::Io },
    )]
    #[test_macro(tokio::test(flavor = "multi_thread"))]
    async fn caught_up_answers_from_the_current_high_water_mark(
        scenario: Scenario,
        expected: fn(i32) -> CaughtUp,
    ) {
        let log = HwmFlakyLog::new(4);
        let mp = metadata_partition_for(&tp(), log.partition_count());
        // The expectation takes the metadata partition, which a NotReady
        // answer names.
        let expected = expected(mp);
        let (m, got) = match scenario {
            Scenario::Seeded => {
                let m = start_manager_all(log.clone()).await;
                seed_finished(&m, &[seed(10, 0, 99)]).await;
                let got = ask(&m).await;
                (m, got)
            }
            Scenario::Behind => {
                let m = start_manager_all(log.clone()).await;
                seed_finished(&m, &[seed(10, 0, 99)]).await;
                // The pump takes this lock before it applies a record, so
                // holding it keeps the next record committed but unapplied.
                let pump_stall = m.mutation_waiters.stall_pump().await;
                let event = MetadataEvent::AddSegment(started(11, 100, 199)).encode();
                log.publish(mp, event).await.unwrap();
                let got = ask(&m).await;
                drop(pump_stall);
                (m, got)
            }
            Scenario::Unassigned => {
                let m = start_manager(log.clone());
                let got = ask(&m).await;
                (m, got)
            }
            Scenario::NotReady => {
                let m = start_manager(log.clone());
                log.set_fail_hwm(true);
                m.reconcile_assignment(&[mp]).await;
                let got = ask(&m).await;
                (m, got)
            }
            Scenario::HwmFailure => {
                let m = start_manager_all(log.clone()).await;
                log.set_fail_hwm(true);
                let got = ask(&m).await;
                (m, got)
            }
            Scenario::Shutdown => {
                let m = start_manager_all(log.clone()).await;
                m.shutdown();
                let got = ask(&m).await;
                (m, got)
            }
        };
        check!(got == expected);
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn caught_up_turns_true_once_the_pump_applies_a_peer_write() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let reader = start_manager_all(log.clone()).await;
        let writer = start_manager_all(log).await;
        let stall = reader.mutation_waiters.stall_pump().await;
        let w = writer.clone();
        on_blocking(move || {
            w.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        assert!(ask(&reader).await == CaughtUp::Answer(false));
        drop(stall);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while ask(&reader).await != CaughtUp::Answer(true) {
            assert!(
                std::time::Instant::now() < deadline,
                "reader never caught up"
            );
            tokio::task::yield_now().await;
        }
        assert!(reader.list_remote_log_segments(&tp()).unwrap().len() == 1);
        reader.shutdown();
        writer.shutdown();
    }
}
