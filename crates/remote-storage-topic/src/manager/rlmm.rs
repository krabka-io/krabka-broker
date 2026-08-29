//! The [`RemoteLogMetadataManager`] implementation for the topic-backed
//! manager, with the publish-and-wait bridge its mutation methods use.
//!
//! Every mutation serializes an event, publishes it to the metadata log, and
//! blocks until the consumer pump has applied the published offset, so the
//! bridge belongs beside the trait methods that depend on it. Each gated read
//! consults [`ReadGate`] before it delegates to the inner cache.

use std::sync::Arc;

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemoteStorageError, TopicIdPartition,
};

use super::{TopicBasedRemoteLogMetadataManager, assignment::ReadGate};
use crate::{
    error::MetadataLogError, log::MetadataEventLog, partitioning::metadata_partition_for,
    serde::MetadataEvent,
};

impl TopicBasedRemoteLogMetadataManager {
    async fn wait_for_offset(&self, partition: i32, offset: i64) {
        let idx = usize::try_from(partition).expect("partition non-negative");
        let mut rx = self.applied_tx.subscribe();
        loop {
            {
                let applied = self.applied.lock().expect("applied mutex poisoned");
                if applied[idx] >= offset {
                    return;
                }
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn publish_and_wait(
        &self,
        tp: &TopicIdPartition,
        event: Bytes,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata_partition_for(tp, self.log.partition_count());
        let log = Arc::clone(&self.log);
        // Caller is on a non-runtime (spawn_blocking) thread; block_on
        // is safe and gives us the assigned offset to wait on.
        self.runtime.block_on(async {
            let offset = log
                .publish(partition, event)
                .await
                .map_err(MetadataLogError::into_storage)?;
            self.wait_for_offset(partition, offset).await;
            Ok::<_, RemoteStorageError>(())
        })
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
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // Not this broker's partition → genuine miss, do NOT serve any
            // stale cache.
            ReadGate::Unassigned => Ok(None),
            // Assigned but not caught up → retryable, distinct from a miss.
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => {
                self.inner
                    .remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
            }
        }
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            ReadGate::Unassigned => Ok(None),
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => self
                .inner
                .highest_offset_for_epoch(topic_id_partition, leader_epoch),
        }
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // Not this broker's partition → it does not own it, so it must
            // not serve any stale segments it happened to consume earlier.
            ReadGate::Unassigned => Ok(Vec::new()),
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => self.inner.list_remote_log_segments(topic_id_partition),
        }
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.inner
            .list_remote_log_segments_by_epoch(topic_id_partition, leader_epoch)
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.topic_id_partition.clone();
        let event = MetadataEvent::PartitionDelete(metadata).encode();
        self.publish_and_wait(&tp, event)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_remote_storage::{CustomMetadata, RemotePartitionDeleteState};
    use uuid::Uuid;

    use super::*;
    use crate::{
        log::InProcessMetadataEventLog,
        manager::test_support::{
            finish, on_blocking, start_manager, start_manager_all, started, tp,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn add_finish_query_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = start_manager_all(log).await;
        let m2 = m.clone();
        on_blocking(move || {
            m2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let m2 = m.clone();
        on_blocking(move || m2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

        let got = m
            .remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42)
            .unwrap()
            .expect("segment found");
        check!(got.remote_log_segment_id().id == Uuid::from_u128(10));
        check!(got.custom_metadata() == Some(&CustomMetadata(vec![7])));
        check!(m.highest_offset_for_epoch(&tp(), LeaderEpoch(0)).unwrap() == Some(99));
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
}
