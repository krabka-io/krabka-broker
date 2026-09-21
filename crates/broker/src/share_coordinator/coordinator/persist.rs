//! The durable append path for `__share_group_state` and the best-effort prune
//! of that partition's log prefix.
//!
//! `persist_record` is the single write path that `initialize`, `write`, and
//! `delete` share. `maybe_prune` is the KIP-932 log trim that follows a folded
//! `ShareSnapshot`. Both talk to the partition log rather than to the delivery
//! state machine, so they live apart from `state_machine`.

use std::sync::Arc;

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::Mutex;
use tracing::warn;

use super::{ShareCoordinator, ShareErrorCode, ShareStateError, message};
use crate::{
    codes,
    error::BrokerError,
    share_coordinator::{
        bootstrap,
        persistence::{ShareStateKey, encode_state_key},
        pruning::redundant_offset,
        state::SharePartitionState,
    },
};

/// A failed append to `__share_group_state`.
#[derive(Debug, thiserror::Error)]
pub(super) enum AppendError {
    /// The partition log is not open on this broker.
    #[error("__share_group_state-{0} not local")]
    NotLocal(PartitionIndex),
    /// The partition log refused the append.
    #[error(transparent)]
    Failed(BrokerError),
}

impl AppendError {
    /// The error of the append as the coordinator answers it.
    ///
    /// The code follows Kafka's
    /// `CoordinatorOperationExceptionHelper.handleOperationException`.
    pub(super) fn share_error(&self) -> ShareStateError {
        let append_code = match self {
            Self::NotLocal(_) => codes::UNKNOWN_TOPIC_OR_PARTITION,
            // A local append that fails on the log or the writer task is a
            // storage failure in Kafka's terms.
            Self::Failed(BrokerError::Io(_) | BrokerError::Log(_) | BrokerError::Txn(_)) => {
                codes::KAFKA_STORAGE_ERROR
            }
            Self::Failed(error) => codes::from_broker_error(error),
        };
        ShareStateError::Operation {
            code: operation_error_code(append_code),
            message: append_message(append_code),
        }
    }
}

/// Kafka's `handleOperationException` mapping from an append error code to
/// the code of the coordinator answer.
fn operation_error_code(append_code: ShareErrorCode) -> ShareErrorCode {
    match append_code {
        codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::NOT_ENOUGH_REPLICAS
        | codes::REQUEST_TIMED_OUT => codes::COORDINATOR_NOT_AVAILABLE,
        codes::NOT_LEADER_OR_FOLLOWER | codes::KAFKA_STORAGE_ERROR => codes::NOT_COORDINATOR,
        codes::MESSAGE_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => other,
    }
}

/// The message of the append error, before the mapping.
fn append_message(append_code: ShareErrorCode) -> &'static str {
    match append_code {
        codes::UNKNOWN_TOPIC_OR_PARTITION => message::UNKNOWN_TOPIC_OR_PARTITION,
        codes::KAFKA_STORAGE_ERROR => message::KAFKA_STORAGE_ERROR,
        codes::NOT_COORDINATOR => message::NOT_COORDINATOR,
        _ => message::UNKNOWN_SERVER_ERROR,
    }
}

impl ShareCoordinator {
    /// Appends one `(key, value)` record to `__share_group_state`-`p`.
    ///
    /// This method returns the base offset of the record. A `None` value writes
    /// a tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`AppendError::NotLocal`] if the partition log is not open
    /// locally, and [`AppendError::Failed`] if `produce_batch` fails.
    pub(super) async fn persist_record(
        &self,
        state_partition: PartitionIndex,
        key: ShareStateKey,
        value: Option<Bytes>,
    ) -> Result<Offset, AppendError> {
        let part = self
            .partitions
            .get(bootstrap::TOPIC, state_partition)
            .ok_or(AppendError::NotLocal(state_partition))?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(encode_state_key(&key)),
            value,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await.map_err(AppendError::Failed)
    }

    /// Prunes the log prefix of `state_partition` on a best-effort basis.
    ///
    /// This method computes `redundant_offset`, the smallest
    /// `last_snapshot_offset` across every live key mapped to that state
    /// partition. If `redundant_offset` is more than the current
    /// `log_start_offset` of the partition, the method trims the log up to it.
    /// Every retained key keeps its latest snapshot, so the trim is safe. The
    /// method logs each error and then discards it. A prune never fails a
    /// write.
    pub(super) async fn maybe_prune(&self, state_partition: PartitionIndex) {
        let Some(part) = self.partitions.get(bootstrap::TOPIC, state_partition) else {
            return;
        };

        // Collect this partition's keys' last-snapshot offsets.
        let handles: Vec<Arc<Mutex<SharePartitionState>>> = self
            .state
            .iter()
            .filter(|e| {
                let (g, t, p) = e.key();
                self.state_partition_for(g, t, *p) == state_partition
            })
            .map(|e| e.value().clone())
            .collect();

        let mut offsets = Vec::with_capacity(handles.len());
        for h in handles {
            offsets.push(h.lock().await.last_snapshot_offset);
        }

        let Some(redundant) = redundant_offset(&offsets) else {
            return;
        };
        if redundant > part.log_start_offset()
            && let Err(e) = part.trim_to_offset(redundant).await
        {
            warn!(
                partition = state_partition.get(),
                error = %e,
                "share-state log prune failed; continuing"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        partition_registry::PartitionRegistry,
        share_coordinator::{
            config::ShareCoordinatorConfig,
            coordinator::test_support::{
                batch, image_with_topic, lead_all, open_state_partition, share_write,
            },
        },
    };

    #[tokio::test]
    async fn snapshot_fold_after_threshold_resets_counter() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
            open_state_partition(&reg, dir.path(), p);
        }
        // Small threshold so a few writes trigger a fold.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 3,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(krabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([9; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        for i in 0..3 {
            let base = i64::from(i) * 10;
            coord
                .write(
                    &image_with_topic(tid, 1),
                    "g",
                    tid,
                    0,
                    share_write((1, 1), (0, 0), vec![batch(base, base + 9)]),
                )
                .await
                .unwrap();
        }

        let st = coord.state_for_test("g", tid, 0).await.expect("present");
        // After the 3rd update crossed the threshold, a snapshot was folded
        // and the counter reset.
        assert!(st.updates_since_snapshot == 0);
        assert!(st.snapshot_epoch == 1);
    }

    /// After a snapshot fold, `maybe_prune` must trim the state-partition log.
    ///
    /// The trim goes up to the redundant offset, which is the offset of the
    /// folded snapshot. The `log_start_offset` of the partition then advances
    /// past 0. If the prune does not run, `log_start_offset` stays at 0.
    #[tokio::test]
    async fn snapshot_fold_prunes_log_prefix() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
            open_state_partition(&reg, dir.path(), p);
        }
        // Fold after 2 updates so a snapshot lands a few records in.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 2,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(krabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([13; 16]);
        let state_partition = coord.state_partition_for("g", &tid, 0);
        let part = reg
            .get(bootstrap::TOPIC, state_partition)
            .expect("state partition open");

        // record 0: initialize snapshot.
        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        // records 1,2: updates; the 2nd crosses the threshold and folds a
        // snapshot at record 3, then prunes up to it.
        coord
            .write(
                &image_with_topic(tid, 1),
                "g",
                tid,
                0,
                share_write((1, 1), (0, 0), vec![batch(0, 9)]),
            )
            .await
            .unwrap();
        coord
            .write(
                &image_with_topic(tid, 1),
                "g",
                tid,
                0,
                share_write((1, 1), (0, 0), vec![batch(10, 19)]),
            )
            .await
            .unwrap();

        // The folded snapshot's offset is the sole key's last-snapshot offset,
        // which is > 0 and exceeds the log's initial start (0), so the prune
        // advanced the prefix. Without pruning the start stays at 0.
        let start = part.log_start_offset();
        check!(start > 0);
    }
}
