//! The durable append path for `__share_group_state` and the best-effort prune
//! of that partition's log prefix.
//!
//! `persist_record` is the single write path that `initialize`, `write`, and
//! `delete` share. `maybe_prune` is the KIP-932 log trim that the prune timer
//! in `jobs` runs. Both talk to the partition log rather than to the delivery
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
    /// The append took longer than `share.coordinator.write.timeout.ms`.
    #[error("__share_group_state append timed out")]
    TimedOut,
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
            Self::TimedOut => codes::REQUEST_TIMED_OUT,
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
        codes::REQUEST_TIMED_OUT => message::REQUEST_TIMED_OUT,
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
    /// locally, [`AppendError::Failed`] if `produce_batch` fails, and
    /// [`AppendError::TimedOut`] if it does not finish within the configured
    /// write timeout.
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

        // Kafka's `share.coordinator.state.topic.compression.codec`. The
        // state topic keeps `compression.type=producer`, so the log stores
        // the batch in this codec.
        let mut batch = RecordBatch::default();
        batch.attributes = batch
            .attributes
            .with_compression(self.config.state_topic_compression_codec);
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(encode_state_key(&key)),
            value,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        tokio::time::timeout(self.config.write_timeout, part.produce_batch(batch))
            .await
            .map_err(|_| AppendError::TimedOut)?
            .map_err(AppendError::Failed)
    }

    /// Prunes the log prefix of `state_partition` on a best-effort basis.
    ///
    /// This method computes `redundant_offset`, the smallest
    /// `last_snapshot_offset` across every live key mapped to that state
    /// partition. If `redundant_offset` is more than the current
    /// `log_start_offset` of the partition, the method trims the log up to it.
    /// Every retained key keeps its latest snapshot, so the trim is safe. The
    /// method logs each error and then discards it.
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
    use assert2::check;

    use super::*;

    /// Kafka's `handleOperationException` codes for each append failure,
    /// with the message of the error before the mapping.
    #[test]
    fn append_errors_answer_as_kafka() {
        let rows = [
            (
                AppendError::NotLocal(PartitionIndex(3)),
                codes::COORDINATOR_NOT_AVAILABLE,
                message::UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                AppendError::TimedOut,
                codes::COORDINATOR_NOT_AVAILABLE,
                message::REQUEST_TIMED_OUT,
            ),
        ];
        for (error, code, message) in rows {
            check!(
                error.share_error() == ShareStateError::Operation { code, message },
                "{error}"
            );
        }
    }
}
