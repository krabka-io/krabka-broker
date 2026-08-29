//! The [`Partition`] methods that drive its single writer task. Each one sends
//! a [`WriterMessage`] and awaits the writer's acknowledgement, and they are
//! grouped here because they share that one request-response shape.

use krabka_log::Offset;
use krabka_protocol::records::RecordBatch;
use tokio::sync::oneshot;

use crate::{
    error::BrokerError,
    partition::{Partition, ProduceData, ProduceJob, WriterMessage},
};

impl Partition {
    /// Push `overrides` through the writer actor so the partition's `Log`
    /// picks up the new `retention.ms`, `retention.bytes`, and
    /// `segment.bytes` on the next retention or roll tick. The caller has
    /// already validated `overrides`; see `config_keys`. The call is
    /// idempotent, so the same map pushed twice is a cheap noop.
    /// `ReplicatorSupervisor::reconcile` calls this every time the metadata
    /// image changes.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Replication` if the writer is dead or the
    /// ack is dropped.
    pub(crate) async fn apply_log_config_overrides(
        &self,
        overrides: &std::collections::BTreeMap<String, String>,
        base: &krabka_log::LogConfig,
    ) -> Result<(), BrokerError> {
        let merged = crate::config_keys::apply_to_log_config(overrides, base);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::SetLogConfig {
                config: merged,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?;
        Ok(())
    }

    /// Append a leader-assigned batch to the local log and keep its
    /// `base_offset`. The per-partition replicator on a follower broker
    /// calls this. It sends the batch through the writer task so the batch
    /// stays ordered with produce appends. On a follower the produce handler
    /// rejects those appends anyway, but the channel ordering is still part
    /// of the invariant.
    pub async fn replicate_batch(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Replicate { batch, ack: ack_tx })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Truncate the log to `offset` and drop all records at offsets
    /// `>= offset`. The replicator's `OFFSET_OUT_OF_RANGE` recovery path
    /// calls this, and so does the KIP-320 in-band `diverging_epoch`
    /// truncation path, which passes the leader's epoch boundary and not 0.
    pub async fn truncate_to(&self, offset: Offset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Truncate {
                offset,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Drop every segment and recreate the active segment at `new_base`.
    /// The request goes through the writer task, so it stays ordered with
    /// appends.
    pub async fn reset_to(&self, new_base: Offset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::ResetTo {
                new_base,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Send a trim request through the writer actor. Returns the resulting
    /// `log_start_offset`. The `DeleteRecords` handler calls this.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` if the writer is dead, the ack is dropped,
    /// or the underlying `Log::trim_to_offset` fails (negative offset).
    pub async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::TrimToOffset {
                new_start,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Send a `WriterMessage::Compact` to the partition's writer
    /// actor and await the ack. The broker-wide [`Cleaner`] ticker
    /// calls this.
    pub async fn compact_log(&self) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Compact { ack: ack_tx })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("compact ack dropped".into()))?
    }

    /// Append `batch` to the local log at the next assigned offset. The append
    /// goes through the partition's writer task, so it stays ordered with
    /// all other produce appends. Returns the assigned `base_offset`.
    ///
    /// `TxnCoordinator::put` uses this to persist `__transaction_state`
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the writer task is dead or the ack
    /// channel closes before the writer replies.
    pub(crate) async fn produce_batch(&self, batch: RecordBatch) -> Result<Offset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(batch),
                ack: ack_tx,
            }))
            .await
            .map_err(|_| BrokerError::Txn("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Txn("ack dropped".into()))?
    }

    /// Append an internally built COMMIT marker with a coordinator-supplied
    /// commit stamp. The partition writer keeps it ordered with all produce
    /// and replication appends.
    ///
    /// # Errors
    /// Returns an error if the writer task is unavailable or the log rejects
    /// the marker/stamp pair.
    pub(crate) async fn produce_commit_marker(
        &self,
        batch: RecordBatch,
        commit_stamp: u64,
    ) -> Result<Offset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::OwnedCommitMarker {
                    batch,
                    commit_stamp,
                },
                ack: ack_tx,
            }))
            .await
            .map_err(|_| BrokerError::Txn("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Txn("ack dropped".into()))?
    }

    /// Append an internally built control batch, such as a transaction ABORT
    /// marker or a barrier marker.
    ///
    /// The partition writer keeps it ordered with all produce and replication
    /// appends, and appends it without the compression rewrite that
    /// [`Self::produce_batch`] applies.
    ///
    /// The caller stamps `partition_leader_epoch` before it calls this
    /// function. The writer does not stamp it, and a batch that keeps the
    /// default of zero carries a false leader epoch in its header.
    ///
    /// # Errors
    /// Returns [`BrokerError::Txn`] if the writer task is dead or the ack
    /// channel closes before the writer replies, or the log rejects the batch.
    pub(crate) async fn produce_control_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<Offset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::OwnedControl(batch),
                ack: ack_tx,
            }))
            .await
            .map_err(|_| BrokerError::Txn("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Txn("ack dropped".into()))?
    }

    /// Test-only: shift the partition's in-memory `log_start_offset` to
    /// `new_start`. The request goes through the writer task to keep the
    /// single-writer invariant on the underlying `Log`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn test_set_log_start(&self, new_start: Offset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::TestSetLogStart {
                new_start,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::partition::test_support::test_partition_with_writer;

    #[tokio::test]
    async fn test_set_log_start_updates_log_start_through_writer() {
        let (p, _td) = test_partition_with_writer();

        p.test_set_log_start(Offset(5))
            .await
            .expect("set log start");

        assert!(p.log_start_offset() == 5);
    }
}
