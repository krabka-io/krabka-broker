//! A benchmark seam over the replicator's per-batch work: the
//! `RecordBatch::encoded_len` walk it does for the replication-bytes metric,
//! and the [`Partition::replicate_batch`] call that carries the same batch to
//! the follower's log.
//!
//! The loop that runs these two steps in production
//! ([`super::response::handle_partition_response`]) also holds a leader
//! connection, a fetch session and a replication-target guard, none of which
//! the "what does the extra `encoded_len` walk cost" question depends on. This
//! module is the partition and its writer task with none of that, so
//! `benches/perf_deferrals.rs` can time the walk against the append it sits in
//! front of.
//!
//! [`ReplicaSeam::replicate`] is the production `replicate_batch`: the same
//! channel send to the same writer actor, which runs the same `append_at`.

use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64},
    },
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, LogError, Offset};
use krabka_protocol::records::RecordBatch;

use crate::{
    delivery::DeliveryHandles,
    error::BrokerError,
    log_dir_status::LogDirRegistry,
    partition::{Partition, WriterMessage, initial_replication_target},
    producer_state::ProducerState,
    replica_state::ReplicaState,
};

/// A follower partition over `dir`, with its writer actor running.
///
/// The caller owns `dir`; the seam does not create or remove it, because a
/// temp-directory crate is a dev-dependency and this module also compiles in a
/// plain `test-helpers` build.
pub struct ReplicaSeam {
    partition: Partition,
}

impl ReplicaSeam {
    /// Open a log under `dir` and spawn the partition writer that drains its
    /// [`WriterMessage`] channel.
    ///
    /// Must be called from inside a Tokio runtime: the writer is a spawned
    /// task, exactly as it is under `Broker`.
    ///
    /// # Errors
    ///
    /// Returns [`LogError`] when the log directory cannot be opened.
    pub fn spawn(dir: &Path) -> Result<Self, LogError> {
        let log = Arc::new(Mutex::new(Log::open(dir, LogConfig::default())?));
        let log_dir = Arc::new(ArcSwap::from_pointee(dir.to_path_buf()));
        let (writer_tx, rx) = tokio::sync::mpsc::channel::<WriterMessage>(8);
        let append_notify = Arc::new(tokio::sync::Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
        let hw_advance_notify = Arc::new(tokio::sync::Notify::new());
        // The writer and the partition share one set of delivery handles, as
        // they do in production: the writer refreshes the mirror the partition
        // reads.
        let delivery = DeliveryHandles::new();
        let writer = tokio::spawn(crate::partition_writer::run_with_sequencer(
            ("bench-topic".to_string(), PartitionIndex(0)),
            (Arc::clone(&log), Arc::clone(&log_dir)),
            rx,
            (
                Arc::clone(&append_notify),
                Arc::clone(&replica_state),
                Arc::clone(&hw_advance_notify),
                delivery.clone(),
            ),
            (
                LogDirRegistry::default(),
                Arc::new(ProducerState::new()),
                None,
            ),
            (
                crate::config::BrokerConfig::default().producer_id_expiration,
                crate::config::BrokerConfig::default().max_produce_group,
            ),
            None,
        ));
        Ok(Self {
            partition: Partition {
                topic: "bench-topic".to_string(),
                index: PartitionIndex(0),
                log_dir,
                log,
                writer_tx,
                // Empty, exactly as `broker::partition_spawn` leaves it: this
                // seam replicates data batches and never materializes a
                // transaction marker, so nothing ever reaches this map.
                marker_materialization: Arc::new(tokio::sync::Mutex::new(
                    std::collections::HashMap::default(),
                )),
                append_notify,
                replica_state,
                hw_advance_notify,
                current_leader: Arc::new(AtomicU64::new(0)),
                current_leader_epoch: Arc::new(AtomicI32::new(0)),
                delivery,
                replication_target: initial_replication_target(None),
                diskless: false,
                writer_handle: Arc::new(Mutex::new(Some(writer))),
            },
        })
    }

    /// The offset the leader would assign to the next replicated batch.
    ///
    /// `append_at` rejects a batch whose `base_offset` is not the log end, so
    /// a caller stamps this into the batch the way the leader already has.
    #[must_use]
    pub fn next_offset(&self) -> Offset {
        self.partition.log_end_offset()
    }

    /// Append one leader-assigned batch through the writer actor.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the writer rejects the append or dies.
    pub async fn replicate(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        self.partition.replicate_batch(batch).await
    }
}

#[cfg(test)]
mod tests;
