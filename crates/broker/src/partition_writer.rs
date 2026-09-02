//! Spawned actor task that serializes appends for a single partition.
//!
//! The task owns the only `&mut Log` reference, through the shared
//! `Arc<Mutex<Log>>`. Reads bypass the actor. They take the same mutex for a
//! short time. The actor sends ordered acks back to producers. It also wakes
//! long-poll Fetch consumers with a shared `Notify` after each successful
//! append.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::Log;
use krabka_units::Time;
use tokio::sync::{Notify, mpsc};

use crate::{
    delivery::DeliveryHandles, log_dir_status::LogDirRegistry, partition::WriterMessage,
    producer_state::ProducerState, replica_state::ReplicaState,
};

mod append;
mod compaction;
mod mutations;
mod produce;
mod storage;
mod swap;

#[cfg(test)]
mod test_support;

// `run_writer!` must be in textual scope before the test modules that expand
// it, because a `macro_rules!` macro is only visible after its definition.
#[cfg(test)]
#[macro_use]
mod writer_macros;

#[cfg(test)]
mod tests;

// Only crate::wal's test modules reach the append helper through this path, so
// the re-export is test-gated: in a normal build it would be dead.
#[cfg(test)]
pub(crate) use self::append::run_produce_append_batch;
pub(crate) use self::storage::storage_failure_error;
use self::{
    compaction::handle_compact,
    mutations::{handle_replicate, handle_reset, handle_trim, handle_truncate},
    produce::handle_produce,
    storage::lock_log,
    swap::swap_future_log,
};

/// Loop on the receive side of the partition's `WriterMessage` channel.
///
/// The loop stops when the channel closes, that is, when all senders drop.
#[cfg(test)]
pub async fn run(
    identity: (String, PartitionIndex),
    storage: (Arc<Mutex<Log>>, Arc<ArcSwap<PathBuf>>),
    rx: mpsc::Receiver<WriterMessage>,
    signals: (
        Arc<Notify>,
        Arc<tokio::sync::Mutex<ReplicaState>>,
        Arc<Notify>,
        DeliveryHandles,
    ),
    services: (
        LogDirRegistry,
        Arc<ProducerState>,
        Option<crate::wal::SharedWal>,
    ),
) {
    run_with_sequencer(
        identity,
        storage,
        rx,
        signals,
        services,
        (
            crate::config::BrokerConfig::default().producer_id_expiration,
            crate::config::BrokerConfig::default().max_produce_group,
        ),
        None,
    )
    .await;
}

pub async fn run_with_sequencer(
    identity: (String, PartitionIndex),
    storage: (Arc<Mutex<Log>>, Arc<ArcSwap<PathBuf>>),
    mut rx: mpsc::Receiver<WriterMessage>,
    signals: (
        Arc<Notify>,
        Arc<tokio::sync::Mutex<ReplicaState>>,
        Arc<Notify>,
        DeliveryHandles,
    ),
    services: (
        LogDirRegistry,
        Arc<ProducerState>,
        Option<crate::wal::SharedWal>,
    ),
    limits: (Time, usize),
    sequencer: Option<Arc<dyn crate::wal::OffsetSequencer>>,
) {
    let (topic, partition) = identity;
    let (log, log_dir) = storage;
    let (append_notify, replica_state, hw_advance_notify, delivery) = signals;
    let (log_dir_status, producer_state, wal) = services;
    let (producer_id_expiration, max_produce_group) = limits;
    // `pending` holds a non-Produce message that was pulled off the channel
    // while group-draining Produce jobs (see the Produce arm). It is handled on
    // the next iteration so control messages are never reordered ahead of the
    // produces that preceded them in the channel.
    let mut pending: Option<WriterMessage> = None;
    loop {
        let msg = match pending.take() {
            Some(m) => m,
            None => match rx.recv().await {
                Some(m) => m,
                None => break, // channel closed: every sender dropped
            },
        };
        match msg {
            WriterMessage::Produce(first) => {
                handle_produce(
                    (&topic, partition),
                    (first, max_produce_group),
                    &mut rx,
                    &mut pending,
                    (&log, &log_dir, &log_dir_status),
                    (&append_notify, &replica_state, &hw_advance_notify),
                    (wal.as_ref(), sequencer.as_ref()),
                )
                .await;
            }
            WriterMessage::SyncDurable { leo, ack } => {
                let result = if let Some(wal) = wal.as_ref() {
                    if replica_state.lock().await.hw >= leo {
                        Ok(())
                    } else {
                        match wal.sync_durable(leo).await {
                            Ok(durable) => {
                                let mut state = replica_state.lock().await;
                                let previous = state.hw;
                                state.recompute_hw_for_wal_durable(durable);
                                if state.hw > previous {
                                    hw_advance_notify.notify_waiters();
                                }
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    }
                } else {
                    let log = Arc::clone(&log);
                    storage::run_log_mutation(
                        move || {
                            lock_log(&log)
                                .sync()
                                .map_err(crate::error::BrokerError::from)
                        },
                        "durable sync task panicked",
                        (&log_dir, &log_dir_status),
                    )
                    .await
                };
                let _ = ack.send(result);
            }
            WriterMessage::Replicate { batch, ack } => {
                handle_replicate(&log, &log_dir, &log_dir_status, batch, ack, &append_notify).await;
            }
            WriterMessage::Truncate { offset, ack } => {
                handle_truncate(
                    &log,
                    (&log_dir, &log_dir_status),
                    &replica_state,
                    wal.as_ref(),
                    offset,
                    ack,
                )
                .await;
            }
            WriterMessage::ResetTo { new_base, ack } => {
                handle_reset(
                    &log,
                    (&log_dir, &log_dir_status),
                    &replica_state,
                    wal.as_ref(),
                    new_base,
                    ack,
                )
                .await;
            }
            WriterMessage::TrimToOffset { new_start, ack } => {
                handle_trim(
                    &log,
                    (&log_dir, &log_dir_status),
                    wal.as_ref(),
                    new_start,
                    ack,
                )
                .await;
            }
            WriterMessage::SetLogConfig { config, ack } => {
                lock_log(&log).set_config(config);
                let _ = ack.send(());
            }
            WriterMessage::Compact { ack } => {
                handle_compact(
                    (&topic, partition),
                    (&log, &log_dir, &log_dir_status),
                    &producer_state,
                    producer_id_expiration,
                    ack,
                )
                .await;
            }
            #[cfg(any(test, feature = "test-helpers"))]
            WriterMessage::TestSetLogStart { new_start, ack } => {
                let result = lock_log(&log)
                    .set_log_start_offset(new_start)
                    .map_err(crate::error::BrokerError::from);
                let _ = ack.send(result);
            }
            WriterMessage::SwapFutureLog {
                target_log_dir,
                future_log,
                future_path,
                target_partition_path,
                ack,
            } => {
                let result = swap_future_log(
                    &log,
                    &log_dir,
                    target_log_dir,
                    &future_log,
                    &future_path,
                    &target_partition_path,
                );
                let _ = ack.send(result);
                // No `append_notify` — swap doesn't deliver new data,
                // and consumers re-read from the swapped `log` against
                // identical offsets.
            }
        }
        // Every arm above can move the log end, the log start, or the delivery
        // policy itself, so one refresh here covers all of them rather than
        // leaving the mirror stale after the arms that do not append.
        publish_delivery(&log, &delivery);
    }
}

/// Refresh the partition's delivery watermark after a writer message, and
/// re-arm the broker-wide delivery scheduler when the log is left with a batch
/// waiting.
///
/// The writer runs this after every message, because a truncation, a trim, a
/// compaction, a log-dir swap and a config change all move something the
/// watermark is derived from, not only an append.
///
/// This is a liveness step, not a correctness one. A fetch recomputes the
/// watermark under the log mutex it already holds, so a skipped refresh delays
/// a parked consumer and can never let it read a batch early. On a topic that
/// delivers immediately the call costs one uncontended mutex and no I/O.
fn publish_delivery(log: &Mutex<Log>, delivery: &DeliveryHandles) {
    if let Some(state) = delivery.publish_now(log)
        && let Some(deadline_ms) = state.next_deadline_ms
    {
        delivery.wake_scheduler(deadline_ms);
    }
}
