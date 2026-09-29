//! The writer's Compact arm and the producer snapshot that feeds it.
//!
//! Compaction is the one writer message that has to consult producer state
//! before it touches the log, because an active producer id must survive the
//! rewrite, so that lookup lives next to the arm that needs it.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, Offset};
use krabka_units::Time;

use super::storage::{flag_storage_failure, lock_log, storage_failure_error};
use crate::{
    log_dir_status::LogDirRegistry, producer_state::ProducerState, replica_state::ReplicaState,
};

async fn active_producers_for_compaction(
    producer_state: &ProducerState,
    topic: &str,
    partition: PartitionIndex,
    now_ms: i64,
    producer_id_expiration: Time,
) -> std::collections::HashMap<krabka_log::ProducerId, krabka_log::ProducerLastRecord> {
    producer_state
        .active_snapshot(topic, partition, now_ms, producer_id_expiration)
        .await
        .into_iter()
        .map(|(producer_id, last)| (krabka_log::ProducerId(producer_id), last))
        .collect()
}

pub(super) async fn handle_compact(
    identity: (&str, PartitionIndex),
    storage: (&Arc<Mutex<Log>>, &Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    producer_state: &ProducerState,
    producer_id_expiration: Time,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let (topic, partition) = identity;
    let (log, log_dir, log_dir_status) = storage;
    // The pass is bounded at the last stable offset, not the high watermark:
    // Kafka's `LogCleanerManager.cleanableOffsets` takes `lastStableOffset`,
    // because the records between the LSO and the high watermark belong to
    // transactions with no marker yet. Rewriting them before the commit or
    // abort is known would leave the output segment's aborted-transaction
    // index unable to learn about a marker that arrives later. The watermark
    // is read inside the writer actor, which is the only task that moves the
    // log, so no append or truncation can land between the read and the
    // rewrite.
    let high_watermark = replica_state.lock().await.hw;
    let now = std::time::SystemTime::now();
    let now_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        });
    let active_producers = active_producers_for_compaction(
        producer_state,
        topic,
        partition,
        now_ms,
        producer_id_expiration,
    )
    .await;
    let log_for_blocking = Arc::clone(log);
    let join = crate::blocking::spawn_blocking(move || {
        let mut log = lock_log(&log_for_blocking);
        let last_stable_offset = log.last_stable_offset(high_watermark);
        let context = krabka_log::CompactionContext {
            now,
            last_stable_offset,
            active_producers,
        };
        log.compact(&context)
            .map_err(crate::error::BrokerError::from)
    });
    let result = match join.await {
        Ok(value) => value,
        Err(join_err) => Err(storage_failure_error("compact task panicked", join_err)),
    };
    if let Err(err) = &result {
        flag_storage_failure(err, log_dir, log_dir_status);
    }
    let _ = ack.send(result);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::millis;

    use super::*;

    #[tokio::test]
    async fn nondefault_ttl_controls_producer_compaction_snapshot() {
        let state = ProducerState::new();
        state
            .commit("t", PartitionIndex(0), (7, 0), (0, 0), (12, 100, false))
            .await;

        let expired =
            active_producers_for_compaction(&state, "t", PartitionIndex(0), 102, millis(2)).await;
        let active =
            active_producers_for_compaction(&state, "t", PartitionIndex(0), 102, millis(3)).await;

        assert!(expired.is_empty());
        assert!(
            active
                == [(
                    krabka_log::ProducerId(7),
                    krabka_log::ProducerLastRecord {
                        last_data_offset: Some(Offset(12)),
                        producer_epoch: 0,
                    },
                )]
                .into_iter()
                .collect()
        );
    }
}
