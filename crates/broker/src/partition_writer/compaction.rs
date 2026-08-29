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
use crate::{log_dir_status::LogDirRegistry, producer_state::ProducerState};

async fn active_producers_for_compaction(
    producer_state: &ProducerState,
    topic: &str,
    partition: PartitionIndex,
    now_ms: i64,
    producer_id_expiration: Time,
) -> std::collections::HashMap<krabka_log::ProducerId, Offset> {
    producer_state
        .active_snapshot(topic, partition, now_ms, producer_id_expiration)
        .await
        .into_iter()
        .map(|(producer_id, offset)| (krabka_log::ProducerId(producer_id), Offset(offset)))
        .collect()
}

pub(super) async fn handle_compact(
    identity: (&str, PartitionIndex),
    storage: (&Arc<Mutex<Log>>, &Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    producer_state: &ProducerState,
    producer_id_expiration: Time,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let (topic, partition) = identity;
    let (log, log_dir, log_dir_status) = storage;
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
    let context = krabka_log::CompactionContext {
        now,
        active_producers,
    };
    let log_for_blocking = Arc::clone(log);
    let join = tokio::task::spawn_blocking(move || {
        lock_log(&log_for_blocking)
            .compact(&context)
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
            .commit("t", PartitionIndex(0), (7, 0), (0, 0), (12, 0))
            .await;
        let last_activity_ms = state.snapshot("t", PartitionIndex(0)).await[0]
            .1
            .last_activity_ms;

        let expired = active_producers_for_compaction(
            &state,
            "t",
            PartitionIndex(0),
            last_activity_ms + 2,
            millis(1),
        )
        .await;
        let active = active_producers_for_compaction(
            &state,
            "t",
            PartitionIndex(0),
            last_activity_ms + 2,
            millis(2),
        )
        .await;

        assert!(expired.is_empty());
        assert!(
            active
                == [(krabka_log::ProducerId(7), Offset(12))]
                    .into_iter()
                    .collect()
        );
    }
}
