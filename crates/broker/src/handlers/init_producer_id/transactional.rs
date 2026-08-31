//! The transactional sub-path of `InitProducerId`: coordinator-side
//! allocation, epoch bump, and KIP-939 recovery of a prepared transaction.
//!
//! Once the request has passed the ACL preamble and this broker is confirmed
//! as the coordinator for the transactional id, everything that follows is one
//! state machine over the persisted `TxnEntry`. A fresh id allocates, a
//! prepared id is recovered, and a reused id aborts any ongoing transaction
//! before it bumps the epoch, so the transitions and the abort-marker fan-out
//! they depend on stay together.

use std::sync::Arc;

use krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;

use super::identity::{next_init_producer_identity, stage_recovery_identity};
use crate::{
    codes,
    error::BrokerError,
    txn::{
        coordinator::TxnCoordinator,
        state::{TxnEntry, TxnState},
        util::now_millis,
    },
};

/// Transactional sub-path: allocate or bump-epoch for `tid`.
// cargo-mutants: live transaction-coordinator orchestration; the response
// identity and error mapping are exercised by broker transaction integration.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    tid: &str,
    txnv: crate::txn::version::TxnVersion,
    txn_timeout: i32,
    enable_2pc: bool,
    keep_prepared_txn: bool,
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();

    match coord.get(tid) {
        None => {
            // Fresh tid — allocate a new producer id.
            let (pid, epoch) = coord.producer_ids.allocate().await?;
            let entry = TxnEntry::new_empty(tid.to_string(), pid, epoch, txn_timeout, now_ms);
            coord.put(entry, txnv).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                producer_id: pid.get(),
                producer_epoch: epoch,
                ..Default::default()
            })
        }
        Some(existing) => {
            if keep_prepared_txn {
                let recovery = {
                    let mut entry = existing.lock().await;
                    if entry.state == TxnState::Ongoing {
                        let ongoing_pid = entry.producer_id;
                        let ongoing_epoch = entry.producer_epoch;
                        if enable_2pc {
                            entry.txn_timeout_ms = i32::MAX;
                        }
                        let (next_pid, next_epoch) =
                            stage_recovery_identity(&mut entry, &coord.producer_ids).await?;
                        entry.last_update_ms = now_ms;
                        Some((
                            entry.clone(),
                            next_pid,
                            next_epoch,
                            ongoing_pid,
                            ongoing_epoch,
                        ))
                    } else {
                        None
                    }
                };
                if let Some((snapshot, next_pid, next_epoch, ongoing_pid, ongoing_epoch)) = recovery
                {
                    coord.put(snapshot, txnv).await?;
                    return Ok(InitProducerIdResponse {
                        error_code: codes::NONE,
                        producer_id: next_pid.get(),
                        producer_epoch: next_epoch,
                        ongoing_txn_producer_id: ongoing_pid.get(),
                        ongoing_txn_producer_epoch: ongoing_epoch,
                        ..Default::default()
                    });
                }
                let state = existing.lock().await.state;
                if matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort) {
                    return Ok(InitProducerIdResponse {
                        error_code: codes::CONCURRENT_TRANSACTIONS,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    });
                }
            }

            // Reusing tid — bump epoch (KIP-1319 v2). If prior state was
            // Ongoing, write PrepareAbort + dispatch abort markers before
            // responding.
            let aborted_ongoing = {
                let mut e = existing.lock().await;
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    let request_pid = crate::txn::handlers::end_txn::client_producer_identity(&e).0;
                    e.state = TxnState::PrepareAbort;
                    crate::txn::handlers::end_txn::prepare_completion_identities(
                        &mut e,
                        txnv,
                        &coord.producer_ids,
                    )
                    .await?;
                    e.last_update_ms = now_ms;
                    let entry_clone = e.clone();
                    drop(e); // release lock while we fan out markers
                    coord.put(entry_clone.clone(), txnv).await?;
                    dispatch_abort_markers(coord, &entry_clone).await?;
                    // Re-acquire + transition to CompleteAbort.
                    let mut e2 = existing.lock().await;
                    e2.state = TxnState::CompleteAbort;
                    e2.last_update_ms = now_millis();
                    let (completed_pid, completed_epoch) =
                        crate::txn::handlers::end_txn::completion_producer_identity(&e2);
                    if completed_pid != request_pid {
                        e2.prev_producer_id = request_pid;
                    }
                    e2.producer_id = completed_pid;
                    e2.producer_epoch = completed_epoch;
                    e2.next_producer_id = krabka_log::ProducerId(-1);
                    e2.next_producer_epoch = -1;
                    e2.partitions.clear();
                    let snap = e2.clone();
                    drop(e2);
                    coord.put(snap, txnv).await?;
                    true
                } else {
                    false
                }
            };

            // Bump epoch on the existing entry. Persist a new TxnEntry with
            // new epoch, Empty state, cleared partitions.
            let current = coord.get(tid).unwrap_or(existing);
            let mut e3 = current.lock().await;
            let (new_pid, new_epoch) = if aborted_ongoing && txnv.verified() {
                (e3.producer_id, e3.producer_epoch)
            } else {
                next_init_producer_identity(&e3, txnv, &coord.producer_ids).await?
            };
            *e3 = TxnEntry::new_empty(tid.to_string(), new_pid, new_epoch, txn_timeout, now_ms);
            let snap = e3.clone();
            drop(e3);
            coord.put(snap.clone(), txnv).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the entry's `ProducerId` into the raw-`i64` wire field.
                producer_id: snap.producer_id.get(),
                producer_epoch: snap.producer_epoch,
                ..Default::default()
            })
        }
    }
}

async fn dispatch_abort_markers(
    coord: &TxnCoordinator,
    entry: &TxnEntry,
) -> Result<(), BrokerError> {
    coord
        .dispatch_transaction_markers(entry, crate::txn::marker::MarkerType::Abort)
        .await
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};

    use super::*;
    use crate::txn::state::TopicPartition;

    /// `dispatch_abort_markers` appends an abort control-marker batch to each
    /// locally-led partition in the entry's partition set. Each append advances
    /// that partition's LEO by one. A whole-function `Ok(())` replacement would
    /// skip the dispatch entirely and leave the LEO at 0.
    #[tokio::test]
    async fn dispatch_abort_markers_appends_marker_to_local_partition() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );

        // Materialize a local partition for `__transaction_state`-style data.
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        assert!(part.log_end_offset() == 0);
        partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&part));

        // Build a txn entry that names this partition.
        let mut entry = TxnEntry::new_empty("tx-1".to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "orders".to_string(),
            partition: PartitionIndex(0),
        });

        dispatch_abort_markers(&coord, &entry)
            .await
            .expect("dispatch markers");

        // The abort marker is a single control record → LEO advances to 1.
        assert!(
            part.log_end_offset() == 1,
            "abort marker must be appended (LEO 1), got {:?}",
            part.log_end_offset()
        );
    }

    /// Without remote transport, a partition that is not hosted locally must
    /// fail the abort. Advancing the transaction without its marker would leave
    /// an open transaction in the data partition.
    #[tokio::test]
    async fn dispatch_abort_markers_rejects_missing_remote_transport() {
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );
        let mut entry = TxnEntry::new_empty("tx-2".to_string(), ProducerId(2000), 0, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "ghost".to_string(),
            partition: PartitionIndex(0),
        });
        assert!(dispatch_abort_markers(&coord, &entry).await.is_err());
    }
}
