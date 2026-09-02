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
            // A `Dead` entry is one the KIP-98 expiry sweep marked under this
            // very lock before it appended the tid's tombstone. This call was
            // parked on the lock while that happened, so its handle is no
            // longer the coordinator's: reviving from it would persist a
            // producer identity for a transactional id whose tombstone is
            // already in the log, and race a second `InitProducerId` that
            // found no entry and took the fresh-id path above. Kafka answers
            // a metadata object mid-transition with `CONCURRENT_TRANSACTIONS`,
            // which the client retries; the retry finds no entry and
            // allocates cleanly.
            if e3.state == TxnState::Dead {
                return Ok(InitProducerIdResponse {
                    error_code: codes::CONCURRENT_TRANSACTIONS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                });
            }
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
    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};
    use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};

    use super::*;
    use crate::{
        partition::Partition,
        partition_registry::PartitionRegistry,
        txn::{bootstrap, state::TopicPartition, version::TxnVersion},
    };

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

    /// Kafka's `transactional.id.expiration.ms` default.
    const EXPIRY_MS: i64 = 604_800_000;

    /// Opens `__transaction_state-0` as a real log under `dir` and returns it.
    fn transaction_state_partition(dir: &std::path::Path) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(dir, bootstrap::TOPIC, 0);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            PartitionIndex(0),
            dir.to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open log"),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        )
    }

    /// A coordinator that leads the single `__transaction_state` partition,
    /// with one committed transactional id already persisted into it.
    async fn coordinator_with_completed_transaction(
        dir: &std::path::Path,
        tid: &str,
    ) -> (Arc<TxnCoordinator>, Arc<Partition>) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: bootstrap::TOPIC.to_string(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: bootstrap::TOPIC.to_string(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            ..Default::default()
        }));

        let partitions = Arc::new(PartitionRegistry::new());
        let part = transaction_state_partition(dir);
        partitions.insert(
            bootstrap::TOPIC.into(),
            PartitionIndex(0),
            Arc::clone(&part),
        );
        let coordinator = TxnCoordinator::new(
            NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        );
        coordinator.refresh_leader_partitions(&image).await;

        let mut entry = TxnEntry::new_empty(tid.to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        entry.last_update_ms = 0;
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
        (Arc::new(coordinator), part)
    }

    /// The KIP-98 expiry sweep and an `InitProducerId` already parked on the
    /// entry's lock must never both persist an identity for one transactional
    /// id.
    ///
    /// The sweep unpublishes the entry from the coordinator's map while this
    /// call holds a clone of its `Arc`. Reviving through that detached handle
    /// would append a producer identity for a tid whose tombstone is already
    /// in the log, and a second `InitProducerId` that found no entry would
    /// allocate a competing one -- two live identities for one id, with
    /// whichever append landed last deciding the coordinator's state. The
    /// sweep marks the entry `Dead` under the same lock, so the parked call
    /// wakes to Kafka's `CONCURRENT_TRANSACTIONS` and retries onto the
    /// fresh-id path.
    ///
    /// The runtime is single-threaded and each task is stepped to its park
    /// with an explicit yield, so the interleaving is the same every run:
    /// the sweep queues on the lock first, the call queues behind it.
    #[tokio::test]
    async fn an_init_parked_on_the_expiry_sweep_does_not_revive_the_tombstoned_id() {
        const TID: &str = "tid-parked-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        assert!(part.log_end_offset() == 1);

        // The test holds the entry lock, so both tasks below park on it in
        // the order they are stepped.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let sweep = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                coordinator
                    .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
                    .await
            })
        };
        tokio::task::yield_now().await;

        let init = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                handle_transactional(
                    &coordinator,
                    TID,
                    TxnVersion::Verified,
                    60_000,
                    false,
                    false,
                )
                .await
            })
        };
        tokio::task::yield_now().await;

        drop(guard);
        let expired = sweep.await.expect("sweep task");
        let response = init.await.expect("init task").expect("init responds");

        check!(expired == vec![TID.to_string()]);
        check!(
            response
                == InitProducerIdResponse {
                    error_code: codes::CONCURRENT_TRANSACTIONS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
        );
        // The id stays expired: nothing was published back into the map, and
        // the log ends at the tombstone the sweep appended.
        check!(coordinator.get(TID).is_none());
        check!(part.log_end_offset() == 2);
    }
}
