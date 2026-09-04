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
use krabka_verified::transaction::InitProducerIdFencingDecision;

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
///
/// `request_identity` is the `(producer_id, producer_epoch)` the caller
/// claims, `(-1, -1)` when it claims none. Kafka's
/// `TransactionCoordinator.prepareInitProducerIdTransit` fences a stale claim
/// with `PRODUCER_FENCED` before it matches on the persisted state, so an
/// ongoing transaction is aborted only for the producer that owns it
/// (KIP-360). It does that inside `txnMetadata.inLock`, together with the
/// transit it prepares, and so does every branch below: the check and the
/// mutation it admits share one guard.
///
/// A transactional id with no entry is not fenced, whatever the caller
/// claims. Kafka's `isValidProducerId` opens with
/// `txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH`, which the
/// metadata it just created always satisfies: a producer recovering from
/// `UNKNOWN_PRODUCER_ID` names its old identity, and the freshly allocated one
/// is what it gets back.
pub(super) async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    tid: &str,
    txnv: crate::txn::version::TxnVersion,
    txn_timeout: i32,
    enable_2pc: bool,
    keep_prepared_txn: bool,
    request_identity: (i64, i16),
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
            // KIP-360: a caller that names a producer identity must name the
            // live one, or the epoch this entry held before an epoch fence
            // whose abort failed. Kafka runs that check inside
            // `txnMetadata.inLock` together with the transit it prepares, so
            // every branch below re-runs it under the very lock that performs
            // its mutation. Two overlapping v3 calls that name the same live
            // identity therefore serialize on that lock, and the one that
            // loses finds the epoch the winner already advanced. A zombie
            // neither recovers a prepared transaction nor aborts the ongoing
            // transaction of the producer that fenced it.
            if keep_prepared_txn {
                let recovery = {
                    let mut entry = existing.lock().await;
                    if is_fenced(&entry, request_identity) {
                        return Ok(fenced_response());
                    }
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
                let entry = existing.lock().await;
                if is_fenced(&entry, request_identity) {
                    return Ok(fenced_response());
                }
                let state = entry.state;
                drop(entry);
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
                if is_fenced(&e, request_identity) {
                    return Ok(fenced_response());
                }
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    let (request_pid, fenced_from_epoch) =
                        crate::txn::handlers::end_txn::client_producer_identity(&e);
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
                    // `put` republishes the tid under a fresh handle, so the
                    // one this call started from is no longer the entry a
                    // concurrent `coord.get` finds. Everything below must act
                    // on the published entry.
                    let published = coord.get(tid).unwrap_or_else(|| Arc::clone(&existing));
                    if let Err(error) = dispatch_abort_markers(coord, &entry_clone).await {
                        // KIP-360: the epoch fence is persisted but the abort
                        // it was prepared for did not complete. The producer
                        // that owns the transaction still holds
                        // `fenced_from_epoch`, so record it on the published
                        // entry and let only that producer retry its
                        // `InitProducerId`. Kafka keeps `hasFailedEpochFence`
                        // in memory too: a coordinator that loses it fails
                        // closed, and the producer is fenced.
                        let mut fenced = published.lock().await;
                        fenced.last_producer_epoch = fenced_from_epoch;
                        fenced.has_failed_epoch_fence = true;
                        drop(fenced);
                        return Err(error);
                    }
                    // Re-acquire + transition to CompleteAbort.
                    let mut e2 = published.lock().await;
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
            // The check comes before the identity one below because a `Dead`
            // entry holds no live mapping to be fenced against, exactly like
            // the tid that was never there: Kafka's `isValidProducerId` opens
            // by admitting freshly created metadata, and the retry this answer
            // asks for finds no entry and allocates.
            if e3.state == TxnState::Dead {
                return Ok(InitProducerIdResponse {
                    error_code: codes::CONCURRENT_TRANSACTIONS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                });
            }
            // The abort above already advanced the entry past the identity
            // this call named, so only a call that has changed nothing yet
            // revalidates here -- under the same lock as the epoch bump below.
            if !aborted_ongoing && is_fenced(&e3, request_identity) {
                return Ok(fenced_response());
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

/// Whether `request_identity` is fenced against `entry`'s live identity.
///
/// The caller holds `entry`'s lock, and the mutation the verdict admits runs
/// under that same guard.
fn is_fenced(entry: &TxnEntry, request_identity: (i64, i16)) -> bool {
    let (entry_pid, entry_epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    krabka_verified::transaction::init_producer_id_fencing_decision(
        entry_pid.get(),
        entry_epoch,
        entry.last_producer_epoch,
        entry.has_failed_epoch_fence,
        request_identity.0,
        request_identity.1,
    ) == InitProducerIdFencingDecision::Fenced
}

/// Kafka's `initTransactionError(Errors.PRODUCER_FENCED)`.
fn fenced_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::PRODUCER_FENCED,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
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

    /// Seeds one `TxnEntry` into a coordinator that already leads its
    /// `__transaction_state` partition.
    async fn seed(coordinator: &TxnCoordinator, entry: TxnEntry) {
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
    }

    /// A transactional id the coordinator has no entry for is answered with a
    /// freshly allocated identity even when the caller names one of its own.
    ///
    /// Kafka's `isValidProducerId` opens with `txnMetadata.producerEpoch ==
    /// RecordBatch.NO_PRODUCER_EPOCH`, and the metadata
    /// `handleInitProducerId` creates for an unknown id always satisfies it:
    /// the case the clause exists for is a producer recovering from
    /// `UNKNOWN_PRODUCER_ID`, which names the identity it last held and must
    /// get the new one back rather than `PRODUCER_FENCED`.
    #[tokio::test]
    async fn an_unknown_transactional_id_is_not_fenced_by_a_supplied_identity() {
        const SEEDED: &str = "tid-seeded";
        const UNKNOWN: &str = "tid-unknown";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), SEEDED).await;
        check!(coordinator.get(UNKNOWN).is_none());

        let response = handle_transactional(
            &coordinator,
            UNKNOWN,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (4242, 7),
        )
        .await
        .expect("init responds");

        check!(response.error_code == codes::NONE);
        check!(response.producer_id != 4242);
        check!(response.producer_epoch == 0);
    }

    /// Two `InitProducerId` v3 calls that name the same live identity race for
    /// one transactional id. Exactly one may win: the identity check and the
    /// epoch bump it admits run under one lock, so the loser wakes to the
    /// epoch the winner already wrote and is fenced.
    ///
    /// With the check hoisted above the mutation, both calls read epoch 3,
    /// both bump, and the first caller's response is already stale when it
    /// reaches the client -- two live producers for one transactional id.
    ///
    /// The runtime is single-threaded and both tasks are stepped to their park
    /// on the entry lock with an explicit yield, so the interleaving is the
    /// same every run.
    #[tokio::test]
    async fn overlapping_inits_for_one_identity_fence_the_loser() {
        const TID: &str = "tid-overlapping-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Both calls park on the entry lock while the test holds it.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let mut calls = Vec::new();
        for _ in 0..2 {
            let coordinator = Arc::clone(&coordinator);
            calls.push(tokio::spawn(async move {
                handle_transactional(
                    &coordinator,
                    TID,
                    TxnVersion::Verified,
                    60_000,
                    false,
                    false,
                    (1000, 3),
                )
                .await
            }));
            tokio::task::yield_now().await;
        }
        drop(guard);

        let mut answered: Vec<i16> = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answered.push(response.error_code);
        }
        answered.sort_unstable();
        check!(answered == vec![codes::NONE, codes::PRODUCER_FENCED]);

        // One bump landed, not two.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!((entry.producer_id, entry.producer_epoch) == (ProducerId(1000), 4));
    }

    /// A failed abort-marker fan-out must leave the failed-epoch fence on the
    /// entry the coordinator publishes, not on the handle this call started
    /// from: `put` republishes the tid under a fresh `Arc`, so a write to the
    /// superseded handle is invisible to the retry that comes to read it.
    ///
    /// KIP-360 lets exactly the producer that still holds the pre-fence epoch
    /// retry, and that allowance is what the retry below exercises. Without
    /// it the producer is answered `PRODUCER_FENCED` and its transaction is
    /// stuck in `PrepareAbort`.
    #[tokio::test]
    async fn a_failed_abort_fan_out_records_the_fence_on_the_published_entry() {
        const TID: &str = "tid-failed-fence";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // An ongoing transaction over a partition this broker does not host:
        // the abort marker cannot be delivered, so the fan-out fails after
        // the epoch fence is already persisted.
        let mut ongoing = TxnEntry::new_empty(TID.to_string(), ProducerId(1000), 3, 60_000, 0);
        ongoing.state = TxnState::Ongoing;
        ongoing.partitions.insert(TopicPartition {
            topic: "ghost".to_string(),
            partition: PartitionIndex(0),
        });
        seed(&coordinator, ongoing).await;

        let failed = handle_transactional(
            &coordinator,
            TID,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (1000, 3),
        )
        .await;
        check!(failed.is_err());

        let published = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(published.has_failed_epoch_fence);
        check!(published.last_producer_epoch == 3);
        check!(published.state == TxnState::PrepareAbort);

        // The owner of the fenced-from epoch retries and is admitted; a
        // zombie that names any other epoch is not.
        let zombie = handle_transactional(
            &coordinator,
            TID,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (1000, 2),
        )
        .await
        .expect("zombie responds");
        check!(zombie.error_code == codes::PRODUCER_FENCED);

        let retry = handle_transactional(
            &coordinator,
            TID,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (1000, 3),
        )
        .await
        .expect("retry responds");
        check!(retry.error_code == codes::NONE);
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
                    (-1, -1),
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
