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
use krabka_verified::transaction::InitProducerIdIdentityDecision;

use super::identity::{next_init_producer_identity, stage_recovery_identity};
use crate::{
    codes,
    error::BrokerError,
    txn::{
        coordinator::{TxnCoordinator, completion::completion_for},
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
                    if let Some(response) = pending_completion_response(&entry, request_identity) {
                        return Ok(response);
                    }
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
                if let Some(response) = pending_completion_response(&entry, request_identity) {
                    return Ok(response);
                }
                if is_fenced(&entry, request_identity) {
                    return Ok(fenced_response());
                }
            }

            // Reusing tid. Kafka fences the epoch of an ongoing transaction,
            // aborts it, and answers CONCURRENT_TRANSACTIONS. Otherwise it
            // bumps the epoch and answers the new identity.
            {
                // Lock order: the state-partition write lock, then the entry
                // lock, as `EndTxn`, the reaper and the completion task take
                // them. Every append takes the partition lock, so the entry
                // read under it is the published one.
                let state_partition_write = coord.lock_state_partition_for(tid).await;
                let current = coord.get(tid).unwrap_or_else(|| Arc::clone(&existing));
                let mut e = current.lock().await;
                if let Some(response) = pending_completion_response(&e, request_identity) {
                    return Ok(response);
                }
                if is_fenced(&e, request_identity) {
                    return Ok(fenced_response());
                }
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    let (request_pid, fenced_from_epoch) =
                        crate::txn::handlers::end_txn::client_producer_identity(&e);
                    // Stage on a clone: until the PrepareAbort record is
                    // durable, other callers must still see Ongoing.
                    let mut prepared = e.clone();
                    prepared.state = TxnState::PrepareAbort;
                    // Kafka `prepareFenceProducerEpoch`: the epoch of the
                    // ongoing transaction is raised before the abort, so every
                    // abort marker fences the producer at its partitions. The
                    // epoch is never answered to the client. It is not raised
                    // again after a fence whose abort failed, and never past
                    // `i16::MAX`.
                    if !prepared.has_failed_epoch_fence && prepared.producer_epoch < i16::MAX {
                        prepared.producer_epoch += 1;
                    }
                    crate::txn::handlers::end_txn::prepare_completion_identities(
                        &mut prepared,
                        txnv,
                        &coord.producer_ids,
                    )
                    .await?;
                    prepared.last_update_ms = now_ms;
                    coord
                        .put_under_state_partition_lock(prepared.clone(), txnv)
                        .await?;
                    *e = prepared.clone();
                    drop(e);
                    drop(state_partition_write);
                    // `put` republishes the tid under a fresh handle, so the
                    // one this call started from is no longer the entry a
                    // concurrent `coord.get` finds. Everything below must act
                    // on the published entry.
                    let published = coord.get(tid).unwrap_or(current);
                    if let Err(error) = dispatch_abort_markers(coord, &prepared).await {
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
                        // The PrepareAbort record is durable. Kafka answers
                        // the fence with CONCURRENT_TRANSACTIONS and finishes
                        // the abort in its marker channel; the producer
                        // retries.
                        tracing::warn!(
                            tid,
                            %error,
                            "InitProducerId: abort marker fan-out failed; queued for completion"
                        );
                        coord.request_completion(tid);
                        return Ok(concurrent_transactions_response());
                    }
                    // Re-acquire + transition to CompleteAbort, staged on a
                    // clone so a failed append leaves PrepareAbort for the
                    // completion task.
                    let mut completed = published.lock().await.clone();
                    completed.state = TxnState::CompleteAbort;
                    completed.last_update_ms = now_millis();
                    let (completed_pid, completed_epoch) =
                        crate::txn::handlers::end_txn::completion_producer_identity(&completed);
                    if completed_pid != request_pid {
                        completed.prev_producer_id = request_pid;
                    }
                    completed.producer_id = completed_pid;
                    completed.producer_epoch = completed_epoch;
                    completed.next_producer_id = krabka_log::ProducerId(-1);
                    completed.next_producer_epoch = -1;
                    completed.partitions.clear();
                    if let Err(error) = coord.put(completed, txnv).await {
                        tracing::warn!(
                            tid,
                            %error,
                            "InitProducerId: CompleteAbort append failed; queued for completion"
                        );
                        coord.request_completion(tid);
                        return Ok(concurrent_transactions_response());
                    }
                    // Kafka answers the fenced producer
                    // `CONCURRENT_TRANSACTIONS`. The client retries, and that
                    // retry takes the epoch bump below on the completed
                    // transaction.
                    return Ok(concurrent_transactions_response());
                }
            }

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
                return Ok(concurrent_transactions_response());
            }
            if let Some(response) = pending_completion_response(&e3, request_identity) {
                return Ok(response);
            }
            if let Some(response) = retried_bump_response(&e3, request_identity) {
                return Ok(response);
            }
            if is_fenced(&e3, request_identity) {
                return Ok(fenced_response());
            }
            let (previous_pid, previous_epoch) =
                crate::txn::handlers::end_txn::client_producer_identity(&e3);
            let (new_pid, new_epoch) =
                next_init_producer_identity(&e3, &coord.producer_ids).await?;
            *e3 = TxnEntry::new_empty(tid.to_string(), new_pid, new_epoch, txn_timeout, now_ms);
            // Kafka's `prepareIncrementProducerEpoch` and
            // `prepareProducerIdRotation` record the epoch the entry held, so
            // a retry of this call is recognised. A caller that named no
            // identity records no last epoch.
            if request_identity.0 >= 0 {
                e3.last_producer_epoch = previous_epoch;
            }
            if new_pid != previous_pid {
                e3.prev_producer_id = previous_pid;
            }
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

/// What `request_identity` may do to `entry` (KIP-360).
///
/// The caller holds `entry`'s lock, and the mutation the verdict admits runs
/// under that same guard.
fn identity_decision(
    entry: &TxnEntry,
    request_identity: (i64, i16),
) -> InitProducerIdIdentityDecision {
    let (entry_pid, entry_epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    krabka_verified::transaction::init_producer_id_identity_decision(
        entry_pid.get(),
        entry_epoch,
        entry.last_producer_epoch,
        entry.prev_producer_id.get(),
        request_identity.0,
        request_identity.1,
    )
}

/// Whether `request_identity` is fenced against `entry`'s live identity.
fn is_fenced(entry: &TxnEntry, request_identity: (i64, i16)) -> bool {
    identity_decision(entry, request_identity) == InitProducerIdIdentityDecision::Fenced
}

/// Kafka `prepareIncrementProducerEpoch`: a retry of a bump that already
/// happened answers the entry's identity and writes nothing.
fn retried_bump_response(
    entry: &TxnEntry,
    request_identity: (i64, i16),
) -> Option<InitProducerIdResponse> {
    if identity_decision(entry, request_identity) != InitProducerIdIdentityDecision::Retry {
        return None;
    }
    let (pid, epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    Some(InitProducerIdResponse {
        error_code: codes::NONE,
        producer_id: pid.get(),
        producer_epoch: epoch,
        ..Default::default()
    })
}

/// Kafka `prepareInitProducerIdTransit` for an entry whose `Prepare*` record
/// is durable and not yet complete.
///
/// Kafka checks only the producer ID here, not the epoch
/// (`isValidProducerId`): a caller that names no producer ID, the entry's
/// producer ID, or the prior producer ID at an exhausted epoch gets
/// `CONCURRENT_TRANSACTIONS` and retries after the completion. Any other
/// producer ID is `PRODUCER_FENCED`. Returns `None` for any other state.
///
/// Before this check, `keepPreparedTxn=false` overwrote a `PrepareCommit` with
/// a new empty entry, which erased a commit decision whose markers some
/// partitions may already hold.
fn pending_completion_response(
    entry: &TxnEntry,
    (request_pid, request_epoch): (i64, i16),
) -> Option<InitProducerIdResponse> {
    completion_for(entry.state)?;
    let names_this_transaction = request_pid < 0
        || request_pid == entry.producer_id.get()
        || (entry.has_staged_producer_identity() && request_pid == entry.next_producer_id.get())
        || (request_pid == entry.prev_producer_id.get() && request_epoch >= i16::MAX - 1);
    Some(if names_this_transaction {
        concurrent_transactions_response()
    } else {
        fenced_response()
    })
}

/// Kafka's `initTransactionError(Errors.CONCURRENT_TRANSACTIONS)`.
fn concurrent_transactions_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::CONCURRENT_TRANSACTIONS,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
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
        let coordinator = Arc::new(TxnCoordinator::new(
            NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        ));
        coordinator
            .refresh_leader_partitions(&image)
            .await
            .finished()
            .await;

        let mut entry = TxnEntry::new_empty(tid.to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        entry.last_update_ms = 0;
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
        (coordinator, part)
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

    /// A coordinator that leads `__transaction_state-0`, with one ongoing
    /// transaction at `(1000, 3)` over a local `orders-0` data partition. The
    /// data partition comes back so a test can read the abort marker.
    async fn coordinator_with_ongoing_transaction(
        dir: &std::path::Path,
        tid: &str,
    ) -> (Arc<TxnCoordinator>, Arc<Partition>) {
        let (coordinator, _state) = coordinator_with_completed_transaction(dir, tid).await;
        let data_dir = crate::log_dir::partition_dir(dir, "orders", 0);
        std::fs::create_dir_all(&data_dir).expect("create the data partition directory");
        let data = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.to_path_buf(),
            Log::open(&data_dir, LogConfig::default()).expect("open the data log"),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        coordinator
            .partitions
            .insert("orders".into(), PartitionIndex(0), Arc::clone(&data));
        let handle = coordinator.get(tid).expect("the seeded entry");
        let ongoing = {
            let mut entry = handle.lock().await;
            entry.state = TxnState::Ongoing;
            entry.partitions.insert(TopicPartition {
                topic: "orders".to_string(),
                partition: PartitionIndex(0),
            });
            entry.clone()
        };
        coordinator
            .put(ongoing, TxnVersion::Verified)
            .await
            .expect("seed the ongoing transaction");
        (coordinator, data)
    }

    /// The producer epoch of the one control marker in `partition`.
    fn marker_producer_epoch(partition: &Partition) -> Option<i16> {
        let read = partition
            .read_log(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read the data partition");
        read.batches.first().map(|batch| batch.producer_epoch)
    }

    /// Kafka `prepareInitProducerIdTransit` on an `ONGOING` transaction:
    /// `prepareFenceProducerEpoch` raises the epoch, the coordinator aborts
    /// the transaction at that epoch, and the client gets
    /// `CONCURRENT_TRANSACTIONS`. The retry then bumps the epoch a second
    /// time.
    #[tokio::test]
    async fn an_ongoing_transaction_is_fenced_aborted_and_answered_concurrent() {
        struct Case {
            name: &'static str,
            txnv: TxnVersion,
            /// The epoch the abort completes at, which is also the epoch its
            /// markers carry. Transaction version 2 bumps the epoch again for
            /// the markers, as `prepareAbortOrCommit` does.
            aborted_epoch: i16,
            /// The epoch the retry of a producer that names no identity
            /// hands to the client.
            retried_epoch: i16,
        }
        let cases = [
            Case {
                name: "transaction version 1",
                txnv: TxnVersion::Flexible,
                aborted_epoch: 4,
                retried_epoch: 5,
            },
            Case {
                name: "transaction version 2",
                txnv: TxnVersion::Verified,
                aborted_epoch: 5,
                retried_epoch: 6,
            },
        ];
        for case in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let (coordinator, data) =
                coordinator_with_ongoing_transaction(dir.path(), "tid-fenced").await;

            let fenced = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (1000, 3),
            )
            .await
            .expect("init responds");
            check!(
                fenced
                    == InitProducerIdResponse {
                        error_code: codes::CONCURRENT_TRANSACTIONS,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    },
                "{}",
                case.name
            );
            let entry = coordinator
                .get("tid-fenced")
                .expect("entry")
                .lock()
                .await
                .clone();
            check!(
                (entry.state, entry.producer_id, entry.producer_epoch)
                    == (
                        TxnState::CompleteAbort,
                        ProducerId(1000),
                        case.aborted_epoch
                    ),
                "{}",
                case.name
            );
            // The abort marker fenced the producer at its data partition: the
            // marker carries an epoch above the one the client still holds.
            check!(
                marker_producer_epoch(&data) == Some(case.aborted_epoch),
                "{}: marker epoch",
                case.name
            );

            // The producer that held the fenced epoch is fenced when it
            // names it again: the fence bump is what the abort markers
            // carried, and Kafka's `prepareIncrementProducerEpoch` matches an
            // expected epoch against the current and the last epoch only.
            let stale = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (1000, 3),
            )
            .await
            .expect("init responds");
            check!(
                stale
                    == InitProducerIdResponse {
                        error_code: codes::PRODUCER_FENCED,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    },
                "{}: the stale identity",
                case.name
            );

            // `initTransactions()` names no identity, so the retry bumps the
            // epoch a second time, as the comment in
            // `prepareInitProducerIdTransit` says.
            let retried = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (-1, -1),
            )
            .await
            .expect("init responds");
            check!(
                retried
                    == InitProducerIdResponse {
                        producer_id: 1000,
                        producer_epoch: case.retried_epoch,
                        ..Default::default()
                    },
                "{}: the retry",
                case.name
            );
        }
    }

    /// KIP-360: a retry of an epoch bump whose response was lost gets the
    /// epoch the coordinator already wrote, and bumps nothing. Kafka's
    /// `prepareIncrementProducerEpoch` recognises it by the last epoch, which
    /// every bump records.
    #[tokio::test]
    async fn a_retried_epoch_bump_answers_the_epoch_it_already_wrote() {
        const TID: &str = "tid-retried-bump";

        struct Step {
            name: &'static str,
            request: (i64, i16),
            answer: (i16, i64, i16),
            /// Whether the call appended a `__transaction_state` record.
            appends: bool,
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        let steps = [
            Step {
                name: "the live identity bumps the epoch",
                request: (1000, 3),
                answer: (codes::NONE, 1000, 4),
                appends: true,
            },
            Step {
                name: "the retry answers the bumped epoch and writes nothing",
                request: (1000, 3),
                answer: (codes::NONE, 1000, 4),
                appends: false,
            },
            Step {
                name: "the bumped epoch bumps again",
                request: (1000, 4),
                answer: (codes::NONE, 1000, 5),
                appends: true,
            },
            Step {
                name: "an older epoch is fenced",
                request: (1000, 3),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                appends: false,
            },
            Step {
                name: "another producer id is fenced",
                request: (1001, 5),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                appends: false,
            },
            Step {
                name: "a caller that names no identity bumps",
                request: (-1, -1),
                answer: (codes::NONE, 1000, 6),
                appends: true,
            },
            Step {
                name: "and records no last epoch, so the old epoch is fenced",
                request: (1000, 5),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                appends: false,
            },
        ];
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for step in steps {
            let before = part.log_end_offset().0;
            let response = handle_transactional(
                &coordinator,
                TID,
                TxnVersion::Verified,
                60_000,
                false,
                false,
                step.request,
            )
            .await
            .expect("init responds");
            expected.push((step.name, step.answer, step.appends));
            actual.push((
                step.name,
                (
                    response.error_code,
                    response.producer_id,
                    response.producer_epoch,
                ),
                part.log_end_offset().0 > before,
            ));
        }
        assert!(actual == expected);
    }

    /// Kafka rotates the producer id once the epoch is exhausted, records the
    /// rotated id as the previous one, and admits the old id at that exhausted
    /// epoch as a retry (`isValidProducerId`).
    #[tokio::test]
    async fn a_rotation_at_the_exhausted_epoch_admits_the_old_identity_as_a_retry() {
        const TID: &str = "tid-rotated";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        {
            let handle = coordinator.get(TID).expect("the seeded entry");
            let mut entry = handle.lock().await;
            entry.producer_epoch = i16::MAX - 1;
        }

        let rotated = handle_transactional(
            &coordinator,
            TID,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (1000, i16::MAX - 1),
        )
        .await
        .expect("init responds");
        check!(rotated.error_code == codes::NONE);
        check!(rotated.producer_id != 1000);
        check!(rotated.producer_epoch == 0);
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.prev_producer_id == ProducerId(1000));
        check!(entry.last_producer_epoch == i16::MAX - 1);

        let retried = handle_transactional(
            &coordinator,
            TID,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            (1000, i16::MAX - 1),
        )
        .await
        .expect("init responds");
        check!(
            (
                retried.error_code,
                retried.producer_id,
                retried.producer_epoch
            ) == (codes::NONE, rotated.producer_id, 0)
        );
    }

    /// Two `InitProducerId` v3 calls that name the same live identity race for
    /// one transactional id. Exactly one bump lands: the identity check and
    /// the epoch bump it admits run under one lock, so the loser wakes to the
    /// epoch the winner already wrote.
    ///
    /// Kafka cannot tell that second call from the retry of a lost response,
    /// so `prepareIncrementProducerEpoch` answers both with the same identity
    /// and writes once (`expectedProducerEpoch == lastProducerEpoch`).
    ///
    /// With the check hoisted above the mutation, both calls read epoch 3,
    /// both bump, and the first caller's response is already stale when it
    /// reaches the client -- two live producers for one transactional id.
    ///
    /// The runtime is single-threaded and both tasks are stepped to their park
    /// on the entry lock with an explicit yield, so the interleaving is the
    /// same every run.
    #[tokio::test]
    async fn overlapping_inits_for_one_identity_answer_one_bumped_identity() {
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

        let mut answered = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answered.push((
                response.error_code,
                response.producer_id,
                response.producer_epoch,
            ));
        }
        check!(answered == vec![(codes::NONE, 1000, 4), (codes::NONE, 1000, 4)]);

        // One bump landed, not two.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!((entry.producer_id, entry.producer_epoch) == (ProducerId(1000), 4));
    }

    /// A failed abort fan-out records the epoch fence on the entry the
    /// coordinator publishes, not on the handle this call started from: `put`
    /// republishes the tid under a fresh `Arc`, so a write to the superseded
    /// handle is invisible to the retry that comes to read it.
    ///
    /// The `PrepareAbort` record is durable, so the call answers
    /// `CONCURRENT_TRANSACTIONS` and queues the abort for completion, as
    /// Kafka's fence abort does. While the abort is pending, every caller
    /// that names the transaction's producer ID gets the same answer. Once
    /// the abort completes, KIP-360 lets exactly the producer that still holds
    /// the pre-fence epoch retry, and a zombie that names any other epoch is
    /// fenced.
    #[tokio::test]
    async fn a_failed_abort_fan_out_records_the_fence_and_completes_before_a_retry() {
        const TID: &str = "tid-failed-fence";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // An ongoing transaction over a partition this broker does not host
        // yet: the abort marker cannot be delivered, so the fan-out fails
        // after the epoch fence is already persisted.
        let mut ongoing = TxnEntry::new_empty(TID.to_string(), ProducerId(1000), 3, 60_000, 0);
        ongoing.state = TxnState::Ongoing;
        ongoing.partitions.insert(TopicPartition {
            topic: "ghost".to_string(),
            partition: PartitionIndex(0),
        });
        seed(&coordinator, ongoing).await;

        let init = |identity| {
            let coordinator = Arc::clone(&coordinator);
            async move {
                handle_transactional(
                    &coordinator,
                    TID,
                    TxnVersion::Verified,
                    60_000,
                    false,
                    false,
                    identity,
                )
                .await
                .expect("InitProducerId responds")
            }
        };
        let concurrent = InitProducerIdResponse {
            error_code: codes::CONCURRENT_TRANSACTIONS,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };

        check!(init((1000, 3)).await == concurrent);
        let published = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(published.has_failed_epoch_fence);
        check!(published.last_producer_epoch == 3);
        check!(published.state == TxnState::PrepareAbort);

        // While the abort is pending, the producer ID decides, not the epoch.
        for identity in [(1000, 2), (1000, 3), (-1, -1)] {
            check!(init(identity).await == concurrent, "{identity:?}");
        }
        check!(init((2000, 0)).await == fenced_response());

        // The partition appears, and the completion task finishes the abort.
        let ghost_dir = crate::log_dir::partition_dir(dir.path(), "ghost", 0);
        std::fs::create_dir_all(&ghost_dir).expect("create ghost partition dir");
        let ghost = crate::broker::spawn_partition(
            "ghost".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&ghost_dir, LogConfig::default()).expect("open ghost log"),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        coordinator
            .partitions
            .insert("ghost".into(), PartitionIndex(0), ghost);
        check!(
            coordinator.complete_prepared_transaction(TID).await
                == crate::txn::coordinator::completion::CompletionAttempt::Completed
        );

        check!(init((1000, 2)).await.error_code == codes::PRODUCER_FENCED);
        check!(init((1000, 3)).await.error_code == codes::NONE);
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
