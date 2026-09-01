//! Tests for KIP-98 transactional-id expiry: the pure decision core over
//! every `TxnState`, and the sweep driven against a live coordinator whose
//! `__transaction_state-0` partition is a real log on disk.

use std::{path::Path, sync::Arc};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset, ProducerId};
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use krabka_units::mebibytes;
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

use super::*;
use crate::{
    partition::Partition, partition_registry::PartitionRegistry, txn::bootstrap,
    txn::two_pc::NO_TIMEOUT_MS, txn::version::TxnVersion,
};

const TID: &str = "tid-expiry";
/// Kafka's `transactional.id.expiration.ms` default.
const EXPIRY_MS: i64 = 604_800_000;

// ── The pure decision core ────────────────────────────────────────────

/// Every `TxnState`, with the elapsed time exactly at the expiry, so the
/// state is the only thing that can decide the answer.
#[test]
fn only_terminal_and_idle_states_expire() {
    for (state, want) in [
        (TxnState::Empty, true),
        (TxnState::Dead, true),
        (TxnState::CompleteCommit, true),
        (TxnState::CompleteAbort, true),
        (TxnState::Ongoing, false),
        (TxnState::PrepareCommit, false),
        (TxnState::PrepareAbort, false),
    ] {
        check!(
            should_expire_transactional_id(state, 0, EXPIRY_MS, EXPIRY_MS) == want,
            "{state:?}"
        );
    }
}

/// KIP-939: no clock, however far in the future, expires a prepared 2PC
/// transaction. `i64::MAX` is the strongest statement of "indefinitely" the
/// type allows.
#[test]
fn a_prepared_transaction_never_expires_at_any_clock() {
    for state in [TxnState::PrepareCommit, TxnState::PrepareAbort] {
        for now_ms in [EXPIRY_MS, EXPIRY_MS * 1_000, i64::MAX] {
            check!(
                !should_expire_transactional_id(state, 0, now_ms, EXPIRY_MS),
                "{state:?} at {now_ms}"
            );
        }
    }
}

/// The boundary Kafka uses is `now - last >= expiration`, so the instant the
/// expiry elapses is already expired and the millisecond before it is not.
#[test]
fn expiry_boundary_is_inclusive() {
    for (now_ms, want) in [
        (EXPIRY_MS - 1, false),
        (EXPIRY_MS, true),
        (EXPIRY_MS + 1, true),
    ] {
        check!(
            should_expire_transactional_id(TxnState::CompleteCommit, 0, now_ms, EXPIRY_MS) == want,
            "{now_ms}"
        );
    }
}

/// A backwards clock must never expire anything, and must not overflow on the
/// way to that answer.
#[test]
fn a_backwards_clock_expires_nothing() {
    check!(!should_expire_transactional_id(
        TxnState::CompleteCommit,
        EXPIRY_MS * 2,
        0,
        EXPIRY_MS
    ));
    check!(!should_expire_transactional_id(
        TxnState::CompleteCommit,
        i64::MAX,
        i64::MIN,
        EXPIRY_MS
    ));
}

// ── The revival guard ─────────────────────────────────────────────────

#[test]
fn still_matches_accepts_the_snapshot_it_came_from_and_rejects_a_revival() {
    let decided = complete_commit_entry(0);
    check!(still_matches(&decided, &decided));

    for mutate in [
        (|e: &mut TxnEntry| e.producer_id = ProducerId(7)) as fn(&mut TxnEntry),
        |e: &mut TxnEntry| e.producer_epoch += 1,
        |e: &mut TxnEntry| e.state = TxnState::Ongoing,
        |e: &mut TxnEntry| e.last_update_ms += 1,
    ] {
        let mut revived = decided.clone();
        mutate(&mut revived);
        check!(!still_matches(&revived, &decided));
    }
}

// ── The live sweep ────────────────────────────────────────────────────

/// A `TxnEntry` that committed at `last_update_ms` and has sat in
/// `CompleteCommit` ever since.
fn complete_commit_entry(last_update_ms: i64) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 3, 60_000, 0);
    entry.state = TxnState::CompleteCommit;
    entry.last_update_ms = last_update_ms;
    entry
}

/// A KIP-939 2PC transaction that an external transaction manager prepared at
/// `last_update_ms` and has not yet resolved. The [`NO_TIMEOUT_MS`] sentinel is
/// what marks it 2PC on disk.
fn prepared_two_pc_entry(last_update_ms: i64) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(2000), 1, NO_TIMEOUT_MS, 0);
    entry.state = TxnState::PrepareCommit;
    entry.last_update_ms = last_update_ms;
    entry
}

/// Opens `__transaction_state-0` as a real log under `dir`.
fn transaction_state_partition(dir: &Path) -> Arc<Partition> {
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

/// A metadata image where `__transaction_state` has one partition, led by
/// `leader`.
fn image_with_leader(leader: NodeId) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_string(),
        topic_id: Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_string(),
        partition: 0,
        leader,
        replicas: vec![leader],
        isr: vec![leader],
        ..Default::default()
    }));
    image
}

/// A coordinator that leads the single `__transaction_state` partition, with
/// `entry` already persisted into it. The tempdir must outlive the coordinator,
/// so it comes back with it.
async fn seeded_coordinator(entry: TxnEntry, leader: NodeId) -> (TxnCoordinator, TempDir) {
    let dir = tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.to_string(),
        PartitionIndex(0),
        transaction_state_partition(dir.path()),
    );
    let coordinator = TxnCoordinator::new(
        NodeId(1),
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        mebibytes(1),
    );
    coordinator
        .refresh_leader_partitions(&image_with_leader(leader))
        .await;
    coordinator
        .put(entry, TxnVersion::Verified)
        .await
        .expect("seed __transaction_state");
    (coordinator, dir)
}

/// The `(key, value)` pairs of every record in `__transaction_state-0`, with
/// each key decoded to its transactional id.
fn transaction_state_records(coordinator: &TxnCoordinator) -> Vec<(String, Option<Vec<u8>>)> {
    let part = coordinator
        .partitions
        .get(bootstrap::TOPIC, PartitionIndex(0))
        .expect("partition is local");
    let mut out = Vec::new();
    let mut offset = part.log_start_offset();
    loop {
        let read = part.read_log(offset, mebibytes(1)).expect("read log");
        if read.batches.is_empty() {
            return out;
        }
        for batch in &read.batches {
            for record in &batch.records {
                let key = crate::txn::log_record::decode_key(
                    record.key.as_ref().expect("record carries a key"),
                )
                .expect("record carries a TransactionLogKey");
                out.push((key, record.value.as_ref().map(|v| v.to_vec())));
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
}

#[tokio::test]
async fn an_expired_completed_transaction_is_tombstoned_and_dropped() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;
    let before = transaction_state_records(&coordinator);
    assert!(before.len() == 1, "{before:?}");
    assert!(coordinator.get(TID).is_some());

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;

    check!(expired == vec![TID.to_string()]);
    // What `DescribeTransactions` reads: the tid is no longer known, so the
    // handler answers TRANSACTIONAL_ID_NOT_FOUND.
    check!(coordinator.get(TID).is_none());
    check!(coordinator.snapshot().await.is_empty());
    check!(coordinator.tid_for_pid(ProducerId(1000)).is_none());
    // The seeded value record, then the tombstone: a null value under the same
    // byte-exact `TransactionLogKey`.
    let after = transaction_state_records(&coordinator);
    check!(after.len() == 2, "{after:?}");
    check!(after[1] == (TID.to_string(), None));
}

#[tokio::test]
async fn a_transaction_inside_the_expiry_window_survives() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS - 1, EXPIRY_MS)
        .await;

    check!(expired.is_empty());
    check!(coordinator.get(TID).is_some());
    check!(transaction_state_records(&coordinator).len() == 1);
}

/// KIP-939: a prepared 2PC transaction is the external transaction manager's
/// to resolve. The sweep leaves it alone however long it waits.
#[tokio::test]
async fn a_prepared_two_pc_transaction_survives_indefinitely() {
    let (coordinator, _dir) = seeded_coordinator(prepared_two_pc_entry(0), NodeId(1)).await;

    for now_ms in [EXPIRY_MS + 1, EXPIRY_MS * 1_000, i64::MAX] {
        let expired = coordinator.expire_transactional_ids(now_ms, EXPIRY_MS).await;
        check!(expired.is_empty(), "{now_ms}");
        check!(coordinator.get(TID).is_some(), "{now_ms}");
    }
    // No tombstone was ever appended: only the seeded value record is there.
    let records = transaction_state_records(&coordinator);
    check!(records.len() == 1, "{records:?}");
    check!(records[0].1.is_some());
}

/// A `__transaction_state` partition that moved to another broker belongs to
/// that broker's sweep, not this one's.
#[tokio::test]
async fn a_partition_this_broker_no_longer_leads_is_skipped() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(2)).await;

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;

    check!(expired.is_empty());
    check!(coordinator.get(TID).is_some());
    check!(transaction_state_records(&coordinator).len() == 1);
}
