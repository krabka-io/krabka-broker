//! The `__consumer_offsets` log walk and the group seeding that follows it.
//!
//! One pass over a partition's log turns every persisted record into
//! coordinator state. The pass collects the classic groups and the committed
//! offsets in a [`Replayed`] accumulator, feeds the next-gen, share, and
//! streams records straight into the coordinator's seed map, and then
//! classifies each group and spawns its actor.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use krabka_ids::PartitionIndex;
use krabka_protocol::records::RecordBatch;
use krabka_units::{ByteSize, mebibytes};

use super::{
    OFFSETS_TOPIC,
    apply::{
        apply_group_metadata, apply_next_gen_record, apply_share_record, apply_streams_record,
    },
};
use crate::{
    coordinator::{
        GroupCoordinator,
        persistence::{self, GroupMetadataValue, Key, OffsetCommitValue},
        unified::{
            classic_state::{ClassicGroup as ClassicState, OffsetEntry},
            group::{CoordinatorGroup, GroupKind},
        },
    },
    error::BrokerError,
    partition_registry::PartitionRegistry,
};

/// How much of `__consumer_offsets` one replay read returns before the walk
/// advances to the next offset.
const REPLAY_READ_MAX: ByteSize = mebibytes(1);

/// Bootstrap-time accumulator.
///
/// Committed offsets are protocol-agnostic. Replay collects them per group and
/// attaches them once it knows the group's kind. A classic `GroupMetadata`
/// builds a `ClassicState` in place. Next-gen records feed the coordinator's
/// own seed accumulator through the `replay_*` methods, and
/// `finalize_bootstrap` drains it.
#[derive(Default)]
pub(super) struct Replayed {
    pub(super) classic: HashMap<String, ClassicState>,
    pub(super) committed: HashMap<String, HashMap<(String, i32), OffsetEntry>>,
    /// When each replayed classic group last became empty, read from the k2
    /// `GroupMetadata` value's `current_state_timestamp_ms`.
    ///
    /// The offset-retention sweep measures from this moment, so a broker that
    /// restarts does not hand every dead group another full
    /// `offsets.retention.minutes`.
    pub(super) empty_since: HashMap<String, i64>,
    /// KIP-447: the offset commits of transactions the log ends without a
    /// marker for, keyed by group and then by the producer that wrote them.
    ///
    /// Those offsets are not committed, so they never reach `committed`. The
    /// transaction is still open, though: its marker can still arrive after
    /// the reload, so the group has to keep answering a `require_stable`
    /// `OffsetFetch` with `UNSTABLE_OFFSET_COMMIT` for these keys, exactly as
    /// it did before.
    pub(super) pending_txn: HashMap<String, HashMap<i64, PendingTxnKeys>>,
}

/// One producer's unresolved transactional offset commits for one group, as
/// replay recovered them from the log.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct PendingTxnKeys {
    /// Highest offsets-log position the producer's deferred records were found
    /// at. The marker that resolves them can only arrive after the reload, and
    /// so lands above it, which is what keeps the group actor from mistaking
    /// this recovered mark for one the marker already resolved.
    pub(super) written_at: i64,
    pub(super) keys: HashSet<(String, i32)>,
}

impl Replayed {
    pub(super) fn merge(&mut self, other: Self) {
        self.classic.extend(other.classic);
        self.committed.extend(other.committed);
        self.empty_since.extend(other.empty_since);
        self.pending_txn.extend(other.pending_txn);
    }
}

/// Replay one newly-led offsets partition into the coordinator after a
/// metadata leadership change.
pub(crate) async fn replay_partition(
    partitions: &PartitionRegistry,
    coordinator: &Arc<GroupCoordinator>,
    partition_id: PartitionIndex,
) -> Result<(), BrokerError> {
    let partition = partitions.get(OFFSETS_TOPIC, partition_id).ok_or_else(|| {
        BrokerError::Startup(format!(
            "newly-led {OFFSETS_TOPIC}-{} is not materialized locally",
            partition_id.get()
        ))
    })?;
    // The log guard is not `Send`, so it lives and dies inside this block:
    // `finalize` awaits, and this function runs inside a spawned task.
    let replayed = {
        let log = partition.log.lock().map_err(|_| {
            BrokerError::Startup(format!(
                "{OFFSETS_TOPIC}-{} log lock poisoned during leadership replay",
                partition_id.get()
            ))
        })?;
        replay_records(&log, coordinator)?
    };
    finalize(coordinator, replayed).await;
    Ok(())
}

/// Walk every `RecordBatch` in the log from offset 0 to `log_end_offset()`
/// and apply each record's key/value into the accumulator (classic + offsets)
/// or, for next-gen records, the coordinator's seed accumulator.
pub(super) fn replay_records(
    log: &krabka_log::Log,
    coordinator: &Arc<GroupCoordinator>,
) -> Result<Replayed, BrokerError> {
    struct DeferredRecord {
        key: Key,
        value: Option<bytes::Bytes>,
        timestamp_ms: i64,
        /// Offset of the batch the record arrived in, kept for the records
        /// that turn out to belong to a transaction the log never resolved.
        written_at: i64,
    }

    let mut acc = Replayed::default();
    let mut pending_transactions: HashMap<i64, Vec<DeferredRecord>> = HashMap::new();
    let mut next = log.log_start_offset();
    let end = log.log_end_offset();
    while next < end {
        let out = log.read(next, REPLAY_READ_MAX)?;
        if out.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &out.batches {
            if batch.attributes.is_control_batch() {
                let committed = batch
                    .records
                    .first()
                    .and_then(|record| record.key.as_deref())
                    .is_some_and(|key| key.len() >= 4 && key[2..4] == 1_i16.to_be_bytes());
                if let Some(records) = pending_transactions.remove(&batch.producer_id)
                    && committed
                {
                    for record in records {
                        match record.value {
                            Some(value) => apply_record_at_timestamp(
                                coordinator,
                                &mut acc,
                                record.key,
                                &value,
                                record.timestamp_ms,
                            )?,
                            None => apply_tombstone(coordinator, &mut acc, record.key),
                        }
                    }
                }
                advanced_to =
                    krabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                continue;
            }
            for record in &batch.records {
                let Some(key_bytes) = &record.key else {
                    continue;
                };
                let key = persistence::parse_key(key_bytes)?;
                if batch.attributes.is_transactional() {
                    pending_transactions
                        .entry(batch.producer_id)
                        .or_default()
                        .push(DeferredRecord {
                            key,
                            value: record.value.clone(),
                            timestamp_ms: batch.max_timestamp,
                            written_at: batch.base_offset,
                        });
                    continue;
                }
                match &record.value {
                    Some(value_bytes) => {
                        apply_record(coordinator, &mut acc, key, value_bytes, batch)?;
                    }
                    None => {
                        apply_tombstone(coordinator, &mut acc, key);
                    }
                }
            }
            // The loop threads the log's `Offset` cursor (`next`/`end` feed
            // `Log::read`); wrap the batch-derived next offset back into `Offset`.
            advanced_to =
                krabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    // Whatever is still deferred belongs to a transaction the log ends without
    // a marker for. Its offset commits are not visible and must not be
    // applied, but the transaction can still be resolved after the reload, so
    // its keys carry forward as KIP-447 pending marks.
    for (producer_id, records) in pending_transactions {
        for record in records {
            if let Key::OffsetCommit {
                group_id,
                topic,
                partition,
            } = record.key
            {
                let pending = acc
                    .pending_txn
                    .entry(group_id)
                    .or_default()
                    .entry(producer_id)
                    .or_default();
                pending.written_at = pending.written_at.max(record.written_at);
                pending.keys.insert((topic, partition));
            }
        }
    }
    Ok(acc)
}

pub(super) fn apply_record(
    coordinator: &Arc<GroupCoordinator>,
    acc: &mut Replayed,
    key: Key,
    value_bytes: &bytes::Bytes,
    batch: &RecordBatch,
) -> Result<(), BrokerError> {
    apply_record_at_timestamp(coordinator, acc, key, value_bytes, batch.max_timestamp)
}

fn apply_record_at_timestamp(
    coordinator: &Arc<GroupCoordinator>,
    acc: &mut Replayed,
    key: Key,
    value_bytes: &bytes::Bytes,
    timestamp_ms: i64,
) -> Result<(), BrokerError> {
    match key {
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => {
            let v = OffsetCommitValue::decode_value(value_bytes)?;
            acc.committed.entry(group_id).or_default().insert(
                (topic, partition),
                OffsetEntry {
                    offset: v.offset,
                    leader_epoch: v.leader_epoch,
                    metadata: v.metadata,
                    commit_timestamp_ms: v.commit_timestamp_ms,
                    expire_timestamp_ms: v.expire_timestamp_ms,
                },
            );
        }
        Key::GroupMetadata { group_id } => {
            let v = GroupMetadataValue::decode_value(value_bytes)?;
            // A snapshot with no members is the moment the group emptied. A
            // pre-version-2 value has no such timestamp and decodes as -1.
            if v.members.is_empty() && v.current_state_timestamp_ms > 0 {
                acc.empty_since
                    .insert(group_id.clone(), v.current_state_timestamp_ms);
            } else {
                acc.empty_since.remove(&group_id);
            }
            let state = acc
                .classic
                .entry(group_id.clone())
                .or_insert_with(|| ClassicState::new(group_id));
            apply_group_metadata(state, v, timestamp_ms);
        }
        Key::NextGen(ng_key) => {
            apply_next_gen_record(coordinator, ng_key, value_bytes)?;
        }
        Key::Share(share_key) => {
            apply_share_record(coordinator, share_key, value_bytes)?;
        }
        Key::Streams(streams_key) => apply_streams_record(coordinator, streams_key, value_bytes)?,
    }
    Ok(())
}

/// Apply a tombstone, which is a record with `value = None`.
///
/// Every family honors its own tombstones, which is what Kafka's
/// `GroupMetadataManager.loadGroupsAndOffsets` does: a null-valued offset key
/// drops that committed offset, and a null-valued group key drops the group.
/// A group tombstone does NOT drop the group's offsets, because those are
/// separate keys with their own records; the offset-retention sweep and
/// `OffsetDelete` write both when both should go.
pub(super) fn apply_tombstone(coordinator: &Arc<GroupCoordinator>, acc: &mut Replayed, key: Key) {
    match key {
        Key::NextGen(ng_key) => coordinator.replay_next_gen_tombstone(&ng_key),
        Key::Share(share_key) => coordinator.replay_share_tombstone(&share_key),
        Key::Streams(streams_key) => coordinator.replay_streams_tombstone(&streams_key),
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => {
            if let Some(offsets) = acc.committed.get_mut(&group_id) {
                offsets.remove(&(topic, partition));
                // The outer entry has to go with the last offset under it.
                // `finalize` reads `committed`'s keys as "this group survived
                // replay", so a group id left behind with an empty map spawns
                // a classic actor for a group the log has already tombstoned,
                // and `ListGroups` reports a reaped group again after every
                // restart.
                if offsets.is_empty() {
                    acc.committed.remove(&group_id);
                }
            }
        }
        Key::GroupMetadata { group_id } => {
            acc.classic.remove(&group_id);
            acc.empty_since.remove(&group_id);
        }
    }
}

/// Decide each group's kind and seed its actor.
///
/// The next-gen groups are the groups that accumulated next-gen records.
/// `finalize_bootstrap` spawns them, and the function attaches their committed
/// offsets and open-transaction marks afterward. Every other group with
/// classic metadata, committed offsets, or an open transaction replays as a
/// classic actor.
///
/// Recovered state reaches a spawned actor through its mailbox, so the
/// function awaits each hand-off rather than dropping what does not fit: a
/// group can have more open producer transactions than the mailbox holds, and
/// a mark lost here is a partition that answers a `require_stable`
/// `OffsetFetch` with a stale committed offset until the next replay.
pub(super) async fn finalize(coordinator: &Arc<GroupCoordinator>, mut replayed: Replayed) {
    // Next-gen group ids are those present in the coordinator's seed map.
    let next_gen_ids: std::collections::HashSet<String> =
        coordinator.seeds.iter().map(|e| e.key().clone()).collect();

    // Spawn + seed next-gen (consumer) actors.
    coordinator.finalize_bootstrap();

    // Attach committed offsets to consumer groups; the rest are classic.
    let committed_groups: Vec<String> = replayed.committed.keys().cloned().collect();
    for gid in committed_groups {
        if next_gen_ids.contains(&gid)
            && let Some(offsets) = replayed.committed.remove(&gid)
            && let Some(handle) = coordinator.find(&gid)
        {
            let entries: Vec<((String, i32), OffsetEntry)> = offsets.into_iter().collect();
            let (tx, _rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(
                    crate::coordinator::unified::actor::GroupActorMessage::UpdateCommitted {
                        entries,
                        reply: tx,
                    },
                )
                .await
                .is_err()
            {
                tracing::warn!(group = %gid, "replay could not seed the group's committed offsets");
            }
        }
    }

    // Attach open-transaction marks to consumer groups, so a `require_stable`
    // fetch keeps reporting UNSTABLE_OFFSET_COMMIT across the reload.
    let pending_groups: Vec<String> = replayed.pending_txn.keys().cloned().collect();
    for gid in pending_groups {
        if next_gen_ids.contains(&gid)
            && let Some(by_producer) = replayed.pending_txn.remove(&gid)
            && let Some(handle) = coordinator.find(&gid)
        {
            for (producer_id, pending) in by_producer {
                let (tx, _rx) = tokio::sync::oneshot::channel();
                if handle
                    .tx
                    .send(
                        crate::coordinator::unified::actor::GroupActorMessage::AddPendingTxnOffsets {
                            producer_id,
                            written_at: pending.written_at,
                            keys: pending.keys.into_iter().collect(),
                            reply: tx,
                        },
                    )
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        group = %gid,
                        producer_id,
                        "replay could not seed the group's open transaction marks"
                    );
                }
            }
        }
    }

    // Classic groups: those with classic metadata, plus offset-only and
    // open-transaction-only groups that are not next-gen.
    let classic_ids: std::collections::HashSet<String> = replayed
        .classic
        .keys()
        .cloned()
        .chain(replayed.committed.keys().cloned())
        .chain(replayed.pending_txn.keys().cloned())
        .filter(|gid| !next_gen_ids.contains(gid))
        .collect();
    for gid in classic_ids {
        let state = replayed
            .classic
            .remove(&gid)
            .unwrap_or_else(|| ClassicState::new(gid.clone()));
        let committed_offsets = replayed.committed.remove(&gid).unwrap_or_default();
        let mut group =
            CoordinatorGroup::seeded(gid.clone(), GroupKind::Classic(state), committed_offsets);
        group.empty_since_ms = replayed.empty_since.remove(&gid);
        for (producer_id, pending) in replayed.pending_txn.remove(&gid).unwrap_or_default() {
            group.add_pending_txn_offsets(producer_id, pending.written_at, pending.keys);
        }
        coordinator.seed_classic(&gid, Box::new(group));
    }
}
