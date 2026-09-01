//! The `__consumer_offsets` half of a transaction marker: reading back the
//! offset commits the transaction wrote, and resolving them against the local
//! group actors.
//!
//! A transactional offset commit is invisible to a consumer until the
//! transaction commits, so the records sit in the offsets log with no offset
//! behind them in the actor — only KIP-447's pending mark, which makes a
//! `require_stable` `OffsetFetch` answer `UNSTABLE_OFFSET_COMMIT`. When a
//! marker lands, this module rescans the log from the transaction's first
//! record and decodes every `OffsetCommit` key the producer wrote. A commit
//! publishes those offsets and drops the marks; an abort drops the marks and
//! publishes nothing. Either way the group stops reporting the partitions as
//! unstable.

use std::{collections::HashMap, sync::Arc};

use crate::{
    coordinator::{
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            GroupCoordinator,
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::OffsetEntry,
        },
    },
    error::BrokerError,
    txn::marker::MarkerType,
};

pub(super) type CommittedOffsets = HashMap<String, Vec<((String, i32), OffsetEntry)>>;

pub(super) fn pending_offset_entries(
    partition: &crate::partition::Partition,
    producer_id: krabka_log::ProducerId,
) -> Result<CommittedOffsets, BrokerError> {
    let log = partition.log.lock().map_err(|_| {
        BrokerError::Replication("offsets log lock poisoned while applying txn marker".into())
    })?;
    let Some(mut next) = log.pending_transaction_start(producer_id) else {
        return Ok(HashMap::new());
    };
    let end = log.log_end_offset();
    let mut offsets: CommittedOffsets = HashMap::new();
    while next < end {
        let read = log.read(next, krabka_units::mebibytes(1))?;
        if read.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &read.batches {
            if batch.producer_id == producer_id.get()
                && batch.attributes.is_transactional()
                && !batch.attributes.is_control_batch()
            {
                for record in &batch.records {
                    let (Some(key), Some(value)) = (&record.key, &record.value) else {
                        continue;
                    };
                    if let Key::OffsetCommit {
                        group_id,
                        topic,
                        partition,
                    } = parse_key(key)?
                    {
                        let value = OffsetCommitValue::decode_value(value)?;
                        offsets.entry(group_id).or_default().push((
                            (topic, partition),
                            OffsetEntry {
                                offset: value.offset,
                                leader_epoch: value.leader_epoch,
                                metadata: value.metadata,
                                commit_timestamp_ms: value.commit_timestamp_ms,
                                expire_timestamp_ms: value.expire_timestamp_ms,
                            },
                        ));
                    }
                }
            }
            advanced_to =
                krabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(offsets)
}

/// Resolve a transaction's offset commits against the local group actors.
///
/// A commit publishes the offsets and clears the producer's KIP-447 pending
/// marks in one actor turn, so no fetch can observe a partition that is
/// neither unstable nor updated. An abort clears the marks and publishes
/// nothing, leaving the group's stable offsets where they were.
///
/// A commit uses the get-or-create factory, which detects and replaces a
/// closed actor: the offsets have to become visible even if the actor failed
/// since the transaction began, without waiting for a marker retry the durable
/// commit marker will never trigger. An abort has nothing to publish, so it
/// only touches a live actor that already exists: it never resurrects a group
/// that went away with its marks, and it never fails the marker over one. The
/// marker is already durable by the time this runs, and an actor that is gone
/// took its pending marks with it, so a failed hand-off leaves nothing
/// unresolved -- only a commit, whose offsets would otherwise be lost from
/// memory, still reports the failure.
///
/// One transaction can hold offset commits for several groups on the same
/// offsets partition, and every one of them has to be resolved. The marker is
/// durable before this runs and it ended the log's pending transaction, so a
/// retry would find nothing to rescan: a group skipped here keeps its pending
/// marks for ever. The loop therefore visits every group and reports a
/// failure only once it has, rather than returning at the first one.
///
/// `resolved_through` is the marker's own offset in the offsets log. It goes
/// to the actor with the resolution because a `TxnOffsetCommit` marks its keys
/// after its append is durable, so a mark for this very transaction can still
/// be in flight; the actor compares the two log positions and drops it.
pub(super) async fn resolve_pending_offsets(
    coordinator: &Arc<GroupCoordinator>,
    producer_id: krabka_log::ProducerId,
    marker_type: MarkerType,
    resolved_through: i64,
    offsets: CommittedOffsets,
) -> Result<(), BrokerError> {
    let commit = marker_type == MarkerType::Commit;
    let mut unresolved: Vec<String> = Vec::new();
    for (group_id, entries) in offsets {
        let handle = if commit {
            Some(coordinator.get_or_create_group(&group_id, GroupKindTag::Classic))
        } else {
            coordinator.find(&group_id).filter(|h| !h.tx.is_closed())
        };
        let Some(handle) = handle else {
            continue;
        };
        let (reply, response) = tokio::sync::oneshot::channel();
        let resolved = handle
            .tx
            .send(GroupActorMessage::ResolveTxnOffsets {
                producer_id: producer_id.get(),
                resolved_through,
                committed: if commit { entries } else { Vec::new() },
                reply,
            })
            .await
            .is_ok()
            && response.await.is_ok();
        if !resolved {
            tracing::warn!(
                group = %group_id,
                "WriteTxnMarkers: could not resolve the transaction's offset commits"
            );
            unresolved.push(group_id);
        }
    }
    if commit && !unresolved.is_empty() {
        return Err(BrokerError::Txn(format!(
            "could not resolve transactional offset commits for groups {}",
            unresolved.join(", ")
        )));
    }
    Ok(())
}
