//! The `__consumer_offsets` half of a committed transaction marker: reading
//! back the offset commits the transaction wrote, and publishing them to the
//! local group actors.
//!
//! A transactional offset commit is invisible to a consumer until the
//! transaction commits, so the records sit in the offsets log with no actor
//! state behind them. When the commit marker lands, this module rescans the
//! log from the transaction's first record, decodes every `OffsetCommit` key
//! the producer wrote, and hands the results to the owning group actor.

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

pub(super) async fn apply_committed_offsets(
    coordinator: &Arc<GroupCoordinator>,
    offsets: CommittedOffsets,
) -> Result<(), BrokerError> {
    for (group_id, entries) in offsets {
        // The factory detects and replaces a closed actor. This makes the
        // in-memory publication robust after an actor failure without
        // requiring a marker retry after the durable commit marker exists.
        let handle = coordinator.get_or_create_group(&group_id, GroupKindTag::Classic);
        let (reply, response) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::UpdateCommitted { entries, reply })
            .await
            .is_err()
            || response.await.is_err()
        {
            tracing::warn!(
                group = %group_id,
                "WriteTxnMarkers: could not publish committed transactional offsets"
            );
            return Err(BrokerError::Txn(format!(
                "could not publish committed transactional offsets for group {group_id}"
            )));
        }
    }
    Ok(())
}
