//! KIP-405 hand-off to the remote tier for an offset the local log no
//! longer holds, and the KFC-1 activation check that keeps a batch the
//! tier returns from going out before it is due.

use krabka_log::{DeliveryPolicy, LeaderEpoch, Offset};
use krabka_protocol::{
    Encode, owned::fetch_response::AbortedTransaction, primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_units::convert::TimeExt as _;

use super::plan::PendingRead;
use crate::{broker::Broker, codes, partition::Partition};

/// Whether a batch the remote tier returned may go out to a consumer now.
///
/// The local log no longer holds this batch, so the partition's delivery
/// watermark says nothing about it: that watermark is derived from the records
/// the log still has, and it is clamped to at or above the log start. The
/// evidence that survived the copy is the batch's own `max_timestamp`, which is
/// the activation time KFC-1 defines, and this applies the log's own rule to
/// it. A batch is active once its activation time plus the declared clock bound
/// is at or before the broker's clock reading, so delivery is never early.
///
/// A topic that delivers immediately answers `true` without reading the
/// timestamp.
fn remote_batch_is_deliverable(
    policy: DeliveryPolicy,
    uncertainty_ms: i64,
    max_timestamp: i64,
    now_ms: i64,
) -> bool {
    policy != DeliveryPolicy::Scheduled || max_timestamp <= now_ms.saturating_sub(uncertainty_ms)
}

/// KIP-405: try to serve `p`'s requested offset from the remote tier when the
/// local log returned `OFFSET_OUT_OF_RANGE` and the topic has
/// `remote.storage.enable=true`.
///
/// On success the function replaces the partition's error and records, and
/// returns the encoded batch size. On a miss, on an error, or for a
/// non-tiered topic, it leaves `p.out` untouched and returns `None`.
///
/// A consumer read of a scheduled topic is capped here as well. The remote path
/// serves whole batches with no offset limit, and it is the one read path the
/// local delivery watermark cannot bound, so it checks the batch's own
/// activation time instead. See [`remote_batch_is_deliverable`].
pub(super) async fn try_remote_read(
    broker: &Broker,
    p: &mut PendingRead,
    part: &Partition,
) -> Option<usize> {
    let reader = broker.remote_reader.clone()?;
    let (remote_storage_enable, delivery_policy, delivery_uncertainty_ms) = {
        let log = part.log.lock().expect("log mutex poisoned");
        let config = log.config_snapshot();
        (
            config.remote_storage_enable,
            config.delivery_policy,
            config.delivery_clock_uncertainty.millis_i64_trunc(),
        )
    };
    if !remote_storage_enable {
        return None;
    }
    if p.topic_id == WireUuid::ZERO {
        // Without a topic_id we can't build `TopicIdPartition` keyed the
        // same way the RLMM stores entries (Kafka's equality is by id +
        // partition).
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);
    let tp = krabka_remote_storage::TopicIdPartition::new(
        topic_id,
        p.topic_name.clone(),
        p.partition_index,
    );
    // Atomic stores the raw epoch; wrap into `LeaderEpoch` for the
    // remote-reader / RLMM seam that follows.
    let current_leader_epoch = LeaderEpoch(
        part.current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire),
    );
    // Resolve the leader epoch that *owned* the requested fetch offset from
    // the local leader-epoch checkpoint (Kafka's `epochForOffset`).  The
    // checkpoint is only appended-to / truncated-from-end (never pruned from
    // the start on local eviction), so tiered offsets that are no longer
    // stored locally still resolve to their copy-time epoch.  Fall back to
    // the current leader epoch when the checkpoint has no entries (empty /
    // fresh log) so behavior is at least as good as before.
    let leader_epoch = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .epoch_for_offset(Offset(p.fetch_offset))
            .unwrap_or(current_leader_epoch)
    };
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);

    match reader
        .fetch_batch(&tp, leader_epoch, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(batch)) => {
            // A follower is never gated: it replicates a scheduled record, and
            // counts it toward the ISR, before any consumer may see it.
            if !p.is_follower_fetch
                && !remote_batch_is_deliverable(
                    delivery_policy,
                    delivery_uncertainty_ms,
                    batch.max_timestamp,
                    part.delivery.now_ms(),
                )
            {
                // Answer as the local path answers a batch that is not due: an
                // empty partition and no error. `OFFSET_OUT_OF_RANGE` would
                // send the consumer to its reset policy and lose the record it
                // is waiting for, and the batch is due later, not never.
                tracing::debug!(
                    topic = %p.topic_name,
                    partition = p.partition_index,
                    offset = p.fetch_offset,
                    max_timestamp = batch.max_timestamp,
                    "remote-reader: batch is not due yet; holding it back"
                );
                p.out.error_code = codes::NONE;
                if p.read_committed {
                    p.out.aborted_transactions = Some(Vec::new());
                }
                return Some(0);
            }
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            p.out.error_code = codes::NONE;
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.

            // KIP-405 read-committed: surface the aborted-transaction list
            // from the segment's `.txnindex` so the consumer drops aborted
            // records client-side, mirroring the local `aborted_in_range`
            // call in `do_read` — bounded here to the single batch this read
            // returns (inclusive last offset), since the local path bounds by
            // the returned window over the LSO. `Some(empty)` is the correct
            // read-committed signal (read-uncommitted leaves it `None`).
            if p.read_committed && !p.is_follower_fetch {
                let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
                let aborts = match reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                {
                    Ok(aborts) => aborts,
                    Err(e) => {
                        // Degrade to "no aborts" but make it observable: an
                        // empty list in read-committed means the consumer may
                        // surface aborted records as committed.
                        tracing::warn!(
                            topic = %p.topic_name,
                            partition = p.partition_index,
                            offset = p.fetch_offset,
                            error = %e,
                            "remote-reader: aborted_transactions failed; returning empty abort list"
                        );
                        Vec::new()
                    }
                };
                p.out.aborted_transactions = Some(
                    aborts
                        .into_iter()
                        .map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            p.out.records = Some(batch.into());
            Some(bytes_est)
        }
        Ok(None) => None,
        Err(krabka_remote_storage::RemoteStorageError::NotReady { partition }) => {
            // The metadata partition that would answer this read is assigned
            // to this broker but its consumer has not caught up yet. Leave
            // OFFSET_OUT_OF_RANGE (retryable) — NOT a definitive miss — so the
            // client retries. Expected churn during catch-up, so log at debug.
            tracing::debug!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                metadata_partition = partition,
                "remote-reader: metadata partition not yet caught up; \
                 leaving OFFSET_OUT_OF_RANGE for client retry"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                error = %e,
                "remote-reader: fetch_batch failed; leaving OFFSET_OUT_OF_RANGE"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::DeliveryPolicy;

    #[test]
    fn a_remote_batch_is_held_back_until_its_activation_time_plus_the_bound() {
        // Immediate delivery reads no timestamp at all.
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Immediate,
            250,
            10_000,
            0
        ));
        // Scheduled: due at 10_000, and the 250 ms clock bound is added to it.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_249
        ));
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_250
        ));
        // The bound is added to the activation time, never subtracted from it,
        // so a clock at the far end of its own uncertainty still never delivers
        // early.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_000
        ));
        // A saturating subtraction keeps the far end of the clock range safe.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            i64::MIN
        ));
    }
}
