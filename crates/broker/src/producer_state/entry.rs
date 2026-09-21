//! The per-producer sequence record and the per-partition map that holds it.
//!
//! `ProducerEntry` is one idempotent producer's last-accepted-batch state on a
//! partition, and `PartitionProducerState` is the map of those records that a
//! per-partition mutex guards. They sit in their own file because every other
//! part of the tracker reads or writes them.

use std::collections::HashMap;

use krabka_log::ProducerId;

use crate::partition::LogOffset;

/// Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`: the number of a
/// producer's most recent batches whose retry answers as a duplicate.
pub const NUM_BATCHES_TO_RETAIN: usize = 5;

/// One earlier accepted batch of a producer, kept so that a retry of it
/// answers as a duplicate. Kafka's `BatchMetadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedBatch {
    pub base_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: LogOffset,
    pub last_offset: LogOffset,
    /// The batch's max timestamp, as the log stored it.
    pub timestamp: i64,
}

/// The batches a producer accepted before its last one, oldest first, at the
/// producer's current epoch. The last batch lives in the entry's own fields.
pub type EarlierBatches = [Option<RetainedBatch>; NUM_BATCHES_TO_RETAIN - 1];

/// No earlier batch: a new producer, a new epoch, or recovered state.
pub const NO_EARLIER_BATCHES: EarlierBatches = [None; NUM_BATCHES_TO_RETAIN - 1];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Last absolute offset of the last accepted batch for this producer
    /// (`base_offset + last_offset_delta`).
    /// [`ProducerState::truncate`](super::ProducerState::truncate) reads it to
    /// drop entries whose batch was truncated off the log.
    pub last_offset: LogOffset,
    pub base_offset: LogOffset,
    /// Timestamp of the last accepted batch for this producer.
    pub last_timestamp: i64,
    /// Wall-clock millis of the last `commit` that touched this entry.
    /// [`ProducerState::expire_older_than`](super::ProducerState::expire_older_than)
    /// uses it to evict idle idempotent-producer state. This matches Kafka's
    /// `producer.id.expiration.ms`, which expires by inactivity.
    pub last_activity_ms: i64,
    /// Up to four batches accepted before the last one, at `epoch`, oldest
    /// first. With the last batch they are Kafka's five retained batches.
    pub earlier: EarlierBatches,
}

impl ProducerEntry {
    /// The last accepted batch, or `None` for a marker-only entry.
    #[must_use]
    pub fn last_batch(&self) -> Option<RetainedBatch> {
        let delta = i32::try_from(self.last_offset.checked_sub(self.base_offset)?).ok()?;
        (self.last_offset >= 0).then(|| RetainedBatch {
            base_sequence: krabka_verified::decrement_sequence(self.last_sequence, delta),
            last_sequence: self.last_sequence,
            base_offset: self.base_offset,
            last_offset: self.last_offset,
            timestamp: self.last_timestamp,
        })
    }

    /// The retained batch with exactly this sequence range, as Kafka's
    /// `ProducerStateEntry.findDuplicateBatch` looks it up. A batch at another
    /// epoch is never a duplicate.
    #[must_use]
    pub fn duplicate_of(
        &self,
        producer_epoch: i16,
        base_sequence: i32,
        last_sequence: i32,
    ) -> Option<RetainedBatch> {
        if producer_epoch != self.epoch {
            return None;
        }
        self.earlier
            .iter()
            .flatten()
            .copied()
            .chain(self.last_batch())
            .find(|batch| {
                batch.base_sequence == base_sequence && batch.last_sequence == last_sequence
            })
    }

    /// The earlier batches after `self` accepts one more batch at `epoch`:
    /// the current last batch joins them and the oldest leaves at capacity.
    /// A new epoch clears them, as Kafka's `maybeUpdateProducerEpoch` does.
    #[must_use]
    pub fn earlier_after_append(&self, epoch: i16) -> EarlierBatches {
        if epoch != self.epoch {
            return NO_EARLIER_BATCHES;
        }
        let Some(last) = self.last_batch() else {
            return NO_EARLIER_BATCHES;
        };
        let mut next = NO_EARLIER_BATCHES;
        let kept = self
            .earlier
            .iter()
            .flatten()
            .copied()
            .chain(std::iter::once(last));
        let count = self.earlier.iter().flatten().count() + 1;
        for (slot, batch) in next
            .iter_mut()
            .zip(kept.skip(count.saturating_sub(NUM_BATCHES_TO_RETAIN - 1)))
        {
            *slot = Some(batch);
        }
        next
    }
}

#[derive(Debug, Default)]
pub struct PartitionProducerState {
    pub entries: HashMap<ProducerId, ProducerEntry>,
}
