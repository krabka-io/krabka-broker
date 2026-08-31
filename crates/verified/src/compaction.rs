//! KIP-534 log-compaction decision core.
//!
//! This core comes out of `krabka-log`, so that Creusot can verify it. The host
//! crate re-exports these functions, and the stateright model in
//! `krabka-log/src/compact_model.rs` drives these exact functions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Per-record facts the retain decision needs.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RecordMeta {
    /// Whether the record has a key.
    pub has_key: bool,
    /// Whether the record has a non-null value.
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct BatchMeta {
    /// Whether the batch is a transactional control batch.
    pub is_control: bool,
    /// Producer id for transactional batches. It is negative for
    /// non-transactional batches.
    pub producer_id: i64,
    /// The batch's existing delete horizon, which is `base_timestamp` when bit
    /// 6 is set. It is `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Whether a producer's transactional DATA still survives compaction.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// At least one of this producer's data records survives compaction.
    DataSurvives,
    /// All of this producer's data records have been compacted away.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RetainDecision {
    /// Keep the record as-is.
    Keep,
    /// Keep the record, but stamp its batch with this delete horizon:
    /// `base_timestamp = horizon`, with bit 6 set.
    SetHorizon(i64),
    /// Drop the record.
    Delete,
}

/// Whether a compaction batch-stream decode is complete, made progress, or
/// encountered corrupt input.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum CompactionDecodeStep {
    /// The input is exhausted. This is the only successful completion state.
    Done,
    /// One batch decoded and consumed at least one byte.
    Continue,
    /// Nonempty input did not decode with strict progress.
    Corrupt,
}

/// Classify one compaction batch-stream decode step.
///
/// A decode failure can never be mistaken for end-of-stream: completion is
/// admitted only when no input remains, and a successful decode must consume
/// at least one byte.
#[ensures((result == CompactionDecodeStep::Done) == (remaining_before@ == 0))]
#[ensures((result == CompactionDecodeStep::Continue) == (remaining_before@ > 0
    && decode_succeeded
    && remaining_after@ < remaining_before@))]
#[ensures((result == CompactionDecodeStep::Corrupt) == (remaining_before@ > 0
    && (!decode_succeeded || remaining_after@ >= remaining_before@)))]
#[must_use]
pub const fn compaction_decode_step(
    remaining_before: usize,
    decode_succeeded: bool,
    remaining_after: usize,
) -> CompactionDecodeStep {
    if remaining_before == 0 {
        CompactionDecodeStep::Done
    } else if decode_succeeded && remaining_after < remaining_before {
        CompactionDecodeStep::Continue
    } else {
        CompactionDecodeStep::Corrupt
    }
}

/// Decide whether retention may evict one oldest-prefix segment.
///
/// The host classifies whether the segment timestamp is known and whether its
/// native retention duration comparison has expired. An unknown timestamp
/// fails closed for time pressure. Size pressure remains independent and may
/// still evict it.
#[ensures(result == (by_size || (timestamp_known && time_expired)))]
#[ensures(by_size ==> result)]
#[ensures(!timestamp_known && !by_size ==> !result)]
#[ensures(!by_size && result ==> timestamp_known && time_expired)]
#[must_use]
pub const fn retention_segment_evict(
    timestamp_known: bool,
    time_expired: bool,
    by_size: bool,
) -> bool {
    by_size || (timestamp_known && time_expired)
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`.
///
/// The tombstone or marker is retained until the wall clock reaches this
/// value.
#[cfg(creusot)]
#[logic]
pub fn compute_horizon_model(now_ms: i64, delete_retention_ms: i64) -> Int {
    pearlite! {
        if now_ms@ + delete_retention_ms@ > 9223372036854775807 {
            9223372036854775807
        } else if now_ms@ + delete_retention_ms@ < -9223372036854775807 - 1 {
            -9223372036854775807 - 1
        } else {
            now_ms@ + delete_retention_ms@
        }
    }
}

#[ensures(result@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(result@ == if now_ms@ + delete_retention_ms@ > 9223372036854775807 {
    9223372036854775807
} else if now_ms@ + delete_retention_ms@ < -9223372036854775807 - 1 {
    -9223372036854775807 - 1
} else {
    now_ms@ + delete_retention_ms@
})]
#[must_use]
pub const fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

/// The single per-record KIP-534 retain decision.
///
/// Control batches, that is transaction commit and abort markers, are retained
/// as long as their transaction's data survives. After the data is fully
/// compacted away, the marker ages out through the delete horizon. Data records
/// dedup newest-wins. A tombstone, that is a record with a null value, ages out
/// through the delete horizon after it becomes the newest entry for its key.
#[ensures(batch.is_control && (txn == TxnDataState::DataSurvives || txn == TxnDataState::NotTransactional)
    ==> result == RetainDecision::Keep)]
#[ensures(batch.is_control && txn == TxnDataState::DataFullyGone && batch.existing_horizon == None
    ==> exists<h: i64> result == RetainDecision::SetHorizon(h)
        && h@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(forall<h: i64> batch.is_control && txn == TxnDataState::DataFullyGone
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
#[ensures(!batch.is_control && !rec.has_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && !is_newest_for_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && rec.has_value
    ==> result == RetainDecision::Keep)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == None
    ==> exists<h: i64> result == RetainDecision::SetHorizon(h)
        && h@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[ensures(forall<h: i64> !batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
#[must_use]
pub const fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => match batch.existing_horizon {
                Some(h) if now_ms >= h => RetainDecision::Delete,
                Some(_) => RetainDecision::Keep,
                None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
            },
        };
    }
    if !rec.has_key {
        return RetainDecision::Delete;
    }
    if !is_newest_for_key {
        return RetainDecision::Delete;
    }
    if rec.has_value {
        return RetainDecision::Keep;
    }
    // Newest-for-key tombstone: age out via the delete horizon.
    match batch.existing_horizon {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const fn record(has_key: bool, has_value: bool) -> RecordMeta {
        RecordMeta { has_key, has_value }
    }

    const fn batch(is_control: bool, existing_horizon: Option<i64>) -> BatchMeta {
        BatchMeta {
            is_control,
            producer_id: -1,
            existing_horizon,
        }
    }

    #[test]
    fn compute_horizon_saturates_at_i64_bounds() {
        for (_name, timestamp, lag, expected) in [
            ("ordinary addition", 100, 50, 150),
            ("upper saturation", i64::MAX - 1, 50, i64::MAX),
            ("lower saturation", i64::MIN + 1, -50, i64::MIN),
        ] {
            assert2::assert!(compute_horizon(timestamp, lag) == expected);
        }
    }

    #[test]
    fn retention_admission_fails_closed_without_losing_size_pressure() {
        for timestamp_known in [false, true] {
            for time_expired in [false, true] {
                for by_size in [false, true] {
                    assert2::assert!(
                        retention_segment_evict(timestamp_known, time_expired, by_size)
                            == (by_size || (timestamp_known && time_expired))
                    );
                }
            }
        }
    }

    #[test]
    fn compaction_decode_step_truth_table() {
        for remaining_before in 0..=2 {
            for remaining_after in 0..=2 {
                for decode_succeeded in [false, true] {
                    let expected = if remaining_before == 0 {
                        CompactionDecodeStep::Done
                    } else if decode_succeeded && remaining_after < remaining_before {
                        CompactionDecodeStep::Continue
                    } else {
                        CompactionDecodeStep::Corrupt
                    };
                    assert2::assert!(
                        compaction_decode_step(remaining_before, decode_succeeded, remaining_after)
                            == expected
                    );
                }
            }
        }
    }

    #[test]
    fn retain_decision_distinguishes_expired_and_live_horizons() {
        let tombstone = record(true, false);
        let live_value = record(true, true);

        assert2::assert!(
            retain_decision(
                tombstone,
                batch(false, Some(10)),
                true,
                TxnDataState::NotTransactional,
                10,
                50
            ) == RetainDecision::Delete
        );
        assert2::assert!(
            retain_decision(
                tombstone,
                batch(false, Some(10)),
                true,
                TxnDataState::NotTransactional,
                9,
                50
            ) == RetainDecision::Keep
        );
        assert2::assert!(
            retain_decision(
                live_value,
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::Keep
        );
        assert2::assert!(
            retain_decision(
                record(false, true),
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::Delete
        );
    }

    #[test]
    fn retain_decision_stamps_new_tombstone_and_expired_control_marker() {
        assert2::assert!(
            retain_decision(
                record(true, false),
                batch(false, None),
                true,
                TxnDataState::NotTransactional,
                100,
                50
            ) == RetainDecision::SetHorizon(150)
        );
        assert2::assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataFullyGone,
                10,
                50
            ) == RetainDecision::Delete
        );
        assert2::assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataFullyGone,
                9,
                50
            ) == RetainDecision::Keep
        );
        assert2::assert!(
            retain_decision(
                record(true, false),
                batch(true, Some(10)),
                false,
                TxnDataState::DataSurvives,
                10,
                50
            ) == RetainDecision::Keep
        );
    }
}
