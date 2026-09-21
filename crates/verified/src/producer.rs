//! Idempotent-producer sequence arithmetic and deduplication decision.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// The last accepted batch for one producer.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerBatch {
    pub epoch: i16,
    pub last_sequence: i32,
    /// The batch's offset delta, if the host state can represent it as `i32`.
    pub last_offset_delta: Option<i32>,
    pub base_offset: i64,
}

/// Result of classifying an idempotent-producer batch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ProducerDecision {
    Append,
    Duplicate { base_offset: i64 },
    OutOfOrder,
    Fenced,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn increment_sequence_model(sequence: i32, increment: i32) -> i32 {
    pearlite! { (sequence + increment) & i32::MAX }
}

/// Advance a Kafka producer sequence modulo `2^31`.
#[cfg_attr(creusot, ensures(result == increment_sequence_model(sequence, increment)))]
#[must_use]
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    sequence.wrapping_add(increment) & i32::MAX
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn decrement_sequence_model(sequence: i32, decrement: i32) -> i32 {
    pearlite! { (sequence - decrement) & i32::MAX }
}

/// Move a Kafka producer sequence backwards modulo `2^31`.
#[cfg_attr(creusot, ensures(result == decrement_sequence_model(sequence, decrement)))]
#[must_use]
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    sequence.wrapping_sub(decrement) & i32::MAX
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn matches_last_batch_model(
    last: ProducerBatch,
    base_sequence: i32,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        exists<committed_delta: i32>
            last.last_offset_delta == Some(committed_delta)
            && base_sequence == decrement_sequence_model(last.last_sequence, committed_delta)
            && increment_sequence_model(base_sequence, last_offset_delta) == last.last_sequence
    }
}

#[cfg_attr(
    creusot,
    ensures(result == matches_last_batch_model(last, base_sequence, last_offset_delta))
)]
fn matches_last_batch(last: ProducerBatch, base_sequence: i32, last_offset_delta: i32) -> bool {
    let Some(committed_delta) = last.last_offset_delta else {
        return false;
    };
    base_sequence == decrement_sequence(last.last_sequence, committed_delta)
        && increment_sequence(base_sequence, last_offset_delta) == last.last_sequence
}

/// Classify an incoming batch against the last accepted producer batch.
///
/// A batch at a higher epoch than `last` must start at sequence 0. Any other
/// first sequence is out of order. This is Kafka's
/// `ProducerAppendInfo.checkSequence`, which throws
/// `OutOfOrderSequenceException` when the epoch changes and the first
/// sequence is not 0. A producer with no entry can start at any sequence.
#[cfg_attr(creusot, ensures(last == None ==> result == ProducerDecision::Append))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ < accepted.epoch@
        ==> result == ProducerDecision::Fenced))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ > accepted.epoch@ && base_sequence == 0i32
        ==> result == ProducerDecision::Append))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ > accepted.epoch@ && base_sequence != 0i32
        ==> result == ProducerDecision::OutOfOrder))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ == accepted.epoch@
        && base_sequence == increment_sequence_model(accepted.last_sequence, 1i32)
        ==> result == ProducerDecision::Append))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ == accepted.epoch@
        && base_sequence != increment_sequence_model(accepted.last_sequence, 1i32)
        && matches_last_batch_model(accepted, base_sequence, last_offset_delta)
        ==> result == ProducerDecision::Duplicate { base_offset: accepted.base_offset }))]
#[cfg_attr(creusot, ensures(forall<accepted: ProducerBatch>
    last == Some(accepted) && producer_epoch@ == accepted.epoch@
        && base_sequence != increment_sequence_model(accepted.last_sequence, 1i32)
        && !matches_last_batch_model(accepted, base_sequence, last_offset_delta)
        ==> result == ProducerDecision::OutOfOrder))]
#[must_use]
pub fn producer_decision(
    last: Option<ProducerBatch>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> ProducerDecision {
    let Some(last) = last else {
        return ProducerDecision::Append;
    };
    if producer_epoch < last.epoch {
        return ProducerDecision::Fenced;
    }
    if producer_epoch > last.epoch {
        if base_sequence == 0 {
            return ProducerDecision::Append;
        }
        return ProducerDecision::OutOfOrder;
    }
    if base_sequence == increment_sequence(last.last_sequence, 1) {
        return ProducerDecision::Append;
    }
    if matches_last_batch(last, base_sequence, last_offset_delta) {
        return ProducerDecision::Duplicate {
            base_offset: last.base_offset,
        };
    }
    ProducerDecision::OutOfOrder
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn sequence_arithmetic_wraps_at_signed_maximum() {
        assert!(increment_sequence(i32::MAX, 1) == 0);
        assert!(increment_sequence(i32::MAX - 1, 3) == 1);
        assert!(decrement_sequence(0, 1) == i32::MAX);
    }

    #[test]
    fn producer_decision_covers_all_outcomes() {
        let last = ProducerBatch {
            epoch: 2,
            last_sequence: 6,
            last_offset_delta: Some(2),
            base_offset: 10,
        };
        // (label, last, epoch, base sequence, last offset delta, decision)
        let cases = [
            (
                "no entry, any sequence",
                None,
                0,
                17,
                0,
                ProducerDecision::Append,
            ),
            ("lower epoch", Some(last), 1, 4, 2, ProducerDecision::Fenced),
            (
                "higher epoch at 0",
                Some(last),
                3,
                0,
                0,
                ProducerDecision::Append,
            ),
            (
                "higher epoch continues the sequence",
                Some(last),
                3,
                7,
                0,
                ProducerDecision::OutOfOrder,
            ),
            (
                "higher epoch repeats the last batch",
                Some(last),
                3,
                4,
                2,
                ProducerDecision::OutOfOrder,
            ),
            (
                "same epoch, next sequence",
                Some(last),
                2,
                7,
                0,
                ProducerDecision::Append,
            ),
            (
                "same epoch, last batch again",
                Some(last),
                2,
                4,
                2,
                ProducerDecision::Duplicate { base_offset: 10 },
            ),
            (
                "same epoch, gap",
                Some(last),
                2,
                5,
                2,
                ProducerDecision::OutOfOrder,
            ),
            (
                "same epoch, same base sequence but different delta",
                Some(last),
                2,
                4,
                3,
                ProducerDecision::OutOfOrder,
            ),
        ];
        for (label, last, epoch, base_sequence, delta, expected) in cases {
            assert!(
                producer_decision(last, epoch, base_sequence, delta) == expected,
                "case: {label}"
            );
        }
    }
}
