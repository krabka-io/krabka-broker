//! Idempotent-producer sequence arithmetic and deduplication decision.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// The sequence range of one batch a producer's entry retains: Kafka's
/// `BatchMetadata` `firstSeq` and `lastSeq`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RetainedSequenceRange {
    pub base_sequence: i32,
    pub last_sequence: i32,
}

/// The fields of one producer's tracked entry that the sequence check reads.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerEntryFacts {
    /// The entry's producer epoch.
    pub epoch: i16,
    /// The last sequence the entry accepted.
    pub last_sequence: i32,
}

/// Result of classifying an idempotent-producer batch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ProducerDecision {
    Append,
    /// The batch repeats the retained batch at this index of the slice the
    /// caller passed.
    Duplicate {
        retained: usize,
    },
    OutOfOrder,
    Fenced,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn sequence_modulo_2_31(sequence: Int) -> Int {
    pearlite! { sequence.rem_euclid(2147483648) }
}

/// Advance a Kafka producer sequence modulo `2^31`.
///
/// The result is `sequence + increment` reduced modulo `2^31`. For the
/// nonnegative operands Kafka uses, that is exactly
/// `DefaultRecordBatch.incrementSequence`: the sum when it fits in an `i32`,
/// otherwise the sum minus `2^31`.
#[ensures(result@ == sequence_modulo_2_31(sequence@ + increment@))]
#[must_use]
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    // Reduce the sum into `[0, 2^31)` without leaving `i32`: adding `2^31` is
    // subtracting `i32::MIN`, and subtracting `2^31` is adding it.
    match sequence.checked_add(increment) {
        Some(sum) if sum >= 0 => sum,
        Some(sum) => sum - i32::MIN,
        // The sum passed `i32::MAX`, so both operands are positive.
        None if increment > 0 => sequence + (increment + i32::MIN),
        // The sum fell below `i32::MIN`, so both operands are negative.
        None => (sequence - i32::MIN) + (increment - i32::MIN),
    }
}

/// Move a Kafka producer sequence backwards modulo `2^31`.
///
/// The result is `sequence - decrement` reduced modulo `2^31`. For the
/// nonnegative operands Kafka uses, that is exactly
/// `DefaultRecordBatch.decrementSequence`: the difference when it is not
/// negative, otherwise the difference plus `2^31`.
#[ensures(result@ == sequence_modulo_2_31(sequence@ - decrement@))]
#[must_use]
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    // The same reduction as `increment_sequence`, for a difference.
    match sequence.checked_sub(decrement) {
        Some(difference) if difference >= 0 => difference,
        Some(difference) => difference - i32::MIN,
        // The difference passed `i32::MAX`: `sequence >= 0 > decrement`.
        None if decrement < 0 => sequence + (i32::MIN - decrement),
        // The difference fell below `i32::MIN`: `sequence < 0 < decrement`.
        None => (sequence - i32::MIN) - (decrement + i32::MIN),
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn retained_matches(
    slot: Option<RetainedSequenceRange>,
    base_sequence: i32,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        match slot {
            Some(range) => range.base_sequence@ == base_sequence@
                && range.last_sequence@
                    == sequence_modulo_2_31(base_sequence@ + last_offset_delta@),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn retained_duplicate_exists(
    entry: Option<ProducerEntryFacts>,
    retained: Seq<Option<RetainedSequenceRange>>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        match entry {
            Some(tracked) => tracked.epoch@ == producer_epoch@
                && exists<i: Int> 0 <= i && i < retained.len()
                    && retained_matches(retained[i], base_sequence, last_offset_delta),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn first_retained_duplicate(
    entry: Option<ProducerEntryFacts>,
    retained: Seq<Option<RetainedSequenceRange>>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
    index: Int,
) -> bool {
    pearlite! {
        match entry {
            Some(tracked) => tracked.epoch@ == producer_epoch@
                && 0 <= index && index < retained.len()
                && retained_matches(retained[index], base_sequence, last_offset_delta)
                && forall<j: Int> 0 <= j && j < index
                    ==> !retained_matches(retained[j], base_sequence, last_offset_delta),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn sequence_decision(
    entry: Option<ProducerEntryFacts>,
    producer_epoch: i16,
    base_sequence: i32,
    log_empty: bool,
    trunk_rules: bool,
) -> ProducerDecision {
    pearlite! {
        match entry {
            None => if trunk_rules && log_empty && base_sequence@ != 0 {
                ProducerDecision::OutOfOrder
            } else {
                ProducerDecision::Append
            },
            Some(tracked) => if producer_epoch@ < tracked.epoch@ {
                ProducerDecision::Fenced
            } else if producer_epoch@ > tracked.epoch@ {
                if base_sequence@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder }
            } else if base_sequence@ == sequence_modulo_2_31(tracked.last_sequence@ + 1) {
                ProducerDecision::Append
            } else {
                ProducerDecision::OutOfOrder
            },
        }
    }
}

/// Classify an incoming batch against a producer's tracked entry and the
/// sequence ranges of the batches it retains.
///
/// This is Kafka's `UnifiedLog.analyzeAndValidateProducerState` for a client
/// append. It first looks the batch up among the retained batches
/// (`ProducerStateEntry.findDuplicateBatch`): at the entry's epoch, a batch
/// whose base sequence and last sequence (`base_sequence + last_offset_delta`
/// modulo `2^31`) equal a retained batch's is a duplicate of the first such
/// batch. `None` slots never match. Only a batch that repeats no retained
/// batch goes on to `ProducerAppendInfo.checkProducerEpoch` and
/// `checkSequence`:
///
/// - a producer with no entry can start at any sequence, except that under
///   `trunk_rules` it must start at sequence 0 on a partition whose log is
///   empty (`log_empty`) and is otherwise out of order;
/// - a lower epoch is fenced;
/// - a higher epoch must start at sequence 0 and is otherwise out of order;
/// - the same epoch must continue at the last sequence plus one modulo `2^31`
///   and is otherwise out of order.
///
/// The empty-log rule is Kafka trunk's (KAFKA-15591, in
/// `ProducerAppendInfo.checkSequence`); Kafka 4.3.1 does not have it.
/// `log_empty` is Kafka's `ProducerStateManager.mapEndOffset() == 0`: no
/// record has ever been appended to the partition, so no producer state can
/// have been lost, and `trunk_rules` says whether the host serves Kafka
/// trunk's behaviour.
///
/// The host supplies the retained batches at the entry's epoch; a batch at
/// any other epoch never matches them.
#[ensures(match result {
    ProducerDecision::Duplicate { retained: index } => first_retained_duplicate(
        entry, retained@, producer_epoch, base_sequence, last_offset_delta, index@),
    _ => !retained_duplicate_exists(
            entry, retained@, producer_epoch, base_sequence, last_offset_delta)
        && result == sequence_decision(
            entry, producer_epoch, base_sequence, log_empty, trunk_rules),
})]
#[must_use]
pub fn producer_decision(
    entry: Option<ProducerEntryFacts>,
    retained: &[Option<RetainedSequenceRange>],
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
    log_empty: bool,
    trunk_rules: bool,
) -> ProducerDecision {
    let Some(tracked) = entry else {
        if trunk_rules && log_empty && base_sequence != 0 {
            return ProducerDecision::OutOfOrder;
        }
        return ProducerDecision::Append;
    };
    if producer_epoch == tracked.epoch {
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let mut i = 0usize;
        #[invariant(i@ <= retained@.len())]
        #[invariant(forall<j: Int> 0 <= j && j < i@
            ==> !retained_matches(retained@[j], base_sequence, last_offset_delta))]
        #[variant(retained@.len() - i@)]
        while i < retained.len() {
            if let Some(range) = retained[i]
                && range.base_sequence == base_sequence
                && range.last_sequence == last_sequence
            {
                return ProducerDecision::Duplicate { retained: i };
            }
            i += 1;
        }
    }
    if producer_epoch < tracked.epoch {
        return ProducerDecision::Fenced;
    }
    if producer_epoch > tracked.epoch {
        if base_sequence == 0 {
            return ProducerDecision::Append;
        }
        return ProducerDecision::OutOfOrder;
    }
    if base_sequence == increment_sequence(tracked.last_sequence, 1) {
        return ProducerDecision::Append;
    }
    ProducerDecision::OutOfOrder
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn sequence_arithmetic_matches_kafka_wraparound() {
        // (sequence, step, incremented, decremented), from Kafka's
        // `DefaultRecordBatch.incrementSequence` / `decrementSequence`.
        let cases = [
            (0, 0, 0, 0),
            (5, 3, 8, 2),
            (i32::MAX, 1, 0, i32::MAX - 1),
            (i32::MAX - 1, 3, 1, i32::MAX - 4),
            (0, 1, 1, i32::MAX),
            (2, 5, 7, i32::MAX - 2),
            (i32::MAX, i32::MAX, i32::MAX - 1, 0),
            // Outside Kafka's nonnegative domain the result is still the
            // exact value modulo `2^31`, so a `-1` (no sequence) advances to 0.
            (-1, 1, 0, i32::MAX - 1),
            (i32::MIN, -1, i32::MAX, 1),
            (i32::MIN, i32::MIN, 0, 0),
            (5, i32::MIN, 5, 5),
            (-3, i32::MAX, i32::MAX - 3, i32::MAX - 1),
        ];
        for (sequence, step, incremented, decremented) in cases {
            assert!(increment_sequence(sequence, step) == incremented);
            assert!(decrement_sequence(sequence, step) == decremented);
        }
    }

    const ENTRY: ProducerEntryFacts = ProducerEntryFacts {
        epoch: 2,
        last_sequence: 6,
    };

    /// Kafka's five retained batches for a producer that sent sequences
    /// 0..=6 at epoch 2 in batches `[0]`, `[1, 2]`, `[3]`, `[4, 5, 6]`, with
    /// one slot unused.
    const RETAINED: [Option<RetainedSequenceRange>; 5] = [
        None,
        Some(RetainedSequenceRange {
            base_sequence: 0,
            last_sequence: 0,
        }),
        Some(RetainedSequenceRange {
            base_sequence: 1,
            last_sequence: 2,
        }),
        Some(RetainedSequenceRange {
            base_sequence: 3,
            last_sequence: 3,
        }),
        Some(RetainedSequenceRange {
            base_sequence: 4,
            last_sequence: 6,
        }),
    ];

    #[test]
    fn producer_decision_covers_all_outcomes() {
        // (label, entry, epoch, base sequence, last offset delta, decision)
        let cases = [
            (
                "no entry, any sequence",
                None,
                0,
                17,
                0,
                ProducerDecision::Append,
            ),
            (
                "lower epoch",
                Some(ENTRY),
                1,
                4,
                2,
                ProducerDecision::Fenced,
            ),
            (
                "higher epoch at 0",
                Some(ENTRY),
                3,
                0,
                0,
                ProducerDecision::Append,
            ),
            (
                "higher epoch continues the sequence",
                Some(ENTRY),
                3,
                7,
                0,
                ProducerDecision::OutOfOrder,
            ),
            (
                "higher epoch repeats the last batch",
                Some(ENTRY),
                3,
                4,
                2,
                ProducerDecision::OutOfOrder,
            ),
            (
                "same epoch, next sequence",
                Some(ENTRY),
                2,
                7,
                0,
                ProducerDecision::Append,
            ),
            (
                "same epoch, last batch again",
                Some(ENTRY),
                2,
                4,
                2,
                ProducerDecision::Duplicate { retained: 4 },
            ),
            (
                "same epoch, an earlier retained batch again",
                Some(ENTRY),
                2,
                1,
                1,
                ProducerDecision::Duplicate { retained: 2 },
            ),
            (
                "same epoch, the oldest retained batch again",
                Some(ENTRY),
                2,
                0,
                0,
                ProducerDecision::Duplicate { retained: 1 },
            ),
            (
                "same epoch, a sub-range of a retained batch",
                Some(ENTRY),
                2,
                1,
                0,
                ProducerDecision::OutOfOrder,
            ),
            (
                "same epoch, gap",
                Some(ENTRY),
                2,
                9,
                0,
                ProducerDecision::OutOfOrder,
            ),
            (
                "same epoch, same base sequence but different delta",
                Some(ENTRY),
                2,
                4,
                3,
                ProducerDecision::OutOfOrder,
            ),
        ];
        for (label, entry, epoch, base_sequence, delta, expected) in cases {
            assert!(
                producer_decision(entry, &RETAINED, epoch, base_sequence, delta, false, false)
                    == expected,
                "case: {label}"
            );
        }
    }

    /// Kafka trunk's KAFKA-15591 rule, which Kafka 4.3.1 does not have: a
    /// producer with no entry must start at sequence 0 on a partition that has
    /// never held a record. Every other input keeps the 4.3.1 decision.
    #[test]
    fn a_producer_with_no_state_on_an_empty_log_starts_at_zero_under_trunk_rules() {
        // (label, entry, base sequence, log empty, trunk rules, decision)
        let cases = [
            (
                "never appended, no entry, sequence 0",
                None,
                0,
                true,
                true,
                ProducerDecision::Append,
            ),
            (
                "never appended, no entry, sequence 7",
                None,
                7,
                true,
                true,
                ProducerDecision::OutOfOrder,
            ),
            (
                "has records, no entry, sequence 7",
                None,
                7,
                false,
                true,
                ProducerDecision::Append,
            ),
            (
                "has records, entry expired, sequence 0",
                None,
                0,
                false,
                true,
                ProducerDecision::Append,
            ),
            (
                "never appended, no entry, sequence 7, Kafka 4.3.1",
                None,
                7,
                true,
                false,
                ProducerDecision::Append,
            ),
            (
                "never appended, an entry continues its sequence",
                Some(ENTRY),
                7,
                true,
                true,
                ProducerDecision::Append,
            ),
            (
                "never appended, an entry at a gap",
                Some(ENTRY),
                9,
                true,
                true,
                ProducerDecision::OutOfOrder,
            ),
        ];
        for (label, entry, base_sequence, log_empty, trunk_rules, expected) in cases {
            assert!(
                producer_decision(
                    entry,
                    &RETAINED,
                    ENTRY.epoch,
                    base_sequence,
                    0,
                    log_empty,
                    trunk_rules,
                ) == expected,
                "case: {label}"
            );
        }
    }

    #[test]
    fn sequence_continues_across_wraparound() {
        let entry = ProducerEntryFacts {
            epoch: 0,
            last_sequence: i32::MAX,
        };
        let retained = [Some(RetainedSequenceRange {
            base_sequence: i32::MAX - 1,
            last_sequence: i32::MAX,
        })];
        assert!(
            producer_decision(Some(entry), &retained, 0, 0, 3, false, false)
                == ProducerDecision::Append
        );
        assert!(
            producer_decision(Some(entry), &retained, 0, i32::MAX - 1, 1, false, false)
                == ProducerDecision::Duplicate { retained: 0 }
        );
        assert!(
            producer_decision(Some(entry), &[], 0, i32::MAX - 1, 1, false, false)
                == ProducerDecision::OutOfOrder
        );
    }
}
