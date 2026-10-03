//! Exhaustive stateright enumeration of the idempotent-producer tracker: the
//! host's retained-batch bookkeeping ([`ProducerEntry::retained_batches`] and
//! [`super::entry::earlier_after_completion`]) around the proved
//! `producer_decision` kernel, as `check_retained` runs them. There is one
//! producer id per partition, and the broker serializes its requests, so the
//! model enumerates every bounded submit sequence.
//!
//! The oracle is independent of the host's representation. The state carries a
//! ghost [`Window`]: the producer's epoch, its last accepted sequence, and the
//! sequence ranges of the batches it accepted at that epoch, newest last, cut
//! to Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN` (five). Every submit
//! is classified by the host and by [`kafka_decision`] over the window, and a
//! disagreement is recorded in the state, where the `always` property
//! `decision_matches_kafka` turns it into a counterexample path.
//!
//! Batches hold one or two records (`last_offset_delta` 0 or 1), so retained
//! ranges differ in shape and a retry must match both ends of one. A batch is
//! placed at offset `base_sequence`, which keeps offsets out of the
//! fingerprint's growth while still giving every batch in the window a distinct
//! offset that a duplicate answer must echo.
//!
//! See the design spec `crates/broker/docs/transaction-coordinator-design.md`.

use std::collections::VecDeque;

use krabka_verified::increment_sequence;
use stateright::{Checker, Model, Property};

use super::{
    Decision, ProducerEntry, RetainedBatch,
    decision::{Checked, SequenceContext, check_retained},
    entry::{EarlierBatches, NUM_BATCHES_TO_RETAIN},
};
use crate::partition::LogOffset;

const MAX_STATES: usize = 2_000_000;

const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// The counts grew (from 13 and 92) when the model started to carry the host's
// retained batches and two-record batches, and to fill the five-batch window:
// the old model kept `earlier` empty, so only a retry of the last batch was
// ever a duplicate.
const PINNED_UNIQUE_STATES_BASIC: usize = 731;

const PINNED_UNIQUE_STATES_WIDE: usize = 4_233;

struct ProducerModel {
    max_epoch: i16,
    max_seq: i32,
}

/// A retained batch as the fingerprint holds it: its sequence range. Its
/// offsets follow from the range (see the module docs).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Range {
    base: i32,
    last: i32,
}

impl Range {
    fn batch(self) -> RetainedBatch {
        RetainedBatch {
            base_sequence: self.base,
            last_sequence: self.last,
            base_offset: LogOffset::from(self.base),
            last_offset: LogOffset::from(self.last),
            timestamp: 0,
        }
    }

    fn of(batch: RetainedBatch) -> Self {
        Self {
            base: batch.base_sequence,
            last: batch.last_sequence,
        }
    }
}

/// The host's tracked entry, projected onto what the fingerprint can hash.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct HostEntry {
    epoch: i16,
    last: Range,
    earlier: [Option<Range>; NUM_BATCHES_TO_RETAIN - 1],
}

impl HostEntry {
    fn entry(&self) -> ProducerEntry {
        let last = self.last.batch();
        ProducerEntry {
            epoch: self.epoch,
            last_sequence: last.last_sequence,
            last_offset: last.last_offset,
            base_offset: last.base_offset,
            last_timestamp: 0,
            entry_timestamp: 0,
            current_txn_first_offset: None,
            earlier: self.earlier.map(|slot| slot.map(Range::batch)),
        }
    }

    /// The entry after the host accepts `batch` at `epoch`: exactly what
    /// `ProducerState::commit` stores.
    fn after_append(existing: Option<&Self>, epoch: i16, batch: Range) -> Self {
        let (_, earlier): (bool, EarlierBatches) = super::entry::earlier_after_completion(
            existing.map(HostEntry::entry),
            epoch,
            batch.batch(),
        );
        Self {
            epoch,
            last: Range {
                base: batch.base,
                last: increment_sequence(batch.base, batch.last - batch.base),
            },
            earlier: earlier.map(|slot| slot.map(Range::of)),
        }
    }
}

/// The ghost: what Kafka's producer state holds, written the plain way.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Window {
    epoch: i16,
    last_sequence: i32,
    /// The accepted batches at `epoch`, oldest first, at most five.
    retained: VecDeque<Range>,
}

/// Kafka's answer to one batch, with a duplicate's retained range.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Answer {
    Append,
    Duplicate(Range),
    OutOfOrder,
    Fenced,
}

impl Answer {
    fn of(checked: Checked) -> Self {
        match checked.decision {
            Decision::Append => Self::Append,
            Decision::Duplicate { base_offset } => {
                let batch = checked
                    .duplicate
                    .expect("a duplicate names its retained batch");
                assert2::assert!(batch.base_offset == base_offset);
                Self::Duplicate(Range::of(batch))
            }
            Decision::OutOfOrder => Self::OutOfOrder,
            Decision::Fenced => Self::Fenced,
        }
    }
}

/// A submit whose outcome a `sometimes` witness looks for. It is recorded only
/// on the transition that produced it and cleared by the next, so it adds at
/// most a few states per reachable entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Witness {
    /// A retry of the last accepted batch answered as a duplicate.
    DuplicateOfLast,
    /// A retry of a retained batch other than the last answered as a
    /// duplicate: only the five-batch window can answer it.
    DuplicateOfOlder,
    /// A retry of the oldest of five retained batches answered as a
    /// duplicate.
    DuplicateOfOldest,
    /// With the five-batch window full, a retry of sequences below it, which
    /// only an evicted batch can have held, was refused as out of order
    /// rather than deduplicated.
    RetryBelowWindowRefused,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ProdState {
    host: Option<HostEntry>,
    window: Option<Window>,
    witness: Option<Witness>,
    /// The first submit on which the host and Kafka disagreed. It is only ever
    /// set by a defect, so it costs no states on a correct host.
    violation: Option<(i16, Range, Answer, Answer)>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ProdAction {
    /// Submit the batch `range` at `epoch`.
    Submit(i16, Range),
}

#[path = "producer_state_model/helpers.rs"]
mod helpers;
use helpers::kafka_decision;

#[path = "producer_state_model/checker.rs"]
mod checker;

#[path = "producer_state_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "producer_state_model/tests.rs"]
mod tests;
