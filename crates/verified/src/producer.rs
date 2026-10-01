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

mod sequence_decision;
pub use sequence_decision::{decrement_sequence, increment_sequence};
#[cfg(creusot)]
pub use sequence_decision::{
    first_retained_duplicate, retained_duplicate_exists, retained_matches, sequence_decision,
    sequence_modulo_2_31,
};

mod decision;
pub use decision::producer_decision;

mod completion;
#[cfg(creusot)]
pub use completion::completion_offset;
pub use completion::producer_completion_window;

#[cfg(test)]
mod tests;
