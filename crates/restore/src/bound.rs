//! The restore bound: every predicate an operator gave, and what it decides.
//!
//! This module owns the point in time the restore stops at. It compiles the
//! `--to-offset`, `--to-timestamp`, `--exclude-key`, `--exclude-header`,
//! `--exclude-producer-id`, and `--exclude-offset` flags into one predicate
//! set, and answers two questions about the archived bytes. For a batch it
//! answers whether the batch passes through untouched, is dropped whole, or
//! has to be re-encoded because only some of its records survive; the
//! borrowed batch view makes that decision without an owned decode of the
//! batches that pass. For one record inside a batch that must be re-encoded,
//! it answers whether the record survives. The exclude patterns match the raw
//! key and header bytes. This module decodes no payload and knows no schema.

use crabka_ids::{Offset, ProducerId};
use crabka_protocol::records::{RecordBatchBorrowed, RecordBorrowed};

use crate::{
    args::{PartitionRef, RestoreArgs},
    error::RestoreError,
};

/// What the bound decides about one archived batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDecision {
    /// No predicate touches the batch. It is written verbatim, so its bytes
    /// stay byte-identical to the archived copy.
    Keep,
    /// Every record in the batch is excluded. The batch is not written.
    Drop,
    /// Some records survive. The batch is re-encoded from the records that
    /// [`Predicates::decide_record`] keeps, so its bytes differ from the
    /// archived copy.
    Filter,
}

/// What the bound decides about one record inside a filtered batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordDecision {
    /// The record is written.
    Keep,
    /// The record is not written.
    Drop,
}

/// The compiled predicate set.
#[derive(Debug)]
pub struct Predicates;

impl Predicates {
    /// Compile the bound flags into a predicate set.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when the flags describe a
    /// bound that can never keep a record.
    pub fn from_args(_args: &RestoreArgs) -> Result<Self, RestoreError> {
        todo!("compile the offset, timestamp, key, header, producer, and range predicates")
    }

    /// The highest offset the restore keeps in `partition`, when
    /// `--to-offset` names it.
    #[must_use]
    pub fn offset_bound(&self, _partition: &PartitionRef) -> Option<Offset> {
        todo!("look up the partition's --to-offset bound")
    }

    /// Decide the fate of one archived batch.
    ///
    /// `base_offset` is the offset the batch holds in the *target* partition,
    /// which is the archived offset unless an earlier batch was filtered.
    #[must_use]
    pub fn decide_batch(
        &self,
        _partition: &PartitionRef,
        _batch: &RecordBatchBorrowed<'_>,
    ) -> BatchDecision {
        todo!("decide whether the batch passes, is dropped, or must be re-encoded")
    }

    /// Decide the fate of one record inside a batch that must be re-encoded.
    #[must_use]
    pub fn decide_record(
        &self,
        _partition: &PartitionRef,
        _offset: Offset,
        _timestamp_ms: i64,
        _producer_id: ProducerId,
        _record: &RecordBorrowed<'_>,
    ) -> RecordDecision {
        todo!("apply the key, header, producer, offset, and timestamp predicates")
    }
}
