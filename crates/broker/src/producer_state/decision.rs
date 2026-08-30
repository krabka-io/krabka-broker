//! The pure idempotent-producer dedup and ordering decision, plus the async
//! `check` that wraps it in the per-partition lock.
//!
//! `check_pure` classifies an incoming batch as an append, a duplicate, an
//! out-of-order sequence, or a fenced epoch from the tracked entry alone, so
//! the exhaustive state model and the property test can drive the KIP-98
//! classification without a broker.

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
pub use krabka_verified::ProducerDecision as Decision;
use krabka_verified::{ProducerBatch, producer_decision};

use super::{ProducerEntry, ProducerState};

/// Pure idempotent-producer dedup/ordering decision.
///
/// The async `check` is a thin lock-acquiring wrapper over this function. The
/// decision is a separate function so that the tests can exhaustively test and
/// property-test it in isolation. The caller has already validated that the
/// two sequence fields are non-negative. See `producer_state_model.rs`.
pub(crate) fn check_pure(
    entry: Option<&ProducerEntry>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> Decision {
    producer_decision(
        entry.map(|entry| ProducerBatch {
            epoch: entry.epoch,
            last_sequence: entry.last_sequence,
            last_offset_delta: entry
                .last_offset
                .checked_sub(entry.base_offset)
                .and_then(|delta| i32::try_from(delta).ok()),
            base_offset: entry.base_offset,
        }),
        producer_epoch,
        base_sequence,
        last_offset_delta,
    )
}

impl ProducerState {
    /// Decide whether to append the incoming batch.
    ///
    /// `base_sequence` is the wire `base_sequence`. `last_offset_delta` is
    /// the batch's `last_offset_delta` field. Together they imply the
    /// batch's `last_sequence = base_sequence + last_offset_delta`.
    pub async fn check(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Decision {
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        check_pure(
            s.entries.get(&ProducerId(producer_id)),
            producer_epoch,
            base_sequence,
            last_offset_delta,
        )
    }
}

#[cfg(test)]
mod fuzz {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::{Decision, ProducerEntry, check_pure};

    proptest! {
        /// Large-N randomized submit sequences over `check_pure`.
        ///
        /// The accepted-append log per epoch is a contiguous, duplicate-free,
        /// monotonic prefix. A lower epoch is fenced. A higher epoch resets
        /// the baseline. This test complements the exhaustive
        /// `producer_state_model` at a scale the BFS cannot reach: epoch 0..6,
        /// base_seq 0..200, and up to 400 ops.
        #[test]
        fn idempotent_log_invariants(
            ops in proptest::collection::vec(
                (0i16..6, 0i32..200), // (producer_epoch, base_sequence)
                0..400usize,
            )
        ) {
            let mut entry: Option<ProducerEntry> = None;
            let mut next_offset: i64 = 0;
            // Reference: per-epoch highest accepted sequence (must stay contiguous).
            let mut hi: HashMap<i16, i32> = HashMap::new();
            for (epoch, base_seq) in ops {
                let d = check_pure(entry.as_ref(), epoch, base_seq, 0);
                match d {
                    Decision::Append => {
                        if let Some(e) = &entry {
                            if epoch == e.epoch {
                                prop_assert_eq!(
                                    base_seq,
                                    e.last_sequence + 1,
                                    "same-epoch Append must be contiguous"
                                );
                            } else {
                                prop_assert!(epoch > e.epoch, "Append epoch must be fresh");
                            }
                        }
                        // Per-epoch contiguity: an accepted seq for a fresh epoch
                        // starts the prefix; a same-epoch accept extends it by 1.
                        if let Some(p) = hi.get(&epoch).copied() {
                            prop_assert_eq!(
                                base_seq,
                                p + 1,
                                "accepted sequence must extend the per-epoch prefix"
                            );
                        }
                        hi.insert(epoch, base_seq);
                        entry = Some(ProducerEntry {
                            epoch,
                            last_sequence: base_seq,
                            last_offset: next_offset,
                            base_offset: next_offset,
                            last_timestamp: 0,
                            last_activity_ms: 0,
                        });
                        next_offset += 1;
                    }
                    Decision::Duplicate { .. } => {
                        let e = entry.as_ref().expect("Duplicate implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq == e.last_sequence,
                            "single-record duplicate must match the committed sequence"
                        );
                    }
                    Decision::OutOfOrder => {
                        let e = entry.as_ref().expect("OutOfOrder implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq != e.last_sequence && base_seq != e.last_sequence + 1,
                            "OutOfOrder must be neither a retry nor the next sequence"
                        );
                    }
                    Decision::Fenced => {
                        let e = entry.as_ref().expect("Fenced implies an entry");
                        prop_assert!(epoch < e.epoch, "Fenced must be a stale epoch");
                    }
                }
            }
        }
    }
}
