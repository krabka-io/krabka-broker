//! One producer's state as its partition log holds it, for the models that
//! interleave produces with the partition writer.
//!
//! A model keeps its log as a list of single-record batches. [`LoggedProducer`]
//! replays one producer's batches through the log's own update,
//! [`ProducerSnapshotEntry::add_data_batch`], as
//! `Log::update_data_producer_entry` runs it on every append, and answers from
//! that state with the code the broker runs over a real log.

use {
    super::{ProducerEntry, recovery::entries_from_snapshot},
    crate::partition::LogOffset,
    krabka_log::{
        Offset, ProducerBatchMetadata, ProducerId, ProducerSnapshotEntry, RecoveredProducer,
    },
    std::collections::VecDeque,
};

/// One producer's entry and earlier batches in a partition log, or `None`
/// when the log holds no batch of it.
#[derive(Clone, Debug, Default)]
pub struct LoggedProducer(
    Option<(
        ProducerSnapshotEntry,
        Option<VecDeque<ProducerBatchMetadata>>,
    )>,
);

impl LoggedProducer {
    /// The state after the log appends `batches` in order, each a
    /// single-record batch of `producer` at `epoch` with this sequence and
    /// offset and a max timestamp of 0.
    pub fn replay(
        producer: i64,
        epoch: i16,
        batches: impl IntoIterator<Item = (i32, LogOffset)>,
    ) -> Self {
        let mut state = None;
        for (sequence, offset) in batches {
            let (entry, earlier) = state.get_or_insert_with(|| {
                (
                    ProducerSnapshotEntry::empty(ProducerId(producer), epoch),
                    None,
                )
            });
            entry.add_data_batch(earlier, epoch, ProducerBatchMetadata {
                last_sequence: sequence,
                last_offset: Offset(offset),
                offset_delta: 0,
                timestamp: 0,
            });
        }
        Self(state)
    }

    /// The first offset of the retained batch that a single-record client
    /// batch at `epoch` and `sequence` repeats, or `None` when it repeats
    /// none. This is the writer's lookup, `duplicate_of` in
    /// `partition_writer/append.rs`, which answers a repeat with that offset
    /// and appends nothing.
    pub fn duplicate_of(&self, epoch: i16, sequence: i32) -> Option<LogOffset> {
        let (entry, earlier) = self.0.as_ref()?;
        entry
            .find_duplicate_batch(earlier.iter().flatten().copied(), epoch, (sequence, 0))
            .map(|retained| retained.base_offset().0)
    }

    /// The tracker entry that a promotion installs from this state:
    /// `Partition::install_local_leadership` hands `Log::recovered_producers`
    /// to `ProducerState::rebuild_from_snapshot`, which projects it with
    /// `entries_from_snapshot`.
    pub fn tracked_entry(&self) -> Option<ProducerEntry> {
        let (entry, earlier) = self.0.clone()?;
        entries_from_snapshot(vec![RecoveredProducer {
            entry,
            earlier: earlier.map(Vec::from).unwrap_or_default(),
        }])
        .remove(&entry.producer_id)
    }
}
