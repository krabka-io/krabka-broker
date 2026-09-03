//! `Log`: a sorted collection of `Segment`s with append, read, and truncate.
//!
//! This file holds the `Log` type itself. Every operation over it lives in a
//! submodule named for the concern it covers, and each of those submodules
//! adds its own `impl Log` block: [`self::open`] for opening a directory and
//! rebuilding recovered state, [`self::append`] and [`self::verbatim`] for the
//! two write paths, [`self::read`] for the three read paths, and so on. The
//! private fields below stay in this module so that every one of those
//! submodules, as a descendant, can reach them.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use krabka_ids::{Offset, ProducerId};

use crate::{
    config::LogConfig, io::LogIo, leader_epoch_checkpoint::LeaderEpochCheckpoint,
    producer_snapshot::ProducerSnapshotEntry, segment::Segment, stamp_index::StampIndex,
    stamp_source::StampSource, txn_index::TxnIndex,
};

mod append;
mod compaction;
mod control;
mod delivery;
mod open;
mod read;
mod stamp;
mod state;
mod sync;
#[cfg(test)]
mod test_support;
mod tick;
mod tiering;
mod timestamp;
mod transaction;
mod truncate;
mod verbatim;

pub use self::{
    compaction::CompactionContext,
    control::BARRIER_CONTROL_TYPE,
    read::{RawRead, ReadOutput},
    tiering::SegmentExport,
    verbatim::VerbatimBatch,
};

crate::sendfile_cfg! {
    pub use self::read::RawReadDesc;
}

/// A Kafka-format log: a sorted collection of [`Segment`]s plus a single
/// active segment that accepts appends.
///
/// `Log` has one writer and many concurrent readers. Mutation takes
/// `&mut self`; `read`, `log_start_offset`, and the other read methods take
/// `&self`. Construct one with [`Log::open`].
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    config: std::sync::Arc<std::sync::RwLock<LogConfig>>,
    io: std::sync::Arc<dyn LogIo>,
    segments: Vec<Segment>,
    active: Option<Segment>,
    dir_sync_needed: bool,
    /// The global log start (Kafka's `logStartOffset`): the first offset any
    /// reader may ask for, wherever the records for it live.
    ///
    /// It is a pointer of its own rather than a value derived from the
    /// segments on disk, because KIP-405 gives a tiered partition two floors
    /// and only one of them follows local files. `DeleteRecords`, ordinary
    /// retention, a trim and a reset move this one; dropping a local segment
    /// whose copy is in the remote tier does not, and that is what leaves the
    /// band `[log_start_offset(), local_log_start_offset())` for the remote
    /// read path to serve. [`Log::local_log_start_offset`] is the second
    /// floor.
    start_offset: Offset,

    /// Last-Stable-Offset: the offset before the first record of any
    /// in-flight transaction. Defaults to `log_end_offset()` when no
    /// transactions are in flight.
    lso: Offset,

    /// In-flight transactions: `producer_id` → first offset of this
    /// producer's currently-open txn. The log clears the entry when it
    /// applies a commit or abort marker for that `producer_id`.
    pending: HashMap<ProducerId, Offset>,

    /// Exact data-batch ranges for each in-flight transaction. A commit
    /// marker stamps only these ranges, so interleaved transactions and
    /// ordinary records never inherit another transaction's commit stamp.
    pending_stamp_ranges: HashMap<ProducerId, Vec<(Offset, Offset)>>,

    /// Last transaction-coordinator epoch observed in a durable commit or
    /// abort marker for each producer. The marker value carries this field,
    /// so log recovery can rebuild it without a separate sidecar.
    coordinator_epochs: HashMap<ProducerId, i32>,

    /// Producer sequence, epoch, and transaction metadata persisted in
    /// Kafka-compatible `.snapshot` files at segment boundaries.
    producer_state: HashMap<ProducerId, ProducerSnapshotEntry>,

    /// Active segment's `TxnIndex`. The log reopens it on segment roll.
    active_txn_index: TxnIndex,

    /// Immutable transaction indexes keyed by sealed segment base offset.
    sealed_txn_indexes: BTreeMap<Offset, TxnIndex>,

    /// Injected source of the additional internal stamp coordinate. `None`
    /// is the default and means this partition stamps nothing. Behavior and
    /// all wire-exact bytes stay exactly as they are without a stamp.
    /// [`Log::set_stamp_source`] sets this field. The broker shares one tenant
    /// source across all hosted partitions when internal stamping is enabled.
    stamp_source: Option<std::sync::Arc<dyn StampSource>>,

    /// Open `.stampindex` sidecars keyed by segment base offset. Transactional
    /// data can commit after its segment rolls, so the log keeps sealed
    /// indexes available instead of retaining only the active one.
    stamp_indexes: BTreeMap<Offset, StampIndex>,

    /// Per-partition leader-epoch checkpoint. All segments share it, and
    /// epoch history accumulates over the log's lifetime.
    epoch_checkpoint: LeaderEpochCheckpoint,

    /// External next-offset authority used by diskless recovery. The broker
    /// sets this after it reads the committed `KRaft` frontier. Caller-supplied
    /// append-at bases must then equal
    /// `max(log_end_offset, reconciled_frontier)`.
    reconciled_frontier: Offset,

    /// Cached deliver-at-time watermark: the first offset whose activation
    /// time has not passed. It only ever moves forward, and it is derived
    /// from the batch timestamps on disk, so nothing persists it.
    /// [`Log::advance_delivery_watermark`] recomputes it.
    delivery_watermark: Offset,

    /// Activation time of the batch that stopped the last activation walk.
    /// While that instant is still in the future, no walk can get past that
    /// batch, so a repeat advance answers from this field and does no I/O.
    /// `None` means the last walk found nothing waiting, or that a truncation
    /// or a compaction may have removed the batch it named.
    delivery_pending_ms: Option<i64>,
}
