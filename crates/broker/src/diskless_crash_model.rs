//! Diskless WAL crash-restart model for partial durability windows.
//!
//! This model is small on purpose. It composes the [crash-window recovery] rules
//! with the [distributed WAL quorum] and stateless appenders. The
//! [diskless WAL design] defines both slices.
//!
//! `KRaft` can reserve offsets before the bytes fsync. An object PUT can come
//! before the index commit. An fsync can tear the active tail. Any WAL member
//! can start a reservation. A trim must stop at the committed index frontier.
//!
//! DRIVEN: three production kernels decide the steps that carry the
//! properties. `krabka_verified::consensus::majority_watermark` computes the
//! quorum-durable frontier (`wal_acked`) from the live WAL members, as
//! `WalShardEngine::record_durable_offset` does over voter-reported durable
//! offsets. `krabka_verified::offset_allocator::wal_reservation_frontier`
//! folds the pending reservation chain and
//! `krabka_verified::offset_allocator::reserve_offsets` places the next one,
//! as the controller's `V1PartitionOffsetAdvance` submit path does, so
//! `reservations_gap_free_and_unique` checks what those kernels produce under
//! every interleaving of two appenders and the crash windows.
//! `krabka_verified::diskless::diskless_trim_decision` places every trim, as
//! the flusher does, so `trim_at_committed_index_frontier` checks its output
//! against the index and quorum frontiers.
//!
//! MODELED: the WAL members, the fsync, the object PUT, and the index commit
//! are counters, not logs or objects. Every reservation the model makes stays
//! pending in the controller (the image frontier is 0), so the reservation
//! frontier is the fold over all of them. The flusher's trim runs with no
//! safety lag against `wal_acked` as the high watermark. At most one WAL
//! member is lost. The idempotent-producer dedup rebuild is not modelled:
//! the model has no producer sequences to rebuild, so any property about it
//! would hold by construction; `diskless::recovery`'s unit tests rebuild the
//! dedup state from a real recovered log instead.
//!
//! [crash-window recovery]: ../docs/diskless-wal-design.md#crash-window-recovery
//! [distributed WAL quorum]: ../docs/diskless-wal-design.md#distributed-wal-quorum-and-stateless-appenders
//! [diskless WAL design]: ../docs/diskless-wal-design.md

use stateright::{Checker, Model, Property};

const MAX_OFFSET: i64 = 2;

const APPENDERS: usize = 2;

const MAX_DEPTH: usize = 24;

const TARGET_STATE_COUNT: usize = 100_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 6_341;

const WITNESS_KRAFT_FSYNC_GAP: u8 = 1 << 0;

const WITNESS_PUT_BEFORE_INDEX: u8 = 1 << 1;

const WITNESS_MID_FSYNC: u8 = 1 << 2;

const WITNESS_TRIM_AT_INDEX: u8 = 1 << 3;

const WITNESS_MINORITY_WAL_LOSS: u8 = 1 << 4;

const WITNESS_STATELESS_APPEND: u8 = 1 << 5;

const WITNESS_SEQUENCER_HANDOFF: u8 = 1 << 6;

const WAL_NODES: usize = 3;

const WAL_MAJORITY: usize = 2;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CrashState {
    kraft_next: i64,
    log_end: i64,
    wal_nodes: [i64; WAL_NODES],
    wal_lost: [bool; WAL_NODES],
    wal_acked: i64,
    /// Last quorum-durable frontier observed before a sequencer handoff.
    /// `wal_acked` may never fall below it.
    handoff_wal_acked: i64,
    /// Client-visible frontier. Unlike `wal_acked`, this may temporarily
    /// regress while a new sequencer re-derives its view from durable media.
    advertised_hwm: i64,
    sequencer_epoch: u8,
    object_frontier: i64,
    index_frontier: i64,
    trimmed: i64,
    /// The log end the last fsync acknowledged to the producer.
    producer_committed: i64,
    reservations: Vec<(i64, i64)>,
    appenders_seen: u8,
    witnesses: u8,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Act {
    ReserveVia(usize),
    FsyncAppend,
    CrashBeforeFsync,
    CrashMidFsync,
    PutObject,
    CommitIndex,
    Trim,
    LoseWalNode(usize),
    SequencerHandoff,
}

#[derive(Clone, Debug)]
struct CrashModel;

#[path = "diskless_crash_model/checker.rs"]
mod checker;

#[path = "diskless_crash_model/helpers.rs"]
mod helpers;
use helpers::{fsync_quorum, reserve_via_controller, surviving_wal_frontier};

#[path = "diskless_crash_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "diskless_crash_model/tests.rs"]
mod tests;
