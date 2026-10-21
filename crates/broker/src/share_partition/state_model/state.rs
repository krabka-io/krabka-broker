//! The fingerprinted state that the search enumerates and the actions that
//! move between two states.
//!
//! The two types sit apart from the transitions so that a reader can see the
//! whole search space, which is the real `AcquisitionState`, the state it last
//! wrote, and a small envelope, on one screen.

use {
    crate::share_partition::state::{AckType, AcquisitionState},
    krabka_log::Offset,
};

/// The fingerprinted model state. It holds the REAL machine plus the small
/// finite clock and the produced-record high-watermark.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct ShareState {
    pub(super) sm: AcquisitionState,
    pub(super) clock: u8,
    pub(super) hwm: Offset,
    /// The log's own start offset (retention / `DeleteRecords`), tracked apart
    /// from `sm.start_offset` (the SPSO). It only ever moves forward, and
    /// `AdvanceLogStart` is the one action that moves it, calling
    /// `sm.advance_past_log_start` with the new value.
    pub(super) log_start: Offset,
    /// What a reload reads back: `sm` as of its last `WriteShareGroupState`,
    /// already round-tripped through `to_persist_batches` and `load_from`.
    /// A transition that leaves `sm` dirty writes it at once, as the
    /// acknowledgement path, the fetch handler and the lock sweeper do; one
    /// that leaves it clean, such as an acquisition, writes nothing.
    pub(super) durable: AcquisitionState,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum ShareAction {
    /// Append one record to the log (raise the produced high-watermark).
    Produce,
    /// Leader pulls produced-but-unmaterialized records into the window.
    Materialize,
    /// `member` acquires up to `max_records` Available records.
    Acquire { member: u8, max_records: i32 },
    /// `member` acknowledges `[first, last]` it holds.
    Acknowledge {
        member: u8,
        first: Offset,
        last: Offset,
        ack: AckType,
    },
    /// `member` renews, that is, extends, the lock on `[first, last]` it holds.
    Renew {
        member: u8,
        first: Offset,
        last: Offset,
    },
    /// KFC-1: hold `[first, last]` back because its delivery time has not
    /// arrived.
    Defer { first: Offset, last: Offset },
    /// KFC-1: drop the whole deferral, as an acquire pass does before it
    /// re-derives one from the log and the clock.
    PromoteDeferred,
    /// Sweep expired acquisition locks back to Available.
    ExpireLocks,
    /// Advance the logical clock by one lock-duration.
    Tick,
    /// Leader failover: the new leader loads the last written state. Acquired
    /// drops to Available, the locks are lost, and a change that was never
    /// written, such as an acquisition's delivery count, is lost with them.
    Reload,
    /// The log start offset (retention / `DeleteRecords`) moves to
    /// `new_start`, past the current SPSO. Archives `Available`/`Deferred`
    /// records below it and advances the SPSO, per
    /// `AcquisitionState::advance_past_log_start`.
    AdvanceLogStart { new_start: Offset },
}
