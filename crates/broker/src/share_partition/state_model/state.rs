//! The fingerprinted state that the search enumerates and the actions that
//! move between two states.
//!
//! The two types sit apart from the transitions so that a reader can see the
//! whole search space, which is the real `AcquisitionState` plus a two-field
//! envelope, on one screen.

use krabka_log::Offset;

use crate::share_partition::state::{AckType, AcquisitionState};

/// The fingerprinted model state. It holds the REAL machine plus the small
/// finite clock and the produced-record high-watermark.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct ShareState {
    pub(super) sm: AcquisitionState,
    pub(super) clock: u8,
    pub(super) hwm: Offset,
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
    /// Leader failover: persist and reload. Acquired drops to Available, and
    /// the locks are lost.
    Reload,
}
