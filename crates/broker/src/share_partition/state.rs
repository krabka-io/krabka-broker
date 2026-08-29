//! In-memory share-partition acquisition state machine (KIP-932).
//!
//! This is the pure core that the share-partition leader drives. It owns no
//! I/O. It tracks the per-offset delivery state over the live window
//! `[start_offset, end_offset)`, that is SPSO to SPEO, as a list of contiguous
//! `InFlightBatch` runs. It answers `acquire`, `acknowledge`, and
//! `expire_locks` queries. Bytes, logs, locks, and persistence live elsewhere.
//!
//! Delivery-state codes match Kafka's on-the-wire values: `DS_AVAILABLE=0`,
//! `DS_ACQUIRED=1`, `DS_ACKNOWLEDGED=2`, and `DS_ARCHIVED=4`. `Acquired` is
//! transient, and the machine persists it back as `Available(0)`. `Deferred`
//! is KFC-1's refinement of `Available` and persists as `Available(0)` too, so
//! scheduled delivery adds no code to the wire encoding.
//!
//! This file holds the types and the constructor. The transitions sit in one
//! module per concern: `acquire` grows the window and hands records out,
//! `acknowledge` applies a consumer's verdict, `locks` runs the acquisition
//! lock lifetime, `deferral` implements KFC-1 scheduled delivery, `window`
//! keeps the run list in shape, and `persistence` maps to and from the share
//! coordinator's records.

use std::time::Instant;

use krabka_log::Offset;

mod acknowledge;
mod acquire;
mod deferral;
mod locks;
mod persistence;
mod window;

#[cfg(test)]
mod test_support;

/// Saturating `i64 -> i32` conversion for record counts. Offset ranges do not
/// overflow `i32` in practice, but the counter type is `i32` to match the
/// persister.
fn clamp_i32(n: i64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

pub const DS_AVAILABLE: i8 = 0;
pub const DS_ACQUIRED: i8 = 1;
pub const DS_ACKNOWLEDGED: i8 = 2;
pub const DS_ARCHIVED: i8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordState {
    Available,
    Acquired,
    Acknowledged,
    Archived,
    /// KFC-1: `Available`, but the record's delivery time has not arrived.
    ///
    /// It has no delivery-state code of its own. The share coordinator stores
    /// it as `Available`, so a coordinator that reloads after a leader change
    /// re-derives the deferral against its own clock instead of inheriting the
    /// old leader's reading of a clock it cannot check.
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckType {
    Gap,
    Accept,
    Release,
    Reject,
}

impl AckType {
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Gap),
            1 => Some(Self::Accept),
            2 => Some(Self::Release),
            3 => Some(Self::Reject),
            _ => None,
        }
    }
}

/// A contiguous run of offsets acquired by a single `acquire` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange {
    pub first: Offset,
    pub last: Offset,
    pub delivery_count: i16,
}

/// One contiguous run of offsets `[first_offset, last_offset]` with the same
/// delivery state and delivery count. The lock fields have a meaning only
/// while `state == RecordState::Acquired`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InFlightBatch {
    first_offset: Offset,
    last_offset: Offset,
    state: RecordState,
    delivery_count: i16,
    acquired_by: Option<String>,
    lock_deadline: Option<Instant>,
}

impl InFlightBatch {
    fn len(&self) -> i64 {
        self.last_offset.0 - self.first_offset.0 + 1
    }
}

/// The in-memory acquisition state for one share partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AcquisitionState {
    /// Share-partition start offset (SPSO): the lowest offset that is not yet
    /// terminally acknowledged or archived.
    pub start_offset: Offset,
    /// Share-partition end offset (SPEO): one past the highest materialized
    /// offset. It equals `start_offset` when the window is empty.
    pub end_offset: Offset,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub dirty: bool,
    /// Count of offsets that have reached a terminal state, Acknowledged or
    /// Archived, since `new` or `load_from`. This is the persister's
    /// `delivery_complete_count`.
    delivery_complete_count: i32,
    batches: Vec<InFlightBatch>,
}

impl AcquisitionState {
    #[must_use]
    pub fn new(start_offset: Offset) -> Self {
        Self {
            start_offset,
            end_offset: start_offset,
            state_epoch: 0,
            leader_epoch: 0,
            dirty: false,
            delivery_complete_count: 0,
            batches: Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "state_model.rs"]
mod state_model;
