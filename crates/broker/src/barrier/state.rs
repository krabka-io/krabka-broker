//! The in-memory state of one barrier group, and the pure decisions over it.
//!
//! The coordinator holds one [`GroupEntry`] per group behind a mutex. Recovery
//! folds the `__barrier_state` records of a partition into these entries with
//! [`apply_record`], and an injection turns its frozen target set and its
//! collected offsets into a cut with [`build_cut`].
//!
//! Every function here is pure, so a unit test drives it without a log, a
//! metadata image, or a partition.

mod cut;
mod group;
mod record;
mod retention;
mod schedule;
#[cfg(test)]
mod test_support;

pub(crate) use self::{
    cut::{TargetPartition, build_cut, expand_targets},
    group::{GroupEntry, GroupSpec, PendingInjection, next_epoch},
    record::{StateRecord, apply_record},
    retention::expired_cut_epochs,
    schedule::{is_due, schedule_next},
};

/// The epoch of a group that has never injected.
///
/// The first injection allocates epoch 1.
pub(crate) const NO_EPOCH_YET: i64 = 0;
