//! Pure admission and sync-cadence arithmetic for the audit spool.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// State transition for one attempted audit-spool append.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct SpoolAppendDecision {
    pub accepted: bool,
    pub new_bytes: u64,
    pub sync: bool,
    pub next_unsynced: u64,
}

/// Admission result for a signed audit checkpoint at the current chain head.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AuditCheckpointAdmission {
    Admit,
    RejectSignature,
    RejectHead,
    RejectSequence,
}

/// Admission result for a records-lost marker.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AuditLossMarkerAdmission {
    /// The marker is accepted and `generation` becomes the last accepted one.
    Admit {
        generation: u64,
    },
    Reject,
}

/// Fail-open audit losses: the pending count `PendingLosses` holds, or the
/// batch a durable records-lost marker reports. `generation` is the
/// `loss_generation` a marker for these losses names.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash))]
pub struct AuditLosses {
    pub generation: u64,
    pub count: u64,
}

mod spool_append_decision;
pub use spool_append_decision::{
    audit_checkpoint_admission, audit_loss_marker_admission, settle_loss_batch,
    spool_append_decision,
};

#[cfg(test)]
mod tests;
