//! Pure admission and sync-cadence arithmetic for the audit spool.

use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// State transition for one attempted audit-spool append.
    pub struct SpoolAppendDecision {
        pub accepted: bool,
        pub new_bytes: u64,
        pub sync: bool,
        pub next_unsynced: u64,
    }
}

model_types! {
    @derives (derive(DeepModel))
        (derive(Debug, Clone, Copy, PartialEq, Eq));
    /// Admission result for a signed audit checkpoint at the current chain head.
    pub enum AuditCheckpointAdmission {
        Admit,
        RejectSignature,
        RejectHead,
        RejectSequence,
    }

    /// Admission result for a records-lost marker.
    pub enum AuditLossMarkerAdmission {
        /// The marker is accepted and `generation` becomes the last accepted one.
        Admit {
            generation: u64,
        },
        Reject,
    }
}

model_types! {
    @derives (derive(std::clone::Clone, Copy, DeepModel))
        (derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash));
    /// Fail-open audit losses: the pending count `PendingLosses` holds, or the
    /// batch a durable records-lost marker reports. `generation` is the
    /// `loss_generation` a marker for these losses names.
    pub struct AuditLosses {
        pub generation: u64,
        pub count: u64,
    }
}

mod spool_append_decision;
pub use spool_append_decision::{
    audit_checkpoint_admission, audit_loss_marker_admission, settle_loss_batch,
    spool_append_decision,
};

#[cfg(test)]
mod tests;
