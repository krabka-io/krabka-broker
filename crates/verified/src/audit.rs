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

/// Admission result for the two supported records-lost marker shapes.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AuditLossMarkerAdmission {
    AdmitLegacy,
    AdmitPersisted,
    Reject,
}

/// Bind a verified checkpoint to the exact nonempty chain position and head.
#[ensures((result == AuditCheckpointAdmission::Admit) == (
    signature_valid && head_matches && expected_seq@ > 0
        && checkpoint_seq_high@ + 1 == expected_seq@))]
#[ensures((result == AuditCheckpointAdmission::RejectSignature) == !signature_valid)]
#[ensures((result == AuditCheckpointAdmission::RejectHead)
    == (signature_valid && !head_matches))]
#[ensures((result == AuditCheckpointAdmission::RejectSequence) == (
    signature_valid && head_matches
        && (expected_seq@ == 0 || checkpoint_seq_high@ + 1 != expected_seq@)))]
#[must_use]
pub fn audit_checkpoint_admission(
    signature_valid: bool,
    head_matches: bool,
    expected_seq: u64,
    checkpoint_seq_high: u64,
) -> AuditCheckpointAdmission {
    if !signature_valid {
        AuditCheckpointAdmission::RejectSignature
    } else if !head_matches {
        AuditCheckpointAdmission::RejectHead
    } else if expected_seq == 0 || checkpoint_seq_high != expected_seq - 1 {
        AuditCheckpointAdmission::RejectSequence
    } else {
        AuditCheckpointAdmission::Admit
    }
}

/// Admit exactly a positive legacy marker or a positive, strictly newer
/// persisted-generation marker.
#[ensures((result == AuditLossMarkerAdmission::AdmitLegacy) == (
    header_matches && field_count@ == 1 && count@ > 0 && !generation_present))]
#[ensures((result == AuditLossMarkerAdmission::AdmitPersisted) == (
    header_matches && field_count@ == 2 && count@ > 0 && generation_present
        && generation@ > previous_generation@))]
#[ensures((result == AuditLossMarkerAdmission::Reject) == !(
    header_matches && (
        (field_count@ == 1 && count@ > 0 && !generation_present)
        || (field_count@ == 2 && count@ > 0 && generation_present
            && generation@ > previous_generation@))))]
#[must_use]
pub fn audit_loss_marker_admission(
    header_matches: bool,
    field_count: u64,
    count: u64,
    generation_present: bool,
    generation: u64,
    previous_generation: u64,
) -> AuditLossMarkerAdmission {
    if !header_matches || count == 0 {
        AuditLossMarkerAdmission::Reject
    } else if field_count == 1 && !generation_present {
        AuditLossMarkerAdmission::AdmitLegacy
    } else if field_count == 2 && generation_present && generation > previous_generation {
        AuditLossMarkerAdmission::AdmitPersisted
    } else {
        AuditLossMarkerAdmission::Reject
    }
}

/// Admit one frame and advance the successful-append sync cadence.
#[requires(sync_every@ > 0)]
#[requires(unsynced@ < sync_every@)]
#[ensures(result.accepted
    == (current_bytes@ <= max_bytes@ && frame_bytes@ <= max_bytes@ - current_bytes@))]
#[ensures(result.accepted ==> result.new_bytes@ == current_bytes@ + frame_bytes@)]
#[ensures(result.accepted ==> result.new_bytes@ <= max_bytes@)]
#[ensures(!result.accepted ==> result.new_bytes@ == current_bytes@)]
#[ensures(result.sync
    == (result.accepted && unsynced@ + 1 >= sync_every@))]
#[ensures(result.next_unsynced@ < sync_every@)]
#[ensures(result.next_unsynced@ == if result.accepted {
    if unsynced@ + 1 >= sync_every@ { 0 } else { unsynced@ + 1 }
} else {
    unsynced@
})]
#[must_use]
pub fn spool_append_decision(
    current_bytes: u64,
    frame_bytes: u64,
    max_bytes: u64,
    unsynced: u64,
    sync_every: u64,
) -> SpoolAppendDecision {
    if current_bytes > max_bytes || frame_bytes > max_bytes - current_bytes {
        return SpoolAppendDecision {
            accepted: false,
            new_bytes: current_bytes,
            sync: false,
            next_unsynced: unsynced,
        };
    }

    let next_unsynced = unsynced + 1;
    let sync = next_unsynced >= sync_every;
    SpoolAppendDecision {
        accepted: true,
        new_bytes: current_bytes + frame_bytes,
        sync,
        next_unsynced: if sync { 0 } else { next_unsynced },
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn append_boundaries_and_cadence_do_not_wrap() {
        check!(
            spool_append_decision(u64::MAX - 1, 1, u64::MAX, 1, 3)
                == SpoolAppendDecision {
                    accepted: true,
                    new_bytes: u64::MAX,
                    sync: false,
                    next_unsynced: 2,
                }
        );
        check!(
            spool_append_decision(u64::MAX, 1, u64::MAX, 2, 3)
                == SpoolAppendDecision {
                    accepted: false,
                    new_bytes: u64::MAX,
                    sync: false,
                    next_unsynced: 2,
                }
        );
        let at_cadence = spool_append_decision(0, 0, 0, u64::MAX - 1, u64::MAX);
        check!(at_cadence.sync && at_cadence.next_unsynced == 0);
    }

    #[test]
    fn checkpoint_requires_an_exact_nonempty_position() {
        use AuditCheckpointAdmission::{Admit, RejectHead, RejectSequence, RejectSignature};

        check!(audit_checkpoint_admission(true, true, 3, 2) == Admit);
        check!(audit_checkpoint_admission(false, true, 3, 2) == RejectSignature);
        check!(audit_checkpoint_admission(true, false, 3, 2) == RejectHead);
        check!(audit_checkpoint_admission(true, true, 0, 0) == RejectSequence);
        check!(audit_checkpoint_admission(true, true, u64::MAX, u64::MAX) == RejectSequence);
    }

    #[test]
    fn loss_marker_shape_count_and_generation_are_exact() {
        use AuditLossMarkerAdmission::{AdmitLegacy, AdmitPersisted, Reject};

        check!(audit_loss_marker_admission(true, 1, 3, false, 0, 0) == AdmitLegacy);
        check!(audit_loss_marker_admission(true, 2, 3, true, 2, 1) == AdmitPersisted);
        check!(audit_loss_marker_admission(true, 2, 0, true, 2, 1) == Reject);
        check!(audit_loss_marker_admission(true, 2, 3, true, 1, 1) == Reject);
        check!(audit_loss_marker_admission(true, 2, 3, true, u64::MAX, u64::MAX) == Reject);
    }
}
