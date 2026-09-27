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

/// Admit exactly a marker whose body is the two-field
/// `{records_lost, loss_generation}` shape with a positive count and a
/// generation strictly newer than the last accepted one.
#[ensures(match result {
    AuditLossMarkerAdmission::Admit { generation: admitted } => header_matches
        && field_count@ == 2
        && count@ > 0
        && generation == Some(admitted)
        && admitted@ > previous_generation@,
    AuditLossMarkerAdmission::Reject => !(header_matches && field_count@ == 2 && count@ > 0
        && match generation {
            Some(generation) => generation@ > previous_generation@,
            None => false,
        }),
})]
#[must_use]
pub fn audit_loss_marker_admission(
    header_matches: bool,
    field_count: u64,
    count: u64,
    generation: Option<u64>,
    previous_generation: u64,
) -> AuditLossMarkerAdmission {
    match generation {
        Some(generation)
            if header_matches
                && field_count == 2
                && count > 0
                && generation > previous_generation =>
        {
            AuditLossMarkerAdmission::Admit { generation }
        }
        _ => AuditLossMarkerAdmission::Reject,
    }
}

/// Settle the pending losses `state` against a durable marker that reports
/// `batch`, at the writer's commit and at open-time reconciliation alike.
///
/// A marker settles only its own generation, and only the count it reports.
/// Losses that `AuditHandle::emit` added after the writer's snapshot stay
/// pending, and move to the next generation. The host only ever moves a
/// generation forward, so no marker already in the spool names that one: no
/// later reconciliation settles them a second time, and the next marker names
/// a generation newer than the last, as [`audit_loss_marker_admission`]
/// requires. A marker of another generation settles nothing.
///
/// A batch larger than the pending count settles it to zero. The host never
/// produces one -- a batch is a snapshot of the pending count, which only grows
/// within a generation until settled -- so that case is clamped, not
/// conserved. At `u64::MAX` the generation saturates rather than bumps; that
/// takes 2^64 settlements and is the host's to rule out.
#[ensures(state.generation != batch.generation
    ==> result.generation == state.generation && result.count == state.count)]
#[ensures(state.generation == batch.generation && batch.count@ <= state.count@
    ==> result.count@ + batch.count@ == state.count@)]
#[ensures(state.generation == batch.generation && batch.count@ > state.count@
    ==> result.count@ == 0)]
#[ensures(state.generation == batch.generation
    ==> (result.generation != state.generation)
        == (result.count@ > 0 && state.generation@ < u64::MAX@))]
#[ensures(result.generation != state.generation
    ==> result.generation@ == state.generation@ + 1)]
#[must_use]
pub fn settle_loss_batch(state: AuditLosses, batch: AuditLosses) -> AuditLosses {
    if state.generation != batch.generation {
        return state;
    }
    if batch.count >= state.count {
        return AuditLosses {
            generation: state.generation,
            count: 0,
        };
    }
    AuditLosses {
        generation: if state.generation < u64::MAX {
            state.generation + 1
        } else {
            state.generation
        },
        count: state.count - batch.count,
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
        check!(
            spool_append_decision(0, 0, 0, u64::MAX - 1, u64::MAX)
                == SpoolAppendDecision {
                    accepted: true,
                    new_bytes: 0,
                    sync: true,
                    next_unsynced: 0,
                }
        );
    }

    #[test]
    fn loss_settlement_keeps_later_losses_in_a_fresh_generation() {
        let losses = |generation, count| AuditLosses { generation, count };
        // (case, pending, marker's batch, pending after settlement)
        for (case, state, batch, expected) in [
            (
                "the marker reports every pending loss",
                losses(4, 2),
                losses(4, 2),
                losses(4, 0),
            ),
            (
                "a loss emitted after the snapshot moves to a fresh generation",
                losses(4, 3),
                losses(4, 2),
                losses(5, 1),
            ),
            (
                "another generation's marker settles nothing",
                losses(5, 1),
                losses(4, 2),
                losses(5, 1),
            ),
            (
                "an over-reporting batch clamps to zero",
                losses(4, 1),
                losses(4, 2),
                losses(4, 0),
            ),
            (
                "the last generation saturates",
                losses(u64::MAX, 3),
                losses(u64::MAX, 1),
                losses(u64::MAX, 2),
            ),
        ] {
            check!(settle_loss_batch(state, batch) == expected, "{case}");
        }
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
        use AuditLossMarkerAdmission::{Admit, Reject};

        // (header matches, field count, count, generation, previous, expected)
        for (header, fields, count, generation, previous, expected) in [
            (true, 2, 3, Some(2), 1, Admit { generation: 2 }),
            (true, 2, 3, Some(1), 0, Admit { generation: 1 }),
            // A one-field body without a generation is not a marker.
            (true, 1, 3, None, 0, Reject),
            (true, 2, 3, None, 0, Reject),
            (true, 3, 3, Some(2), 1, Reject),
            (false, 2, 3, Some(2), 1, Reject),
            (true, 2, 0, Some(2), 1, Reject),
            (true, 2, 3, Some(1), 1, Reject),
            (true, 2, 3, Some(u64::MAX), u64::MAX, Reject),
        ] {
            check!(
                audit_loss_marker_admission(header, fields, count, generation, previous)
                    == expected
            );
        }
    }
}
