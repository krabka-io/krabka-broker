//! Controller recovery replay-bound decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReplayRecordDecision {
    Apply(i64),
    Skip,
}

#[ensures(match result {
    ReplayRecordDecision::Apply(offset) => record_delta@ >= 0
        && batch_base@ < end@
        && control_batch == replay_control
        && batch_base@ <= i64::MAX@ - record_delta@
        && offset@ == batch_base@ + record_delta@
        && from@ <= offset@
        && offset@ < end@,
    ReplayRecordDecision::Skip => !(record_delta@ >= 0
        && batch_base@ < end@
        && control_batch == replay_control
        && batch_base@ <= i64::MAX@ - record_delta@
        && from@ <= batch_base@ + record_delta@
        && batch_base@ + record_delta@ < end@),
})]
#[must_use]
pub fn replay_record_decision(
    batch_base: i64,
    record_delta: i32,
    from: i64,
    end: i64,
    control_batch: bool,
    replay_control: bool,
) -> ReplayRecordDecision {
    if record_delta < 0 || batch_base >= end || control_batch != replay_control {
        return ReplayRecordDecision::Skip;
    }
    let delta = i64::from(record_delta);
    if batch_base > i64::MAX - delta {
        return ReplayRecordDecision::Skip;
    }
    let offset = batch_base + delta;
    if offset < from || offset >= end {
        ReplayRecordDecision::Skip
    } else {
        ReplayRecordDecision::Apply(offset)
    }
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReplayCursorDecision {
    Advance(i64),
    Stop,
}

/// The keyed barrier-state record that recovery is folding.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierRecoveryRecordKind {
    Group,
    InjectionStart,
    Cut,
}

/// The only state mutation one ordered barrier record may perform.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierRecoveryFoldAction {
    DefineGroup,
    RemoveGroup,
    SetPending,
    KeepPending,
    ClearPending,
    UpsertCut { retire_pending: bool },
    RemoveCut,
}

/// Whether recovery may close an interrupted barrier injection.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierRecoveryFinalizeDecision {
    NoPending,
    MalformedPending,
    UnknownCoordinator,
    FencedCoordinator,
    FinalizePartial,
}

/// Select the exact mutation for one decoded barrier-state record.
///
/// The host applies this result to the entry named by the decoded group key,
/// in log order. A consumed epoch suppresses a later retry of its
/// injection-start, and only a matching epoch may clear the pending injection.
#[ensures(match result {
    BarrierRecoveryFoldAction::DefineGroup => {
        kind == BarrierRecoveryRecordKind::Group && value_present
    }
    BarrierRecoveryFoldAction::RemoveGroup => {
        kind == BarrierRecoveryRecordKind::Group && !value_present
    }
    BarrierRecoveryFoldAction::SetPending => {
            kind == BarrierRecoveryRecordKind::InjectionStart
            && value_present
            && !epoch_already_consumed
    }
    BarrierRecoveryFoldAction::KeepPending => {
        kind == BarrierRecoveryRecordKind::InjectionStart
            && ((value_present && epoch_already_consumed)
                || (!value_present && pending_epoch != Some(record_epoch)))
    }
    BarrierRecoveryFoldAction::ClearPending => {
        kind == BarrierRecoveryRecordKind::InjectionStart
            && !value_present
            && pending_epoch == Some(record_epoch)
    }
    BarrierRecoveryFoldAction::UpsertCut { retire_pending } => {
        kind == BarrierRecoveryRecordKind::Cut
            && value_present
            && retire_pending == (pending_epoch == Some(record_epoch))
    }
    BarrierRecoveryFoldAction::RemoveCut => {
        kind == BarrierRecoveryRecordKind::Cut && !value_present
    }
})]
#[must_use]
pub fn barrier_recovery_fold_action(
    kind: BarrierRecoveryRecordKind,
    value_present: bool,
    record_epoch: i64,
    pending_epoch: Option<i64>,
    epoch_already_consumed: bool,
) -> BarrierRecoveryFoldAction {
    match kind {
        BarrierRecoveryRecordKind::Group => {
            if value_present {
                BarrierRecoveryFoldAction::DefineGroup
            } else {
                BarrierRecoveryFoldAction::RemoveGroup
            }
        }
        BarrierRecoveryRecordKind::InjectionStart => {
            if value_present {
                if epoch_already_consumed {
                    BarrierRecoveryFoldAction::KeepPending
                } else {
                    BarrierRecoveryFoldAction::SetPending
                }
            } else if pending_epoch == Some(record_epoch) {
                BarrierRecoveryFoldAction::ClearPending
            } else {
                BarrierRecoveryFoldAction::KeepPending
            }
        }
        BarrierRecoveryRecordKind::Cut => {
            if value_present {
                BarrierRecoveryFoldAction::UpsertCut {
                    retire_pending: pending_epoch == Some(record_epoch),
                }
            } else {
                BarrierRecoveryFoldAction::RemoveCut
            }
        }
    }
}

/// Decide whether an interrupted injection can be finalized conservatively.
///
/// A valid finalization has a current coordinator at or above the frozen
/// coordinator epoch and at least one valid target partition. The recovery
/// adapter supplies no observed marker offsets, so this decision can only
/// authorize a partial cut.
#[ensures((result == BarrierRecoveryFinalizeDecision::FinalizePartial)
    == (has_pending
        && frozen_coordinator_epoch@ >= 0
        && targets_valid
        && match current_coordinator_epoch {
            Some(current) => current@ >= frozen_coordinator_epoch@,
            None => false,
        }))]
#[must_use]
pub fn barrier_recovery_finalize_decision(
    has_pending: bool,
    current_coordinator_epoch: Option<i32>,
    frozen_coordinator_epoch: i32,
    targets_valid: bool,
) -> BarrierRecoveryFinalizeDecision {
    if !has_pending {
        return BarrierRecoveryFinalizeDecision::NoPending;
    }
    if frozen_coordinator_epoch < 0 || !targets_valid {
        return BarrierRecoveryFinalizeDecision::MalformedPending;
    }
    let Some(current) = current_coordinator_epoch else {
        return BarrierRecoveryFinalizeDecision::UnknownCoordinator;
    };
    if current < frozen_coordinator_epoch {
        BarrierRecoveryFinalizeDecision::FencedCoordinator
    } else {
        BarrierRecoveryFinalizeDecision::FinalizePartial
    }
}

#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => next == Some(next_offset)
        && next_offset@ > cursor@,
    ReplayCursorDecision::Stop => match next {
        None => true,
        Some(next_offset) => next_offset@ <= cursor@,
    },
})]
#[must_use]
pub fn replay_cursor_decision(cursor: i64, next: Option<i64>) -> ReplayCursorDecision {
    match next {
        Some(next_offset) if next_offset > cursor => ReplayCursorDecision::Advance(next_offset),
        Some(_) | None => ReplayCursorDecision::Stop,
    }
}

/// Validate one decoded replay batch and compute its exclusive next cursor.
///
/// Compacted logs may contain forward gaps. Overlap, malformed spans, batches
/// outside the captured replay bound, and an unrepresentable successor stop
/// replay without advancing.
#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => match batch {
        Some((base, last_delta)) => cursor@ < end@
            && last_delta@ >= 0
            && base@ >= cursor@
            && next_offset@ == base@ + last_delta@ + 1
            && next_offset@ > cursor@
            && next_offset@ <= end@
            && next_offset@ <= i64::MAX@,
        None => false,
    },
    ReplayCursorDecision::Stop => match batch {
        None => true,
        Some((base, last_delta)) => cursor@ >= end@
            || last_delta@ < 0
            || base@ < cursor@
            || base@ + last_delta@ + 1 > i64::MAX@
            || base@ + last_delta@ + 1 > end@,
    },
})]
#[must_use]
pub fn replay_batch_cursor_decision(
    cursor: i64,
    end: i64,
    batch: Option<(i64, i32)>,
) -> ReplayCursorDecision {
    if cursor >= end {
        return ReplayCursorDecision::Stop;
    }
    let Some((base, last_delta)) = batch else {
        return ReplayCursorDecision::Stop;
    };
    let Some(next) = crate::restore::restore_batch_step(cursor, base, last_delta) else {
        return ReplayCursorDecision::Stop;
    };
    if next > end {
        ReplayCursorDecision::Stop
    } else {
        ReplayCursorDecision::Advance(next)
    }
}

#[ensures(result == (!pending_exists && is_downgrade))]
#[must_use]
pub const fn should_capture_first_downgrade(pending_exists: bool, is_downgrade: bool) -> bool {
    !pending_exists && is_downgrade
}

#[cfg(test)]
mod tests {
    use super::{
        BarrierRecoveryFinalizeDecision, BarrierRecoveryFoldAction, BarrierRecoveryRecordKind,
        ReplayCursorDecision, ReplayRecordDecision, barrier_recovery_finalize_decision,
        barrier_recovery_fold_action, replay_batch_cursor_decision, replay_cursor_decision,
        replay_record_decision, should_capture_first_downgrade,
    };

    #[test]
    fn barrier_fold_preserves_order_and_retires_only_matching_epochs() {
        use BarrierRecoveryFoldAction::{
            ClearPending, KeepPending, RemoveCut, SetPending, UpsertCut,
        };
        use BarrierRecoveryRecordKind::{Cut, InjectionStart};

        assert2::check!(
            barrier_recovery_fold_action(InjectionStart, true, 7, None, false) == SetPending
        );
        assert2::check!(
            barrier_recovery_fold_action(Cut, true, 7, Some(7), false)
                == UpsertCut {
                    retire_pending: true,
                }
        );
        assert2::check!(
            barrier_recovery_fold_action(InjectionStart, true, 7, None, true) == KeepPending
        );
        assert2::check!(
            barrier_recovery_fold_action(InjectionStart, false, 6, Some(7), false) == KeepPending
        );
        assert2::check!(
            barrier_recovery_fold_action(InjectionStart, false, 7, Some(7), false) == ClearPending
        );
        assert2::check!(barrier_recovery_fold_action(Cut, false, 7, Some(9), true) == RemoveCut);
    }

    #[test]
    fn barrier_finalization_is_partial_only_for_a_valid_owned_pending_cut() {
        use BarrierRecoveryFinalizeDecision::{
            FencedCoordinator, FinalizePartial, MalformedPending, NoPending, UnknownCoordinator,
        };

        for (facts, expected) in [
            ((false, Some(3), 3, true), NoPending),
            ((true, Some(3), -1, true), MalformedPending),
            ((true, Some(3), 3, false), MalformedPending),
            ((true, None, 3, true), UnknownCoordinator),
            ((true, Some(2), 3, true), FencedCoordinator),
            ((true, Some(3), 3, true), FinalizePartial),
            ((true, Some(4), 3, true), FinalizePartial),
        ] {
            let (has_pending, current, frozen, targets_valid) = facts;
            assert2::check!(
                barrier_recovery_finalize_decision(has_pending, current, frozen, targets_valid,)
                    == expected
            );
        }
    }

    #[test]
    fn record_replay_is_bounded_and_type_separated() {
        use ReplayRecordDecision::{Apply, Skip};

        assert2::assert!(replay_record_decision(10, 1, 10, 12, false, false) == Apply(11));
        assert2::assert!(replay_record_decision(10, 2, 10, 12, false, false) == Skip);
        assert2::assert!(replay_record_decision(10, -1, 0, 12, false, false) == Skip);
        assert2::assert!(replay_record_decision(i64::MAX, 1, 0, i64::MAX, false, false) == Skip);
        assert2::assert!(replay_record_decision(10, 0, 10, 12, true, false) == Skip);
        assert2::assert!(replay_record_decision(10, 0, 10, 12, true, true) == Apply(10));
    }

    #[test]
    fn cursor_and_downgrade_decisions_are_fail_closed() {
        use ReplayCursorDecision::{Advance, Stop};

        assert2::assert!(replay_cursor_decision(10, Some(11)) == Advance(11));
        assert2::assert!(replay_cursor_decision(10, Some(10)) == Stop);
        assert2::assert!(replay_cursor_decision(10, Some(9)) == Stop);
        assert2::assert!(replay_cursor_decision(10, None) == Stop);
        assert2::assert!(should_capture_first_downgrade(false, true));
        assert2::assert!(!should_capture_first_downgrade(true, true));
    }

    #[test]
    fn batch_cursor_is_bounded_progress_or_stop() {
        use ReplayCursorDecision::{Advance, Stop};

        for (cursor, end, batch, expected) in [
            (10, 20, None, Stop),
            (10, 20, Some((10, 2)), Advance(13)),
            (10, 20, Some((15, 1)), Advance(17)),
            (10, 20, Some((9, 2)), Stop),
            (10, 20, Some((10, -1)), Stop),
            (10, 12, Some((10, 2)), Stop),
            (20, 20, Some((20, 0)), Stop),
            (
                i64::MAX - 1,
                i64::MAX,
                Some((i64::MAX - 1, 0)),
                Advance(i64::MAX),
            ),
            (i64::MAX - 1, i64::MAX, Some((i64::MAX - 1, 1)), Stop),
        ] {
            assert2::assert!(replay_batch_cursor_decision(cursor, end, batch) == expected);
        }
    }
}
