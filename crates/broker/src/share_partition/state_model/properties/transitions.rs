use assert2::assert;

use super::*;

impl ShareModel {
    pub(super) fn model_next_state(
        &self,
        last: &<Self as Model>::State,
        action: <Self as Model>::Action,
    ) -> Option<<Self as Model>::State> {
        let mut state = last.clone();
        match action {
            ShareAction::Produce => {
                if state.hwm >= self.max_offset {
                    return None;
                }
                state.hwm += 1;
            }
            ShareAction::Materialize => {
                let before = state.sm.end_offset;
                state.sm.materialize(state.hwm, self.max_inflight);
                if state.sm.end_offset == before {
                    return None; // no-op: nothing materialized
                }
            }
            ShareAction::Acquire {
                member,
                max_records,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                let deferred = deferred_offsets(&state.sm);
                let handed_out = state.sm.acquire(
                    &name,
                    max_records,
                    krabka_log::Offset(i64::MAX),
                    now,
                    LOCK,
                    self.max_attempts,
                );
                for range in &handed_out {
                    for raw in range.first.0..=range.last.0 {
                        assert!(
                            !deferred.contains(&Offset(raw)),
                            "acquire handed out deferred offset {raw}"
                        );
                    }
                }
            }
            ShareAction::Defer { first, last: hi } => {
                state.sm.defer_internal(first, hi);
                if state.sm == last.sm {
                    return None; // nothing in the range was Available
                }
            }
            ShareAction::PromoteDeferred => {
                state.sm.promote_deferred();
                if state.sm == last.sm {
                    return None; // nothing was deferred
                }
            }
            ShareAction::Acknowledge {
                member,
                first,
                last: hi,
                ack,
            } => {
                let name = Self::member_name(member);
                if state
                    .sm
                    .acknowledge(&name, first, hi, ack, self.max_attempts)
                    .is_err()
                {
                    return None; // inapplicable ack: no transition
                }
            }
            ShareAction::Renew {
                member,
                first,
                last: hi,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.renew(&name, first, hi, now, LOCK).is_err() {
                    return None; // inapplicable renew: no transition
                }
            }
            ShareAction::ExpireLocks => {
                let now = self.now(state.clock);
                state.sm.expire_locks(now, self.max_attempts);
            }
            ShareAction::Tick => {
                if state.clock >= self.max_tick {
                    return None;
                }
                state.clock += 1;
            }
            ShareAction::Reload => {
                let deferred = deferred_offsets(&state.sm);
                let window = (state.sm.start_offset, state.sm.end_offset);
                let (start, _, batches) = state.sm.to_persist_batches();
                let mut fresh = AcquisitionState::new(start);
                fresh.load_from(start, state.sm.state_epoch, state.sm.leader_epoch, &batches);
                // KFC-1: `Deferred` persists as `Available`, so the new leader
                // re-derives it from the log and its own clock. The model's
                // clock has not moved, so the same offsets come back deferred,
                // and the round trip must lose none of them.
                for off in &deferred {
                    fresh.defer_internal(*off, *off);
                }
                assert!(
                    (fresh.start_offset, fresh.end_offset) == window,
                    "reload lost part of the window: {window:?} -> {:?}",
                    (fresh.start_offset, fresh.end_offset)
                );
                assert!(
                    deferred_offsets(&fresh) == deferred,
                    "reload lost the deferral: {deferred:?} -> {:?}",
                    deferred_offsets(&fresh)
                );
                state.sm = fresh;
            }
            ShareAction::AdvanceLogStart { new_start } => {
                if new_start <= state.log_start {
                    return None; // log start never moves backward
                }
                state.sm.advance_past_log_start(new_start);
                state.log_start = new_start;
            }
        }
        assert_transition(&last.sm, &state.sm, action);
        Some(state)
    }
}
