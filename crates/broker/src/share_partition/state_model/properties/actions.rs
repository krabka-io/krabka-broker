use super::*;

impl ShareModel {
    pub(super) fn model_init_states() -> Vec<<Self as Model>::State> {
        vec![ShareState {
            sm: AcquisitionState::new(Offset(0)),
            clock: 0,
            hwm: Offset(0),
            log_start: Offset(0),
        }]
    }
}

impl ShareModel {
    pub(super) fn model_actions(
        &self,
        state: &<Self as Model>::State,
        actions: &mut Vec<<Self as Model>::Action>,
    ) {
        let has_available = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        let has_acquired = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Acquired);

        if state.hwm < self.max_offset {
            actions.push(ShareAction::Produce);
        }
        // Materialize only when there are produced-but-unmaterialized records and
        // no Available batch remains (the real `materialize` no-ops otherwise).
        if state.sm.end_offset < state.hwm && !has_available {
            actions.push(ShareAction::Materialize);
        }
        if has_available {
            for member in 0..self.members {
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: 1,
                });
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: i32::MAX,
                });
            }
        }
        // Data-dependent: ack/renew only over ranges a member actually holds.
        for member in 0..self.members {
            let name = Self::member_name(member);
            for (first, last) in acquired_runs(&state.sm, &name) {
                for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                    actions.push(ShareAction::Acknowledge {
                        member,
                        first,
                        last,
                        ack,
                    });
                }
                actions.push(ShareAction::Renew {
                    member,
                    first,
                    last,
                });
                // A split (first half) exercises partial-ack / partial-renew.
                if last > first {
                    let mid = first + (last.0 - first.0) / 2;
                    for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                        actions.push(ShareAction::Acknowledge {
                            member,
                            first,
                            last: mid,
                            ack,
                        });
                    }
                    actions.push(ShareAction::Renew {
                        member,
                        first,
                        last: mid,
                    });
                }
            }
        }
        if self.allow_defer {
            // Every sub-range of the window that covers something the schedule
            // could still hold back. Ranges rather than single offsets, so the
            // model exercises the splits `defer_internal` makes at its edges.
            for first in state.sm.start_offset.0..state.sm.end_offset.0 {
                for last in first..state.sm.end_offset.0 {
                    let defers_something = (first..=last).any(|raw| {
                        offset_state(&state.sm, Offset(raw)) == Some(RecordState::Available)
                    });
                    if defers_something {
                        actions.push(ShareAction::Defer {
                            first: Offset(first),
                            last: Offset(last),
                        });
                    }
                }
            }
            if !deferred_offsets(&state.sm).is_empty() {
                actions.push(ShareAction::PromoteDeferred);
            }
        }
        if has_acquired {
            actions.push(ShareAction::ExpireLocks);
        }
        if state.clock < self.max_tick {
            actions.push(ShareAction::Tick);
        }
        if self.allow_reload && state.sm.end_offset > state.sm.start_offset {
            actions.push(ShareAction::Reload);
        }
        if self.allow_log_start_advance {
            for raw in (state.log_start.0 + 1)..=state.sm.end_offset.0 {
                actions.push(ShareAction::AdvanceLogStart {
                    new_start: Offset(raw),
                });
            }
        }
    }
}
