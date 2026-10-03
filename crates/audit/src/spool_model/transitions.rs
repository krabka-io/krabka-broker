use super::*;

impl SpoolModel {
    pub(super) fn reopen(self, mut state: SpoolState) -> Option<SpoolState> {
        match state.replay {
            ReplayPhase::Idle => {}
            ReplayPhase::Poisoned { offset, .. } | ReplayPhase::Delivered { offset, .. } => {
                let decision =
                    replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
                if decision == ReplayRecovery::RequireExplicitRecovery {
                    // `Spool::open` returns the poison error before it
                    // reconciles losses.
                    state.runtime = Runtime::Stopped;
                    state.witnesses |= SAW_UNCERTAIN_POISON;
                    return Some(state);
                }
            }
            ReplayPhase::CursorCommitted { offset } => {
                let decision =
                    replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
                if decision != ReplayRecovery::ClearPoison {
                    return None;
                }
                state.replay = ReplayPhase::Idle;
                state.witnesses |= SAW_COMMITTED_POISON;
            }
        }
        // `PendingLosses::reconcile`: a durable marker of the sidecar's
        // generation settles it.
        let sidecar = state.sidecar;
        let marker = state
            .durable_markers()
            .find(|m| m.generation == sidecar.generation);
        if sidecar.count > 0
            && let Some(marker) = marker
        {
            state.memory = (self.reconcile)(state.sidecar, marker);
            persist_sidecar(&mut state);
            state.accounted = state.accounted.saturating_add(marker.count);
            state.witnesses |= SAW_LOSS_RECONCILE;
        }
        Some(state)
    }
}
