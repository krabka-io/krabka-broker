use super::*;

pub(super) fn bit(record: u8) -> u8 {
    1 << record
}

fn pending_mask(state: &SpoolState) -> u8 {
    state.volatile | state.durable
}

/// Append one frame through the production admission kernel. It returns the
/// record id, or `None` when the spool is full.
pub(super) fn spool_append(state: &mut SpoolState) -> Option<u8> {
    let decision = spool_append_decision(
        u64::from(pending_mask(state).count_ones()),
        1,
        MAX_BYTES,
        state.unsynced,
        SYNC_EVERY,
    );
    if !decision.accepted {
        return None;
    }
    let id = state.next_record;
    let record = bit(id);
    state.next_record += 1;
    if decision.sync {
        state.durable |= state.volatile | record;
        state.durable_history |= state.volatile | record;
        state.volatile = 0;
    } else {
        state.volatile |= record;
    }
    state.unsynced = decision.next_unsynced;
    Some(id)
}

pub(super) fn sync(state: &mut SpoolState) {
    state.durable |= state.volatile;
    state.durable_history |= state.volatile;
    state.volatile = 0;
    state.unsynced = 0;
}

pub(super) fn persist_sidecar(state: &mut SpoolState) {
    state.sidecar = state.memory;
    state.unpersisted = 0;
}

/// `PendingLosses::commit` before `settle_loss_batch`: it settled the
/// marker's count but left any remainder in the marker's generation.
pub(super) fn superseded_commit(state: Losses, batch: Losses) -> Losses {
    if state.generation != batch.generation {
        return state;
    }
    Losses {
        generation: state.generation,
        count: state.count.saturating_sub(batch.count),
    }
}

/// `PendingLosses::reconcile` before `settle_loss_batch`: a marker of the
/// sidecar's generation zeroed the whole count.
pub(super) fn superseded_reconcile(state: Losses, _marker: Losses) -> Losses {
    Losses {
        generation: state.generation,
        count: 0,
    }
}
