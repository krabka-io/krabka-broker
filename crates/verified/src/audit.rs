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
}
