use creusot_std::prelude::*;

use super::{AuditLosses, settle_loss_batch};

/// Replaying a durable loss marker twice cannot subtract pending losses twice.
/// Generation exhaustion is a real ceiling of the saturating host protocol.
#[requires(state.generation@ < u64::MAX@)]
#[ensures(result)]
pub(super) fn loss_settlement_is_idempotent(state: AuditLosses, marker: AuditLosses) -> bool {
    let settled = settle_loss_batch(state, marker);
    let replayed = settle_loss_batch(settled, marker);
    settled.count == replayed.count && settled.generation == replayed.generation
}
