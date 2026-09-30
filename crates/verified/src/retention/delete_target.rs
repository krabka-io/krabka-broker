use creusot_std::prelude::*;

use super::RemoteRetentionSegment;
#[cfg(creusot)]
use super::remote_retention_model;

/// Count the oldest finished remote segments that Kafka's remote retention
/// deletes.
///
/// `size_debt` is the total size minus `retention.bytes`, or zero when
/// `retention.bytes` is unset or not exceeded. A tier that accepts no delete
/// (`deletes_allowed` false) selects nothing.
///
/// The result equals `remote_retention_model` from the oldest segment.
/// That fold states the Kafka rule.
#[ensures(result@ == if deletes_allowed {
    remote_retention_model(segments@, 0, size_debt@)
} else {
    0
})]
#[ensures(result@ <= segments@.len())]
#[must_use]
pub fn remote_retention_prefix(
    deletes_allowed: bool,
    segments: &[RemoteRetentionSegment],
    size_debt: u64,
) -> usize {
    if !deletes_allowed {
        return 0;
    }
    let mut debt = size_debt;
    let mut len = 0usize;
    #[invariant(len@ <= segments@.len())]
    #[invariant(remote_retention_model(segments@, len@, debt@)
        == remote_retention_model(segments@, 0, size_debt@))]
    #[variant(segments@.len() - len@)]
    while len < segments.len() {
        let segment = segments[len];
        if segment.log_start_breached {
            // Deleted below the floor; the size debt is untouched.
        } else if segment.time_expired {
            debt = debt.saturating_sub(segment.size);
        } else if debt > 0 && segment.size <= debt {
            debt -= segment.size;
        } else {
            break;
        }
        len += 1;
    }
    len
}

/// The exclusive delete-through target after an inclusive last offset.
///
/// No selected segment gives no target. An inclusive last offset of
/// `i64::MAX` has no representable successor, so it fails closed with no
/// target too.
#[ensures(match (last_offset, result) {
    (None, result) => result == None,
    (Some(last), None) => last@ == i64::MAX@,
    (Some(last), Some(target)) => last@ < i64::MAX@ && target@ == last@ + 1,
})]
#[must_use]
pub fn retention_delete_target(last_offset: Option<i64>) -> Option<i64> {
    match last_offset {
        Some(last) if last < i64::MAX => Some(last + 1),
        Some(_) | None => None,
    }
}
