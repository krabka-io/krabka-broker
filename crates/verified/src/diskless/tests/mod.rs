use assert2::check;

use super::*;

fn policy(
    retention_ms: Option<i64>,
    retention_bytes: Option<u64>,
    log_start_offset: i64,
    now_ms: i64,
) -> DisklessRetentionPolicy {
    DisklessRetentionPolicy {
        retention_ms,
        retention_bytes,
        log_start_offset,
        now_ms,
    }
}

mod reclaim_needs_the_grace_period_and_no_reference;

mod logical_range_selects_the_cover_or_the_successor_after_a_gap;
