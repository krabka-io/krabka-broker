//! KIP-1241 delayed-copy eligibility, before remote IO begins.

use creusot_std::prelude::*;

/// Whether either effective copy limit releases a sealed segment.
///
/// Zero means immediate copying, a negative limit is disabled, and the
/// newest record timestamp is measured against the supplied clock. Kafka
/// treats a future timestamp as eligible. The byte count includes both
/// newer active data and newer sealed segments, with saturating arithmetic.
/// Effective-limit derivation, segment ordering, clocks, leadership and
/// remote durability remain caller obligations.
#[ensures(result == (
    lag_ms@ == 0 || lag_bytes@ == 0
    || (lag_ms@ > 0 && (now_ms@ < timestamp_ms@ || now_ms@ - timestamp_ms@ >= lag_ms@))
    || (lag_bytes@ > 0 && active_bytes@ + sealed_after@ >= lag_bytes@)
))]
#[must_use]
pub fn remote_copy_allowed(
    lag_ms: i64,
    lag_bytes: i64,
    now_ms: i64,
    timestamp_ms: i64,
    active_bytes: u64,
    sealed_after: u64,
) -> bool {
    lag_ms == 0
        || lag_bytes == 0
        || (lag_ms > 0
            && (now_ms < timestamp_ms || now_ms.abs_diff(timestamp_ms) >= lag_ms.abs_diff(0)))
        || (lag_bytes > 0 && active_bytes.saturating_add(sealed_after) >= lag_bytes.abs_diff(0))
}

#[cfg(test)]
mod tests {
    use {super::remote_copy_allowed, assert2::check};

    #[yare::parameterized(
        disabled = { -1, -1, i64::MAX, i64::MIN, u64::MAX, u64::MAX, false },
        immediate_time = { 0, -1, 0, 0, 0, 0, true },
        immediate_bytes = { -1, 0, 0, 0, 0, 0, true },
        future = { 2, -1, i64::MIN, i64::MAX, 0, 0, true },
        before_age = { 2, -1, 3, 2, 0, 0, false },
        exact_age = { 2, -1, 4, 2, 0, 0, true },
        extreme_age = { i64::MAX, -1, i64::MAX, i64::MIN, 0, 0, true },
        below_bytes = { -1, 2, 0, 0, 1, 0, false },
        exact_bytes = { -1, 2, 0, 0, 1, 1, true },
        saturated_bytes = { -1, i64::MAX, 0, 0, u64::MAX, 1, true },
    )]
    fn boundaries(
        lag_ms: i64,
        lag_bytes: i64,
        now_ms: i64,
        timestamp_ms: i64,
        active_bytes: u64,
        sealed_after: u64,
        expected: bool,
    ) {
        check!(
            remote_copy_allowed(
                lag_ms,
                lag_bytes,
                now_ms,
                timestamp_ms,
                active_bytes,
                sealed_after
            ) == expected
        );
    }
}
