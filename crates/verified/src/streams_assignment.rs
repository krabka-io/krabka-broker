//! Overflow-safe standby replica admission for native Streams assignors.
#[cfg(creusot)]
use creusot_std::prelude::ensures;

/// Admit precisely one additional copy when the replica limit permits it.
/// Invalid negative counters are rejected rather than silently repaired.
#[must_use]
#[cfg_attr(creusot, ensures(match result {
    Some(next) => current@ >= 0 && current@ < maximum@ && next@ == current@ + 1 && next@ <= maximum@,
    None => current@ < 0 || current@ >= maximum@,
}))]
pub const fn next_standby_count(current: i32, maximum: i32) -> Option<i32> {
    if current < 0 || current >= maximum {
        None
    } else {
        Some(current + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::next_standby_count;
    #[test]
    fn standby_limit_is_exact_and_cannot_overflow() {
        assert2::assert!(next_standby_count(0, 1) == Some(1));
        assert2::assert!(next_standby_count(1, 1).is_none());
        assert2::assert!(next_standby_count(0, 0).is_none());
        assert2::assert!(next_standby_count(-1, 2).is_none());
        assert2::assert!(next_standby_count(i32::MAX - 1, i32::MAX) == Some(i32::MAX));
        assert2::assert!(next_standby_count(i32::MAX, i32::MAX).is_none());
    }
}
