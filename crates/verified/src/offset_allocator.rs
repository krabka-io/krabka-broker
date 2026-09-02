//! Pure offset-reservation kernel for diskless WAL sequencers.

use creusot_std::prelude::*;

/// Admit reservations only after the current leader's epoch marker commits.
///
/// That marker follows every inherited log entry, so crossing it makes the
/// applied metadata image an exact durable base for a new reservation.
#[ensures(result == (high_watermark@ > epoch_start_offset@))]
#[must_use]
pub fn wal_reservation_epoch_ready(high_watermark: i64, epoch_start_offset: i64) -> bool {
    high_watermark > epoch_start_offset
}

/// Extend an exact pending-reservation chain by one range.
///
/// `pending_base` must equal the current frontier. This rejects gaps,
/// overlaps, zero-sized reservations, negative coordinates, and overflow.
#[ensures(match result {
    Some(next) => current@ >= 0
        && pending_base@ == current@
        && pending_count@ > 0
        && next@ == current@ + pending_count@
        && next@ <= i64::MAX@,
    None => current@ < 0
        || pending_base@ != current@
        || pending_count@ <= 0
        || current@ + pending_count@ > i64::MAX@,
})]
#[must_use]
pub fn wal_reservation_frontier(
    current: i64,
    pending_base: i64,
    pending_count: i64,
) -> Option<i64> {
    if current < 0 || pending_base != current || pending_count <= 0 {
        return None;
    }
    current.checked_add(pending_count)
}

/// Admit a controller reservation response bound to the exact request and
/// leader epoch. `-1` means the caller cannot observe the controller epoch;
/// the response must still carry a nonnegative epoch committed by the leader.
#[ensures(match result {
    Some(admitted_base) => request_matches
        && response_epoch@ >= 0
        && (request_epoch@ == -1
            || (request_epoch@ >= 0
                && observed_epoch@ == request_epoch@
                && response_epoch@ == request_epoch@))
        && base@ >= 0
        && count@ > 0
        && admitted_base@ == base@
        && base@ + count@ <= i64::MAX@,
    None => !request_matches
        || response_epoch@ < 0
        || (request_epoch@ != -1
            && (request_epoch@ < 0
                || observed_epoch@ != request_epoch@
                || response_epoch@ != request_epoch@))
        || base@ < 0
        || count@ <= 0
        || base@ + count@ > i64::MAX@,
})]
#[must_use]
pub fn wal_reservation_response(
    request_matches: bool,
    request_epoch: i64,
    observed_epoch: i64,
    response_epoch: i64,
    base: i64,
    count: i64,
) -> Option<i64> {
    if !request_matches || response_epoch < 0 {
        return None;
    }
    if request_epoch != -1
        && (request_epoch < 0 || observed_epoch != request_epoch || response_epoch != request_epoch)
    {
        return None;
    }
    if base < 0 || count <= 0 {
        return None;
    }
    base.checked_add(count).map(|_| base)
}

/// Reserve `count` offsets from `next`, returning `(base, next_after)`.
#[must_use]
#[cfg_attr(creusot, requires(next@ >= 0))]
#[cfg_attr(creusot, requires(count@ > 0))]
#[cfg_attr(creusot, requires(next@ + count@ <= i64::MAX@))]
#[cfg_attr(creusot, ensures(result.0@ == next@))]
#[cfg_attr(creusot, ensures(result.1@ == next@ + count@))]
pub const fn reserve_offsets(next: i64, count: i64) -> (i64, i64) {
    (next, next.saturating_add(count))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn reserve_offsets_returns_base_and_advanced_next() {
        assert!(reserve_offsets(11, 3) == (11, 14));
    }

    #[test]
    fn reserve_offsets_saturates_on_overflow() {
        assert!(reserve_offsets(i64::MAX - 1, 3) == (i64::MAX - 1, i64::MAX));
    }

    #[test]
    fn pending_frontier_is_exact_contiguous_and_bounded() {
        assert!(wal_reservation_frontier(10, 10, 3) == Some(13));
        assert!(wal_reservation_frontier(10, 9, 3).is_none());
        assert!(wal_reservation_frontier(10, 11, 3).is_none());
        assert!(wal_reservation_frontier(10, 10, 0).is_none());
        assert!(wal_reservation_frontier(-1, -1, 1).is_none());
        assert!(wal_reservation_frontier(i64::MAX, i64::MAX, 1).is_none());
    }

    #[test]
    fn leader_epoch_must_be_committed_before_reservation() {
        assert!(!wal_reservation_epoch_ready(7, 7));
        assert!(wal_reservation_epoch_ready(8, 7));
    }

    #[test]
    fn response_admission_binds_identity_epoch_and_exact_range() {
        assert!(wal_reservation_response(true, 7, 7, 7, 11, 3) == Some(11));
        assert!(wal_reservation_response(true, -1, -1, 7, 11, 3) == Some(11));
        for rejected in [
            wal_reservation_response(false, 7, 7, 7, 11, 3),
            wal_reservation_response(true, 7, 8, 7, 11, 3),
            wal_reservation_response(true, 7, 7, 6, 11, 3),
            wal_reservation_response(true, -2, -2, 7, 11, 3),
            wal_reservation_response(true, -1, -1, -1, 11, 3),
            wal_reservation_response(true, 7, 7, 7, -1, 3),
            wal_reservation_response(true, 7, 7, 7, 11, 0),
            wal_reservation_response(true, 7, 7, 7, i64::MAX, 1),
        ] {
            assert!(rejected.is_none());
        }
    }
}
