//! The offset boundary decisions a `DeleteRecords` trim makes before it
//! touches a log: resolving the requested target, rejecting one that is out of
//! range, and capping the resolved target at the delivery watermark.
//!
//! The first two delegate to the verified `krabka_verified` rules, so this
//! module is the seam between the handler's raw `i64` wire offsets and those
//! proofs. The KFC-1 cap lives beside them because it is the last step of the
//! same decision.

use krabka_log::Offset;

pub(super) fn target_offset(requested_offset: i64, high_watermark: i64) -> i64 {
    krabka_verified::delete_records_target(requested_offset, high_watermark)
}

pub(super) fn offset_out_of_range(target: i64, log_end_offset: i64) -> bool {
    krabka_verified::delete_records_offset_out_of_range(target, log_end_offset)
}

/// KFC-1: the offset a trim may actually reach.
///
/// `watermark` is the partition's delivery watermark, and `None` on a topic
/// that delivers immediately. Such a topic has every durable record visible
/// already, so the resolved target stands and this is the identity.
///
/// A topic that schedules delivery stops the trim at the watermark. The `-1`
/// sentinel resolves to the high watermark, and on a scheduled partition that
/// sits above every record that has not come due, so a routine trim would
/// delete records the broker promised to deliver and no consumer was allowed
/// to read. An explicit target is capped for the same reason: what the cap
/// removes is exactly the undelivered tail, and the response reports the log
/// start offset the trim reached.
pub(super) fn delivery_capped(target: Offset, watermark: Option<Offset>) -> Offset {
    watermark.map_or(target, |visible| target.min(visible))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn offset_helpers_cover_delete_records_boundaries() {
        check!(target_offset(-1, 42) == 42);
        check!(target_offset(-2, 42) == -2);
        check!(target_offset(7, 42) == 7);

        check!(!offset_out_of_range(0, 10));
        check!(!offset_out_of_range(10, 10));
        check!(offset_out_of_range(-1, 10));
        check!(offset_out_of_range(11, 10));
    }

    #[test]
    fn the_delivery_cap_only_lowers_a_target_above_the_watermark() {
        let cases = [
            // A topic that delivers immediately has no watermark to cap with.
            (Offset(9), None, Offset(9)),
            (Offset(9), Some(Offset(4)), Offset(4)),
            (Offset(4), Some(Offset(4)), Offset(4)),
            (Offset(2), Some(Offset(4)), Offset(2)),
            // Nothing is visible yet, so nothing may be deleted.
            (Offset(9), Some(Offset(0)), Offset(0)),
        ];
        for (target, watermark, expected) in cases {
            check!(
                delivery_capped(target, watermark) == expected,
                "{target:?} {watermark:?}"
            );
        }
    }
}
