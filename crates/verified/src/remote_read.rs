//! Remote-segment admission and relative-offset arithmetic.

use creusot_std::prelude::*;

/// Admit a remote segment for one requested offset and derive its relative
/// offset for Kafka's `u32` sparse index.
///
/// A segment is usable only after its copy finished, when it contains the
/// requested offset, and when the offset falls inside the requested leader
/// epoch's `[epoch_start, next_epoch_start)` subrange. The last epoch has no
/// next boundary and runs through the segment end. A segment wider than the
/// relative-index representation fails closed rather than truncating or
/// defaulting the relative offset.
#[ensures(match result {
    Some(delta) => match epoch_start {
        Some(epoch_start) => copy_finished
            && start_offset@ <= epoch_start@
            && epoch_start@ <= requested_offset@
            && requested_offset@ <= end_offset@
            && match next_epoch_start {
                Some(next) => epoch_start@ < next@
                    && next@ <= end_offset@
                    && requested_offset@ < next@,
                None => true,
            }
            && delta@ == requested_offset@ - start_offset@,
        None => false,
    },
    None => match epoch_start {
        Some(epoch_start) => !copy_finished
            || epoch_start@ < start_offset@
            || requested_offset@ < epoch_start@
            || requested_offset@ > end_offset@
            || match next_epoch_start {
                Some(next) => next@ <= epoch_start@
                    || next@ > end_offset@
                    || requested_offset@ >= next@,
                None => false,
            }
            || requested_offset@ - start_offset@ > u32::MAX@,
        None => true,
    },
})]
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the mathematical delta is checked to fit u32 before conversion"
)]
#[allow(
    clippy::manual_let_else,
    clippy::question_mark,
    reason = "explicit matching avoids unsupported Creusot v0.13 residual-control obligations"
)]
pub fn remote_read_relative_offset(
    start_offset: i64,
    end_offset: i64,
    requested_offset: i64,
    copy_finished: bool,
    epoch_start: Option<i64>,
    next_epoch_start: Option<i64>,
) -> Option<u32> {
    let epoch_start = match epoch_start {
        Some(epoch_start) => epoch_start,
        None => return None,
    };
    if !copy_finished
        || epoch_start < start_offset
        || requested_offset < epoch_start
        || requested_offset > end_offset
    {
        return None;
    }
    match next_epoch_start {
        Some(next) if next <= epoch_start || next > end_offset || requested_offset >= next => {
            return None;
        }
        _ => {}
    }

    let delta = i128::from(requested_offset) - i128::from(start_offset);
    if delta > i128::from(u32::MAX) {
        None
    } else {
        Some(delta as u32)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn admits_exact_finished_lineage_range_and_rejects_every_other_case() {
        for (start, end, requested, finished, epoch_start, next_epoch, expected) in [
            (100, 199, 100, true, Some(100), None, Some(0)),
            (100, 199, 199, true, Some(100), None, Some(99)),
            (100, 199, 99, true, Some(100), None, None),
            (100, 199, 200, true, Some(100), None, None),
            (200, 100, 150, true, Some(200), None, None),
            (100, 199, 150, false, Some(100), None, None),
            (100, 199, 150, true, None, None, None),
            (0, 99, 49, true, Some(0), Some(50), Some(49)),
            (0, 99, 50, true, Some(0), Some(50), None),
            (0, 99, 49, true, Some(50), None, None),
            (0, 99, 50, true, Some(50), None, Some(50)),
            (0, 99, 49, true, Some(0), Some(100), None),
            (
                i64::MIN,
                i64::MAX,
                i64::MAX,
                true,
                Some(i64::MIN),
                None,
                None,
            ),
            (
                i64::MIN,
                i64::MAX,
                i64::MIN,
                true,
                Some(i64::MIN),
                None,
                Some(0),
            ),
            (
                i64::MIN,
                i64::MAX,
                i64::MIN + i64::from(u32::MAX),
                true,
                Some(i64::MIN),
                None,
                Some(u32::MAX),
            ),
        ] {
            check!(
                remote_read_relative_offset(
                    start,
                    end,
                    requested,
                    finished,
                    epoch_start,
                    next_epoch,
                ) == expected
            );
        }
    }
}
