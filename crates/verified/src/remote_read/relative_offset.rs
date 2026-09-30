use creusot_std::prelude::*;

/// Compute the inclusive end of a capped remote-segment fetch.
///
/// A zero cap means read to the segment end. A finite end exists only when the
/// exclusive mathematical end stays strictly inside the segment.
#[ensures(match result {
    Some(end) => max_bytes@ > 0
        && start_position@ + max_bytes@ < segment_size@
        && end@ == start_position@ + max_bytes@ - 1
        && end@ < segment_size@,
    None => max_bytes@ == 0 || start_position@ + max_bytes@ >= segment_size@,
})]
#[must_use]
pub fn remote_fetch_end_position(
    start_position: u32,
    segment_size: u32,
    max_bytes: u32,
) -> Option<u32> {
    if max_bytes == 0 {
        return None;
    }
    // An end past `u32::MAX` is past every `u32` segment size too.
    let exclusive_end = start_position.checked_add(max_bytes)?;
    if exclusive_end >= segment_size {
        None
    } else {
        Some(exclusive_end - 1)
    }
}

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
pub fn remote_read_relative_offset(
    start_offset: i64,
    end_offset: i64,
    requested_offset: i64,
    copy_finished: bool,
    epoch_start: Option<i64>,
    next_epoch_start: Option<i64>,
) -> Option<u32> {
    let epoch_start = epoch_start?;
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

    // `start_offset <= epoch_start <= requested_offset`, so the distance is
    // the delta and cannot overflow.
    let delta = requested_offset.abs_diff(start_offset);
    if delta > u64::from(u32::MAX) {
        None
    } else {
        // The clamp is the identity here; it hands the cast a range the
        // compiler can see.
        Some(delta.min(0xffff_ffff) as u32)
    }
}
