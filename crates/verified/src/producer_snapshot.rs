//! Producer-snapshot selection, validation, replay, and invalidation decisions.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};

/// Return the index of the greatest nonnegative snapshot offset at or below
/// the captured log end. Input order is irrelevant.
#[ensures(result == None ==>
    forall<i: Int> 0 <= i && i < offsets@.len()
        ==> offsets@[i]@ < 0 || offsets@[i]@ > log_end@)]
#[ensures(match result {
    Some(selected) => selected@ < offsets@.len()
        && offsets@[selected@]@ >= 0
        && offsets@[selected@]@ <= log_end@
        && (forall<i: Int> 0 <= i && i < offsets@.len()
            && offsets@[i]@ >= 0 && offsets@[i]@ <= log_end@
            ==> offsets@[i]@ <= offsets@[selected@]@),
    None => true,
})]
#[must_use]
pub fn producer_snapshot_latest_index(offsets: &[i64], log_end: i64) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(match best {
        None => forall<j: Int> 0 <= j && j < i@
            ==> offsets@[j]@ < 0 || offsets@[j]@ > log_end@,
        Some(selected) => selected@ < i@
            && offsets@[selected@]@ >= 0
            && offsets@[selected@]@ <= log_end@
            && (forall<j: Int> 0 <= j && j < i@
                && offsets@[j]@ >= 0 && offsets@[j]@ <= log_end@
                ==> offsets@[j]@ <= offsets@[selected@]@),
    })]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        let eligible = offsets[i] >= 0 && offsets[i] <= log_end;
        if eligible {
            match best {
                None => best = Some(i),
                Some(selected) if offsets[i] > offsets[selected] => best = Some(i),
                Some(_) => {}
            }
        }
        i += 1;
    }
    best
}

/// Validate one decoded producer entry against its snapshot's exclusive
/// offset boundary.
#[ensures(result == (snapshot_offset@ >= 0
    && identity.0@ >= 0
    && identity.1@ >= 0
    && transaction.0@ >= -1
    && ((last_record.1@ == -1 && last_record.0@ == -1 && last_record.2@ == 0)
        || (last_record.1@ >= 0
            && last_record.0@ >= 0
            && last_record.2@ >= 0
            && last_record.1@ >= last_record.2@
            && last_record.1@ < snapshot_offset@))
    && (transaction.1@ == -1
        || (transaction.1@ >= 0
            && transaction.1@ < snapshot_offset@
            && transaction.1@ <= last_record.1@))))]
#[must_use]
pub fn producer_snapshot_entry_valid(
    snapshot_offset: i64,
    identity: (i64, i16),
    last_record: (i32, i64, i32),
    transaction: (i32, i64),
) -> bool {
    let (producer_id, producer_epoch) = identity;
    let (last_sequence, last_offset, offset_delta) = last_record;
    let (coordinator_epoch, transaction_first_offset) = transaction;
    snapshot_offset >= 0
        && producer_id >= 0
        && producer_epoch >= 0
        && coordinator_epoch >= -1
        && ((last_offset == -1 && last_sequence == -1 && offset_delta == 0)
            || (last_offset >= 0
                && last_sequence >= 0
                && offset_delta >= 0
                && last_offset >= i64::from(offset_delta)
                && last_offset < snapshot_offset))
        && (transaction_first_offset == -1
            || (transaction_first_offset >= 0
                && transaction_first_offset < snapshot_offset
                && transaction_first_offset <= last_offset))
}

/// Select the exact replay cursor after loading an optional snapshot.
#[ensures(match result {
    Some(start) => log_start@ >= 0
        && log_end@ >= log_start@
        && start@ >= log_start@
        && start@ <= log_end@
        && match snapshot_offset {
            Some(snapshot) => snapshot@ >= 0
                && snapshot@ <= log_end@
                && (start@ == snapshot@ || start@ == log_start@),
            None => start@ == log_start@,
        },
    None => log_start@ < 0
        || log_end@ < log_start@
        || match snapshot_offset {
            Some(snapshot) => snapshot@ < 0 || snapshot@ > log_end@,
            None => false,
        },
})]
#[must_use]
pub fn producer_snapshot_replay_start(
    log_start: i64,
    log_end: i64,
    snapshot_offset: Option<i64>,
) -> Option<i64> {
    if log_start < 0 || log_end < log_start {
        return None;
    }
    match snapshot_offset {
        Some(snapshot) if snapshot >= 0 && snapshot <= log_end => Some(snapshot.max(log_start)),
        Some(_) => None,
        None => Some(log_start),
    }
}

/// Retain exactly the valid snapshot offsets at or below a truncation cut.
#[ensures(result == (snapshot_offset@ >= 0 && snapshot_offset@ <= cut@))]
#[must_use]
pub const fn producer_snapshot_retained(snapshot_offset: i64, cut: i64) -> bool {
    snapshot_offset >= 0 && snapshot_offset <= cut
}

#[cfg(test)]
mod tests {
    use super::{
        producer_snapshot_entry_valid, producer_snapshot_latest_index,
        producer_snapshot_replay_start, producer_snapshot_retained,
    };

    #[test]
    fn latest_snapshot_ignores_order_future_and_negative_offsets() {
        assert2::check!(producer_snapshot_latest_index(&[], 10) == None);
        assert2::check!(producer_snapshot_latest_index(&[-1, 11], 10) == None);
        assert2::check!(producer_snapshot_latest_index(&[5, 10, 3, 12], 10) == Some(1));
        assert2::check!(producer_snapshot_latest_index(&[10, 10], 10) == Some(0));
        assert2::check!(producer_snapshot_latest_index(&[i64::MAX], i64::MAX) == Some(0));
    }

    #[test]
    fn entry_validation_is_exact_and_snapshot_bounded() {
        assert2::check!(producer_snapshot_entry_valid(
            10,
            (0, 0),
            (-1, -1, 0),
            (-1, -1)
        ));
        assert2::check!(producer_snapshot_entry_valid(10, (7, 2), (4, 9, 3), (0, 5)));
        assert2::check!(!producer_snapshot_entry_valid(
            10,
            (7, 2),
            (4, 10, 3),
            (0, 5)
        ));
        assert2::check!(!producer_snapshot_entry_valid(
            10,
            (7, 2),
            (4, 9, 3),
            (0, 10)
        ));
        assert2::check!(!producer_snapshot_entry_valid(
            10,
            (7, 2),
            (-1, -1, 1),
            (-1, -1)
        ));
        assert2::check!(!producer_snapshot_entry_valid(
            10,
            (7, 2),
            (0, 0, 1),
            (-1, -1)
        ));
        assert2::check!(!producer_snapshot_entry_valid(
            10,
            (7, 2),
            (4, 9, 3),
            (-2, -1)
        ));
        assert2::check!(producer_snapshot_entry_valid(
            i64::MAX,
            (i64::MAX, i16::MAX),
            (i32::MAX, i64::MAX - 1, i32::MAX),
            (i32::MAX, i64::MAX - 2),
        ));
    }

    #[test]
    fn replay_start_and_truncation_retention_cover_boundaries() {
        assert2::check!(producer_snapshot_replay_start(5, 10, None) == Some(5));
        assert2::check!(producer_snapshot_replay_start(5, 10, Some(3)) == Some(5));
        assert2::check!(producer_snapshot_replay_start(5, 10, Some(7)) == Some(7));
        assert2::check!(producer_snapshot_replay_start(5, 10, Some(11)) == None);
        assert2::check!(producer_snapshot_replay_start(-1, 10, None) == None);
        assert2::check!(producer_snapshot_retained(10, 10));
        assert2::check!(!producer_snapshot_retained(11, 10));
        assert2::check!(!producer_snapshot_retained(-1, 10));
    }
}
