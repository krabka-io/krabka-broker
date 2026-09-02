//! `KRaft` checkpoint selection and retention decisions.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};

/// Whether `candidate` is lexicographically newer than `current`.
#[ensures(result == (candidate_end@ > current_end@
    || (candidate_end@ == current_end@ && candidate_epoch@ > current_epoch@)))]
#[must_use]
pub fn checkpoint_id_newer(
    candidate_end: i64,
    candidate_epoch: i32,
    current_end: i64,
    current_epoch: i32,
) -> bool {
    candidate_end > current_end || (candidate_end == current_end && candidate_epoch > current_epoch)
}

/// Return the index of the maximum `(end offset, epoch)` checkpoint id.
#[ensures(match result {
    Some(selected) => selected@ < ids@.len()
        && (forall<i: Int> 0 <= i && i < ids@.len()
            ==> ids@[selected@].0@ > ids@[i].0@
                || (ids@[selected@].0@ == ids@[i].0@
                    && ids@[selected@].1@ >= ids@[i].1@)),
    None => ids@.len() == 0,
})]
#[must_use]
pub fn latest_checkpoint_index(ids: &[(i64, i32)]) -> Option<usize> {
    if matches!(ids.len(), 0) {
        return None;
    }
    let mut selected = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= ids@.len())]
    #[invariant(selected@ < i@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@
        ==> ids@[selected@].0@ > ids@[k].0@
            || (ids@[selected@].0@ == ids@[k].0@
                && ids@[selected@].1@ >= ids@[k].1@))]
    #[variant(ids@.len() - i@)]
    while i < ids.len() {
        if checkpoint_id_newer(ids[i].0, ids[i].1, ids[selected].0, ids[selected].1) {
            selected = i;
        }
        i += 1;
    }
    Some(selected)
}

/// Retain exactly the checkpoint whose full id equals the selected id.
#[ensures(result == (candidate_end@ == selected_end@
    && candidate_epoch@ == selected_epoch@))]
#[must_use]
pub fn checkpoint_id_retained(
    candidate_end: i64,
    candidate_epoch: i32,
    selected_end: i64,
    selected_epoch: i32,
) -> bool {
    candidate_end == selected_end && candidate_epoch == selected_epoch
}

#[cfg(test)]
mod tests {
    use super::{checkpoint_id_retained, latest_checkpoint_index};

    #[test]
    fn latest_index_covers_empty_ties_and_lexicographic_order() {
        assert2::check!(latest_checkpoint_index(&[]) == None);
        assert2::check!(latest_checkpoint_index(&[(10, 2)]) == Some(0));
        assert2::check!(latest_checkpoint_index(&[(10, 2), (10, 9), (11, 1)]) == Some(2));
        assert2::check!(latest_checkpoint_index(&[(11, 1), (11, 1), (10, 99)]) == Some(0));
    }

    #[test]
    fn retention_requires_the_full_selected_id() {
        assert2::check!(checkpoint_id_retained(11, 3, 11, 3));
        assert2::check!(!checkpoint_id_retained(10, 3, 11, 3));
        assert2::check!(!checkpoint_id_retained(11, 2, 11, 3));
    }
}
