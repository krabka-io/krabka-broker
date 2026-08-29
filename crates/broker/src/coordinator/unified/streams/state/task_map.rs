//! Arithmetic over a role's task map, the `BTreeMap` from `subtopology_id` to
//! a sorted, deduped partition list.
//!
//! Both functions here are pure and total. The state machine calls them to
//! normalize an incoming assignment and to compute the revoke-before-assign
//! split for a member's active tasks.

use std::collections::BTreeMap;

/// Sorts and dedups every subtopology's partition list, then drops the
/// subtopology entries that end up empty. The function is idempotent.
pub(super) fn normalize_task_map(
    mut map: BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<i32>> {
    map.retain(|_, parts| {
        parts.sort_unstable();
        parts.dedup();
        !parts.is_empty()
    });
    map
}

/// Splits a member's currently-owned active tasks against its new active
/// target. The function *keeps* tasks that are in both, and *revokes* tasks
/// the member owns that the target no longer holds. It normalizes both halves:
/// sorted, deduped, and with empty entries dropped.
pub(super) fn compute_active_revoke_split(
    current: &BTreeMap<String, Vec<i32>>,
    target: &BTreeMap<String, Vec<i32>>,
) -> (BTreeMap<String, Vec<i32>>, BTreeMap<String, Vec<i32>>) {
    let mut revoke: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    let mut keep: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for (sub, parts) in current {
        let target_set: std::collections::HashSet<i32> =
            target.get(sub).into_iter().flatten().copied().collect();
        for &p in parts {
            if target_set.contains(&p) {
                keep.entry(sub.clone()).or_default().push(p);
            } else {
                revoke.entry(sub.clone()).or_default().push(p);
            }
        }
    }
    (normalize_task_map(keep), normalize_task_map(revoke))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{super::test_support::task_map, *};

    #[test]
    fn normalize_sorts_dedups_and_drops_empty() {
        let m = normalize_task_map(task_map(&[("sub0", &[2, 0, 1, 1]), ("sub1", &[])]));
        assert!(m == task_map(&[("sub0", &[0, 1, 2])]));
    }
}
