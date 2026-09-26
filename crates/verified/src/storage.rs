//! Storage-transition admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Segment-prefix and active-segment selection for a tail truncation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalTruncationPlan {
    pub retained_sealed: usize,
    pub keep_active: bool,
}

/// Admit one batch only at the expected logical frontier and compute its
/// inclusive last offset and exclusive successor without signed overflow.
#[ensures(match result {
    Some((last, next)) => expected_base@ >= 0
        && supplied_base@ == expected_base@
        && last_offset_delta@ >= 0
        && last@ == supplied_base@ + last_offset_delta@
        && next@ == last@ + 1
        && supplied_base@ <= last@
        && last@ < next@,
    None => expected_base@ < 0
        || supplied_base@ != expected_base@
        || last_offset_delta@ < 0
        || supplied_base@ + last_offset_delta@ > i64::MAX@
        || supplied_base@ + last_offset_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn local_append_coordinates(
    expected_base: i64,
    supplied_base: i64,
    last_offset_delta: i32,
) -> Option<(i64, i64)> {
    if expected_base < 0 || supplied_base != expected_base || last_offset_delta < 0 {
        return None;
    }
    let last = supplied_base.checked_add(i64::from(last_offset_delta))?;
    let next = last.checked_add(1)?;
    Some((last, next))
}

/// Keep exactly the sealed-segment prefix whose bases precede the cut and keep
/// the current active segment exactly when its base also precedes the cut.
#[requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < sealed_bases@.len() ==> sealed_bases@[i] < sealed_bases@[j])]
#[ensures(result.retained_sealed@ <= sealed_bases@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.retained_sealed@ ==>
    sealed_bases@[i]@ < cut@)]
#[ensures(forall<i: Int> result.retained_sealed@ <= i && i < sealed_bases@.len() ==>
    sealed_bases@[i]@ >= cut@)]
#[ensures(result.keep_active == match active_base {
    Some(base) => base@ < cut@,
    None => false,
})]
#[must_use]
pub fn local_truncation_plan(
    sealed_bases: &[i64],
    active_base: Option<i64>,
    cut: i64,
) -> LocalTruncationPlan {
    let mut retained_sealed = 0usize;
    #[invariant(retained_sealed@ <= sealed_bases@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < retained_sealed@ ==>
        sealed_bases@[i]@ < cut@)]
    #[variant(sealed_bases@.len() - retained_sealed@)]
    while retained_sealed < sealed_bases.len() && sealed_bases[retained_sealed] < cut {
        retained_sealed += 1;
    }
    LocalTruncationPlan {
        retained_sealed,
        keep_active: match active_base {
            Some(base) => base < cut,
            None => false,
        },
    }
}

/// Convert an absolute cut to a segment-relative offset without signed or
/// `u32` overflow. Segment bases and log cuts are nonnegative Kafka offsets.
#[ensures(match result {
    Some(relative) => segment_base@ >= 0
        && cut@ >= segment_base@
        && relative@ == cut@ - segment_base@
        && relative@ <= u32::MAX@,
    None => segment_base@ < 0
        || cut@ < segment_base@
        || cut@ - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn truncation_relative_offset(segment_base: i64, cut: i64) -> Option<u32> {
    if segment_base < 0 || cut < segment_base {
        return None;
    }
    let relative = cut.abs_diff(segment_base);
    if relative > u64::from(u32::MAX) {
        None
    } else {
        // The clamp is the identity here; it hands the cast a range the
        // compiler can see.
        Some(relative.min(0xffff_ffff) as u32)
    }
}

/// One decoded batch belongs to the exact retained prefix iff its inclusive
/// last offset is below the exclusive cut.
#[ensures(result == (batch_last@ < cut@))]
#[must_use]
pub const fn truncation_batch_retained(batch_last: i64, cut: i64) -> bool {
    batch_last < cut
}

/// Clamp a dependent frontier to the new log end.
#[ensures(result@ <= frontier@)]
#[ensures(result@ <= new_end@)]
#[ensures(result@ == frontier@ || result@ == new_end@)]
#[must_use]
pub const fn truncation_frontier(frontier: i64, new_end: i64) -> i64 {
    if frontier < new_end {
        frontier
    } else {
        new_end
    }
}

/// A future log may replace the current log only at the exact same frontier.
#[ensures(result == (current_leo@ == future_leo@))]
#[must_use]
pub const fn future_log_swap_admission(current_leo: i64, future_leo: i64) -> bool {
    current_leo == future_leo
}

/// KIP-405 `RemoteLogSegmentState`, shared by every remote-segment kernel so
/// the host maps its state onto one type.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)
)]
pub enum RemoteSegmentLifecycle {
    CopyStarted,
    CopyFinished,
    DeleteStarted,
    DeleteFinished,
}

/// KIP-405 `RemotePartitionDeleteState`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RemotePartitionDeleteLifecycle {
    Marked,
    Started,
    Finished,
}

/// The forward edges of Kafka's segment lifecycle:
/// `COPY_SEGMENT_STARTED -> COPY_SEGMENT_FINISHED | DELETE_SEGMENT_STARTED`,
/// `COPY_SEGMENT_FINISHED -> DELETE_SEGMENT_STARTED`, and
/// `DELETE_SEGMENT_STARTED -> DELETE_SEGMENT_FINISHED`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn segment_forward_edge(from: RemoteSegmentLifecycle, to: RemoteSegmentLifecycle) -> bool {
    match (from, to) {
        (RemoteSegmentLifecycle::CopyStarted, RemoteSegmentLifecycle::CopyFinished)
        | (RemoteSegmentLifecycle::CopyStarted, RemoteSegmentLifecycle::DeleteStarted)
        | (RemoteSegmentLifecycle::CopyFinished, RemoteSegmentLifecycle::DeleteStarted)
        | (RemoteSegmentLifecycle::DeleteStarted, RemoteSegmentLifecycle::DeleteFinished) => true,
        _ => false,
    }
}

/// The forward edges of Kafka's partition-delete lifecycle:
/// `DELETE_PARTITION_MARKED -> DELETE_PARTITION_STARTED -> DELETE_PARTITION_FINISHED`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn partition_delete_forward_edge(
    from: RemotePartitionDeleteLifecycle,
    to: RemotePartitionDeleteLifecycle,
) -> bool {
    match (from, to) {
        (RemotePartitionDeleteLifecycle::Marked, RemotePartitionDeleteLifecycle::Started)
        | (RemotePartitionDeleteLifecycle::Started, RemotePartitionDeleteLifecycle::Finished) => {
            true
        }
        _ => false,
    }
}

/// Kafka's `RemoteLogSegmentState.isValidTransition` for a known source
/// state: a forward lifecycle edge, or a self transition, which Kafka admits
/// so retries and failover stay idempotent. A segment with no source state is
/// the host's add path, which admits only `CopyStarted`.
#[ensures(result == (from == to || segment_forward_edge(from, to)))]
#[must_use]
pub const fn remote_segment_transition(
    from: RemoteSegmentLifecycle,
    to: RemoteSegmentLifecycle,
) -> bool {
    matches!(
        (from, to),
        (
            RemoteSegmentLifecycle::CopyStarted,
            RemoteSegmentLifecycle::CopyStarted
                | RemoteSegmentLifecycle::CopyFinished
                | RemoteSegmentLifecycle::DeleteStarted
        ) | (
            RemoteSegmentLifecycle::CopyFinished,
            RemoteSegmentLifecycle::CopyFinished | RemoteSegmentLifecycle::DeleteStarted
        ) | (
            RemoteSegmentLifecycle::DeleteStarted,
            RemoteSegmentLifecycle::DeleteStarted | RemoteSegmentLifecycle::DeleteFinished
        ) | (
            RemoteSegmentLifecycle::DeleteFinished,
            RemoteSegmentLifecycle::DeleteFinished
        )
    )
}

/// Kafka's `RemotePartitionDeleteState.isValidTransition`: with no prior
/// state only `DELETE_PARTITION_MARKED` is admitted; otherwise a forward edge
/// or an idempotent self transition.
#[ensures(result == match from {
    None => to == RemotePartitionDeleteLifecycle::Marked,
    Some(from) => from == to || partition_delete_forward_edge(from, to),
})]
#[must_use]
pub const fn remote_partition_delete_transition(
    from: Option<RemotePartitionDeleteLifecycle>,
    to: RemotePartitionDeleteLifecycle,
) -> bool {
    matches!(
        (from, to),
        (None, RemotePartitionDeleteLifecycle::Marked)
            | (
                Some(RemotePartitionDeleteLifecycle::Marked),
                RemotePartitionDeleteLifecycle::Marked | RemotePartitionDeleteLifecycle::Started
            )
            | (
                Some(RemotePartitionDeleteLifecycle::Started),
                RemotePartitionDeleteLifecycle::Started | RemotePartitionDeleteLifecycle::Finished
            )
            | (
                Some(RemotePartitionDeleteLifecycle::Finished),
                RemotePartitionDeleteLifecycle::Finished
            )
    )
}

/// Mutation of the primary remote-metadata cache and its derived epoch index.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RemoteCacheAction {
    /// Reject a stale, conflicting, or resurrection attempt.
    Reject,
    /// Preserve state for an exact retry or an already-absent tombstone.
    Noop,
    /// Store the new readable state and rebuild the derived epoch index.
    StoreFinished,
    /// Store a non-readable state and rebuild the index without it.
    StoreHidden,
    /// Remove primary state and rebuild the index without it.
    Remove,
}

/// Classify one remote-segment cache update against the cached state, `None`
/// when the cache holds no such segment.
///
/// A forward lifecycle edge stores its target: readable at `CopyFinished`,
/// hidden at `DeleteStarted`, removed at `DeleteFinished`. An update for a
/// missing segment is an idempotent tombstone when it is `DeleteFinished` and
/// rejected otherwise, so an update can never resurrect a segment.
///
/// Kafka's `RemoteLogMetadataCache` admits every self transition. This cache
/// is stricter: a self transition is a no-op only for an exact retry (the host
/// compares timestamp, broker and custom metadata) and is rejected as a
/// conflict otherwise.
#[ensures(result == match current {
    None => if target == RemoteSegmentLifecycle::DeleteFinished {
        RemoteCacheAction::Noop
    } else {
        RemoteCacheAction::Reject
    },
    Some(current) => if current == target {
        if exact_retry { RemoteCacheAction::Noop } else { RemoteCacheAction::Reject }
    } else if segment_forward_edge(current, target) {
        match target {
            RemoteSegmentLifecycle::CopyFinished => RemoteCacheAction::StoreFinished,
            RemoteSegmentLifecycle::DeleteFinished => RemoteCacheAction::Remove,
            _ => RemoteCacheAction::StoreHidden,
        }
    } else {
        RemoteCacheAction::Reject
    },
})]
#[must_use]
pub const fn remote_cache_action(
    current: Option<RemoteSegmentLifecycle>,
    target: RemoteSegmentLifecycle,
    exact_retry: bool,
) -> RemoteCacheAction {
    match (current, target) {
        (None, RemoteSegmentLifecycle::DeleteFinished) => RemoteCacheAction::Noop,
        (Some(RemoteSegmentLifecycle::CopyStarted), RemoteSegmentLifecycle::CopyStarted)
        | (Some(RemoteSegmentLifecycle::CopyFinished), RemoteSegmentLifecycle::CopyFinished)
        | (Some(RemoteSegmentLifecycle::DeleteStarted), RemoteSegmentLifecycle::DeleteStarted)
        | (Some(RemoteSegmentLifecycle::DeleteFinished), RemoteSegmentLifecycle::DeleteFinished) => {
            if exact_retry {
                RemoteCacheAction::Noop
            } else {
                RemoteCacheAction::Reject
            }
        }
        (Some(RemoteSegmentLifecycle::CopyStarted), RemoteSegmentLifecycle::CopyFinished) => {
            RemoteCacheAction::StoreFinished
        }
        (
            Some(RemoteSegmentLifecycle::CopyStarted | RemoteSegmentLifecycle::CopyFinished),
            RemoteSegmentLifecycle::DeleteStarted,
        ) => RemoteCacheAction::StoreHidden,
        (Some(RemoteSegmentLifecycle::DeleteStarted), RemoteSegmentLifecycle::DeleteFinished) => {
            RemoteCacheAction::Remove
        }
        _ => RemoteCacheAction::Reject,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_truncation_kernels_cover_boundaries_and_invalid_offsets() {
        assert2::check!(
            local_truncation_plan(&[0, 10, 20], Some(30), 20)
                == LocalTruncationPlan {
                    retained_sealed: 2,
                    keep_active: false,
                }
        );
        assert2::check!(
            local_truncation_plan(&[0, 10], Some(15), 20)
                == LocalTruncationPlan {
                    retained_sealed: 2,
                    keep_active: true,
                }
        );
        assert2::check!(
            local_truncation_plan(&[0, 10], Some(20), 20)
                == LocalTruncationPlan {
                    retained_sealed: 2,
                    keep_active: false,
                }
        );

        assert2::check!(truncation_relative_offset(0, 0) == Some(0));
        assert2::check!(truncation_relative_offset(0, i64::from(u32::MAX)) == Some(u32::MAX));
        assert2::check!(truncation_relative_offset(10, 15) == Some(5));
        assert2::check!(truncation_relative_offset(-1, 0).is_none());
        assert2::check!(truncation_relative_offset(10, 9).is_none());
        assert2::check!(truncation_relative_offset(0, i64::from(u32::MAX) + 1).is_none());

        assert2::check!(truncation_batch_retained(19, 20));
        assert2::check!(!truncation_batch_retained(20, 20));
        assert2::check!(truncation_frontier(12, 20) == 12);
        assert2::check!(truncation_frontier(21, 20) == 20);
    }

    #[test]
    fn local_append_coordinates_are_exact_and_fail_closed() {
        assert2::check!(local_append_coordinates(0, 0, 0) == Some((0, 1)));
        assert2::check!(local_append_coordinates(10, 10, 2) == Some((12, 13)));
        assert2::check!(local_append_coordinates(10, 9, 0).is_none());
        assert2::check!(local_append_coordinates(-1, -1, 0).is_none());
        assert2::check!(local_append_coordinates(10, 10, -1).is_none());
        assert2::check!(local_append_coordinates(i64::MAX, i64::MAX, 0).is_none());
        assert2::check!(
            local_append_coordinates(i64::MAX - 1, i64::MAX - 1, 0)
                == Some((i64::MAX - 1, i64::MAX))
        );
    }

    #[test]
    fn future_log_swap_requires_equal_frontiers() {
        assert2::assert!(future_log_swap_admission(7, 7));
        assert2::assert!(!future_log_swap_admission(7, 6));
        assert2::assert!(!future_log_swap_admission(7, 8));
    }

    #[test]
    fn remote_segment_transition_matrix_matches_kafka() {
        use RemoteSegmentLifecycle::{CopyFinished, CopyStarted, DeleteFinished, DeleteStarted};

        let states = [CopyStarted, CopyFinished, DeleteStarted, DeleteFinished];
        // Kafka's `RemoteLogSegmentState.isValidTransition`, row = source.
        let expected = [
            [true, true, true, false],
            [false, true, true, false],
            [false, false, true, true],
            [false, false, false, true],
        ];
        for (from, row) in states.into_iter().zip(expected) {
            for (to, want) in states.into_iter().zip(row) {
                assert2::check!(
                    remote_segment_transition(from, to) == want,
                    "{from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn remote_partition_delete_transition_matrix_matches_kafka() {
        use RemotePartitionDeleteLifecycle::{Finished, Marked, Started};

        // Kafka's `RemotePartitionDeleteState.isValidTransition`, row =
        // source, the first row being no prior state.
        let expected = [
            (None, [true, false, false]),
            (Some(Marked), [true, true, false]),
            (Some(Started), [false, true, true]),
            (Some(Finished), [false, false, true]),
        ];
        for (from, row) in expected {
            for (to, want) in [Marked, Started, Finished].into_iter().zip(row) {
                assert2::check!(
                    remote_partition_delete_transition(from, to) == want,
                    "{from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn remote_cache_actions_are_idempotent_and_never_resurrect() {
        use RemoteCacheAction::{Noop, Reject, Remove, StoreFinished, StoreHidden};
        use RemoteSegmentLifecycle::{CopyFinished, CopyStarted, DeleteFinished, DeleteStarted};

        for (what, current, target, retry, expected) in [
            (
                "an update cannot resurrect",
                None,
                CopyFinished,
                false,
                Reject,
            ),
            (
                "a missing tombstone is idempotent",
                None,
                DeleteFinished,
                false,
                Noop,
            ),
            ("an exact retry", Some(CopyStarted), CopyStarted, true, Noop),
            (
                "a conflicting duplicate",
                Some(CopyStarted),
                CopyStarted,
                false,
                Reject,
            ),
            (
                "the copy finishes",
                Some(CopyStarted),
                CopyFinished,
                false,
                StoreFinished,
            ),
            (
                "an unfinished copy is deleted",
                Some(CopyStarted),
                DeleteStarted,
                false,
                StoreHidden,
            ),
            (
                "a finished copy is deleted",
                Some(CopyFinished),
                DeleteStarted,
                false,
                StoreHidden,
            ),
            (
                "a delete cannot skip its start",
                Some(CopyFinished),
                DeleteFinished,
                false,
                Reject,
            ),
            (
                "the delete finishes",
                Some(DeleteStarted),
                DeleteFinished,
                false,
                Remove,
            ),
            (
                "the lifecycle never moves back",
                Some(DeleteStarted),
                CopyFinished,
                false,
                Reject,
            ),
            (
                "a finished delete stays finished",
                Some(DeleteFinished),
                CopyStarted,
                false,
                Reject,
            ),
        ] {
            assert2::check!(
                remote_cache_action(current, target, retry) == expected,
                "{what}"
            );
        }
    }
}
