use super::*;
#[cfg(test)]
use std::vec;

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
fn local_append_coordinates_admit_the_frontier_or_beyond_and_fail_closed() {
    assert2::check!(local_append_coordinates(0, 0, 0) == Some((0, 1)));
    assert2::check!(local_append_coordinates(10, 10, 2) == Some((12, 13)));
    // A base past the frontier is a hole in the offsets, not a refusal.
    assert2::check!(local_append_coordinates(10, 12, 2) == Some((14, 15)));
    assert2::check!(local_append_coordinates(10, 9, 0).is_none());
    assert2::check!(local_append_coordinates(-1, -1, 0).is_none());
    assert2::check!(local_append_coordinates(10, 10, -1).is_none());
    assert2::check!(local_append_coordinates(i64::MAX, i64::MAX, 0).is_none());
    assert2::check!(
        local_append_coordinates(i64::MAX - 1, i64::MAX - 1, 0) == Some((i64::MAX - 1, i64::MAX))
    );
}

#[test]
fn future_log_swap_requires_equal_frontiers() {
    assert2::assert!(future_log_swap_admission(7, 7));
    assert2::assert!(!future_log_swap_admission(7, 6));
    assert2::assert!(!future_log_swap_admission(7, 8));
}

use {
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

struct RemoteLifecycles {
    segments: Option<[RemoteSegmentLifecycle; 4]>,
    partitions: Option<[Option<RemotePartitionDeleteLifecycle>; 4]>,
    segment_transitions: Option<[[bool; 4]; 4]>,
    partition_transitions: Option<[[bool; 3]; 4]>,
}

#[fixture]
fn lifecycle() -> RemoteLifecycles {
    // Start this scenario with fresh inputs and observations.

    RemoteLifecycles {
        segments: None,
        partitions: None,
        segment_transitions: None,
        partition_transitions: None,
    }
}

#[given("remote segment states copying, copied, deleting and deleted")]
fn segment_states(lifecycle: &mut RemoteLifecycles) {
    use RemoteSegmentLifecycle::{CopyFinished, CopyStarted, DeleteFinished, DeleteStarted};
    lifecycle.segments = Some([CopyStarted, CopyFinished, DeleteStarted, DeleteFinished]);
}

#[when("every remote segment state transition is validated")]
fn segment_transition_graph(lifecycle: &mut RemoteLifecycles) {
    let states = lifecycle
        .segments
        .expect("segment lifecycle states were given");
    lifecycle.segment_transitions =
        Some(states.map(|from| states.map(|to| remote_segment_transition(from, to))));
}

#[derive(DataTableRow)]
struct SegmentTransitionRow {
    source: String,
    copying: String,
    copied: String,
    deleting: String,
    deleted: String,
}

#[then("remote segment transitions are permitted by this matrix:")]
fn segment_permissions(
    lifecycle: &RemoteLifecycles,
    #[datatable] rows: Rows<SegmentTransitionRow>,
) {
    let rows: Vec<_> = rows.into_iter().collect();
    let sources: Vec<_> = rows.iter().map(|row| row.source.as_str()).collect();
    assert2::check!(sources == vec!["copying", "copied", "deleting", "deleted"]);
    let expected: Vec<_> = rows
        .into_iter()
        .map(|row| {
            [row.copying, row.copied, row.deleting, row.deleted].map(|permission| match permission
                .as_str()
            {
                "allowed" => true,
                "forbidden" => false,
                _ => panic!("unknown segment transition permission"),
            })
        })
        .collect();
    let actual = lifecycle
        .segment_transitions
        .expect("segment transitions were validated");
    assert2::check!(actual.as_slice() == expected.as_slice());
}

#[given("remote partition deletion states unregistered, marked, started and finished")]
fn partition_states(lifecycle: &mut RemoteLifecycles) {
    use RemotePartitionDeleteLifecycle::{Finished, Marked, Started};
    lifecycle.partitions = Some([None, Some(Marked), Some(Started), Some(Finished)]);
}

#[when("every remote partition deletion transition is validated")]
fn partition_transition_graph(lifecycle: &mut RemoteLifecycles) {
    use RemotePartitionDeleteLifecycle::{Finished, Marked, Started};
    let states = lifecycle
        .partitions
        .expect("partition deletion states were given");
    lifecycle.partition_transitions = Some(states.map(|from| {
        [Marked, Started, Finished].map(|to| remote_partition_delete_transition(from, to))
    }));
}

#[derive(DataTableRow)]
struct PartitionTransitionRow {
    source: String,
    marked: bool,
    started: bool,
    finished: bool,
}

#[then("remote partition deletion transitions are permitted by this matrix:")]
fn partition_permissions(
    lifecycle: &RemoteLifecycles,
    #[datatable] rows: Rows<PartitionTransitionRow>,
) {
    let rows: Vec<_> = rows.into_iter().collect();
    let sources: Vec<_> = rows.iter().map(|row| row.source.as_str()).collect();
    assert2::check!(sources == vec!["unregistered", "marked", "started", "finished"]);
    let expected: Vec<_> = rows
        .into_iter()
        .map(|row| [row.marked, row.started, row.finished])
        .collect();
    let actual = lifecycle
        .partition_transitions
        .expect("partition transitions were validated");
    assert2::check!(actual.as_slice() == expected.as_slice());
}

scenarios!("tests/features/migrated/src/storage/tests/local_truncation_kernels_cover_boundaries_and_invalid_offsets.feature", fixtures = [lifecycle: RemoteLifecycles]);

#[yare::parameterized(
    cannot_resurrect = {
        "an update cannot resurrect",
        None,
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::Reject,
    },
    missing_tombstone = {
        "a missing tombstone is idempotent",
        None,
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Noop,
    },
    exact_retry = {
        "an exact retry",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyStarted,
        true,
        RemoteCacheAction::Noop,
    },
    conflicting_duplicate = {
        "a conflicting duplicate",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyStarted,
        false,
        RemoteCacheAction::Reject,
    },
    copy_finishes = {
        "the copy finishes",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::StoreFinished,
    },
    unfinished_copy_deleted = {
        "an unfinished copy is deleted",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::DeleteStarted,
        false,
        RemoteCacheAction::StoreHidden,
    },
    finished_copy_deleted = {
        "a finished copy is deleted",
        Some(RemoteSegmentLifecycle::CopyFinished),
        RemoteSegmentLifecycle::DeleteStarted,
        false,
        RemoteCacheAction::StoreHidden,
    },
    delete_skips_start = {
        "a delete cannot skip its start",
        Some(RemoteSegmentLifecycle::CopyFinished),
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Reject,
    },
    delete_finishes = {
        "the delete finishes",
        Some(RemoteSegmentLifecycle::DeleteStarted),
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Remove,
    },
    lifecycle_never_moves_back = {
        "the lifecycle never moves back",
        Some(RemoteSegmentLifecycle::DeleteStarted),
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::Reject,
    },
    finished_delete_stays_finished = {
        "a finished delete stays finished",
        Some(RemoteSegmentLifecycle::DeleteFinished),
        RemoteSegmentLifecycle::CopyStarted,
        false,
        RemoteCacheAction::Reject,
    },
)]
fn remote_cache_actions_are_idempotent_and_never_resurrect(
    what: &'static str,
    current: Option<RemoteSegmentLifecycle>,
    target: RemoteSegmentLifecycle,
    retry: bool,
    expected: RemoteCacheAction,
) {
    assert2::check!(
        remote_cache_action(current, target, retry) == expected,
        "{what}"
    );
}
