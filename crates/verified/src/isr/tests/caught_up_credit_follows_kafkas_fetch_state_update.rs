use super::*;

/// `Replica.updateFetchStateOrThrow` over a leader whose log end was 100
/// at the follower's previous fetch and is 120 now.
#[test]
fn caught_up_credit_follows_kafkas_fetch_state_update() {
    for (label, fetch_offset, last_fetch_leader_log_end, expected) in [
        (
            "the fetch reaches the current log end",
            120,
            100,
            CaughtUpCredit::ThisFetch,
        ),
        (
            "the fetch reaches the previous fetch's log end",
            100,
            100,
            CaughtUpCredit::PreviousFetch,
        ),
        (
            "the fetch falls short of both",
            99,
            100,
            CaughtUpCredit::Unchanged,
        ),
        (
            "the first fetch after a leadership change",
            0,
            -1,
            CaughtUpCredit::PreviousFetch,
        ),
    ] {
        assert2::check!(
            follower_caught_up_credit(fetch_offset, 120, last_fetch_leader_log_end) == expected,
            "{label}"
        );
    }
}

/// `Partition.getOutOfSyncReplicas` for the in-sync rows; krabka's
/// expansion rule for the out-of-sync rows.
#[test]
fn isr_maintenance_keeps_caught_up_followers_and_drops_the_rest() {
    use IsrMemberRole::{InSyncFollower, Leader, OutOfSyncFollower, Unassigned};
    let facts =
        |role, log_end_matches_leader, caught_up_within_lag, fetch_within_lag| IsrMemberFacts {
            role,
            log_end_matches_leader,
            caught_up_within_lag,
            fetch_within_lag,
        };
    for (label, member, expected) in [
        ("the leader stays", facts(Leader, false, false, false), true),
        (
            "a reassigned-away member leaves",
            facts(Unassigned, true, true, true),
            false,
        ),
        (
            "an idle follower at the leader's log end stays",
            facts(InSyncFollower, true, false, false),
            true,
        ),
        (
            "a follower that caught up recently stays",
            facts(InSyncFollower, false, true, true),
            true,
        ),
        (
            "a follower fetching but behind for too long leaves",
            facts(InSyncFollower, false, false, true),
            false,
        ),
        (
            "a follower that stopped fetching behind the leader leaves",
            facts(InSyncFollower, false, false, false),
            false,
        ),
        (
            "a follower that fetched and caught up rejoins",
            facts(OutOfSyncFollower, false, true, true),
            true,
        ),
        (
            "a follower caught up long ago but fetching again stays out",
            facts(OutOfSyncFollower, true, false, true),
            false,
        ),
        (
            "a follower that caught up but stopped fetching stays out",
            facts(OutOfSyncFollower, false, true, false),
            false,
        ),
    ] {
        assert2::check!(isr_maintenance_selected(member) == expected, "{label}");
    }
    assert2::check!(!isr_proposal_changed(0, 0));
    assert2::check!(isr_proposal_changed(1, 0));
    assert2::check!(isr_proposal_changed(0, 1));
    assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
}
