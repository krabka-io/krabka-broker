use super::*;
#[cfg(test)]
use std::vec;

#[cfg(test)]
#[rstest_bdd_macros::then(
    "isr tests caught up credit follows kafkas fetch state update: caught up credit follows kafkas fetch state update for {example}"
)]
fn bdd_verify_caught_up_credit_follows_kafkas_fetch_state_update(example: String) {
    type Example = (i64, i64, CaughtUpCredit);
    let (fetch_offset, last_fetch_leader_log_end, expected): Example = match example.as_str() {
        "reaches current log end" => (120, 100, CaughtUpCredit::ThisFetch),
        "reaches previous log end" => (100, 100, CaughtUpCredit::PreviousFetch),
        "below both log ends" => (99, 100, CaughtUpCredit::Unchanged),
        "first fetch after leadership change" => (0, -1, CaughtUpCredit::PreviousFetch),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(
        follower_caught_up_credit(fetch_offset, 120, last_fetch_leader_log_end) == expected
    );
}

/// `Replica.updateFetchStateOrThrow` over a leader whose log end was 100
/// at the follower's previous fetch and is 120 now.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/isr/tests/caught_up_credit_follows_kafkas_fetch_state_update.feature",
    name = "caught up credit follows kafkas fetch state update"
)]
fn caught_up_credit_follows_kafkas_fetch_state_update(example: String) { let _ = example; }

/// `Partition.getOutOfSyncReplicas` for the in-sync rows; krabka's
/// expansion rule for the out-of-sync rows.
fn member_facts(
    role: IsrMemberRole,
    log_end_matches_leader: bool,
    caught_up_within_lag: bool,
    fetch_within_lag: bool,
) -> IsrMemberFacts {
    IsrMemberFacts {
        role,
        log_end_matches_leader,
        caught_up_within_lag,
        fetch_within_lag,
    }
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "isr tests caught up credit follows kafkas fetch state update: isr maintenance keeps caught up followers and drops the rest for {example}"
)]
fn bdd_verify_isr_maintenance_keeps_caught_up_followers_and_drops_the_rest(example: String) {
    type Example<'a> = (&'a str, IsrMemberFacts, bool);
    let (label, member, expected): Example<'_> = match example.as_str() {
        "leader" => (
            "the leader stays",
            member_facts(IsrMemberRole::Leader, false, false, false),
            true,
        ),
        "reassigned away" => (
            "a reassigned-away member leaves",
            member_facts(IsrMemberRole::Unassigned, true, true, true),
            false,
        ),
        "idle at leader end" => (
            "an idle follower at the leader's log end stays",
            member_facts(IsrMemberRole::InSyncFollower, true, false, false),
            true,
        ),
        "recently caught up" => (
            "a follower that caught up recently stays",
            member_facts(IsrMemberRole::InSyncFollower, false, true, true),
            true,
        ),
        "fetching behind" => (
            "a follower fetching but behind for too long leaves",
            member_facts(IsrMemberRole::InSyncFollower, false, false, true),
            false,
        ),
        "stopped fetching" => (
            "a follower that stopped fetching behind the leader leaves",
            member_facts(IsrMemberRole::InSyncFollower, false, false, false),
            false,
        ),
        "out of sync caught up" => (
            "a follower that fetched and caught up rejoins",
            member_facts(IsrMemberRole::OutOfSyncFollower, false, true, true),
            true,
        ),
        "old catchup fetching" => (
            "a follower caught up long ago but fetching again stays out",
            member_facts(IsrMemberRole::OutOfSyncFollower, true, false, true),
            false,
        ),
        "old catchup stopped" => (
            "a follower that caught up but stopped fetching stays out",
            member_facts(IsrMemberRole::OutOfSyncFollower, false, true, false),
            false,
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(isr_maintenance_selected(member) == expected, "{label}");
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/isr/tests/caught_up_credit_follows_kafkas_fetch_state_update.feature",
    name = "isr maintenance keeps caught up followers and drops the rest"
)]
fn isr_maintenance_keeps_caught_up_followers_and_drops_the_rest(example: String) {
    let _ = example;
}
