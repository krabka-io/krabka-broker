use {
    super::*,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::vec,
};

struct FollowerIsr {
    current_end: i64,
    previous_end: i64,
    member: Option<IsrMemberFacts>,
    credit: Option<CaughtUpCredit>,
    selected: Option<bool>,
}

#[fixture]
fn follower() -> FollowerIsr {
    // Start this scenario with fresh inputs and observations.

    FollowerIsr {
        current_end: 0,
        previous_end: 0,
        member: None,
        credit: None,
        selected: None,
    }
}

#[given(
    "the leader log now ends at {current_end} after ending at {previous_end} on the previous fetch"
)]
fn leader_ends(follower: &mut FollowerIsr, current_end: i64, previous_end: i64) {
    follower.current_end = current_end;
    follower.previous_end = previous_end;
}

#[when("the follower fetches offset {fetch_offset}")]
fn follower_fetch(follower: &mut FollowerIsr, fetch_offset: i64) {
    follower.credit = Some(follower_caught_up_credit(
        fetch_offset,
        follower.current_end,
        follower.previous_end,
    ));
}

#[then("the follower receives caught-up credit for {credit}")]
fn caught_up_time(follower: &FollowerIsr, credit: String) {
    let expected = match credit.as_str() {
        "this-fetch" => CaughtUpCredit::ThisFetch,
        "the-previous-fetch" => CaughtUpCredit::PreviousFetch,
        "neither-fetch" => CaughtUpCredit::Unchanged,
        _ => panic!("unknown caught-up credit: {credit}"),
    };
    assert2::check!(follower.credit == Some(expected));
}

fn member_role(role: &str) -> IsrMemberRole {
    match role {
        "leader" => IsrMemberRole::Leader,
        "unassigned" => IsrMemberRole::Unassigned,
        "in-sync" => IsrMemberRole::InSyncFollower,
        "out-of-sync" => IsrMemberRole::OutOfSyncFollower,
        _ => panic!("unknown ISR member role: {role}"),
    }
}

#[given(
    "an ISR member with role {role}, matching log end {matching_end}, recent catch-up {recent_catchup}, and recent fetch {recent_fetch}"
)]
fn member_facts(
    follower: &mut FollowerIsr,
    role: String,
    matching_end: bool,
    recent_catchup: bool,
    recent_fetch: bool,
) {
    follower.member = Some(IsrMemberFacts {
        role: member_role(&role),
        log_end_matches_leader: matching_end,
        caught_up_within_lag: recent_catchup,
        fetch_within_lag: recent_fetch,
    });
}

#[when("the leader maintains ISR membership")]
fn maintain_membership(follower: &mut FollowerIsr) {
    follower.selected = Some(isr_maintenance_selected(
        follower.member.expect("an ISR member was given"),
    ));
}

#[then("the member is selected for the ISR: {selected}")]
fn selected_member(follower: &FollowerIsr, selected: bool) {
    assert2::check!(follower.selected == Some(selected));
}

scenarios!("tests/features/migrated/src/isr/tests/caught_up_credit_follows_kafkas_fetch_state_update.feature", fixtures = [follower: FollowerIsr]);
