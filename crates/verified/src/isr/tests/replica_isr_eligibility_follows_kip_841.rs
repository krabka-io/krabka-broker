use super::*;
#[cfg(test)]
use std::vec;

/// `Partition.isReplicaIsrEligible` and `isBrokerEpochIsrEligible`.
#[yare::parameterized(
    alive_fetch_epoch = { ELIGIBLE, true },
    fetch_without_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), ..ELIGIBLE },
        true,
    },
    previous_broker_incarnation = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(6), ..ELIGIBLE },
        false,
    },
    no_fetch_for_leader = { IsrEligibilityFacts { fetch_broker_epoch: None, ..ELIGIBLE }, false },
    no_alive_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), alive_broker_epoch: None, ..ELIGIBLE },
        false,
    },
    fenced_broker = { IsrEligibilityFacts { fenced: true, ..ELIGIBLE }, false },
    controlled_shutdown = { IsrEligibilityFacts { shutting_down: true, ..ELIGIBLE }, false },
)]
fn replica_isr_eligibility_follows_kip_841(facts: IsrEligibilityFacts, expected: bool) {
    assert2::check!(replica_isr_eligible(facts) == expected);
}

use {
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

struct IsrCandidate {
    leader_end: i64,
    high_watermark: i64,
    facts: Option<IsrCandidateFacts>,
    selected: Option<bool>,
}

#[fixture]
fn candidate() -> IsrCandidate {
    // Start this scenario with fresh inputs and observations.

    IsrCandidate {
        leader_end: 0,
        high_watermark: 0,
        facts: None,
        selected: None,
    }
}

#[given("an ISR leader with log end {log_end} and committed high watermark {high_watermark}")]
fn candidate_leader(candidate: &mut IsrCandidate, log_end: i64, high_watermark: i64) {
    candidate.leader_end = log_end;
    candidate.high_watermark = high_watermark;
}

#[derive(DataTableRow)]
struct CandidateRow {
    role: String,
    follower_end: i64,
    recently_caught_up: bool,
    leader_epoch_start: String,
    fenced: bool,
}

#[given("this replica is considered for ISR membership:")]
fn replica_candidate(candidate: &mut IsrCandidate, #[datatable] rows: Rows<CandidateRow>) {
    let mut rows = rows.into_iter();
    let row = rows.next().expect("one candidate replica");
    assert2::assert!(rows.next().is_none());
    let role = match row.role.as_str() {
        "leader" => IsrMemberRole::Leader,
        "unassigned" => IsrMemberRole::Unassigned,
        "in-sync" => IsrMemberRole::InSyncFollower,
        "out-of-sync" => IsrMemberRole::OutOfSyncFollower,
        role => panic!("unknown candidate role: {role}"),
    };
    let leader_epoch_start = if row.leader_epoch_start == "unknown" {
        None
    } else {
        Some(
            row.leader_epoch_start
                .parse()
                .expect("leader epoch starts at an offset"),
        )
    };
    candidate.facts = Some(IsrCandidateFacts {
        role,
        follower_log_end: row.follower_end,
        leader_log_end: candidate.leader_end,
        leader_high_watermark: candidate.high_watermark,
        leader_epoch_start,
        caught_up_within_lag: row.recently_caught_up,
        eligibility: IsrEligibilityFacts {
            fenced: row.fenced,
            ..ELIGIBLE
        },
    });
}

#[when("the leader applies ISR shrink and expansion rules")]
fn select_candidate(candidate: &mut IsrCandidate) {
    candidate.selected = Some(isr_candidate_selected(
        candidate.facts.expect("a candidate replica was given"),
    ));
}

#[then("the candidate belongs in the ISR: {selected}")]
fn candidate_membership(candidate: &IsrCandidate, selected: bool) {
    assert2::check!(candidate.selected == Some(selected));
}

scenarios!("tests/features/migrated/src/isr/tests/replica_isr_eligibility_follows_kip_841.feature", fixtures = [candidate: IsrCandidate]);
