//! Break-glass behavior scenarios use the shared principal-bound fixture.
use {
    crate::behavior::{BreakGlass, glass},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/break_glass/two_person_rule.feature",
    name = "a proposal two approvals and the transition they authorize"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proposal_two_approvals_and_the_transition_they_authorize(glass: BreakGlass) {
    glass.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/two_person_rule.feature",
    name = "one principal cannot stand in for two"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_principal_cannot_stand_in_for_two(glass: BreakGlass) { glass.shutdown().await; }

#[scenario(
    path = "tests/features/migrated/tests/break_glass/two_person_rule.feature",
    name = "the proposer cannot approve their own proposal"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proposer_cannot_approve_their_own_proposal(glass: BreakGlass) {
    glass.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/two_person_rule.feature",
    name = "a principal outside the approver set is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_outside_the_approver_set_is_refused(glass: BreakGlass) {
    glass.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/two_person_rule.feature",
    name = "an approval joins to the login that made it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_approval_joins_to_the_login_that_made_it(glass: BreakGlass) { glass.shutdown().await; }
