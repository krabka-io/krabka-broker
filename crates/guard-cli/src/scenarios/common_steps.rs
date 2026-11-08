//! The steps more than one capability uses: a freeze in place, a thaw
//! proposal, the tool's outcome, and an empty registry.

use {
    super::{
        driver::Signing,
        params::{Lifetime, Operator, Outcome, Scope},
        world::GuardScenario,
    },
    assert2::assert,
    rstest_bdd_macros::{given, then},
};

#[given("{operator} has frozen {scope}")]
async fn has_frozen(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    freeze_succeeds(guard, operator, &scope).await;
}

/// `operator` freezes `scope` unsigned, and the freeze lands.
pub async fn freeze_succeeds(guard: &mut GuardScenario, operator: Operator, scope: &Scope) {
    let cluster = guard.cluster().await;
    assert!(
        cluster
            .freeze(operator, scope, Signing::Unsigned)
            .await
            .outcome
            == Outcome::Success
    );
}

/// The proposer opens a thaw proposal and the other approvers, in order,
/// approve it until it holds `approvals`.
#[given("{operator} holds a proposal to thaw {target} with {approvals} approvals")]
async fn holds_thaw_proposal(
    guard: &mut GuardScenario,
    operator: Operator,
    target: String,
    approvals: usize,
) {
    let cluster = guard.cluster().await;
    let proposal = async {
        let (run, proposal) = cluster
            .propose(operator, "thaw-topic-freeze", &target, &Lifetime::Default)
            .await;
        assert!(run.outcome == Outcome::Success);
        let proposal = proposal.expect("the propose opened a proposal");
        let approvers = Operator::ALL
            .into_iter()
            .filter(|approver| *approver != operator);
        for approver in approvers.take(approvals) {
            let run = cluster.approve(approver, proposal, None).await;
            assert!(run.outcome == Outcome::Success);
        }
        proposal
    }
    .await;
    guard.proposal.set(proposal);
}

#[then("the tool reports {outcome}")]
fn tool_reports(guard: &GuardScenario, outcome: Outcome) {
    assert!(*guard.outcome.get() == outcome);
}

#[then("the registry holds no freeze")]
async fn registry_holds_no_freeze(guard: &mut GuardScenario) {
    let cluster = guard.cluster().await;
    assert!(cluster.registry().await == Vec::new());
}
