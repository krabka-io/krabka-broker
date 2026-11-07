//! The steps more than one capability uses: a freeze in place, a thaw
//! proposal, the tool's outcome, and an empty registry.

use {
    super::{
        driver::Signing,
        params::{Lifetime, Operator, Outcome, Scope},
        world::GuardScenario,
    },
    assert2::assert,
    krabka_bdd::TokioMultiThreadContext,
    rstest_bdd_macros::{given, then},
};

#[given("{operator} has frozen {scope}")]
fn has_frozen(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    freeze_succeeds(guard, runtime, operator, &scope);
}

/// `operator` freezes `scope` unsigned, and the freeze lands.
pub fn freeze_succeeds(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: &Scope,
) {
    let cluster = guard.cluster(runtime);
    assert!(
        runtime.block_on(cluster.freeze(operator, scope, Signing::Unsigned)) == Outcome::Success
    );
}

/// The proposer opens a thaw proposal and the other approvers, in order,
/// approve it until it holds `approvals`.
#[given("{operator} holds a proposal to thaw {target} with {approvals} approvals")]
fn holds_thaw_proposal(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    target: String,
    approvals: usize,
) {
    let cluster = guard.cluster(runtime);
    let proposal = runtime.block_on(async {
        let (outcome, proposal) = cluster
            .propose(operator, "thaw-topic-freeze", &target, &Lifetime::Default)
            .await;
        assert!(outcome == Outcome::Success);
        let proposal = proposal.expect("the propose opened a proposal");
        let approvers = Operator::ALL
            .into_iter()
            .filter(|approver| *approver != operator);
        for approver in approvers.take(approvals) {
            assert!(cluster.approve(approver, proposal, None).await == Outcome::Success);
        }
        proposal
    });
    guard.proposal.set(proposal);
}

#[then("the tool reports {outcome}")]
fn tool_reports(guard: &GuardScenario, outcome: Outcome) {
    assert!(*guard.outcome.get() == outcome);
}

#[then("the registry holds no freeze")]
fn registry_holds_no_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = guard.cluster(runtime);
    assert!(runtime.block_on(cluster.registry()) == Vec::new());
}
