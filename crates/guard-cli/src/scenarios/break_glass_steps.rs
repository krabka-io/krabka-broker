//! The break-glass steps: opening, approving and withdrawing a proposal, and
//! what the cluster holds afterwards.

use {
    super::{
        driver,
        params::{KeySource, Lifetime, Operator, Outcome},
        world::GuardScenario,
    },
    assert2::assert,
    krabka_bdd::TokioMultiThreadContext,
    krabka_protocol::krabka::break_glass::BreakGlassApproval,
    krabka_units::{convert::TimeExt as _, parse},
    rstest_bdd_macros::{given, then, when},
};

/// The action every proposal in these scenarios gates.
const ACTION: &str = "delete-topic";

/// The topic every proposal in these scenarios names.
const TARGET: &str = "doomed";

/// One approval, reduced to what the scenarios say about it.
#[derive(Debug, PartialEq, Eq)]
struct Approval {
    principal: String,
    key_id: String,
    signed: bool,
}

impl Approval {
    /// An approval `operator` signed with their own key.
    fn signed_by(operator: Operator) -> Self {
        Self {
            principal: operator.principal(),
            key_id: operator.key_id(),
            signed: true,
        }
    }

    /// An unsigned approval `operator` gave.
    fn unsigned_from(operator: Operator) -> Self {
        Self {
            principal: operator.principal(),
            key_id: String::new(),
            signed: false,
        }
    }
}

impl From<&BreakGlassApproval> for Approval {
    fn from(approval: &BreakGlassApproval) -> Self {
        Self {
            principal: approval.principal.clone(),
            key_id: approval.key_id.clone(),
            signed: !approval.signature.is_empty(),
        }
    }
}

/// Every approval the scenario's proposal holds.
fn approvals(guard: &mut GuardScenario, runtime: &TokioMultiThreadContext) -> Vec<Approval> {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    runtime
        .block_on(cluster.proposal(proposal))
        .approvals
        .iter()
        .map(Approval::from)
        .collect()
}

/// `operator` opens a proposal to delete the doomed topic, asking for
/// `lifetime`, and the scenario remembers what the tool reported and the
/// proposal it opened.
fn propose(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    operator: Operator,
    lifetime: &Lifetime,
) {
    let cluster = guard.cluster(runtime);
    let (outcome, proposal) = runtime.block_on(cluster.propose(operator, ACTION, TARGET, lifetime));
    guard.outcome.set(outcome);
    if let Some(proposal) = proposal {
        guard.proposal.set(proposal);
    }
}

/// `operator` approves the scenario's proposal without signing it, and the
/// scenario remembers what the tool reported.
fn try_approve(guard: &mut GuardScenario, runtime: &TokioMultiThreadContext, operator: Operator) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    let outcome = runtime.block_on(cluster.approve(operator, proposal, None));
    guard.outcome.set(outcome);
}

#[given("{operator} has proposed deleting the doomed topic")]
fn has_proposed(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    let cluster = guard.cluster(runtime);
    let (outcome, proposal) =
        runtime.block_on(cluster.propose(operator, ACTION, TARGET, &Lifetime::Default));
    assert!(outcome == Outcome::Success);
    guard
        .proposal
        .set(proposal.expect("the propose opened a proposal"));
}

#[given("{operator} has approved the proposal")]
fn has_approved(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    assert!(act_on_proposal(guard, runtime, operator, ProposalAct::Approve) == Outcome::Success);
}

#[given("{operator} has withdrawn the proposal")]
fn has_withdrawn(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    assert!(act_on_proposal(guard, runtime, operator, ProposalAct::Withdraw) == Outcome::Success);
}

/// What an operator does to the scenario's proposal in a Given.
#[derive(Clone, Copy)]
enum ProposalAct {
    Approve,
    Withdraw,
}

/// `operator` approves or withdraws the scenario's proposal, unsigned.
fn act_on_proposal(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    operator: Operator,
    act: ProposalAct,
) -> Outcome {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    runtime.block_on(async {
        match act {
            ProposalAct::Approve => cluster.approve(operator, proposal, None).await,
            ProposalAct::Withdraw => cluster.withdraw(operator, proposal).await,
        }
    })
}

#[given("the cluster caps a proposal's lifetime at {cap}")]
fn caps_proposal_lifetime(guard: &mut GuardScenario, cap: String) {
    let cap = parse::time(&cap).expect("the cap is a time with a unit");
    guard.cap_proposal_lifetime(cap);
}

#[when("{operator} approves the proposal signed with {key}")]
fn approves_signed(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    key: KeySource,
) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    let outcome = runtime.block_on(cluster.approve(operator, proposal, Some(key)));
    guard.outcome.set(outcome);
}

#[when("{operator} tries to approve the proposal")]
fn tries_to_approve(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    try_approve(guard, runtime, operator);
}

#[when("{operator} tries to approve the proposal again")]
fn tries_to_approve_again(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    try_approve(guard, runtime, operator);
}

/// The approver runs the shipped entry point, connect step included, rather
/// than the scenario's authenticated client.
#[when("{operator} approves the proposal by running krabka-guard as himself")]
fn approves_through_binary(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    // The shipped tool has no way to say who is running it, which is the gap
    // the scenario keeps open.
    _operator: Operator,
) {
    let proposal = guard.proposal.get().to_string();
    let bootstrap = guard.cluster(runtime).bootstrap();
    let outcome = runtime.block_on(driver::run_binary(&bootstrap, &[
        "break-glass",
        "approve",
        "--proposal",
        &proposal,
    ]));
    guard.outcome.set(outcome);
}

#[when("{operator} proposes deleting the doomed topic for {lifetime}")]
fn proposes_for(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    lifetime: Lifetime,
) {
    propose(guard, runtime, operator, &lifetime);
}

#[when("{operator} tries to propose deleting the doomed topic for {lifetime}")]
fn tries_to_propose_for(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    lifetime: Lifetime,
) {
    propose(guard, runtime, operator, &lifetime);
}

#[then("the proposal holds 1 approval, signed by {operator}")]
fn holds_one_signed_approval(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    assert!(approvals(guard, runtime) == vec![Approval::signed_by(operator)]);
}

#[then("the proposal holds 1 approval, from {operator}")]
fn holds_one_unsigned_approval(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    assert!(approvals(guard, runtime) == vec![Approval::unsigned_from(operator)]);
}

#[then("the proposal holds no approval")]
fn holds_no_approval(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    assert!(approvals(guard, runtime) == Vec::new());
}

#[then("the proposal expires {expiry} after it opened")]
fn expires_after(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    expiry: String,
) {
    let expiry = parse::time(&expiry).expect("the expiry is a time with a unit");
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    let stored = runtime.block_on(cluster.proposal(proposal));
    assert!(stored.expires_at_ms - stored.created_at_ms == expiry.millis_i64());
}

#[then("the tool refuses the command line")]
fn refuses_command_line(guard: &GuardScenario) {
    assert!(*guard.outcome.get() == Outcome::CommandLineRefused);
}

#[then("the cluster holds no proposal")]
fn holds_no_proposal(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = guard.cluster(runtime);
    assert!(runtime.block_on(cluster.proposals()) == Vec::new());
}
