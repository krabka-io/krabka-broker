//! The break-glass steps: opening, approving and withdrawing a proposal, and
//! what the cluster holds afterwards.

use {
    super::{
        driver,
        params::{KeySource, Lifetime, Operator, Outcome},
        transcript::Run,
        world::GuardScenario,
    },
    assert2::assert,
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
async fn approvals(guard: &mut GuardScenario) -> Vec<Approval> {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    cluster
        .proposal(proposal)
        .await
        .approvals
        .iter()
        .map(Approval::from)
        .collect()
}

/// `operator` opens a proposal to delete the doomed topic, asking for
/// `lifetime`, and the scenario remembers what the tool reported and the
/// proposal it opened.
async fn propose(guard: &mut GuardScenario, operator: Operator, lifetime: &Lifetime) {
    let cluster = guard.cluster().await;
    let (run, proposal) = cluster.propose(operator, ACTION, TARGET, lifetime).await;
    guard.record(run);
    if let Some(proposal) = proposal {
        guard.proposal.set(proposal);
    }
}

/// `operator` approves the scenario's proposal without signing it, and the
/// scenario remembers what the tool reported.
async fn try_approve(guard: &mut GuardScenario, operator: Operator) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    let run = cluster.approve(operator, proposal, None).await;
    guard.record(run);
}

#[given("{operator} has proposed deleting the doomed topic")]
async fn has_proposed(guard: &mut GuardScenario, operator: Operator) {
    let cluster = guard.cluster().await;
    let (run, proposal) = cluster
        .propose(operator, ACTION, TARGET, &Lifetime::Default)
        .await;
    assert!(run.outcome == Outcome::Success);
    guard
        .proposal
        .set(proposal.expect("the propose opened a proposal"));
}

#[given("{operator} has approved the proposal")]
async fn has_approved(guard: &mut GuardScenario, operator: Operator) {
    let run = act_on_proposal(guard, operator, ProposalAct::Approve).await;
    assert!(run.outcome == Outcome::Success);
}

#[given("{operator} has withdrawn the proposal")]
async fn has_withdrawn(guard: &mut GuardScenario, operator: Operator) {
    let run = act_on_proposal(guard, operator, ProposalAct::Withdraw).await;
    assert!(run.outcome == Outcome::Success);
}

/// What an operator does to the scenario's proposal in a Given.
#[derive(Clone, Copy)]
enum ProposalAct {
    Approve,
    Withdraw,
}

/// `operator` approves or withdraws the scenario's proposal, unsigned.
async fn act_on_proposal(guard: &mut GuardScenario, operator: Operator, act: ProposalAct) -> Run {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    async {
        match act {
            ProposalAct::Approve => cluster.approve(operator, proposal, None).await,
            ProposalAct::Withdraw => cluster.withdraw(operator, proposal).await,
        }
    }
    .await
}

#[given("the cluster caps a proposal's lifetime at {cap}")]
fn caps_proposal_lifetime(guard: &mut GuardScenario, cap: String) {
    let cap = parse::time(&cap).expect("the cap is a time with a unit");
    guard.cap_proposal_lifetime(cap);
}

#[when("{operator} approves the proposal signed with {key}")]
async fn approves_signed(guard: &mut GuardScenario, operator: Operator, key: KeySource) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    let run = cluster.approve(operator, proposal, Some(key)).await;
    guard.record(run);
}

#[when("{operator} tries to approve the proposal")]
async fn tries_to_approve(guard: &mut GuardScenario, operator: Operator) {
    try_approve(guard, operator).await;
}

#[when("{operator} tries to approve the proposal again")]
async fn tries_to_approve_again(guard: &mut GuardScenario, operator: Operator) {
    try_approve(guard, operator).await;
}

/// The approver runs the shipped entry point, connect step included, rather
/// than the scenario's authenticated client.
#[when("{operator} approves the proposal by running krabka-guard as himself")]
async fn approves_through_binary(
    guard: &mut GuardScenario,
    // The shipped tool has no way to say who is running it, which is the gap
    // the scenario keeps open.
    _operator: Operator,
) {
    let proposal = guard.proposal.get().to_string();
    let bootstrap = guard.cluster().await.bootstrap();
    let run = driver::run_binary(&bootstrap, &[
        "break-glass",
        "approve",
        "--proposal",
        &proposal,
    ])
    .await;
    guard.record(run);
}

#[when("{operator} proposes deleting the doomed topic for {lifetime}")]
async fn proposes_for(guard: &mut GuardScenario, operator: Operator, lifetime: Lifetime) {
    propose(guard, operator, &lifetime).await;
}

#[when("{operator} tries to propose deleting the doomed topic for {lifetime}")]
async fn tries_to_propose_for(guard: &mut GuardScenario, operator: Operator, lifetime: Lifetime) {
    propose(guard, operator, &lifetime).await;
}

#[then("the proposal holds 1 approval, signed by {operator}")]
async fn holds_one_signed_approval(guard: &mut GuardScenario, operator: Operator) {
    assert!(approvals(guard).await == vec![Approval::signed_by(operator)]);
}

#[then("the proposal holds 1 approval, from {operator}")]
async fn holds_one_unsigned_approval(guard: &mut GuardScenario, operator: Operator) {
    assert!(approvals(guard).await == vec![Approval::unsigned_from(operator)]);
}

#[then("the proposal holds no approval")]
async fn holds_no_approval(guard: &mut GuardScenario) {
    assert!(approvals(guard).await == Vec::new());
}

#[then("the proposal expires {expiry} after it opened")]
async fn expires_after(guard: &mut GuardScenario, expiry: String) {
    let expiry = parse::time(&expiry).expect("the expiry is a time with a unit");
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    let stored = cluster.proposal(proposal).await;
    assert!(stored.expires_at_ms - stored.created_at_ms == expiry.millis_i64());
}

#[then("the tool refuses the command line")]
fn refuses_command_line(guard: &GuardScenario) {
    assert!(*guard.outcome.get() == Outcome::CommandLineRefused);
}

#[then("the cluster holds no proposal")]
async fn holds_no_proposal(guard: &mut GuardScenario) {
    let cluster = guard.cluster().await;
    assert!(cluster.proposals().await == Vec::new());
}
