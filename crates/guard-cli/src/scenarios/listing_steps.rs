//! The steps that read the list commands back: what a registry listing says
//! proves each entry, the count it ends with, and the state and approval
//! evidence of each listed proposal.

use {
    super::{
        driver::REASON,
        params::{KeySource, Operator, Outcome, Scope},
        transcript::{ListedApproval, ListedFreeze, Transcript},
        world::GuardScenario,
    },
    assert2::assert,
    rstest_bdd_macros::{given, then, when},
};

/// How many leading signature bytes an approval line shows.
const SIGNATURE_PREFIX: usize = 8;

/// The listing the When wrote, once it reported success.
fn listing(guard: &GuardScenario) -> &Transcript {
    assert!(*guard.outcome.get() == Outcome::Success);
    guard.transcript.get()
}

#[given("{operator} has approved the proposal signed with {key}")]
async fn has_approved_signed(guard: &mut GuardScenario, operator: Operator, key: KeySource) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    let run = cluster.approve(operator, proposal, Some(key)).await;
    assert!(run.outcome == Outcome::Success);
}

#[when("the operator lists the registry without verifying it")]
async fn operator_lists_registry(guard: &mut GuardScenario) {
    let run = guard.cluster().await.list_registry(Operator::Alice).await;
    guard.record(run);
}

#[when("{operator} lists the proposals")]
async fn lists_proposals(guard: &mut GuardScenario, operator: Operator) {
    let run = guard.cluster().await.list_proposals(operator).await;
    guard.record(run);
}

/// Every entry the report lists, with what it says proves it. The registry
/// in these scenarios holds the one entry.
#[then("the report says the freeze on {scope} is {proof}")]
fn report_says_freeze_is(guard: &GuardScenario, scope: Scope, proof: String) {
    let proofs: Vec<_> = guard
        .transcript
        .get()
        .listed_freezes()
        .into_iter()
        .map(|entry| (entry.scope, entry.proof))
        .collect();
    assert!(proofs == vec![(scope.listed(), Some(proof))]);
}

#[then("the report lists the freeze on {scope}, set by {operator}, with no proof")]
fn report_lists_unproved(guard: &GuardScenario, scope: Scope, operator: Operator) {
    let expected = vec![ListedFreeze {
        scope: scope.listed(),
        set_by: operator.principal(),
        reason: REASON.to_owned(),
        proof: None,
    }];
    assert!(listing(guard).listed_freezes() == expected);
}

#[then("the report ends with {summary}")]
fn report_ends_with(guard: &GuardScenario, summary: String) {
    assert!(listing(guard).stdout_lines().last() == Some(&summary.as_str()));
}

/// Every stdout line is a registry entry, so nothing on it reads as a count.
#[then("the report gives no count of proved entries")]
fn report_gives_no_count(guard: &GuardScenario) {
    let transcript = guard.transcript.get();
    assert!(transcript.stdout_lines().len() == transcript.listed_freezes().len());
}

#[then("the tool explains {explanation}")]
fn tool_explains(guard: &GuardScenario, explanation: String) {
    assert!(guard.transcript.get().stderr == format!("{explanation}\n"));
}

#[then("the report shows the proposal as {state}")]
fn report_shows_state(guard: &GuardScenario, state: String) {
    let listed: Vec<_> = listing(guard)
        .listed_proposals()
        .into_iter()
        .map(|proposal| (proposal.id, proposal.state))
        .collect();
    assert!(listed == vec![(*guard.proposal.get(), state)]);
}

/// The approvals the report lists on the scenario's proposal.
fn listed_approvals(guard: &GuardScenario) -> Vec<ListedApproval> {
    let proposal = *guard.proposal.get();
    listing(guard)
        .listed_proposals()
        .into_iter()
        .find(|listed| listed.id == proposal)
        .expect("the report lists the proposal")
        .approvals
}

/// A signed approval shows the first bytes of the signature the broker
/// stored, so an auditor can line it up with the audit event that carries the
/// whole signature.
#[then("the report shows the approval from {operator} as signed by {key_id}")]
async fn report_shows_signed_approval(
    guard: &mut GuardScenario,
    operator: Operator,
    key_id: String,
) {
    let proposal = *guard.proposal.get();
    let stored = guard.cluster().await.proposal(proposal).await;
    let signature = &stored
        .approvals
        .iter()
        .find(|approval| approval.principal == operator.principal())
        .expect("the cluster holds the approval")
        .signature;
    let prefix = hex::encode(signature.get(..SIGNATURE_PREFIX).unwrap_or(signature));
    let expected = vec![ListedApproval {
        principal: operator.principal(),
        evidence: format!("signed by {key_id} ({prefix}...)"),
    }];
    assert!(listed_approvals(guard) == expected);
}

#[then("the report shows the approval from {operator} as unsigned")]
fn report_shows_unsigned_approval(guard: &GuardScenario, operator: Operator) {
    let expected = vec![ListedApproval {
        principal: operator.principal(),
        evidence: "unsigned".to_owned(),
    }];
    assert!(listed_approvals(guard) == expected);
}
