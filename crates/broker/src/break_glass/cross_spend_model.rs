//! Exhaustive stateright enumeration of two proposals that exist at once.
//!
//! [`state_model`](super::state_model) drives one proposal through its whole
//! lifecycle. It cannot see the rule that matters once a cluster holds more
//! than one open proposal: **an approval authorizes the transition it names,
//! and no other.** A model with a single proposal satisfies that rule
//! vacuously, because every consume that succeeds spends the only candidate
//! there is.
//!
//! So this model puts two proposals in one [`MetadataImage`] and lets every
//! interleaving of approve, withdraw, expire and consume run against them. As
//! in the sibling model the transitions call the real decision code --
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) and
//! [`gate::authorize`](crate::break_glass::gate::authorize) -- so a rule the
//! model checks is the rule the broker runs.
//!
//! # The oracle is written by hand
//!
//! Each [`Request`] carries `covered_by`, the set of proposals that may
//! legitimately authorize it. Those flags are written out from KFC-9's
//! target-matching rule rather than computed by calling the broker's own
//! `covers`. Deriving them from the code under test would make the headline
//! property a tautology: the model would only prove that `covers` agrees with
//! itself.
//!
//! # Properties
//!
//! - `no_cross_spend`: no interleaving spends a proposal on a request that
//!   proposal does not cover. This is what one proposal cannot test.
//! - `no_double_spend`: each proposal is spent at most once, now stated per
//!   proposal rather than over a single counter.
//! - `no_under_approved`: no spend of a proposal that fewer than
//!   `required_approvals` distinct principals approved.
//! - `a_withdrawn_proposal_is_never_consumed`, per proposal.
//!
//! # What these properties do and do not carry
//!
//! `no_cross_spend` and `no_double_spend` are load-bearing here, and that was
//! checked rather than assumed: dropping the `action_targets_partition` limit
//! from `gate::covers` fails `a_topic_that_looks_like_a_partition_of_another`
//! and only that scenario, and dropping the consumed check from
//! `unusable_because` fails all three.
//!
//! `no_under_approved` is carried as a guard rather than independently
//! exercised. Replacing `distinct_approvers` with `approvals.len()` in the gate
//! does not fail this model, because
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) refuses a
//! second approval from a principal who already approved, so the two
//! expressions agree on every state this model can reach. The property still
//! earns its place: it would fail the day a change to `decide` let a duplicate
//! through. Making it load-bearing here would mean injecting an approval list
//! the handler cannot produce, which is a different model than this one.
//!
//! The clock is a small integer of logical milliseconds, bounded exactly as the
//! sibling model bounds it, so the state graph stays exhaustive.

use std::time::Duration;

use krabka_metadata::{
    BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord, MetadataImage, MetadataRecord,
};
use krabka_units::millis;
use stateright::{Checker, Model, Property};
use uuid::Uuid;

use crate::{
    break_glass::{
        config::BreakGlassPolicy,
        gate,
        handlers::approve::{self, Attempt},
    },
    config::BreakGlassConfig,
    operator_keys::OperatorKeys,
};

/// The logical millisecond at which both proposals expire.
const EXPIRES_AT: i64 = 2;

/// How many proposals the image holds. Two is the smallest number that can
/// express a cross-spend, and each extra proposal multiplies the state graph.
const PROPOSALS: usize = 2;

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 12;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// One proposal the model holds fixed across the run.
#[derive(Clone, Copy, Debug)]
struct ProposalSpec {
    /// Distinguishes the stored records, and breaks `better_candidate` ties.
    id: u128,
    action: BreakGlassAction,
    target: &'static str,
    /// The principal that opened it. That principal cannot approve *this*
    /// proposal, and may approve the other one.
    proposer: &'static str,
}

/// One transition a gated handler might ask the gate to authorize.
#[derive(Clone, Copy, Debug)]
struct Request {
    action: BreakGlassAction,
    target: &'static str,
    /// The proposals that may legitimately authorize this request, written out
    /// from KFC-9's rule rather than from the code under test. See the module
    /// doc.
    covered_by: [bool; PROPOSALS],
}

/// One proposal, projected onto the fields a transition reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ProposalState {
    /// The approving principals, in the order they approved.
    approvals: Vec<&'static str>,
    /// `true` once an operator withdrew the proposal.
    withdrawn: bool,
    /// `true` once a transition consumed the proposal.
    consumed: bool,
}

/// Both proposals, the clock, and what the run has observed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Universe {
    proposals: Vec<ProposalState>,
    /// The logical clock, in milliseconds.
    now_ms: i64,
    /// How many times each proposal was spent. `no_double_spend` reads it.
    spends: Vec<u8>,
    /// `true` once a spend landed on a proposal that did not cover the
    /// request. The headline property reads it.
    cross_spent: bool,
    /// `true` once a spend succeeded with too few distinct approvers.
    under_approved: bool,
}

/// One step an operator or the controller can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    /// One principal approves one proposal.
    Approve(usize, &'static str),
    /// One principal withdraws one proposal.
    Withdraw(usize, &'static str),
    /// The clock advances by one millisecond.
    Expire,
    /// A gated transition tries to spend whatever covers one request.
    Consume(usize),
}

struct CrossSpendModel {
    config: BreakGlassConfig,
    proposals: [ProposalSpec; PROPOSALS],
    requests: Vec<Request>,
    /// Every principal that can send a request, inside the approver set and
    /// outside it.
    principals: Vec<&'static str>,
}

/// How many different principals appear in `approvals`.
fn distinct(approvals: &[&'static str]) -> usize {
    let mut seen: Vec<&str> = Vec::with_capacity(approvals.len());
    for principal in approvals {
        if !seen.contains(principal) {
            seen.push(principal);
        }
    }
    seen.len()
}

impl CrossSpendModel {
    fn policy(&self) -> BreakGlassPolicy<'_> {
        BreakGlassPolicy::new(&self.config)
    }

    /// The stored record that one proposal's model state stands for.
    fn record(&self, index: usize, state: &Universe) -> BreakGlassProposalRecord {
        let spec = self.proposals[index];
        let proposal = &state.proposals[index];
        BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(spec.id),
            action: spec.action,
            target: spec.target.to_owned(),
            proposer: spec.proposer.to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: 0,
            expires_at_ms: EXPIRES_AT,
            approvals: proposal
                .approvals
                .iter()
                .map(|principal| BreakGlassApproval {
                    principal: (*principal).to_owned(),
                    approved_at_ms: 0,
                    key_id: String::new(),
                    signature: Vec::new(),
                })
                .collect(),
            // `0` is the unconsumed sentinel.
            consumed_at_ms: i64::from(proposal.consumed),
            withdrawn: proposal.withdrawn,
        }
    }

    /// The image a gated handler reads, carrying both proposals at once.
    fn image_of(&self, state: &Universe) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for index in 0..PROPOSALS {
            image.apply(&MetadataRecord::V1BreakGlassProposal(
                self.record(index, state),
            ));
        }
        image
    }

    /// Which proposal a returned record names.
    fn index_of(&self, id: Uuid) -> Option<usize> {
        self.proposals
            .iter()
            .position(|spec| Uuid::from_u128(spec.id) == id)
    }

    /// Apply one approval or one withdrawal through the real handler decision.
    fn settle(&self, state: &mut Universe, index: usize, principal: &'static str, withdraw: bool) {
        let stored = self.record(index, state);
        let attempt = Attempt {
            principal,
            key_id: "",
            signature: &[],
            withdraw,
            now_ms: state.now_ms,
        };
        if let Ok(updated) =
            approve::decide(self.policy(), &OperatorKeys::default(), &stored, &attempt)
        {
            let proposal = &mut state.proposals[index];
            proposal.withdrawn = updated.withdrawn;
            proposal.approvals = updated
                .approvals
                .iter()
                .map(|approval| {
                    self.principals
                        .iter()
                        .copied()
                        .find(|name| *name == approval.principal)
                        .expect("an approval names a principal of the model universe")
                })
                .collect();
        }
    }

    /// Ask the real gate to authorize one request against both proposals.
    fn consume(&self, state: &mut Universe, request_index: usize) {
        let request = self.requests[request_index];
        let image = self.image_of(state);
        let Ok(MetadataRecord::V1BreakGlassProposal(spent)) = gate::authorize(
            &image,
            &self.config,
            request.action,
            request.target,
            state.now_ms,
        ) else {
            return;
        };
        let index = self
            .index_of(spent.proposal_id)
            .expect("the gate spent a proposal that the model put in the image");

        state.spends[index] = state.spends[index].saturating_add(1);
        if !request.covered_by[index] {
            state.cross_spent = true;
        }
        if distinct(&state.proposals[index].approvals) < self.policy().required_approvals() {
            state.under_approved = true;
        }
        state.proposals[index].consumed = true;
    }
}

impl Model for CrossSpendModel {
    type State = Universe;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Universe {
            proposals: vec![
                ProposalState {
                    approvals: Vec::new(),
                    withdrawn: false,
                    consumed: false,
                };
                PROPOSALS
            ],
            now_ms: 0,
            spends: vec![0; PROPOSALS],
            cross_spent: false,
            under_approved: false,
        }]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        for index in 0..PROPOSALS {
            for principal in &self.principals {
                actions.push(Step::Approve(index, principal));
                actions.push(Step::Withdraw(index, principal));
            }
        }
        actions.push(Step::Expire);
        for request in 0..self.requests.len() {
            actions.push(Step::Consume(request));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Step::Approve(index, principal) => self.settle(&mut state, index, principal, false),
            Step::Withdraw(index, principal) => self.settle(&mut state, index, principal, true),
            Step::Expire => state.now_ms = (state.now_ms + 1).min(EXPIRES_AT),
            Step::Consume(request) => self.consume(&mut state, request),
        }
        // Headline safety, per transition, so a counterexample names the step
        // that broke it rather than surfacing at the end of the run.
        assert2::assert!(
            !state.cross_spent,
            "a proposal was spent on a request it does not cover after {action:?}: {state:?}"
        );
        assert2::assert!(
            state.spends.iter().all(|spent| *spent <= 1),
            "a proposal was spent twice after {action:?}: {state:?}"
        );
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_cross_spend", |_, state: &Universe| !state.cross_spent),
            Property::always("no_double_spend", |_, state: &Universe| {
                state.spends.iter().all(|spent| *spent <= 1)
            }),
            Property::always("no_under_approved", |_, state: &Universe| {
                !state.under_approved
            }),
            Property::always(
                "a_withdrawn_proposal_is_never_consumed",
                |_, state: &Universe| {
                    state
                        .proposals
                        .iter()
                        .all(|proposal| !(proposal.withdrawn && proposal.consumed))
                },
            ),
            // Non-vacuity witnesses. Without them a gate that refused every
            // request would satisfy every safety property above.
            Property::sometimes("first_spent", |_, state: &Universe| state.spends[0] == 1),
            Property::sometimes("second_spent", |_, state: &Universe| state.spends[1] == 1),
            Property::sometimes("both_spent", |_, state: &Universe| {
                state.spends.iter().all(|spent| *spent == 1)
            }),
            // The state a cross-spend would need: one proposal fully approved
            // and unspent while the other is the one a request names. If this
            // never held, `no_cross_spend` would pass for want of opportunity.
            Property::sometimes(
                "one_approved_while_the_other_is_requested",
                |model: &CrossSpendModel, state: &Universe| {
                    state.proposals.iter().enumerate().any(|(index, proposal)| {
                        distinct(&proposal.approvals) >= model.config.required_approvals
                            && !proposal.consumed
                            && model
                                .requests
                                .iter()
                                .any(|request| !request.covered_by[index])
                    })
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.now_ms <= EXPIRES_AT
            && state
                .proposals
                .iter()
                .all(|proposal| proposal.approvals.len() <= self.principals.len())
    }
}

fn config(approvers: &[&str], required_approvals: usize) -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: approvers.iter().map(|name| (*name).to_owned()).collect(),
        required_approvals,
        proposal_ttl: millis(u32::try_from(EXPIRES_AT).expect("a small logical expiry")),
        signed_actions: Vec::new(),
        ..BreakGlassConfig::default()
    }
}

fn run(model: CrossSpendModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated, so the run is not exhaustive"
    );
    assert2::assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded"
    );
    checker.assert_properties();
}

/// The approver set both scenarios use. `User:alice` and `User:bob` each open
/// one proposal, so each is barred from their own and free on the other.
const APPROVERS: [&str; 3] = ["User:alice", "User:bob", "User:carol"];

/// Two proposals for the same action on different topics.
///
/// This is the plain cross-spend shape: an approved proposal to delete
/// `orders` must never authorize deleting `payments`, however the approvals
/// interleave.
#[test]
fn one_action_on_two_topics() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteTopic,
                    target: "orders",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteTopic,
                    target: "payments",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "orders",
                    covered_by: [true, false],
                },
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "payments",
                    covered_by: [false, true],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "one_action_on_two_topics",
    );
}

/// A topic whose name reads as a partition of the other.
///
/// `DeleteTopic` does not name a partition, so a proposal on `logs` must not
/// reach the topic `logs-2024` -- the hazard `gate::covers` limits the wider
/// rule to avoid. Modelling it here means an interleaving, not one unit case,
/// has to respect the limit.
#[test]
fn a_topic_that_looks_like_a_partition_of_another() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs-2024",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs",
                    covered_by: [true, false],
                },
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs-2024",
                    covered_by: [false, true],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "a_topic_that_looks_like_a_partition_of_another",
    );
}

/// Two proposals that both cover the same request.
///
/// `DeleteRecords` names a partition, so a proposal on the bare topic `logs`
/// covers `logs-7`, and so does a proposal on `logs-7` itself. Both are
/// legitimate candidates, which is the case `better_candidate` exists to
/// settle. What must hold is that one consume spends exactly one of them, and
/// that neither is spent twice across the whole interleaving.
#[test]
fn two_proposals_that_both_cover_one_partition() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-7",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-7",
                    covered_by: [true, true],
                },
                // Only the bare-topic proposal reaches a different partition.
                Request {
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-3",
                    covered_by: [true, false],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "two_proposals_that_both_cover_one_partition",
    );
}
