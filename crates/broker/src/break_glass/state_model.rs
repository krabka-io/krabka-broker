//! Exhaustive stateright enumeration of one proposal's whole lifecycle.
//!
//! The model drives the REAL decision code. Each transition builds a
//! [`BreakGlassProposalRecord`] out of the model state and calls
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) for an
//! approval and a withdrawal, and
//! [`gate::authorize`](crate::break_glass::gate::authorize) for a consume,
//! against a real [`MetadataImage`]. A rule that the model checks is therefore
//! the rule the broker runs, and not a second copy of it.
//!
//! The alphabet is every interleaving of approve, withdraw, expire, and
//! consume, over a tiny universe of principals. The two headline properties are
//! the promises the feature makes:
//!
//! - `no_double_spend`: no interleaving consumes one proposal twice. One
//!   approval authorizes one transition.
//! - `no_under_approved`: no interleaving consumes a proposal that fewer than
//!   `required_approvals` distinct principals approved. A rule about people
//!   cannot be satisfied by one person acting twice.
//!
//! The clock is a small integer of logical milliseconds. The proposal is
//! created at zero and expires at [`EXPIRES_AT`], and an expire action advances
//! the clock by one. The bound keeps the state graph exhaustive.

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

/// The logical millisecond at which the proposal expires.
const EXPIRES_AT: i64 = 2;

/// The action the model gates. The rule under test does not depend on which
/// one it is, so the model fixes one and varies the people and the clock.
const ACTION: BreakGlassAction = BreakGlassAction::DeleteTopic;

/// The target the proposal names.
const TARGET: &str = "doomed";

/// The principal that opened the proposal. It cannot approve.
const PROPOSER: &str = "User:alice";

const TARGET_STATE_COUNT: usize = 1_000_000;

const MAX_UNIQUE_STATES: usize = 200_000;

const MAX_DEPTH: usize = 20;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_TWO_OF_THREE: usize = 36;

const PINNED_UNIQUE_STATES_THREE_OF_FOUR: usize = 114;

/// One proposal, projected onto the fields a transition reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ProposalState {
    /// The approving principals, in the order they approved.
    approvals: Vec<&'static str>,
    /// `true` once an operator withdrew the proposal.
    withdrawn: bool,
    /// `true` once a transition consumed the proposal.
    consumed: bool,
    /// The logical clock, in milliseconds.
    now_ms: i64,
    /// How many times a consume succeeded. The headline property reads it.
    consumes: u8,
    /// `true` once a consume succeeded with too few distinct approvers. The
    /// second headline property reads it.
    under_approved: bool,
}

/// One step an operator or the controller can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    /// One principal approves the proposal.
    Approve(&'static str),
    /// One principal withdraws the proposal.
    Withdraw(&'static str),
    /// The clock advances by one millisecond.
    Expire,
    /// A gated transition tries to spend the proposal.
    Consume,
}

struct BreakGlassModel {
    config: BreakGlassConfig,
    /// Every principal that can send a request, inside the approver set and
    /// outside it.
    principals: Vec<&'static str>,
}

impl BreakGlassModel {
    fn policy(&self) -> BreakGlassPolicy<'_> {
        BreakGlassPolicy::new(&self.config)
    }

    /// Apply one approval or one withdrawal through the real handler decision.
    fn settle(&self, state: &mut ProposalState, principal: &'static str, withdraw: bool) {
        let stored = record(state);
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
            state.withdrawn = updated.withdrawn;
            state.approvals = updated
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

    /// Try to spend the proposal through the real gate.
    fn consume(&self, state: &mut ProposalState) {
        let image = image_of(state);
        if gate::authorize(&image, &self.config, ACTION, TARGET, state.now_ms).is_ok() {
            state.consumes = state.consumes.saturating_add(1);
            if distinct(&state.approvals) < self.policy().required_approvals() {
                state.under_approved = true;
            }
            state.consumed = true;
        }
    }
}

#[path = "state_model/helpers.rs"]
mod helpers;
use helpers::{config, distinct, image_of, record};

#[path = "state_model/checker.rs"]
mod checker;

#[path = "state_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "state_model/tests.rs"]
mod tests;
