//! Tests for the approve/withdraw decision.
//!
//! These cover the record a first approval builds. The order approvals keep,
//! and the three checks that keep the rule a two-person rule, are checked over
//! the wire, in `tests/features/migrated/tests/break_glass/two_person_rule.feature`.
//! The child modules cover the proposal lifecycle, the operator signature, and
//! the counts one request reports.

use {
    super::*,
    crate::{
        break_glass::gate::tests::{NOW_MS, approval, config, proposal},
        operator_keys::OperatorKeys,
    },
    assert2::check,
    krabka_metadata::{BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord},
};

fn attempt(principal: &str) -> Attempt<'_> {
    Attempt {
        principal,
        key_id: "",
        signature: &[],
        withdraw: false,
        now_ms: NOW_MS,
    }
}

fn pending() -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        approvals: Vec::new(),
        ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
    }
}

fn approved_by_bob() -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        approvals: vec![approval("User:bob")],
        ..pending()
    }
}

#[test]
fn a_second_principal_adds_an_approval_to_the_list() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = pending();

    let updated = decide(
        policy,
        &OperatorKeys::default(),
        &stored,
        &attempt("User:bob"),
    )
    .expect("a second approver may approve");

    let expected = BreakGlassProposalRecord {
        approvals: vec![BreakGlassApproval {
            principal: "User:bob".to_owned(),
            approved_at_ms: NOW_MS,
            key_id: String::new(),
            signature: Vec::new(),
        }],
        ..stored
    };
    check!(updated == expected);
}

// ── the proposal lifecycle ───────────────────────────────────────
//
// The states that take no approval, the expiry boundary, and the withdrawal
// that an expired proposal still takes.
mod lifecycle;

// ── the counts one settled request reports ───────────────────────
//
// The response body and the audit event read the same report, and the audit
// phase follows the outcome and the withdraw flag.
mod reporting;

// ── the operator signature ───────────────────────────────────────
//
// The actions that need a detached signature, and the trust set that binds a
// key id to the approving principal.
mod signatures;
