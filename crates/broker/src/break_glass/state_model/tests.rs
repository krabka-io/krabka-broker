use super::*;

#[test]
fn two_approvals_of_three_approvers() {
    // The default rule. `User:mallory` is outside the approver set, and
    // `User:alice` proposed, so neither can supply an approval.
    run(
        BreakGlassModel {
            config: config(&["User:alice", "User:bob", "User:carol"], 2),
            principals: vec!["User:alice", "User:bob", "User:carol", "User:mallory"],
        },
        "two_approvals_of_three_approvers",
        PINNED_UNIQUE_STATES_TWO_OF_THREE,
    );
}

#[test]
fn three_approvals_of_four_approvers() {
    // A stricter rule, so an interleaving needs three different people before
    // a consume can succeed.
    run(
        BreakGlassModel {
            config: config(&["User:alice", "User:bob", "User:carol", "User:dave"], 3),
            principals: vec!["User:alice", "User:bob", "User:carol", "User:dave"],
        },
        "three_approvals_of_four_approvers",
        PINNED_UNIQUE_STATES_THREE_OF_FOUR,
    );
}
