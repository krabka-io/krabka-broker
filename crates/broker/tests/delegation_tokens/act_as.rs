//! Delegation-token behavior is exercised through actual typed protocol steps.
use crate::cluster::{TokenWorkflow, tokens};
#[rstest_bdd_macros::then("act-as token creation is refused with authorization error 65")]
fn unprivileged_token_act_as(tokens: &TokenWorkflow) {
    assert2::assert!(
        tokens
            .created
            .as_ref()
            .expect("act-as mint attempted")
            .error_code
            == crate::DELEGATION_TOKEN_AUTHORIZATION_FAILED
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/act_as.feature",
    name = "act as super user mints token owned by target"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_super_user_mints_token_owned_by_target(tokens: TokenWorkflow) {
    tokens.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/act_as.feature",
    name = "act as non super user rejected with authorization failed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_non_super_user_rejected_with_authorization_failed(tokens: TokenWorkflow) {
    tokens.shutdown().await;
}
