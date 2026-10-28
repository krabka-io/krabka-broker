//! Delegation-token behavior is exercised through actual typed protocol steps.
use crate::cluster::{TokenWorkflow, tokens, wait_for_token};
#[rstest_bdd_macros::then("renewal is refused with owner mismatch error 63")]
fn unlisted_token_renewal(tokens: &TokenWorkflow) {
    assert2::check!(
        tokens
            .renewed
            .as_ref()
            .expect("renewal attempted")
            .error_code
            == crate::DELEGATION_TOKEN_OWNER_MISMATCH
    );
}
#[rstest_bdd_macros::then("expiry is refused with owner mismatch error 63")]
fn unlisted_token_expiry(tokens: &TokenWorkflow) {
    assert2::check!(
        tokens
            .expired
            .as_ref()
            .expect("expiry attempted")
            .error_code
            == crate::DELEGATION_TOKEN_OWNER_MISMATCH
    );
}
#[rstest_bdd_macros::then("token {alias} retains its original expiry timestamp")]
async fn unauthorized_token_unchanged(tokens: &TokenWorkflow, alias: String) {
    let original = tokens.tokens.get(&alias).expect("named token minted");
    let token = wait_for_token(
        tokens.broker.as_ref().expect("delegation broker started"),
        &original.token_id,
    )
    .await;
    assert2::check!(token.expiry_timestamp_ms == original.expiry_timestamp_ms);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/renewer_gate.feature",
    name = "super user renews and expires only as a listed renewer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn super_user_renews_and_expires_only_as_a_listed_renewer(tokens: TokenWorkflow) {
    tokens.shutdown().await;
}
