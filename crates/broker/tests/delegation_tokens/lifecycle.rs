//! Delegation-token behavior is exercised through actual typed protocol steps.
use crate::cluster::{TokenWorkflow, tokens};

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/lifecycle.feature",
    name = "delegation token lifecycle end to end"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegation_token_lifecycle_end_to_end(tokens: TokenWorkflow) { tokens.shutdown().await; }
