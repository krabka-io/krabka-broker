//! Break-glass behavior scenarios use the shared principal-bound fixture.
use {
    crate::behavior::{BreakGlass, glass},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/break_glass/signatures.feature",
    name = "a signed action takes a signature on every approval"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_action_takes_a_signature_on_every_approval(glass: BreakGlass) {
    glass.shutdown().await;
}
