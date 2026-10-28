//! Break-glass behavior scenarios use the shared principal-bound fixture.
use {
    crate::behavior::{BreakGlass, glass},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/break_glass/expiry.feature",
    name = "an expired proposal authorizes nothing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_proposal_authorizes_nothing(glass: BreakGlass) { glass.shutdown().await; }
