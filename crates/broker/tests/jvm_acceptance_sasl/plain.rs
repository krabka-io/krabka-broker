//! Authenticated record workflows specified in the shared SASL feature.
use {
    crate::authenticated_console::{Authentication, authentication, shutdown_authentication},
    rstest_bdd_macros::scenario,
};

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_records.feature",
    name = "The stock PLAIN client creates produces and consumes under its configured identity"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_sasl_plain_produce_consume(authentication: Authentication) {
    shutdown_authentication(authentication).await;
}
