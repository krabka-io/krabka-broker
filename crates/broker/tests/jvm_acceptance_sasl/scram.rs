//! Authenticated record workflows specified in the shared SASL feature.
use {
    crate::authenticated_console::{Authentication, authentication, shutdown_authentication},
    rstest_bdd_macros::scenario,
};

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_records.feature",
    name = "Provisioned SCRAM-SHA-512 credentials authenticate a complete JVM record round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_sasl_scram_sha512_produce_consume(authentication: Authentication) {
    shutdown_authentication(authentication).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_records.feature",
    name = "Provisioned SCRAM-SHA-256 credentials authenticate a complete JVM record round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_sasl_scram_sha256_produce_consume(authentication: Authentication) {
    shutdown_authentication(authentication).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_authenticated_records.feature",
    name = "The JVM producer continues writing across multiple SCRAM reauthentication windows"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_sasl_scram_sha512_in_band_reauth_under_max_reauth_window(
    authentication: Authentication,
) {
    shutdown_authentication(authentication).await;
}
