//! Mixed stock client vintages observe records across format conversions.
use {
    crate::legacy_round_trip::{RecordExchange, exchange, shutdown},
    rstest_bdd_macros::scenario,
};

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_legacy_record_exchange.feature",
    name = "A modern consumer decodes records from the legacy producer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_legacy_010_produce_modern_consume(exchange: RecordExchange) {
    shutdown(exchange).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_legacy_record_exchange.feature",
    name = "A legacy consumer decodes records from the modern producer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_modern_produce_legacy_010_consume(exchange: RecordExchange) {
    shutdown(exchange).await;
}
