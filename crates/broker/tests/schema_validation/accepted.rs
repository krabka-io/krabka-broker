//! Schema-bound values and tombstones are accepted and exercise the cache.

use {
    crate::harness::{SchemaScenario, schema},
    rstest_bdd_macros::scenario,
};

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/accepted.feature",
    name = "a record framed with a bound schema id is accepted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_framed_with_a_bound_schema_id_is_accepted(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/accepted.feature",
    name = "the cache counters move on a validated produce"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cache_counters_move_on_a_validated_produce(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/accepted.feature",
    name = "a tombstone is accepted on a validated topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tombstone_is_accepted_on_a_validated_topic(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}
