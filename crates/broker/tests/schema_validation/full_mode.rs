//! Full validation decodes the Avro body while ID validation checks its binding.

use {
    crate::harness::{
        KNOWN_ID, SchemaScenario, create_topic, framed, order_avro_body, produce_value, schema,
    },
    rstest_bdd_macros::{given, scenario, when},
};

#[given("validated-full checks the complete schema body")]
async fn full_validation_topic(schema: &mut SchemaScenario) {
    let id = create_topic()
        .broker(schema.broker())
        .client(schema.client())
        .name("validated-full")
        .configs(&[
            ("schema.validation.value", "true"),
            ("schema.validation.mode", "full"),
        ])
        .call()
        .await;
    schema.topics.insert("validated-full".to_owned(), id);
}

#[given("the record names schema 42 but contains six invalid Avro bytes")]
fn invalid_avro_body(schema: &SchemaScenario) {
    schema
        .payload
        .set(Some(framed(KNOWN_ID, b"\xff\xff\xff\xff\xff\xff")));
}

#[when("the producer sends a valid Avro Order to validated-full")]
async fn valid_order_body(schema: &mut SchemaScenario) {
    let out = produce_value(
        schema.client(),
        "validated-full",
        schema.topic("validated-full"),
        Some(framed(KNOWN_ID, &order_avro_body())),
    )
    .await;
    schema.outcomes.insert("validated-full".to_owned(), out);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/full_mode.feature",
    name = "full mode checks the body and id mode does not"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_mode_checks_the_body_and_id_mode_does_not(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}
