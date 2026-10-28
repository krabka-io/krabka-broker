//! Invalid records report their index and never reach the partition log.

use {
    crate::harness::{
        INVALID_RECORD, KNOWN_ID, OTHER_SUBJECT_ID, SchemaScenario, UNKNOWN_ID, batch_with_values,
        create_topic, framed, produce, schema,
    },
    assert2::check,
    bytes::Bytes,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[given("the record value has defect {defect}")]
fn invalid_schema_value(schema: &SchemaScenario, defect: String) {
    let value = match defect.as_str() {
        "unframed text" => Bytes::from_static(b"plain text"),
        "bad magic byte" => Bytes::from_static(&[0x01, 0, 0, 0, 42, b'x']),
        "truncated schema id" => Bytes::from_static(&[0x00, 0, 0]),
        "unregistered schema 99" => framed(UNKNOWN_ID, b"anything"),
        "schema 43 bound to another subject" => framed(OTHER_SUBJECT_ID, b"anything"),
        _ => panic!("a defined Confluent-frame defect"),
    };
    schema.payload.set(Some(value));
}

#[then("the invalid record identifies batch index zero and leaves validated empty")]
fn invalid_record_details(schema: &SchemaScenario) {
    let out = &schema.outcomes["validated"];
    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(!out.record_errors.is_empty(), "no per-record error");
    check!(
        out.record_errors[0].batch_index == 0,
        "{:?}",
        out.record_errors
    );
    check!(
        out.record_errors[0]
            .batch_index_error_message
            .as_ref()
            .is_some_and(|message| !message.is_empty()),
        "empty per-record message"
    );
    check!(out.base_offset == -1, "a rejected row named an offset");
    check!(
        schema.broker().local_log_end_offset("validated", 0) == Some(0),
        "a rejected batch reached the log"
    );
}

#[given("the same broker has an unvalidated control topic")]
async fn unvalidated_control(schema: &mut SchemaScenario) {
    let id = create_topic()
        .broker(schema.broker())
        .client(schema.client())
        .name("control")
        .configs(&[])
        .call()
        .await;
    schema.topics.insert("control".to_owned(), id);
}

#[when("the producer sends a batch whose first record is valid and second is unframed")]
async fn mixed_validity_batch(schema: &mut SchemaScenario) {
    let out = produce(
        schema.client(),
        "validated",
        schema.topic("validated"),
        batch_with_values(vec![
            Some(framed(KNOWN_ID, b"fine")),
            Some(Bytes::from_static(b"plain text")),
        ]),
    )
    .await;
    schema.outcomes.insert("validated".to_owned(), out);
}

#[then("the whole batch is rejected and the sole per-record error identifies index one")]
fn second_record_rejected(schema: &SchemaScenario) {
    let out = &schema.outcomes["validated"];
    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(out.record_errors.len() == 1, "{:?}", out.record_errors);
    check!(
        out.record_errors[0].batch_index == 1,
        "{:?}",
        out.record_errors
    );
    check!(schema.broker().local_log_end_offset("validated", 0) == Some(0));
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/rejected.feature",
    name = "records that fail validation are rejected and not appended"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_that_fail_validation_are_rejected_and_not_appended(
    schema: SchemaScenario,
    defect: String,
) {
    let _ = defect;
    schema.shutdown().await;
    drop(schema);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/rejected.feature",
    name = "an unvalidated topic accepts what a validated one rejects"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unvalidated_topic_accepts_what_a_validated_one_rejects(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/rejected.feature",
    name = "the rejected record is named by its index in the batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rejected_record_is_named_by_its_index_in_the_batch(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}
