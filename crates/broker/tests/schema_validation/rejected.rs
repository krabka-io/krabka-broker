//! The rejection cases, each of which asserts the error code the producer sees
//! *and* that the partition's log end offset did not move.
//!
//! The unvalidated control topic runs the same unframed value through the
//! produce path and accepts it, which is what shows the rejection is the
//! topic's configuration rather than a path every topic now takes.

use assert2::check;
use bytes::Bytes;

use crate::harness::{
    INVALID_RECORD, KNOWN_ID, OTHER_SUBJECT_ID, UNKNOWN_ID, VALIDATED, batch_with_value,
    batch_with_values, boot, create_topic, framed, produce, registry,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_that_fail_validation_are_rejected_and_not_appended() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    let cases: Vec<(&str, Bytes)> = vec![
        // No Confluent frame at all: what a `StringSerializer` writes.
        ("unframed", Bytes::from_static(b"plain text")),
        // A frame whose magic byte is wrong.
        ("bad magic", Bytes::from_static(&[0x01, 0, 0, 0, 42, b'x'])),
        // A frame truncated inside the schema id.
        ("truncated id", Bytes::from_static(&[0x00, 0, 0])),
        // Well framed, but the registry does not know the id.
        ("unknown id", framed(UNKNOWN_ID, b"anything")),
        // Well framed and registered, but under another subject: a producer
        // writing the right format to the wrong topic.
        ("wrong subject", framed(OTHER_SUBJECT_ID, b"anything")),
    ];

    for (name, value) in cases {
        let out = produce(&client, "validated", id, batch_with_value(Some(value))).await;
        check!(out.error_code == INVALID_RECORD, "case {name}: {out:?}");
        check!(
            !out.record_errors.is_empty(),
            "case {name}: no per-record error"
        );
        check!(
            out.record_errors[0].batch_index == 0,
            "case {name}: {:?}",
            out.record_errors
        );
        check!(
            out.record_errors[0]
                .batch_index_error_message
                .as_ref()
                .is_some_and(|m| !m.is_empty()),
            "case {name}: empty message"
        );
        // Nothing was appended, so there is no offset to report: Kafka's
        // pre-append sentinel, not the 0 that would name the head of the log.
        check!(
            out.base_offset == -1,
            "case {name}: a rejected row named an offset"
        );
        // The assertion that matters: nothing was appended.
        check!(
            broker.local_log_end_offset("validated", 0) == Some(0),
            "case {name}: a rejected batch reached the log"
        );
    }

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unvalidated_topic_accepts_what_a_validated_one_rejects() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let control = create_topic(&broker, &client, "control", &[]).await;
    let validated = create_topic(&broker, &client, "validated", VALIDATED).await;

    let unframed = Bytes::from_static(b"plain text");

    let rejected = produce(
        &client,
        "validated",
        validated,
        batch_with_value(Some(unframed.clone())),
    )
    .await;
    let accepted = produce(
        &client,
        "control",
        control,
        batch_with_value(Some(unframed)),
    )
    .await;

    check!(rejected.error_code == INVALID_RECORD, "{rejected:?}");
    check!(accepted.error_code == 0, "{accepted:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(0));
    check!(broker.local_log_end_offset("control", 0) == Some(1));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rejected_record_is_named_by_its_index_in_the_batch() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    // Record 0 is fine; record 1 is not. The batch is rejected whole — its own
    // CRC covers both — and the response says which record caused it.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_values(vec![
            Some(framed(KNOWN_ID, b"fine")),
            Some(Bytes::from_static(b"plain text")),
        ]),
    )
    .await;

    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(out.record_errors.len() == 1, "{:?}", out.record_errors);
    check!(
        out.record_errors[0].batch_index == 1,
        "{:?}",
        out.record_errors
    );
    check!(broker.local_log_end_offset("validated", 0) == Some(0));

    broker.shutdown().await;
}
