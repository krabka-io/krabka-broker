//! The gate itself: a batch at the cap appends, a batch one byte over it is
//! refused with `MESSAGE_TOO_LARGE` (10) and appends nothing.

use assert2::check;

use crate::{
    support,
    wire::{
        KAFKA_DEFAULT, MAX_MESSAGE_BYTES, accepted, create_topic, produce_batch_of_wire_len,
        too_large,
    },
};

/// A per-topic cap of 2048 bytes, the value the boundary was settled at
/// against `apache/kafka:4.1.0`.
const CAP: usize = 2048;

/// The cap is exact, and a refusal costs the log nothing.
///
/// This is the feature in one case. The batch at the cap goes first, so the
/// refusal that follows is the size and not a produce path that never worked;
/// and the batch at the cap after it lands at offset 1, which is the assertion
/// that matters most. A broker that answered `MESSAGE_TOO_LARGE` *and*
/// appended would pass on the error code alone and have written the very
/// 100-MiB batch the cap exists to keep out of the partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_at_the_cap_appends_and_one_byte_over_it_is_refused() {
    let p = support::start().await;
    let topic = create_topic(
        &p.broker,
        &p.client,
        "orders",
        &[(MAX_MESSAGE_BYTES, &CAP.to_string())],
    )
    .await;

    check!(produce_batch_of_wire_len(&p.client, "orders", topic, CAP).await == accepted(0));
    check!(produce_batch_of_wire_len(&p.client, "orders", topic, CAP + 1).await == too_large());
    check!(p.broker.local_log_end_offset("orders", 0) == Some(1));
    check!(produce_batch_of_wire_len(&p.client, "orders", topic, CAP).await == accepted(1));
    check!(p.broker.local_log_end_offset("orders", 0) == Some(2));

    p.broker.shutdown().await;
}

/// A topic that sets no `max.message.bytes` inherits the broker's
/// `message.max.bytes`.
///
/// Kafka reports exactly this as the `DEFAULT_CONFIG` synonym of an unset
/// `max.message.bytes`, and the number is the same 1048588 on both keys. The
/// case exists because the per-topic override is the easy half: a broker that
/// enforced the cap only where an operator had spelled one out would leave
/// every default topic accepting the 100-MiB batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_with_no_override_inherits_the_brokers_message_max_bytes() {
    let p = support::start().await;
    let topic = create_topic(&p.broker, &p.client, "orders", &[]).await;

    check!(
        produce_batch_of_wire_len(&p.client, "orders", topic, KAFKA_DEFAULT).await == accepted(0)
    );
    check!(
        produce_batch_of_wire_len(&p.client, "orders", topic, KAFKA_DEFAULT + 1).await
            == too_large()
    );
    check!(p.broker.local_log_end_offset("orders", 0) == Some(1));

    p.broker.shutdown().await;
}

/// A per-topic override overrides the broker default in both directions.
///
/// The tightened topic refuses a batch the broker default would have taken,
/// and the loosened topic takes one the broker default would have refused.
/// Together they say the produce path reads the topic's value rather than a
/// constant that happens to agree with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_override_binds_below_and_above_the_broker_default() {
    let p = support::start().await;
    let tight = create_topic(
        &p.broker,
        &p.client,
        "tight",
        &[(MAX_MESSAGE_BYTES, &CAP.to_string())],
    )
    .await;
    let loose = create_topic(
        &p.broker,
        &p.client,
        "loose",
        &[(MAX_MESSAGE_BYTES, &(KAFKA_DEFAULT * 2).to_string())],
    )
    .await;

    check!(produce_batch_of_wire_len(&p.client, "tight", tight, CAP + 1).await == too_large());
    check!(p.broker.local_log_end_offset("tight", 0) == Some(0));
    check!(
        produce_batch_of_wire_len(&p.client, "loose", loose, KAFKA_DEFAULT + 1).await
            == accepted(0)
    );

    p.broker.shutdown().await;
}
