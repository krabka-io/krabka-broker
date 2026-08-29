//! What a validated topic admits: a record framed with a schema id bound to
//! the topic's subject, and a tombstone.
//!
//! The cache-counter case sits here too, because the counters only move on a
//! produce that the validator accepted: the first one pays a registry round
//! trip and the second is served from the cache.

use assert2::check;

use crate::harness::{
    KNOWN_ID, VALIDATED, batch_with_value, boot, create_topic, framed, produce, registry,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_framed_with_a_bound_schema_id_is_accepted() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;

    check!(out.error_code == 0, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(1));

    broker.shutdown().await;
}

/// The cache counters must move on a real produce.
///
/// Both were declared, registered and documented, and nothing incremented
/// them: a live broker scraped zero for the life of the process. The unit test
/// that called `record_schema_cache_hit` directly proved the counter counts,
/// not that anything counts with it, so the assertion belongs here — behind an
/// actual produce through the validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cache_counters_move_on_a_validated_produce() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    check!(broker.metrics().schema_validation_cache_misses.get() == 0);
    check!(broker.metrics().schema_validation_cache_hits.get() == 0);

    // First produce of this id: nothing cached, so one registry round trip.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;
    check!(out.error_code == 0, "{out:?}");
    check!(broker.metrics().schema_validation_cache_misses.get() == 1);
    check!(broker.metrics().schema_validation_cache_hits.get() == 0);

    // Same id inside the TTL: served from the cache, and counted as a hit.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;
    check!(out.error_code == 0, "{out:?}");
    check!(broker.metrics().schema_validation_cache_misses.get() == 1);
    check!(broker.metrics().schema_validation_cache_hits.get() == 1);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tombstone_is_accepted_on_a_validated_topic() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    // A null value is a tombstone. Rejecting it would make schema validation
    // and compaction mutually exclusive.
    let out = produce(&client, "validated", id, batch_with_value(None)).await;

    check!(out.error_code == 0, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(1));

    broker.shutdown().await;
}
