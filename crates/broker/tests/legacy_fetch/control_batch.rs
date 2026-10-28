//! The one scenario that produces no records at all: a transaction marker,
//! which has no representation in the v0 or v1 `MessageSet` format and must be
//! dropped from a down-converted Fetch response, leaving an empty record set.
//!
//! The marker is created by committing a real transaction with the Rust
//! producer, because Kafka forbids a client from producing a control batch
//! directly.

use {
    crate::{
        harness::{create_topic, fetch_legacy_raw_at},
        support::{self, producer::producer_record},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_producer::Producer,
    krabka_protocol::{
        Decode, kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

struct LegacyMarker {
    broker: Option<InProcess>,
    producer: Option<Producer>,
    response: Option<LegacyFetchResponse>,
}

#[fixture]
fn marker() -> LegacyMarker {
    // Start this scenario with fresh inputs and observations.

    LegacyMarker {
        broker: None,
        producer: None,
        response: None,
    }
}

#[given("a legacy-enabled broker has a committed transaction and its control marker")]
async fn committed_transaction(marker: &mut LegacyMarker) {
    let p = support::start_legacy().await;
    create_topic(&p.client, "legacy_fetch_ctrl").await;
    p.broker.wait_until_transaction_coordinator_ready().await;
    marker.producer = Some(
        Producer::builder()
            .bootstrap(p.broker.listen_addr().to_string())
            .transactional_id("legacy-fetch-control-marker")
            .build()
            .await
            .unwrap(),
    );
    marker
        .producer
        .as_ref()
        .expect("transactional producer configured")
        .init_transactions()
        .await
        .unwrap();
    let transaction = marker
        .producer
        .as_ref()
        .expect("transactional producer configured")
        .begin_transaction()
        .await
        .unwrap();
    marker
        .producer
        .as_ref()
        .expect("transactional producer configured")
        .send(
            producer_record()
                .topic("legacy_fetch_ctrl".into())
                .maybe_value(Some(Some(Bytes::from_static(b"data-before-marker"))))
                .call(),
        )
        .await
        .expect("transactional produce");
    transaction.commit().await.expect("commit transaction");
    p.broker
        .wait_until_local_log_end_offset("legacy_fetch_ctrl", 0, 2)
        .await;
    marker.broker = Some(p);
}

#[when("Fetch v3 starts at the transaction marker offset")]
async fn fetch_marker(marker: &mut LegacyMarker) {
    let p = marker
        .broker
        .as_ref()
        .expect("a committed transaction exists");
    let body = fetch_legacy_raw_at(p.broker.listen_addr(), "legacy_fetch_ctrl", 3, 1).await;
    marker.response = Some(
        LegacyFetchResponse::decode(&mut &body[..], 3).expect("decode LegacyFetchResponse v3"),
    );
}

#[then("the legacy partition succeeds with an empty nonnull record set")]
fn marker_is_omitted(marker: &LegacyMarker) {
    let response = marker.response.as_ref().expect("Fetch v3 completed");
    let part = &response.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );
    assert!(
        part.records
            .as_ref()
            .is_some_and(|records| records.payload_len() == 0),
        "control batch must be dropped, leaving an empty record set on the wire, but the partition carried {:?}",
        part.records
    );
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/control_batch.feature",
    name = "Legacy fetch omits transaction control batches without returning null records"
)]
#[tokio::test]
async fn fetch_v3_drops_control_batch(marker: LegacyMarker) {
    marker
        .producer
        .take()
        .expect("a transactional producer exists")
        .close()
        .await
        .unwrap();
    marker
        .broker
        .take()
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
    drop(marker);
}
