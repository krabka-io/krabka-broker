//! Produce up-conversion from v0/v1 `MessageSet` to v2.
//!
//! The broker's Produce handler accepts a `RecordsPayload::Legacy` arm. It
//! passes incoming v0/v1 `MessageSet` bytes through
//! `krabka_records_legacy::legacy_to_v2`, and gives the resulting v2 batch to
//! the existing log-append path.
//!
//! These tests exercise the conversion without the wire protocol's version
//! negotiation. They build a Legacy payload directly, assert that the broker
//! stores the up-converted records, and then fetch those records back in the
//! modern v2 form.
#![cfg(test)]

use {
    crate::support::{
        client::create_topic,
        discovery::named_topic_metadata,
        fetch::single_partition_fetch,
        produce::{SinglePartitionProduceSetup, single_partition_produce},
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::Duration,
    support::InProcess,
    tokio::{task, time::Instant},
};
mod support;

use {
    bytes::{Bytes, BytesMut},
    krabka_ids::Offset,
    krabka_protocol::{
        owned::fetch_request::FetchRequest,
        primitives::uuid::Uuid,
        records::{RecordBatch, RecordsPayload},
    },
    krabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set},
};

async fn topic_id_for(p: &support::InProcess, name: &str) -> Uuid {
    let resp = p
        .client
        .send(named_topic_metadata(name))
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response")
}

/// Builds a flat, uncompressed v1 `MessageSet` that carries `values` as
/// successive records at offsets 0 to N-1.
fn build_v1_message_set(values: &[&[u8]]) -> Bytes {
    let recs: Vec<ParsedRecord> = values
        .iter()
        .enumerate()
        .map(|(i, v)| ParsedRecord {
            offset: Offset(i64::try_from(i).expect("offset fits in i64")),
            timestamp: Some(1_700_000_000 + i64::try_from(i).expect("ts offset fits in i64")),
            key: None,
            value: Some(Bytes::copy_from_slice(v)),
        })
        .collect();
    let mut buf = BytesMut::new();
    encode_flat_message_set(recs, Magic::V1, &mut buf);
    buf.freeze()
}

async fn produce_legacy(
    p: &support::InProcess,
    topic: &str,
    topic_id: Uuid,
    records: Bytes,
) -> i16 {
    let req = single_partition_produce(SinglePartitionProduceSetup {
        topic: topic.into(),
        topic_id,
        records: Some(RecordsPayload::Legacy(records)),
        ..Default::default()
    });
    let resp = p.client.send(req).await.expect("Produce");
    resp.responses[0].partition_responses[0].error_code
}

struct LegacyConversion {
    broker: Option<InProcess>,
    topic: String,
    topic_id: Uuid,
    payload: Option<Bytes>,
    produce_error: Option<i16>,
    fetch_errors: Vec<i16>,
    stored: Option<RecordBatch>,
}

#[fixture]
fn conversion() -> LegacyConversion {
    // Start this scenario with fresh inputs and observations.

    LegacyConversion {
        broker: None,
        topic: String::new(),
        topic_id: Uuid::ZERO,
        payload: None,
        produce_error: None,
        fetch_errors: Vec::new(),
        stored: None,
    }
}

async fn conversion_topic(conversion: &mut LegacyConversion, name: &str) {
    let broker = support::start().await;
    create_topic(&broker.client, name, 1).await;
    conversion.topic_id = topic_id_for(&broker, name).await;
    conversion.broker = Some(broker);
    conversion.topic = name.into();
}

fn legacy_value_rows(datatable: Vec<Vec<String>>) -> Vec<Vec<u8>> {
    datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value]: [String; 1] = row.try_into().expect("one record value");
            value.into_bytes()
        })
        .collect()
}

#[given("the legacy producer has these v1 message values")]
async fn legacy_values(conversion: &mut LegacyConversion, datatable: Vec<Vec<String>>) {
    conversion_topic(conversion, "legacy").await;
    let values = legacy_value_rows(datatable);
    let values = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    conversion.payload = Some(build_v1_message_set(&values));
}

#[given("the legacy producer has a framed v1 message set containing corrupt record bytes")]
async fn corrupt_legacy(conversion: &mut LegacyConversion) {
    conversion_topic(conversion, "bad").await;
    let mut garbage = vec![0_u8; 100];
    garbage[8..12].copy_from_slice(&88_i32.to_be_bytes());
    conversion.payload = Some(Bytes::from(garbage));
}

#[when("the message set is produced through the legacy records payload path")]
async fn submit_legacy(conversion: &mut LegacyConversion) {
    let broker = conversion.broker.as_ref().expect("a topic was created");
    let payload = conversion
        .payload
        .take()
        .expect("a legacy message set was provided");
    conversion.produce_error =
        Some(produce_legacy(broker, &conversion.topic, conversion.topic_id, payload).await);
}

#[then("the legacy produce is accepted")]
fn accepted_legacy(conversion: &LegacyConversion) {
    assert!(
        conversion.produce_error == Some(0),
        "Produce up-conversion must succeed: {:?}",
        conversion.produce_error
    );
}

#[when("a modern consumer fetches the committed records")]
async fn fetch_converted(conversion: &mut LegacyConversion) {
    let broker = conversion.broker.as_ref().expect("a topic was created");
    let deadline = Instant::now() + Duration::from_secs(5);
    conversion.stored = loop {
        let response = broker
            .client
            .send(FetchRequest {
                replica_id: -1,
                ..single_partition_fetch()
                    .topic(conversion.topic.clone())
                    .topic_id(conversion.topic_id)
                    .call()
            })
            .await
            .expect("Fetch");
        let partition = &response.responses[0].partitions[0];
        conversion.fetch_errors.push(partition.error_code);
        if let Some(batch) = partition
            .records
            .as_ref()
            .and_then(|records| records.as_v2())
            .and_then(<[_]>::first)
        {
            break Some(batch.clone());
        }
        if Instant::now() >= deadline {
            break None;
        }
        task::yield_now().await;
    };
}

#[then("the fetch succeeds and returns these values in one v2 batch")]
fn converted_values(conversion: &LegacyConversion, datatable: Vec<Vec<String>>) {
    assert!(!conversion.fetch_errors.is_empty());
    assert!(
        conversion.fetch_errors.iter().all(|&code| code == 0),
        "fetch errors: {:?}",
        conversion.fetch_errors
    );
    let batch = conversion
        .stored
        .as_ref()
        .expect("Fetch returns the committed v2 batch within 5s");
    let expected = legacy_value_rows(datatable);
    assert!(batch.records.len() == expected.len());
    let values = batch
        .records
        .iter()
        .map(|record| record.value.as_deref().unwrap_or(&[]).to_vec())
        .collect::<Vec<_>>();
    assert!(values == expected);
}

#[then("the corrupt legacy produce is refused with INVALID_RECORD 87")]
fn invalid_legacy(conversion: &LegacyConversion) {
    assert!(
        conversion.produce_error == Some(87),
        "malformed legacy bytes must surface INVALID_RECORD (87): {:?}",
        conversion.produce_error
    );
}

async fn stop_conversion(conversion: LegacyConversion) {
    conversion
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_legacy_upconvert.feature",
    name = "A v1 message set is stored and fetched as equivalent v2 records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_v1_message_set_is_upconverted_and_round_trips(conversion: LegacyConversion) {
    stop_conversion(conversion).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_legacy_upconvert.feature",
    name = "A corrupt but correctly framed legacy message set is rejected as invalid records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_malformed_legacy_bytes_returns_invalid_record(conversion: LegacyConversion) {
    stop_conversion(conversion).await;
}
