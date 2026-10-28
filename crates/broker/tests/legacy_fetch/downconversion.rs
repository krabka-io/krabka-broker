//! The two record-preserving down-conversions: Fetch v3, which yields a
//! `MessageSet` whose key and value pairs survive the rewrite, and Fetch v0,
//! whose `Magic::V0` framing has no timestamp field at all.
//!
//! The v0 case asserts the magic byte at index 16 of the `MessageSet` directly,
//! because a v1 framing would decode without error yet carry a create-time the
//! v0 client cannot represent.

use {
    crate::{
        harness::{
            create_topic, create_topic_with_partitions, fetch_legacy_raw,
            fetch_legacy_raw_partitions, legacy_bytes, produce_batch, produce_batch_to,
        },
        support,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::metrics::TopicLabel,
    krabka_protocol::{
        Decode,
        kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
        records::{Record, RecordBatch, RecordsPayload},
    },
    krabka_records_legacy::{ParsedRecord, decode_message_set},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

fn decode_legacy_records(legacy_bytes: &[u8]) -> Vec<ParsedRecord> {
    let mut cursor: &[u8] = legacy_bytes;
    decode_message_set(&mut cursor, legacy_bytes.len()).expect("decode_message_set")
}

const PARTITIONS: [i32; 3] = [0, 1, 2];

struct Downconversion {
    process: Option<InProcess>,
    topic: String,
    response: Option<LegacyFetchResponse>,
    conversions_before: Option<u64>,
}

#[fixture]
fn legacy() -> Downconversion {
    // Start this scenario with fresh inputs and observations.

    Downconversion {
        process: None,
        topic: String::new(),
        response: None,
        conversions_before: None,
    }
}

#[given("a modern batch contains keyed records key0=val0 and key1=val1")]
async fn keyed_batch(legacy: &mut Downconversion) {
    let process = support::start_legacy().await;
    create_topic(&process.client, "legacy_fetch_basic").await;
    let batch = RecordBatch {
        records: vec![
            Record {
                offset_delta: 0,
                key: Some(Bytes::from_static(b"key0")),
                value: Some(Bytes::from_static(b"val0")),
                ..Default::default()
            },
            Record {
                offset_delta: 1,
                key: Some(Bytes::from_static(b"key1")),
                value: Some(Bytes::from_static(b"val1")),
                ..Default::default()
            },
        ],
        last_offset_delta: 1,
        ..Default::default()
    };
    produce_batch(process.broker.listen_addr(), "legacy_fetch_basic", batch).await;
    legacy.process = Some(process);
    legacy.topic = "legacy_fetch_basic".into();
}

#[given("a modern record has key k value v and a nonzero create timestamp")]
async fn timestamped_batch(legacy: &mut Downconversion) {
    let process = support::start_legacy().await;
    create_topic(&process.client, "legacy_fetch_v0").await;
    let batch = RecordBatch {
        base_timestamp: 1_700_000_000,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            timestamp_delta: 500,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    };
    produce_batch(process.broker.listen_addr(), "legacy_fetch_v0", batch).await;
    legacy.process = Some(process);
    legacy.topic = "legacy_fetch_v0".into();
}

#[given("three topic partitions each hold their own acknowledged record")]
async fn partitioned_records(legacy: &mut Downconversion) {
    let process = support::start_legacy().await;
    let topic = "legacy_fetch_multi";
    create_topic_with_partitions(&process.client, topic, 3).await;
    for partition in PARTITIONS {
        process
            .broker
            .wait_until_partition_present(topic, partition)
            .await;
        let batch = RecordBatch {
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::from(format!("key{partition}"))),
                value: Some(Bytes::from(format!("val{partition}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        produce_batch_to(process.broker.listen_addr(), topic, partition, batch).await;
        process
            .broker
            .wait_until_high_watermark(topic, partition, 1)
            .await;
    }
    legacy.conversions_before = Some(
        process
            .broker
            .metrics()
            .fetch_message_conversions
            .get_or_create(&TopicLabel {
                topic: topic.into(),
            })
            .get(),
    );
    legacy.process = Some(process);
    legacy.topic = topic.into();
}

#[when("Fetch version {version} requests the modern record batch")]
async fn fetch_one(legacy: &mut Downconversion, version: i16) {
    let process = legacy
        .process
        .as_ref()
        .expect("modern records were produced");
    let response = fetch_legacy_raw(process.broker.listen_addr(), &legacy.topic, version).await;
    legacy.response = Some(
        LegacyFetchResponse::decode(&mut response.as_slice(), version)
            .expect("decode legacy Fetch response"),
    );
}

#[when("Fetch v3 requests all three partitions together")]
async fn fetch_partitions(legacy: &mut Downconversion) {
    let process = legacy
        .process
        .as_ref()
        .expect("modern records were produced");
    let response = fetch_legacy_raw_partitions(
        process.broker.listen_addr(),
        &legacy.topic,
        3,
        &PARTITIONS,
        0,
    )
    .await;
    legacy.response = Some(
        LegacyFetchResponse::decode(&mut response.as_slice(), 3)
            .expect("decode legacy Fetch response"),
    );
}

#[then("the legacy message set preserves both record keys and values")]
fn preserved_records(legacy: &Downconversion) {
    let response = legacy.response.as_ref().expect("Fetch completed");
    assert!(
        response.responses.len() == 1,
        "expected one topic in Fetch response"
    );
    let partition = &response.responses[0].partitions[0];
    let bytes = legacy_bytes(
        partition.error_code,
        partition.records.as_ref(),
        "Fetch v3 must return a Legacy MessageSet",
    );
    let records = decode_legacy_records(&bytes);
    assert!(records.len() == 2, "expected two records in MessageSet");
    for (index, key, value) in [
        (0_usize, b"key0" as &[u8], b"val0" as &[u8]),
        (1, b"key1", b"val1"),
    ] {
        check!(
            records[index].key.as_deref() == Some(key),
            "record {index} key mismatch"
        );
        check!(
            records[index].value.as_deref() == Some(value),
            "record {index} value mismatch"
        );
    }
}

#[then("the v0 message set retains k and v but contains no record timestamp")]
fn no_timestamp(legacy: &Downconversion) {
    let response = legacy.response.as_ref().expect("Fetch completed");
    let partition = &response.responses[0].partitions[0];
    let bytes = legacy_bytes(
        partition.error_code,
        partition.records.as_ref(),
        "Fetch v0 must return a Legacy MessageSet",
    );
    assert!(bytes.len() > 16, "legacy bytes too short");
    assert!(bytes[16] == 0, "expected v0 MessageSet magic byte zero");
    let records = decode_legacy_records(&bytes);
    assert!(records.len() == 1, "expected one record");
    check!(records[0].key.as_deref() == Some(b"k".as_ref()));
    check!(records[0].value.as_deref() == Some(b"v".as_ref()));
    check!(
        records[0].timestamp == None,
        "v0 MessageSet must carry no timestamp"
    );
}

#[then(
    "every returned partition contains its own legacy record and increments conversion metering"
)]
fn every_partition_is_metered(legacy: &Downconversion) {
    let response = legacy.response.as_ref().expect("Fetch completed");
    assert!(
        response.responses.len() == 1,
        "all partitions belong to one topic entry"
    );
    let rows = &response.responses[0].partitions;
    assert!(
        rows.len() == PARTITIONS.len(),
        "one row per requested partition"
    );
    let mut returned = rows
        .iter()
        .map(|row| row.partition_index)
        .collect::<Vec<_>>();
    returned.sort_unstable();
    assert!(
        returned.as_slice() == &PARTITIONS,
        "one distinct response for every requested partition"
    );
    for row in rows {
        check!(
            row.error_code == 0,
            "partition {} error: {}",
            row.partition_index,
            row.error_code
        );
        let bytes = match row.records.as_ref().expect("records field should be Some") {
            RecordsPayload::Legacy(bytes) => bytes.clone(),
            _ => panic!(
                "expected Legacy MessageSet for partition {}",
                row.partition_index
            ),
        };
        let records = decode_legacy_records(&bytes);
        check!(records.len() == 1, "partition {}", row.partition_index);
        check!(
            records[0].value.as_deref() == Some(format!("val{}", row.partition_index).as_bytes()),
            "partition {} carries another partition's record",
            row.partition_index
        );
    }
    let process = legacy
        .process
        .as_ref()
        .expect("modern records were produced");
    let after = process
        .broker
        .metrics()
        .fetch_message_conversions
        .get_or_create(&TopicLabel {
            topic: legacy.topic.clone().into(),
        })
        .get();
    let before = legacy
        .conversions_before
        .expect("the initial conversion counter was recorded");
    assert!(
        after - before == PARTITIONS.len() as u64,
        "one conversion per converted partition; counter moved by {}",
        after - before
    );
}

async fn stop_downconversion(legacy: Downconversion) {
    legacy
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/downconversion.feature",
    name = "Fetch v3 preserves modern record keys and values in legacy framing"
)]
#[tokio::test]
async fn fetch_v3_downconverts_v2_batch_to_v0_messageset(legacy: Downconversion) {
    stop_downconversion(legacy).await;
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/downconversion.feature",
    name = "Fetch v0 removes timestamps that its framing cannot represent"
)]
#[tokio::test]
async fn fetch_v0_downconverts_to_magic_v0_without_timestamps(legacy: Downconversion) {
    stop_downconversion(legacy).await;
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/downconversion.feature",
    name = "Legacy conversion meters every converted partition under one topic"
)]
#[tokio::test]
async fn fetch_v3_meters_every_converted_partition_of_a_topic(legacy: Downconversion) {
    stop_downconversion(legacy).await;
}
