//! The two typed request drivers the test needs on top of the raw wire
//! exchange: `AlterClientQuotas`, which installs the (user, client-id) tuple
//! quota, and `Produce`, which carries an explicit on-wire `client_id` so one
//! test can send the same payload as two different clients.
//!
//! `await_authorized_produce` lives with them because the retry it wraps is a
//! property of the produce driver: a freshly seeded ACL can still be absent
//! from the handler's image snapshot when the first request arrives.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    records::{Record, RecordBatch},
};

use crate::tuple_quota_wire::{round_trip, round_trip_with_client_id, sasl_plain_authenticate};

pub(crate) type QuotaEntity = Vec<(String, Option<String>)>;
pub(crate) type QuotaOperations = Vec<(String, f64, bool)>;
pub(crate) type QuotaEntries = Vec<(QuotaEntity, QuotaOperations)>;

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for AlterClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drives `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
///
/// `entries` is a list of `(entity_components, ops)` where:
/// - `entity_components` is `Vec<(entity_type, entity_name)>`
/// - `ops` is `Vec<(key, value, remove)>`
///
/// Returns the per-entry `(entity, error_code)` pairs.
pub(crate) async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: QuotaEntries,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    const VERSION: i16 = 1; // flexible

    let req = AlterClientQuotasRequest {
        entries: entries
            .into_iter()
            .map(|(entity_parts, ops)| EntryData {
                entity: entity_parts
                    .into_iter()
                    .map(|(entity_type, entity_name)| EntityData {
                        entity_type,
                        entity_name,
                        ..Default::default()
                    })
                    .collect(),
                ops: ops
                    .into_iter()
                    .map(|(key, value, remove)| OpData {
                        key,
                        value,
                        remove,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        validate_only,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode AlterClientQuotas");
    let resp_bytes = round_trip(&mut stream, 49, VERSION, 1, true, &body)
        .await
        .expect("AlterClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterClientQuotasResponse::decode(&mut cur, VERSION)
        .expect("decode AlterClientQuotasResponse");

    resp.entries
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            (entity, e.error_code)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for Produce with explicit on-wire client_id
// ─────────────────────────────────────────────────────────────────────────────

/// Drives a `Produce` request over a fresh SASL/PLAIN connection.
///
/// The function writes `wire_client_id` into the Kafka request header. That is
/// the value the broker sees as the connection's client.id and uses for the
/// quota lookup. It lets one test send two produces with different
/// `client_ids`.
///
/// Returns the full `ProduceResponse`.
async fn drive_produce_sasl_with_client_id(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    wire_client_id: &str,
    topic: &str,
    record_bytes: usize,
    count: usize,
) -> ProduceResponse {
    const VERSION: i16 = 11; // flexible, supports throttle_time_ms

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).unwrap(),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // SASL handshake uses the default test client_id; only the Produce request
    // itself carries wire_client_id.  Each TCP connection creates a fresh
    // broker-side connection state, so the quota window resets per connection.
    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip_with_client_id(
        &mut stream,
        0, // Produce api_key
        VERSION,
        1,
        true, // flexible
        wire_client_id,
        &body,
    )
    .await
    .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse")
}

pub(crate) async fn await_authorized_produce(
    addr: SocketAddr,
    password: &[u8],
    client_id: &str,
) -> ProduceResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = drive_produce_sasl_with_client_id(
            addr,
            "alice",
            password,
            client_id,
            "tuple-quota-topic",
            1024,
            4,
        )
        .await;
        let error_code = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .map_or(-1, |partition| partition.error_code);
        if error_code != 29 {
            return response;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
