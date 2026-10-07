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
use krabka_protocol::{Decode, Encode, owned::produce_response::ProduceResponse};

use crate::{CLIENT_ID, kafka_wire, kafka_wire::quotas::QuotaEntries};

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
    kafka_wire::quotas::drive_alter_client_quotas_sasl(
        addr,
        CLIENT_ID,
        user,
        pass,
        entries,
        validate_only,
    )
    .await
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

    let req = kafka_wire::produce_records(topic, record_bytes, count);

    // Authenticate with the suite client id; Produce uses wire_client_id below.
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, pass)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = kafka_wire::round_trip(
        &mut stream,
        0, // Produce api_key
        VERSION,
        1,
        wire_client_id,
        true, // flexible
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
