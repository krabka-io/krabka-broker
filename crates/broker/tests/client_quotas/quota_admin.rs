//! Wire drivers for the client-quota admin APIs, `AlterClientQuotas` and
//! `DescribeClientQuotas`, each driven over its own authenticated SASL/PLAIN
//! connection.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{Decode, Encode};

use super::wire::{round_trip, sasl_plain_authenticate};

pub type QuotaEntity = Vec<(String, Option<String>)>;
pub type QuotaOperations = Vec<(String, f64, bool)>;
pub type QuotaEntries = Vec<(QuotaEntity, QuotaOperations)>;

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for AlterClientQuotas and DescribeClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drives `AlterClientQuotas` with `api_key=49` over a SASL/PLAIN connection.
///
/// `entries` is a list of `(entity_components, ops)` where:
/// - `entity_components` is `Vec<(entity_type, entity_name)>`, e.g.
///   `vec![("user".into(), Some("alice".into()))]`
/// - `ops` is `Vec<(key, value, remove)>`, e.g.
///   `vec![("producer_byte_rate".into(), 1024.0, false)]`
///
/// Returns the per-entry `(entity, error_code)` pairs.
pub async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: QuotaEntries,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    use krabka_protocol::owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    };

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

    let version: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode AlterClientQuotas");
    let resp_bytes = round_trip(&mut stream, 49, version, 1, true, &body)
        .await
        .expect("AlterClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterClientQuotasResponse::decode(&mut cur, version)
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

/// Drives `DescribeClientQuotas` with `api_key=48` over a SASL/PLAIN connection.
///
/// `components` is a list of `(entity_type, match_type, match_value)`:
/// - `match_type`: 0=EXACT, 1=DEFAULT, 2=ANY
///
/// Returns the list of `(entity, values)` pairs from the response.
pub async fn drive_describe_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    components: Vec<(String, i8, Option<String>)>,
    strict: bool,
) -> Vec<(Vec<(String, Option<String>)>, Vec<(String, f64)>)> {
    use krabka_protocol::owned::{
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::DescribeClientQuotasResponse,
    };

    let req = DescribeClientQuotasRequest {
        components: components
            .into_iter()
            .map(|(entity_type, match_type, match_)| ComponentData {
                entity_type,
                match_type,
                match_,
                ..Default::default()
            })
            .collect(),
        strict,
        ..Default::default()
    };

    let version: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode DescribeClientQuotas");
    let resp_bytes = round_trip(&mut stream, 48, version, 1, true, &body)
        .await
        .expect("DescribeClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeClientQuotasResponse::decode(&mut cur, version)
        .expect("decode DescribeClientQuotasResponse");

    assert!(resp.error_code == 0, "DescribeClientQuotas top-level error");

    resp.entries
        .unwrap_or_default()
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            let values = e.values.into_iter().map(|v| (v.key, v.value)).collect();
            (entity, values)
        })
        .collect()
}
