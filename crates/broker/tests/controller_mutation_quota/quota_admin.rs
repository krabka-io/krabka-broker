//! The `AlterClientQuotas` (`api_key=49`) driver that installs the
//! `controller_mutation_rate` value the tests measure against.
//!
//! It is its own module because it shapes an admin request that has nothing to
//! do with the topic mutations under test: the super-user sends it once to set
//! the rate, and every assertion afterwards is about a different API.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    },
};

use crate::wire::{round_trip, sasl_plain_authenticate};

pub(crate) type QuotaEntity = Vec<(String, Option<String>)>;
pub(crate) type QuotaOperations = Vec<(String, f64, bool)>;
pub(crate) type QuotaEntries = Vec<(QuotaEntity, QuotaOperations)>;

/// Drive `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
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
