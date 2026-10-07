//! Wire drivers for the client-quota admin APIs, `AlterClientQuotas` and
//! `DescribeClientQuotas`, each driven over its own authenticated SASL/PLAIN
//! connection.

use std::net::SocketAddr;

use crate::{CLIENT_ID, kafka_wire, kafka_wire::quotas::QuotaEntries};

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
    kafka_wire::quotas::drive_describe_client_quotas_sasl(
        addr, CLIENT_ID, user, pass, components, strict,
    )
    .await
}
