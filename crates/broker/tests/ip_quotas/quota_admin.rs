//! The two typed drivers that write and read client quotas over an
//! authenticated connection: `AlterClientQuotas` (`api_key=49`) and
//! `DescribeClientQuotas` (`api_key=48`).
//!
//! They sit apart from the raw wire exchange because they own the request and
//! response shaping — the entity and operation tuples a test passes in, and the
//! flattened pairs it gets back — while the exchange itself only moves bytes.

use std::net::SocketAddr;

use crate::{CLIENT_ID, kafka_wire, kafka_wire::quotas::QuotaEntries};

/// Drives `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
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

/// Drives `DescribeClientQuotas` (`api_key=48`) over a SASL/PLAIN
/// connection.
///
/// `components` is a list of `(entity_type, match_type, match_value)`:
/// - `match_type`: 0=EXACT, 1=DEFAULT, 2=ANY
pub(crate) async fn drive_describe_client_quotas_sasl(
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
