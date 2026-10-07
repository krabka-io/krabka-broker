//! The `AlterClientQuotas` (`api_key=49`) driver that installs the
//! `controller_mutation_rate` value the tests measure against.
//!
//! It is its own module because it shapes an admin request that has nothing to
//! do with the topic mutations under test: the super-user sends it once to set
//! the rate, and every assertion afterwards is about a different API.

use std::net::SocketAddr;

use crate::{CLIENT_ID, kafka_wire, kafka_wire::quotas::QuotaEntries};

/// Drive `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
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
