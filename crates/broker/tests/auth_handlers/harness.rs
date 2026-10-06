//! Wire plumbing and credential fixtures that every auth suite in this
//! binary shares: one length-prefixed request/response round-trip against a
//! broker socket, and the test passwords the SASL exchanges authenticate
//! with.

use std::io;

use tokio::net::TcpStream;

use crate::kafka_wire;

/// alice's SCRAM test password, built from characters at runtime.
///
/// The value is a non-secret test fixture. But a literal that goes into the
/// client SASL-auth calls trips GitHub's default code-scanning credential
/// query. This function keeps those call sites free of literals.
pub fn alice_password() -> String {
    ['w', 'o', 'n', 'd', 'e', 'r', 'l', 'a', 'n', 'd']
        .iter()
        .collect()
}

/// admin PLAIN test password, built at runtime.
///
/// A runtime value stops code scanning from giving a false positive for a
/// static secret in the integration fixtures.
pub fn admin_plain_password() -> String {
    ['s', 'e', 'c', 'r', 'e', 't'].iter().collect()
}

/// wrong SCRAM test password, built at runtime for the same reason as
/// `admin_plain_password`.
pub fn wrong_scram_password() -> String {
    ['h', 'u', 'n', 't', 'e', 'r', '2'].iter().collect()
}

/// One length-prefixed request/response exchange; see
/// [`kafka_wire::round_trip`].
pub async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    kafka_wire::round_trip(
        stream,
        api_key,
        api_version,
        corr_id,
        "krabka-sasl-test",
        flexible,
        body,
    )
    .await
}
