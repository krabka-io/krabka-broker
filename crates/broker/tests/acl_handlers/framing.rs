//! The SASL/PLAIN authentication exchange and the length-prefixed
//! request/response framing primitive. Every driver in this suite opens its
//! connection and moves its bytes through the typed exchange, which binds the
//! shared [`kafka_wire`] helpers to this suite's client id.

use std::{io, net::SocketAddr};

use crate::kafka_wire;

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-acl-test";

/// Make one typed, flexible request on a freshly authenticated PLAIN connection.
///
/// # Errors
/// Returns authentication, framing, or named codec errors from the exchange.
pub async fn request_as_plain<Q, R>(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    request: &Q,
    api_key: i16,
    version: i16,
    operation: &str,
) -> io::Result<R>
where
    Q: krabka_protocol::Encode,
    R: for<'de> krabka_protocol::Decode<'de>,
{
    kafka_wire::request_as_plain(
        addr,
        CLIENT_ID,
        (user, password),
        request,
        (api_key, version),
        operation,
    )
    .await
}
