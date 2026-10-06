//! The raw Kafka wire exchange this suite is built on: one length-prefixed
//! request and response over a `TcpStream`, plus the SASL/PLAIN handshake that
//! opens an authenticated connection.
//!
//! Both bind the shared [`kafka_wire`] helpers to this suite's client id. They
//! live in their own module because every typed driver in the suite is written
//! on top of them, and because the on-wire `client_id` they send is what the
//! broker reads when it looks the mutation quota up.

use std::{io, net::SocketAddr};

use tokio::net::TcpStream;

use crate::kafka_wire;

/// The client id every request header in this suite carries.
pub(crate) const CLIENT_ID: &str = "krabka-mutation-quota-test";

/// One length-prefixed request/response exchange; see
/// [`kafka_wire::round_trip`].
pub(crate) async fn round_trip(
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
        CLIENT_ID,
        flexible,
        body,
    )
    .await
}

/// Connects and authenticates with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
pub(crate) async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password).await
}
