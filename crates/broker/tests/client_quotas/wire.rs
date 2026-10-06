//! Length-prefixed request/response framing and the SASL/PLAIN handshake that
//! every client-quota test connection performs.
//!
//! Both bind the shared [`kafka_wire`] helpers to the client id
//! `krabka-quota-test`, which is the id the user/client-id tuple quota matches
//! on.

use std::{io, net::SocketAddr};

use tokio::net::TcpStream;

use crate::kafka_wire;

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-quota-test";

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
        CLIENT_ID,
        flexible,
        body,
    )
    .await
}

/// Connects and authenticates with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
pub async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password).await
}
