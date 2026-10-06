//! The raw length-prefixed request and response exchange this suite speaks
//! Kafka over, with no client library in between.
//!
//! `round_trip` writes a v1 request header by hand and strips the response
//! header back off, which is what lets the typed helpers encode a request body
//! and decode a response body without owning any framing of their own.

use std::io;

use tokio::net::TcpStream;

use crate::kafka_wire;

const CLIENT_ID: &str = "krabka-compaction-test";

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
