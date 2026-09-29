//! Benchmark seam over the generic response-framing path.
//!
//! Two functions make up that path: [`super::response::encode_response`],
//! which copies a handler's body to prepend the 4- or 5-byte response header,
//! and the [`KafkaCodec`] the connection loop wraps its stream in, whose
//! `Encoder<Bytes>` copies that body a second time into the codec's write
//! buffer. The PERF note on `encode_response` weighs replacing the pair
//! with a chained `Buf` and a vectored write; `benches/perf_deferrals.rs` is
//! what put the measured saving into that note.
//!
//! Both functions here are the production ones. The seam exists only because
//! they are crate-internal and a benchmark is an external crate.

use bytes::Bytes;

pub use crate::network::codec::KafkaCodec;
use crate::{
    error::BrokerError,
    handlers::{ApiKeyCode, CorrelationId},
};

/// Prepend the response header to `body`, exactly as the dispatch loop does.
///
/// # Errors
///
/// Returns [`BrokerError`] when the framed response would exceed the int32
/// size prefix of a Kafka frame.
pub fn encode_response(
    api_key: ApiKeyCode,
    correlation_id: CorrelationId,
    body_flexible: bool,
    body: &[u8],
) -> Result<Bytes, BrokerError> {
    super::response::encode_response(api_key, correlation_id, body_flexible, body)
}

/// The Kafka length-delimited codec the connection loop frames its stream
/// with, for requests of up to `max_request_bytes`.
#[must_use]
pub fn codec(max_request_bytes: usize) -> KafkaCodec {
    crate::network::codec::codec(max_request_bytes)
}

/// Bytes the response header occupies for `api_key` at a flexible or
/// non-flexible body, so a prototype can size the segment it writes ahead of
/// the body.
#[must_use]
pub fn response_header_len(api_key: ApiKeyCode, body_flexible: bool) -> usize {
    crate::network::response_header_len(api_key, body_flexible)
}

/// Whether the response header for `api_key` carries the v1 empty
/// tagged-fields byte after the correlation id.
#[must_use]
pub fn response_header_v1(api_key: ApiKeyCode, body_flexible: bool) -> bool {
    crate::network::response_header_v1(api_key, body_flexible)
}

#[cfg(test)]
mod tests;
