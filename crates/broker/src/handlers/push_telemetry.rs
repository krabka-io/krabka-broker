//! `PushTelemetry` (`api_key=72`, KIP-714).
//!
//! The handler runs Kafka's push checks against the client's instance, which
//! it builds from the current subscriptions when this broker holds none. It
//! then decompresses and decodes the OTLP payload, and fans it out to the
//! Prometheus and OTLP sinks.

use bytes::Bytes;
use krabka_compression::{CompressionError, CompressionType};
use krabka_protocol::{
    Decode,
    owned::{
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
};
use uuid::Uuid;

mod prometheus;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::prometheus::flatten_for_prometheus;
use crate::{
    broker::Broker,
    client_metrics::{
        manager::{ClientAttributes, PushCheck, PushDecision},
        otlp,
    },
    codes,
    error::BrokerError,
    handlers::context::TelemetryContext,
};

/// Kafka's `CompressionType.forId`: the ids 0 to 4 and nothing else. The
/// record-batch attribute decode masks the id to three bits, which would take
/// 9 for gzip.
fn compression_for_id(id: i8) -> Option<CompressionType> {
    match id {
        0 => Some(CompressionType::None),
        1 => Some(CompressionType::Gzip),
        2 => Some(CompressionType::Snappy),
        3 => Some(CompressionType::Lz4),
        4 => Some(CompressionType::Zstd),
        _ => None,
    }
}

#[tracing::instrument(
    name = "handle_push_telemetry",
    level = "info",
    skip_all,
    fields(api = "PushTelemetry", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = PushTelemetryRequest::decode(&mut cur, version)?;
    let instance = Uuid::from_bytes(req.client_instance_id.0);
    let codec = compression_for_id(req.compression_type);
    let manager = &broker.client_metrics.manager;

    let attrs = ClientAttributes {
        client_instance_id: instance,
        client_id: ctx.client_id.to_string(),
        software_name: ctx.software_name.to_string(),
        software_version: ctx.software_version.to_string(),
        source_address: ctx.peer.ip().to_string(),
        source_port: ctx.peer.port(),
    };
    let image = broker.controller.current_image();
    let decision = manager.authorize_push(
        &image,
        &attrs,
        PushCheck {
            subscription_id: req.subscription_id,
            terminating: req.terminating,
            compression_supported: codec.is_some(),
            payload_len: req.metrics.len(),
        },
    );

    // Kafka answers every error with `throttle_time_ms` 0; only the request
    // quota raises it.
    let error_code = match decision {
        PushDecision::Reject { error_code } => error_code,
        PushDecision::Accept if req.metrics.is_empty() => codes::NONE,
        PushDecision::Accept => {
            let ct = codec.expect("authorize_push accepts only a supported codec");
            // Kafka's exporter decompresses up to `telemetry.max.bytes` and
            // answers a larger payload with the retriable
            // TELEMETRY_TOO_LARGE. INVALID_RECORD, which stops the client's
            // telemetry, is only for a payload that does not decode.
            match krabka_compression::decompress(ct, &req.metrics, manager.telemetry_max()) {
                Ok(raw) => match otlp::decode_metrics(&raw) {
                    Ok(md) => {
                        let instance_str = instance.to_string();
                        let points = flatten_for_prometheus(&md, &instance_str, ctx.client_id);
                        broker.client_metrics.prometheus.ingest(&points);
                        broker.client_metrics.otlp.forward(md, &instance_str);
                        codes::NONE
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "client-metrics OTLP decode failed");
                        codes::INVALID_RECORD
                    }
                },
                Err(CompressionError::TooLarge { limit }) => {
                    tracing::debug!(limit, "client-metrics payload decompresses past the limit");
                    codes::TELEMETRY_TOO_LARGE
                }
                Err(e) => {
                    tracing::debug!(error = %e, "client-metrics decompress failed");
                    codes::INVALID_RECORD
                }
            }
        }
    };

    let resp = PushTelemetryResponse {
        error_code,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
