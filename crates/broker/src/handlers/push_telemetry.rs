//! `PushTelemetry` (`api_key=72`, KIP-714).
//!
//! The handler validates the push against the client's subscription and
//! throttle state, decompresses and decodes the OTLP payload, and fans it out
//! to the Prometheus and OTLP sinks.

use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_protocol::{
    Decode,
    owned::{
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use uuid::Uuid;

mod decompression;
mod prometheus;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{decompression::decompressed_output_bound, prometheus::flatten_for_prometheus};
use crate::{
    broker::Broker,
    client_metrics::{manager::PushDecision, otlp},
    codes,
    error::BrokerError,
    handlers::context::TelemetryContext,
};

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

    let mut error_code = codes::NONE;
    let mut throttle_time_ms = 0i32;

    let codec =
        CompressionType::from_attribute_bits(u8::try_from(req.compression_type).unwrap_or(0xff));

    match broker.client_metrics.manager.authorize_push(
        instance,
        req.subscription_id,
        req.terminating,
        codec.is_some(),
        req.metrics.len(),
    ) {
        PushDecision::Reject {
            error_code: ec,
            throttle_ms,
        } => {
            error_code = ec;
            throttle_time_ms = throttle_ms;
        }
        PushDecision::Accept => {
            // authorize_push guarantees compression is supported on Accept.
            // A terminating push that later fails to decode still fences the
            // instance and drops those metrics (best-effort, matches Kafka).
            let ct = codec.expect("authorize_push guarantees a supported codec on Accept");
            if !req.metrics.is_empty() {
                // Bound decompressed output to guard against a decompression
                // bomb in the client-metrics payload.
                let max_output = decompressed_output_bound(
                    ByteSize::from_bytes(u64::try_from(req.metrics.len()).unwrap_or(u64::MAX)),
                    broker.config.telemetry_max_decompression_ratio,
                    broker.config.telemetry_decompressed_output_floor,
                    broker.config.telemetry_decompressed_output_ceiling,
                );
                let decoded = match krabka_compression::decompress(ct, &req.metrics, max_output) {
                    Ok(raw) => match otlp::decode_metrics(&raw) {
                        Ok(md) => Some(md),
                        Err(e) => {
                            tracing::debug!(error = %e, "client-metrics OTLP decode failed");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::debug!(error = %e, "client-metrics decompress failed");
                        None
                    }
                };
                if let Some(md) = decoded {
                    let instance_str = instance.to_string();
                    let points = flatten_for_prometheus(&md, &instance_str, ctx.client_id);
                    broker.client_metrics.prometheus.ingest(&points);
                    broker.client_metrics.otlp.forward(md, &instance_str);
                } else {
                    error_code = codes::INVALID_RECORD;
                }
            }
        }
    }

    let resp = PushTelemetryResponse {
        throttle_time_ms,
        error_code,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
