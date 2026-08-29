//! Live-broker tests for the `PushTelemetry` handler.
//!
//! These drive `handle` against a running broker so the subscription lookup,
//! the throttle decision, the decompression step and the OTLP decode are all
//! exercised together, which is the only way to observe the `error_code` the
//! handler returns for a rejected, valid, malformed or empty push.

use assert2::assert;
use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_protocol::{owned::push_telemetry_response, primitives::uuid::Uuid as ProtoUuid};
use opentelemetry_proto::tonic::metrics::v1::{Gauge, Metric, metric, number_data_point};
use prost::Message as _;
use uuid::Uuid;

use super::*;
use crate::{
    client_metrics::manager::SubscriptionDecision,
    handlers::push_telemetry::test_support::{metrics_data, number_point},
};

crate::test_support::codec_helpers!(
    PushTelemetryRequest,
    PushTelemetryResponse,
    version = push_telemetry_response::MAX_VERSION
);

async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|_cfg| {}).await
}

#[tokio::test]
async fn handle_preserves_reject_response_fields_for_unknown_instance() {
    let (broker_handle, _dir) = start_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    let ctx = TelemetryContext {
        client_id: "client-a",
        peer: &"127.0.0.1:9092".parse().unwrap(),
        software_name: "test-client",
        software_version: "1.0.0",
    };
    let req = PushTelemetryRequest {
        client_instance_id: ProtoUuid([9; 16]),
        subscription_id: 12,
        terminating: false,
        compression_type: i8::try_from(CompressionType::Gzip.as_attribute_bits()).unwrap(),
        metrics: Bytes::from_static(b"payload"),
        ..Default::default()
    };

    let resp = handle(
        &broker,
        push_telemetry_response::MAX_VERSION,
        7,
        &encode_request(&req),
        &ctx,
    )
    .expect("handle");
    let resp = decode_response(&resp);

    assert!(resp.throttle_time_ms == 0);
    assert!(resp.error_code == codes::INVALID_REQUEST);
    broker_handle.shutdown().await;
}

async fn push_payload(payload: Bytes) -> i16 {
    let (broker_handle, _dir) = start_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    let instance = Uuid::from_u128(0x1234);
    let peer = "127.0.0.1:9092".parse().unwrap();
    let ctx = TelemetryContext {
        client_id: "client-a",
        peer: &peer,
        software_name: "test-client",
        software_version: "1.0.0",
    };
    let SubscriptionDecision::Assign(assignment) = broker.client_metrics.manager.assign(
        &krabka_metadata::MetadataImage::new(Uuid::nil()),
        &crate::client_metrics::manager::ClientAttributes {
            client_instance_id: instance,
            client_id: ctx.client_id.to_string(),
            software_name: ctx.software_name.to_string(),
            software_version: ctx.software_version.to_string(),
            source_address: ctx.peer.ip().to_string(),
            source_port: ctx.peer.port(),
        },
    ) else {
        panic!("fresh client must receive a subscription");
    };
    let req = PushTelemetryRequest {
        client_instance_id: ProtoUuid(*instance.as_bytes()),
        subscription_id: assignment.subscription_id,
        terminating: false,
        compression_type: i8::try_from(CompressionType::Gzip.as_attribute_bits()).unwrap(),
        metrics: payload,
        ..Default::default()
    };

    let resp = handle(
        &broker,
        push_telemetry_response::MAX_VERSION,
        7,
        &encode_request(&req),
        &ctx,
    )
    .expect("handle");
    let resp = decode_response(&resp);

    broker_handle.shutdown().await;
    resp.error_code
}

#[tokio::test]
async fn valid_payload_is_accepted() {
    let raw = metrics_data(vec![Metric {
        name: "cpu.utilization".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![number_point(number_data_point::Value::AsDouble(0.75))],
        })),
        ..Default::default()
    }])
    .encode_to_vec();
    let payload = krabka_compression::compress(CompressionType::Gzip, &raw)
        .expect("compress telemetry payload");

    assert!(push_payload(payload).await == codes::NONE);
}

#[tokio::test]
async fn malformed_otlp_payload_returns_invalid_record() {
    let payload = krabka_compression::compress(CompressionType::Gzip, b"not-otlp")
        .expect("compress telemetry payload");

    assert!(push_payload(payload).await == codes::INVALID_RECORD);
}

#[tokio::test]
async fn malformed_compressed_payload_returns_invalid_record() {
    assert!(push_payload(Bytes::from_static(b"not-gzip")).await == codes::INVALID_RECORD);
}

#[tokio::test]
async fn empty_payload_is_accepted_without_decode() {
    assert!(push_payload(Bytes::new()).await == codes::NONE);
}
