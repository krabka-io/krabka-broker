//! Live-broker tests for the `PushTelemetry` handler.
//!
//! These drive `handle` against a running broker so the instance lookup, the
//! push checks, the decompression step and the OTLP decode are all exercised
//! together, which is the only way to observe the response the handler
//! returns for a rejected, valid, malformed or empty push.

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{owned::push_telemetry_response, primitives::uuid::Uuid as ProtoUuid};
use opentelemetry_proto::tonic::metrics::v1::{Gauge, Metric, metric, number_data_point};
use prost::Message as _;
use uuid::Uuid;

use super::*;
use crate::{
    client_metrics::manager::{
        SubscriptionDecision, compute_subscription, subscription_id as compute_subscription_id,
    },
    handlers::push_telemetry::test_support::{metrics_data, number_point},
};

crate::test_support::codec_helpers!(
    PushTelemetryRequest,
    PushTelemetryResponse,
    version = push_telemetry_response::MAX_VERSION
);

const GZIP: i8 = 1;
const NONE: i8 = 0;

fn otlp() -> Vec<u8> {
    metrics_data(vec![Metric {
        name: "cpu.utilization".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![number_point(number_data_point::Value::AsDouble(0.75))],
        })),
        ..Default::default()
    }])
    .encode_to_vec()
}

fn gzip(raw: &[u8]) -> Bytes {
    krabka_compression::compress(CompressionType::Gzip, raw).expect("compress telemetry payload")
}

fn response(error_code: i16) -> PushTelemetryResponse {
    PushTelemetryResponse {
        error_code,
        ..Default::default()
    }
}

struct Client<'a> {
    broker: &'a Broker,
    ctx: TelemetryContext<'a>,
}

impl Client<'_> {
    fn attrs(&self, instance: Uuid) -> ClientAttributes {
        ClientAttributes {
            client_instance_id: instance,
            client_id: self.ctx.client_id.to_string(),
            software_name: self.ctx.software_name.to_string(),
            software_version: self.ctx.software_version.to_string(),
            source_address: self.ctx.peer.ip().to_string(),
            source_port: self.ctx.peer.port(),
        }
    }

    fn get(&self, instance: Uuid) -> i32 {
        let image = self.broker.controller.current_image();
        let SubscriptionDecision::Assign(assignment) = self
            .broker
            .client_metrics
            .manager
            .get_subscription(&image, &self.attrs(instance))
        else {
            panic!("fresh client must receive a subscription");
        };
        assignment.subscription_id
    }

    /// The subscription id that the current subscriptions give `instance`,
    /// on any broker.
    fn current_subscription_id(&self, instance: Uuid) -> i32 {
        let image = self.broker.controller.current_image();
        compute_subscription_id(
            &compute_subscription(&image, &self.attrs(instance)),
            instance,
        )
    }

    fn push(
        &self,
        instance: Uuid,
        subscription_id: i32,
        compression_type: i8,
        metrics: Bytes,
    ) -> PushTelemetryResponse {
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid(*instance.as_bytes()),
            subscription_id,
            terminating: false,
            compression_type,
            metrics,
            ..Default::default()
        };
        decode_response(
            &handle(
                self.broker,
                push_telemetry_response::MAX_VERSION,
                7,
                &encode_request(&req),
                &self.ctx,
            )
            .expect("handle"),
        )
    }
}

/// One push after a `GetTelemetrySubscriptions`, with `telemetry.max.bytes`
/// at 1024. Kafka's `CompressionType.forId` takes 0 to 4 only, and a payload
/// that decompresses past `telemetry.max.bytes` is `TELEMETRY_TOO_LARGE`, not
/// `INVALID_RECORD` (#693).
#[tokio::test]
async fn push_after_get_follows_kafkas_payload_checks() {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.client_metrics_telemetry_max = krabka_units::kibibytes(1);
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let peer = "127.0.0.1:9092".parse().unwrap();
    let client = Client {
        broker: &broker,
        ctx: TelemetryContext {
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        },
    };
    let bomb = gzip(&vec![0; 64 * 1024]);
    assert!(bomb.len() <= 1024);
    let rows = [
        ("gzip OTLP", GZIP, gzip(&otlp()), codes::NONE),
        ("uncompressed OTLP", NONE, Bytes::from(otlp()), codes::NONE),
        ("empty payload", GZIP, Bytes::new(), codes::NONE),
        (
            "id 9 masks to gzip",
            9,
            gzip(&otlp()),
            codes::UNSUPPORTED_COMPRESSION_TYPE,
        ),
        (
            "id 12 masks to zstd",
            12,
            gzip(&otlp()),
            codes::UNSUPPORTED_COMPRESSION_TYPE,
        ),
        (
            "negative id",
            -1,
            gzip(&otlp()),
            codes::UNSUPPORTED_COMPRESSION_TYPE,
        ),
        (
            "compressed payload over telemetry.max.bytes",
            NONE,
            Bytes::from(vec![0; 1025]),
            codes::TELEMETRY_TOO_LARGE,
        ),
        (
            "decompressed payload over telemetry.max.bytes",
            GZIP,
            bomb,
            codes::TELEMETRY_TOO_LARGE,
        ),
        (
            "payload that is not OTLP",
            GZIP,
            gzip(b"not-otlp"),
            codes::INVALID_RECORD,
        ),
        (
            "payload that is not gzip",
            GZIP,
            Bytes::from_static(b"not-gzip"),
            codes::INVALID_RECORD,
        ),
    ];
    for (row, (name, compression, payload, expected)) in (100u128..).zip(rows) {
        let instance = Uuid::from_u128(row);
        let subscription_id = client.get(instance);
        assert!(
            client.push(instance, subscription_id, compression, payload) == response(expected),
            "row {name}"
        );
    }
    broker_handle.shutdown().await;
}

/// A push for an instance this broker does not hold: Kafka builds the
/// instance from the current subscriptions and checks the push, and answers
/// `INVALID_REQUEST` only for `Uuid.RESERVED` (#673).
#[tokio::test]
async fn push_without_a_get_builds_the_instance() {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|_cfg| {}).await;
    let broker = broker_handle.broker_arc_for_test();
    let peer = "127.0.0.1:9092".parse().unwrap();
    let client = Client {
        broker: &broker,
        ctx: TelemetryContext {
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        },
    };
    let known = Uuid::from_u128(0x1234);
    let other = Uuid::from_u128(0x5678);
    let rows = [
        (
            "the current subscription id",
            known,
            client.current_subscription_id(known),
            codes::NONE,
        ),
        (
            "another subscription id",
            other,
            client.current_subscription_id(other) ^ 1,
            codes::UNKNOWN_SUBSCRIPTION_ID,
        ),
        ("the zero id", Uuid::nil(), 0, codes::INVALID_REQUEST),
        (
            "Uuid.ONE_UUID",
            Uuid::from_u128(1),
            0,
            codes::INVALID_REQUEST,
        ),
    ];
    for (name, instance, subscription_id, expected) in rows {
        assert!(
            client.push(instance, subscription_id, GZIP, gzip(&otlp())) == response(expected),
            "row {name}"
        );
    }
    broker_handle.shutdown().await;
}
