//! Live-broker tests for the `PushTelemetry` handler.
//!
//! These drive `handle` against a running broker so the instance lookup, the
//! push checks, the decompression step and the OTLP decode are all exercised
//! together, which is the only way to observe the response the handler
//! returns for a rejected, valid, malformed or empty push.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        broker::Broker,
        client_metrics::{
            config::INTERVAL_MS_DEFAULT,
            manager::{
                ClientAttributes, SubscriptionDecision, compute_subscription,
                subscription_id as compute_subscription_id,
            },
            test_support::{gauge_metric, metrics_data, subscribe_to_cpu},
        },
        test_support::{
            self as broker_fixture_support, DenyAll, KafkaErrorCode, peer, start_broker_with,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::{ClientMetricsConfigRecord, MetadataRecord},
    krabka_protocol::{owned::push_telemetry_response, primitives::uuid::Uuid as ProtoUuid},
    krabka_security::Principal,
    opentelemetry_proto::tonic::metrics::v1::number_data_point,
    prost::Message as _,
    std::{net::SocketAddr, sync::Arc},
    uuid::Uuid,
};

krabka_macros::wire_codec_helpers! {
    encode_request(PushTelemetryRequest);
    decode_response(PushTelemetryResponse);
    version [push_telemetry_response::MAX_VERSION];
}

const GZIP: i8 = 1;
const NONE: i8 = 0;

#[tokio::test]
async fn push_requires_cluster_action_and_an_enabled_receiver() {
    for (name, enable, expected) in [
        ("disabled receiver", false, codes::UNSUPPORTED_VERSION),
        (
            "principal without ClusterAction",
            true,
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
    ] {
        let started = broker_fixture_support::start_broker_no_audit_with(move |cfg| {
            cfg.client_metrics_enable = enable;
            cfg.authorizer = Arc::new(DenyAll);
        })
        .await;
        let broker = started.broker.broker_arc_for_test();
        let peer = peer();
        let principal = broker_fixture_support::principal("untrusted");
        let ctx = TelemetryContext {
            principal: &principal,
            connection_id: "connection-a",
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1",
        };
        let response = decode_response(
            &handle(
                &broker,
                push_telemetry_response::MAX_VERSION,
                1,
                &encode_request(&PushTelemetryRequest::default()),
                &ctx,
            )
            .expect("push response"),
        );
        assert!(response.error_code == expected, "row {name}");
        started.broker.shutdown().await;
    }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct SubscriptionId(i32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TelemetryCompressionCode(i8);

#[derive(Clone, Copy, Default)]
enum TelemetryCompression {
    #[default]
    Gzip,
    Uncompressed,
    Malformed(TelemetryCompressionCode),
}

impl TelemetryCompression {
    fn wire_code(self) -> i8 {
        match self {
            Self::Gzip => GZIP,
            Self::Uncompressed => NONE,
            Self::Malformed(code) => code.0,
        }
    }
}

#[derive(krabka_macros::FieldDefaults)]
struct PushSetup {
    #[default(Uuid::from_u128(0x1234))]
    instance: Uuid,
    subscription: SubscriptionId,
    compression: TelemetryCompression,
    metrics: Bytes,
}

fn otlp() -> Vec<u8> {
    metrics_data(vec![gauge_metric(
        "cpu.utilization",
        number_data_point::Value::AsDouble(0.75),
    )])
    .encode_to_vec()
}

fn gzip(raw: &[u8]) -> Bytes {
    krabka_compression::compress(CompressionType::Gzip, raw).expect("compress telemetry payload")
}

fn response(error_code: KafkaErrorCode) -> PushTelemetryResponse {
    PushTelemetryResponse {
        error_code: error_code.0,
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
            connection_id: self.ctx.connection_id.to_owned(),
            client_instance_id: instance,
            client_id: self.ctx.client_id.to_owned(),
            software_name: self.ctx.software_name.to_owned(),
            software_version: self.ctx.software_version.to_owned(),
            source_address: self.ctx.source_address(),
            source_port: self.ctx.peer.port(),
        }
    }

    fn get(&self, instance: Uuid) -> SubscriptionId {
        let image = self.broker.controller.current_image();
        let SubscriptionDecision::Assign(assignment) = self
            .broker
            .client_metrics
            .manager
            .get_subscription(&image, &self.attrs(instance))
        else {
            panic!("fresh client must receive a subscription");
        };
        SubscriptionId(assignment.subscription_id)
    }

    /// The subscription id that the current subscriptions give `instance`,
    /// on any broker.
    fn current_subscription_id(&self, instance: Uuid) -> SubscriptionId {
        let image = self.broker.controller.current_image();
        SubscriptionId(compute_subscription_id(
            &compute_subscription(&image, &self.attrs(instance), INTERVAL_MS_DEFAULT),
            instance,
        ))
    }

    fn push(&self, setup: PushSetup) -> PushTelemetryResponse {
        let PushSetup {
            instance,
            subscription,
            compression,
            metrics,
        } = setup;
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid(*instance.as_bytes()),
            subscription_id: subscription.0,
            terminating: false,
            compression_type: compression.wire_code(),
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

#[derive(Default)]
struct TelemetryPushScenario {
    started: Option<HandlerStartedBroker>,
    broker: Option<Arc<Broker>>,
    identity: Option<(Principal, SocketAddr)>,
    instance: Uuid,
    subscription: Option<SubscriptionId>,
    compression: TelemetryCompression,
    payload: Bytes,
    payload_shape: String,
    response: Option<PushTelemetryResponse>,
}
#[rstest::fixture]
fn telemetry_push() -> TelemetryPushScenario {
    let started = None;
    TelemetryPushScenario {
        started,
        ..Default::default()
    }
}
impl TelemetryPushScenario {
    fn client(&self) -> Client<'_> {
        let (principal, peer) = self.identity.as_ref().unwrap();
        Client {
            broker: self.broker.as_deref().unwrap(),
            ctx: TelemetryContext {
                principal,
                connection_id: "connection-a",
                client_id: "client-a",
                peer,
                software_name: "test-client",
                software_version: "1.0.0",
            },
        }
    }
}
#[rstest_bdd_macros::given(
    "a telemetry client has obtained a subscription with a 1024-byte limit and unstable API versions {unstable}"
)]
async fn subscribed_telemetry_client(telemetry_push: &mut TelemetryPushScenario, unstable: bool) {
    let unstable = if unstable {
        UnstableApiVersions::Enabled
    } else {
        UnstableApiVersions::Disabled
    };
    let started = start_broker_with(|cfg| {
        cfg.client_metrics_telemetry_max = krabka_units::kibibytes(1);
        cfg.client_metrics_enable = true;
        cfg.features.unstable_api_versions = unstable;
    })
    .await;
    telemetry_push.broker = Some(started.broker.broker_arc_for_test());
    telemetry_push.started = Some(started);
    telemetry_push.identity = Some((
        broker_fixture_support::principal("telemetry-client"),
        peer(),
    ));
    telemetry_push.instance = Uuid::from_u128(100);
    telemetry_push.subscription = Some(telemetry_push.client().get(telemetry_push.instance));
}
#[rstest_bdd_macros::given(
    "the telemetry push carries {payload} using compression ID {compression}"
)]
fn telemetry_push_payload(
    telemetry_push: &mut TelemetryPushScenario,
    payload: String,
    compression: i8,
) {
    telemetry_push.compression = match compression {
        0 => TelemetryCompression::Uncompressed,
        1 => TelemetryCompression::Gzip,
        code => TelemetryCompression::Malformed(TelemetryCompressionCode(code)),
    };
    telemetry_push.payload = match payload.as_str() {
        "gzip OTLP" => gzip(&otlp()),
        "uncompressed OTLP" => Bytes::from(otlp()),
        "empty bytes" => Bytes::new(),
        "1025 uncompressed bytes" => Bytes::from(vec![0; 1025]),
        "65536 zero bytes compressed with gzip" => gzip(&vec![0; 64 * 1024]),
        "gzip bytes that are not OTLP" => gzip(b"not-otlp"),
        "bytes that are not gzip" => Bytes::from_static(b"not-gzip"),
        _ => panic!("unknown telemetry payload: {payload}"),
    };
    telemetry_push.payload_shape = payload;
}
#[rstest_bdd_macros::when("the client pushes telemetry using its assigned subscription")]
fn push_subscribed_telemetry(telemetry_push: &mut TelemetryPushScenario) {
    telemetry_push.response = Some(telemetry_push.client().push(PushSetup {
        instance: telemetry_push.instance,
        subscription: telemetry_push.subscription.unwrap(),
        compression: telemetry_push.compression,
        metrics: telemetry_push.payload.clone(),
    }));
}
#[rstest_bdd_macros::then("the complete telemetry response has error {code}")]
async fn check_subscribed_telemetry(telemetry_push: &mut TelemetryPushScenario, code: i16) {
    assert!(telemetry_push.response.as_ref().unwrap() == &response(KafkaErrorCode(code)));
    if telemetry_push.payload_shape == "65536 zero bytes compressed with gzip" {
        assert!(telemetry_push.payload.len() <= 1024);
    }
    telemetry_push
        .started
        .take()
        .unwrap()
        .broker
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_push_telemetry/payload.feature",
    name = "Telemetry payload framing compression decoding and size limits follow the selected Kafka version"
)]
#[tokio::test]
async fn push_after_get_follows_kafkas_payload_checks(
    telemetry_push: TelemetryPushScenario,
    unstable: bool,
    payload: String,
    compression: i8,
    code: i16,
) {
    let _ = (telemetry_push, unstable, payload, compression, code);
}

/// A push for an instance this broker does not hold: Kafka builds the
/// instance from the current subscriptions and checks the push, and answers
/// `INVALID_REQUEST` only for `Uuid.RESERVED` (#673).
#[tokio::test]
async fn push_without_a_get_builds_the_instance() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker);
        boot [broker_fixture_support::start_broker_with(|cfg| cfg.client_metrics_enable = true)];
    };
    // The broker accepts only subscribed metrics, so subscribe every client
    // to the one metric `otlp` pushes.
    subscribe_to_cpu(&broker_handle).await;
    let peer_addr = peer();
    let principal = broker_fixture_support::principal("telemetry-client");
    let client_ctx = TelemetryContext {
        principal: &principal,
        connection_id: "connection-a",
        client_id: "client-a",
        peer: &peer_addr,
        software_name: "test-client",
        software_version: "1.0.0",
    };
    let client = Client {
        broker: &broker,
        ctx: client_ctx,
    };
    let known = Uuid::from_u128(0x1234);
    let other = Uuid::from_u128(0x5678);
    let rows = [
        (
            "the current subscription id",
            known,
            client.current_subscription_id(known),
            KafkaErrorCode(codes::NONE),
        ),
        (
            "another subscription id",
            other,
            SubscriptionId(client.current_subscription_id(other).0 ^ 1),
            KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID),
        ),
        (
            "the zero id",
            Uuid::nil(),
            SubscriptionId(0),
            KafkaErrorCode(codes::INVALID_REQUEST),
        ),
        (
            "Uuid.ONE_UUID",
            Uuid::from_u128(1),
            SubscriptionId(0),
            KafkaErrorCode(codes::INVALID_REQUEST),
        ),
    ];
    for (name, instance, subscription_id, expected) in rows {
        assert!(
            client.push(PushSetup {
                instance,
                subscription: subscription_id,
                metrics: gzip(&otlp()),
                ..Default::default()
            }) == response(expected),
            "row {name}"
        );
    }
    broker_handle.shutdown().await;
}

/// An instance keeps the attributes of its first request, so a push whose
/// header names another `client_id` stays within the subscription the first
/// request was given rather than the one the new header would match.
#[tokio::test]
async fn push_checks_metrics_against_the_instances_first_attributes() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker);
        boot [broker_fixture_support::start_broker_with(|cfg| cfg.client_metrics_enable = true)];
    };
    broker_handle
        .submit_metadata_record_for_test(MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "client-b-only".into(),
                configs: [
                    ("metrics".to_owned(), "cpu.utilization".to_owned()),
                    ("match".to_owned(), "client_id=client-b".to_owned()),
                ]
                .into(),
            },
        ))
        .await
        .expect("submit client-metrics subscription");
    let peer_addr = peer();
    let principal = broker_fixture_support::principal("telemetry-client");
    let client = |client_id| Client {
        broker: &broker,
        ctx: TelemetryContext {
            principal: &principal,
            connection_id: "connection-a",
            client_id,
            peer: &peer_addr,
            software_name: "test-client",
            software_version: "1.0.0",
        },
    };
    let instance = Uuid::from_u128(0x9abc);
    let subscription = client("client-a").get(instance);

    assert!(
        client("client-b").push(PushSetup {
            instance,
            subscription,
            metrics: gzip(&otlp()),
            ..Default::default()
        }) == response(KafkaErrorCode(codes::INVALID_RECORD))
    );
    broker_handle.shutdown().await;
}

/// Connection churn: every connection takes a fresh instance id and a fresh
/// `client_id`, pushes one series and goes. Neither id is the client's to be
/// trusted with, so the per-principal ceiling holds the retained count
/// whether the connections close at once or linger, and closing them all
/// drains the collector to zero.
#[tokio::test]
async fn connection_churn_stays_under_the_ceiling_and_drains_to_zero() {
    const CEILING: usize = 20;
    const CONNECTIONS: u128 = 200;
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_with(|cfg| {
        cfg.client_metrics_enable = true;
        cfg.client_metrics_prom_max_series_per_principal = CEILING;
        cfg.client_metrics_prom_max_series_per_client = 1;
        cfg.client_metrics_prom_max_series_per_instance = 1;
    })
    .await;
    subscribe_to_cpu(&broker_handle).await;
    let broker = broker_handle.broker_arc_for_test();
    let collector = &broker.client_metrics.prometheus;
    let peer = peer();
    let principal = broker_fixture_support::principal("churning-client");
    for (row, (name, close_each, expected_peak)) in (0u128..).zip([
        ("connect, push and disconnect", true, 1),
        ("connect and push, disconnect later", false, CEILING),
    ]) {
        let mut peak = 0;
        let connections = (0..CONNECTIONS)
            .map(|index| {
                (
                    Uuid::from_u128(((row + 1) << 64) | index),
                    format!("{row}-{index}"),
                )
            })
            .collect::<Vec<_>>();
        for (instance, connection_id) in &connections {
            let client = Client {
                broker: &broker,
                ctx: TelemetryContext {
                    principal: &principal,
                    connection_id,
                    client_id: connection_id,
                    peer: &peer,
                    software_name: "test-client",
                    software_version: "1.0.0",
                },
            };
            let subscription = client.get(*instance);
            assert!(
                client.push(PushSetup {
                    instance: *instance,
                    subscription,
                    compression: TelemetryCompression::Gzip,
                    metrics: gzip(&otlp()),
                }) == response(KafkaErrorCode(codes::NONE)),
                "row {name}"
            );
            peak = peak.max(collector.retained_series());
            if close_each {
                broker.client_metrics.connection_closed(connection_id);
            }
        }
        for (_, connection_id) in &connections {
            broker.client_metrics.connection_closed(connection_id);
        }
        assert!(
            (
                peak,
                collector.retained_series(),
                collector.stats().retained.get()
            ) == (expected_peak, 0, 0),
            "row {name}"
        );
    }
    broker_handle.shutdown().await;
}
