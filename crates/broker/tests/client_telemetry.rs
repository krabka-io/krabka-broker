#![cfg(test)]

//! KIP-714 client-metrics telemetry handshake, push, and error-path coverage.
//!
//! Krabka implements the full KIP-714 receiver. The broker:
//!   - Assigns a fresh `client_instance_id` when the caller sends nil, and
//!     answers with the caller's id otherwise.
//!   - Returns `accepted_compression_types = [4,3,1,2]`, that is ZSTD, LZ4,
//!     GZIP, and SNAPPY, plus `telemetry_max_bytes = 1_048_576` and
//!     `delta_temporality = true`.
//!   - With no subscriptions configured, leaves `requested_metrics` empty and
//!     sets `push_interval_ms = 300_000`.
//!   - With a matching subscription, sets `requested_metrics` to the matched
//!     prefix set and `push_interval_ms` to the smallest matched interval.
//!   - Answers a `PushTelemetry` from a reserved instance id with
//!     `error_code 42` (`INVALID_REQUEST`), and builds the instance of any
//!     other id it does not hold from the current subscriptions.
//!   - Answers a `PushTelemetry` with a stale `subscription_id` with
//!     `error_code 117` (`UNKNOWN_SUBSCRIPTION_ID`).
//!   - Answers a `PushTelemetry` with an unsupported `compression_type` with
//!     `error_code 76` (`UNSUPPORTED_COMPRESSION_TYPE`).
//!   - Answers a valid push with `error_code 0`.
//!
//! The tests that call `IncrementalAlterConfigs` to set up subscriptions need
//! a controller-backed single-node cluster, `start_n_node_with(1, ..)`, because
//! that RPC goes through Raft. The simple handshake tests use a single-broker
//! helper that also boots in Bootstrap mode and is its own controller. Both
//! turn `client_metrics_enable` on, because the broker advertises the two
//! KIP-714 RPCs only behind a configured receiver and the client negotiates
//! every request against what the broker advertised.

use {
    crate::support::{
        client::connect_owned, configs::client_metrics_subscription,
        discovery::api_versions_request_for,
    },
    assert2::{assert, check},
    std::{
        collections::HashSet,
        net::SocketAddr,
        str,
        time::{Duration, Instant},
    },
    tokio::time,
};
mod support;

use {
    krabka_protocol::{
        owned::{
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            incremental_alter_configs_response::IncrementalAlterConfigsResponse,
            push_telemetry_request::PushTelemetryRequest,
            push_telemetry_response::PushTelemetryResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    support::start_n_node_with,
};

// ── helpers ───────────────────────────────────────────────────────────────────

/// A single broker that advertises the KIP-714 RPCs, plus a client on it.
///
/// `client_metrics_enable` is off by default, the way a Kafka broker with no
/// `ClientTelemetry` metric reporter advertises no telemetry handshake, and
/// `krabka_client_core` negotiates every request against the table the broker
/// advertised. So a suite that drives the handshake configures a receiver
/// first; `api_versions_withholds_telemetry_apis_without_a_receiver` covers
/// the other setting.
async fn start_with_client_metrics() -> support::InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = krabka_broker::BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.client_metrics_enable = true;
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker start");
    let client = build_client(broker.listen_addr()).await;
    support::InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

type MetricsCluster = Vec<(
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerConfig,
    tempfile::TempDir,
)>;

/// A controller-backed cluster and the client negotiated against it.
struct TelemetryCluster {
    cluster: MetricsCluster,
    client: krabka_client_core::Client,
}

/// Keep the controller-backed cluster alive alongside its negotiated client.
async fn start_one_node_with_client_metrics() -> TelemetryCluster {
    let cluster = start_n_node_with(1, |_, cfg| cfg.client_metrics_enable = true)
        .await
        .expect("start_n_node_with");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;
    TelemetryCluster { cluster, client }
}

async fn build_client(addr: SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "client-telemetry-test",
        "client build",
    )
    .await
}

/// Builds a minimal valid OTLP `MetricsData` payload. It is uncompressed, so
/// `compression_type=0`.
fn sample_otlp_metrics(name: String, value: i64) -> bytes::Bytes {
    use {
        opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            metric::Data, number_data_point::Value,
        },
        prost::Message,
    };
    let dp = NumberDataPoint {
        value: Some(Value::AsInt(value)),
        ..Default::default()
    };
    let metric = Metric {
        name,
        data: Some(Data::Gauge(Gauge {
            data_points: vec![dp],
        })),
        ..Default::default()
    };
    let md = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![metric],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    bytes::Bytes::from(md.encode_to_vec())
}

async fn advertised_apis(client: &krabka_client_core::Client) -> HashSet<i16> {
    let resp = client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    resp.api_keys.iter().map(|k| k.api_key).collect()
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct SubscriptionId(i32);

#[derive(Clone, Copy)]
struct TelemetryCompressionCode(i8);

#[derive(Clone, Copy, Default)]
enum TelemetryCompression {
    #[default]
    Uncompressed,
    Invalid(TelemetryCompressionCode),
}

#[derive(krabka_macros::FieldDefaults)]
struct TelemetryPushSetup {
    #[default(WireUuid::ZERO)]
    client_instance_id: WireUuid,
    subscription_id: SubscriptionId,
    compression: TelemetryCompression,
    metrics: bytes::Bytes,
}

#[bon::builder]
fn telemetry_push_request(
    #[builder(default = WireUuid :: ZERO)] client_instance_id: WireUuid,
    #[builder(into, default)] subscription_id: SubscriptionId,
    #[builder(default)] compression: TelemetryCompression,
    #[builder(default)] metrics: bytes::Bytes,
) -> PushTelemetryRequest {
    let setup = TelemetryPushSetup {
        client_instance_id,
        subscription_id,
        compression,
        metrics,
    };
    let TelemetryPushSetup {
        client_instance_id,
        subscription_id,
        compression,
        metrics,
    } = setup;
    let compression_type = match compression {
        TelemetryCompression::Uncompressed => 0,
        TelemetryCompression::Invalid(code) => code.0,
    };
    PushTelemetryRequest {
        client_instance_id,
        subscription_id: subscription_id.0,
        terminating: false,
        compression_type,
        metrics,
        ..Default::default()
    }
}

use {
    krabka_client_core::Client,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

enum TelemetryBroker {
    Standalone(support::InProcess),
    Controller(TelemetryCluster),
}

impl TelemetryBroker {
    fn client(&self) -> &Client {
        match self {
            Self::Standalone(broker) => &broker.client,
            Self::Controller(broker) => &broker.client,
        }
    }

    fn address(&self) -> SocketAddr {
        match self {
            Self::Standalone(broker) => broker.broker.listen_addr(),
            Self::Controller(broker) => broker.cluster[0].0.listen_addr(),
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Standalone(broker) => {
                broker.client.close();
                broker.broker.shutdown().await;
            }
            Self::Controller(broker) => {
                broker.client.close();
                support::shutdown_cluster(broker.cluster).await;
            }
        }
    }
}

#[derive(Default)]
struct TelemetrySession {
    broker: Option<TelemetryBroker>,
    separate_client: Option<Client>,
    advertised: Option<HashSet<i16>>,
    subscription: Option<GetTelemetrySubscriptionsResponse>,
    alter: Option<IncrementalAlterConfigsResponse>,
    push: Option<PushTelemetryResponse>,
    metrics: bytes::Bytes,
    get_codes: Vec<i16>,
    reconnected_codes: Vec<i16>,
}

#[fixture]
fn telemetry() -> TelemetrySession {
    // Start this scenario with fresh inputs and observations.

    TelemetrySession {
        broker: None,
        separate_client: None,
        advertised: None,
        subscription: None,
        alter: None,
        push: None,
        metrics: bytes::Bytes::new(),
        get_codes: Vec::new(),
        reconnected_codes: Vec::new(),
    }
}

fn hex_bytes(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty(), "hex payload must contain whole bytes");
    pairs
        .iter()
        .map(|pair| {
            u8::from_str_radix(str::from_utf8(pair).expect("ASCII hex pair"), 16).expect("hex byte")
        })
        .collect()
}

fn uuid_hex(value: &str) -> WireUuid {
    WireUuid(
        hex_bytes(value)
            .try_into()
            .expect("UUID must contain sixteen hex bytes"),
    )
}

impl TelemetrySession {
    fn client(&self) -> &Client {
        self.separate_client
            .as_ref()
            .unwrap_or_else(|| self.broker.as_ref().unwrap().client())
    }
}

#[given("a standalone broker with client metrics receiver {enabled}")]
async fn receiver(telemetry: &mut TelemetrySession, enabled: bool) {
    telemetry.broker = Some(TelemetryBroker::Standalone(if enabled {
        start_with_client_metrics().await
    } else {
        support::start().await
    }));
}

#[given("a controller-backed broker with a client metrics receiver")]
async fn controller(telemetry: &mut TelemetrySession) {
    telemetry.broker = Some(TelemetryBroker::Controller(
        start_one_node_with_client_metrics().await,
    ));
}

#[given("an uncompressed OTLP gauge {name} with integer value {value}")]
fn metric(telemetry: &mut TelemetrySession, name: String, value: i64) {
    telemetry.metrics = sample_otlp_metrics(name, value);
}

#[when("ApiVersions is requested")]
async fn versions(telemetry: &mut TelemetrySession) {
    telemetry.advertised = Some(advertised_apis(telemetry.client()).await);
}

#[then("API keys {keys} are advertised {expected}")]
fn api_keys(telemetry: &TelemetrySession, keys: String, expected: bool) {
    let advertised = telemetry.advertised.as_ref().unwrap();
    for key in keys.split(',').map(|key| key.parse::<i16>().unwrap()) {
        assert!(
            advertised.contains(&key) == expected,
            "API key {key}, advertised: {advertised:?}"
        );
    }
}

#[when("GetTelemetrySubscriptions requests instance hex {id}")]
async fn get(telemetry: &mut TelemetrySession, id: String) {
    let response = telemetry
        .client()
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: uuid_hex(&id),
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");
    telemetry.get_codes.push(response.error_code);
    telemetry.subscription = Some(response);
}

#[then("the subscription succeeds and assigns a nonzero instance")]
fn assigned(telemetry: &TelemetrySession) {
    let response = telemetry.subscription.as_ref().unwrap();
    check!(response.error_code == 0);
    check!(response.client_instance_id != WireUuid::ZERO);
}

#[then("the subscription succeeds and echoes instance hex {id}")]
fn echo(telemetry: &TelemetrySession, id: String) {
    let response = telemetry.subscription.as_ref().unwrap();
    assert!(response.error_code == 0);
    assert!(response.client_instance_id == uuid_hex(&id));
}

#[then("requested metrics are {metrics} and the push interval is {interval} ms")]
fn subscription(telemetry: &TelemetrySession, metrics: String, interval: i32) {
    let response = telemetry.subscription.as_ref().unwrap();
    let expected: Vec<String> = if metrics == "empty" {
        Vec::new()
    } else {
        metrics.split(',').map(str::to_owned).collect()
    };
    check!(response.requested_metrics == expected);
    check!(response.push_interval_ms == interval);
}

#[then(
    "compression codes are {codes}, the maximum is {maximum} bytes, and delta temporality is {delta}"
)]
fn capabilities(telemetry: &TelemetrySession, codes: String, maximum: i32, delta: bool) {
    let response = telemetry.subscription.as_ref().unwrap();
    let expected: Vec<i8> = codes.split(',').map(|code| code.parse().unwrap()).collect();
    check!(response.accepted_compression_types == expected);
    check!(response.telemetry_max_bytes == maximum);
    check!(response.delta_temporality == delta);
}

#[when("match-all subscription {name} is configured with interval {interval} ms")]
async fn configure(telemetry: &mut TelemetrySession, name: String, interval: String) {
    telemetry.alter = Some(
        telemetry
            .client()
            .send(
                client_metrics_subscription()
                    .name(&name)
                    .interval(&interval)
                    .call(),
            )
            .await
            .expect("IncrementalAlterConfigs"),
    );
}

#[then("the client metrics configuration has one successful resource response")]
fn configured(telemetry: &TelemetrySession) {
    let response = telemetry.alter.as_ref().unwrap();
    assert!(response.responses.len() == 1);
    assert!(
        response.responses[0].error_code == 0,
        "configuration failed: {response:?}"
    );
}

#[when(
    "PushTelemetry sends instance hex {id}, subscription {subscription}, compression {compression}, and payload hex {payload}"
)]
async fn raw_push(
    telemetry: &mut TelemetrySession,
    id: String,
    subscription: i32,
    compression: i8,
    payload: String,
) {
    let metrics = hex_bytes(&payload);
    telemetry.push = Some(
        telemetry
            .client()
            .send(
                telemetry_push_request()
                    .client_instance_id(uuid_hex(&id))
                    .subscription_id(subscription)
                    .compression(TelemetryCompression::Invalid(TelemetryCompressionCode(
                        compression,
                    )))
                    .metrics(metrics.into())
                    .call(),
            )
            .await
            .expect("PushTelemetry"),
    );
}

#[when(
    "the assigned instance pushes the OTLP gauge with subscription XOR {xor} and compression {compression}"
)]
async fn enrolled_push(telemetry: &mut TelemetrySession, xor: i32, compression: i8) {
    let assigned = telemetry.subscription.as_ref().unwrap();
    telemetry.push = Some(
        telemetry
            .client()
            .send(
                telemetry_push_request()
                    .client_instance_id(assigned.client_instance_id)
                    .subscription_id(assigned.subscription_id ^ xor)
                    .compression(TelemetryCompression::Invalid(TelemetryCompressionCode(
                        compression,
                    )))
                    .metrics(telemetry.metrics.clone())
                    .call(),
            )
            .await
            .expect("PushTelemetry"),
    );
}

#[then("PushTelemetry is exactly a default response with error {error}")]
fn full_push(telemetry: &TelemetrySession, error: i16) {
    assert!(
        telemetry.push.as_ref().unwrap()
            == &PushTelemetryResponse {
                error_code: error,
                ..Default::default()
            }
    );
}

#[then("PushTelemetry returns error {error}")]
fn push_error(telemetry: &TelemetrySession, error: i16) {
    assert!(telemetry.push.as_ref().unwrap().error_code == error);
}

#[when("a separate telemetry connection is opened")]
async fn separate(telemetry: &mut TelemetrySession) {
    telemetry.separate_client =
        Some(build_client(telemetry.broker.as_ref().unwrap().address()).await);
}

#[then("the GetTelemetrySubscriptions response codes are {codes}")]
fn get_codes(telemetry: &TelemetrySession, codes: String) {
    let expected: Vec<i16> = codes.split(',').map(|code| code.parse().unwrap()).collect();
    assert!(telemetry.get_codes == expected);
}

#[when(
    "that connection closes and instance hex {id} reconnects every {retry} ms for at most {budget} ms"
)]
async fn reconnect(telemetry: &mut TelemetrySession, id: String, retry: u64, budget: u64) {
    drop(telemetry.separate_client.take());
    let address = telemetry.broker.as_ref().unwrap().address();
    let deadline = Instant::now() + Duration::from_millis(budget);
    loop {
        let client = build_client(address).await;
        let response = client
            .send(GetTelemetrySubscriptionsRequest {
                client_instance_id: uuid_hex(&id),
                ..Default::default()
            })
            .await
            .expect("GetTelemetrySubscriptions");
        telemetry.reconnected_codes.push(response.error_code);
        if response.error_code != 89 || Instant::now() >= deadline {
            break;
        }
        time::sleep(Duration::from_millis(retry)).await;
    }
}

#[then(
    "reconnection eventually assigns the instance with error {success}, with only error {transient} before it"
)]
fn disconnected_instance(telemetry: &TelemetrySession, success: i16, transient: i16) {
    let (last, before) = telemetry.reconnected_codes.split_last().unwrap();
    assert!(*last == success);
    assert!(before.iter().all(|code| *code == transient));
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "API advertisement follows receiver configuration"
)]
#[tokio::test]
async fn api_advertisement(telemetry: TelemetrySession, enabled: bool, advertised: bool) {
    let _ = (enabled, advertised);
    telemetry.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "get telemetry subscriptions with nil id returns assigned id and no subscription"
)]
#[tokio::test]
async fn get_telemetry_subscriptions_with_nil_id_returns_assigned_id_and_no_subscription(
    telemetry: TelemetrySession,
) {
    telemetry.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "get telemetry subscriptions with set id echoes it"
)]
#[tokio::test]
async fn get_telemetry_subscriptions_with_set_id_echoes_it(telemetry: TelemetrySession) {
    telemetry.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "unregistered pushes distinguish reserved and unknown instances"
)]
#[tokio::test]
async fn unregistered_push(telemetry: TelemetrySession, id: String, error: i16) {
    let _ = (id, error);
    telemetry.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "enrolled pushes validate subscription identity and compression"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrolled_push_contract(
    telemetry: TelemetrySession,
    xor: i32,
    compression: i8,
    error: i16,
) {
    let _ = (xor, compression, error);
    telemetry.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_telemetry.feature",
    name = "closing the connection drops the client instance"
)]
#[tokio::test]
async fn closing_the_connection_drops_the_client_instance(telemetry: TelemetrySession) {
    telemetry.broker.unwrap().shutdown().await;
}
