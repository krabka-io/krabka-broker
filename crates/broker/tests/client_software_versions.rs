#![cfg(test)]

//! KIP-511: `ApiVersions` v3+ client-information validation and the
//! `client_software_versions_total` Prometheus counter.
//!
//! These tests boot a broker on a plaintext loopback listener, with the
//! Prometheus exporter bound on `127.0.0.1:0`. Each test drives a raw
//! `ApiVersions` request over TCP instead of going through the `Client`. That
//! lets the test pin the version and the exact `client_software_name` and
//! `client_software_version` bytes the broker sees.

mod support;

use {
    crate::support::{
        client::{metrics_config, scrape_metrics},
        discovery::api_versions_request_for,
        wire::{
            HeaderEncoding, decode_response_frame, read_frame, request_body_capacity,
            request_frame, write_frame,
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::Broker,
    krabka_protocol::{Encode, owned::api_versions_response::ApiVersionsResponse},
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    std::{io, net::SocketAddr},
    tokio::net::TcpStream,
};

/// The Kafka listener address, the metrics endpoint address, the `BrokerHandle`
/// and the directory that backs it.
struct BootedBroker {
    kafka_addr: SocketAddr,
    metrics_addr: SocketAddr,
    handle: krabka_broker::BrokerHandle,
    _tempdir: tempfile::TempDir,
}

/// Builds a broker with the metrics endpoint enabled. It returns the Kafka
/// listener address, the metrics endpoint address, and the `BrokerHandle`, so
/// that the test can shut down cleanly.
async fn boot() -> BootedBroker {
    let tempdir = tempfile::tempdir().unwrap();
    let cfg = metrics_config(tempdir.path().to_path_buf());

    let handle = Broker::start(cfg).await.expect("broker start");
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");
    BootedBroker {
        kafka_addr,
        metrics_addr,
        handle,
        _tempdir: tempdir,
    }
}

/// Sends one `ApiVersions` request at the requested version with the given
/// client-info fields, and returns the decoded response.
async fn send_api_versions(
    addr: SocketAddr,
    version: i16,
    software_name: &str,
    software_version: &str,
) -> io::Result<ApiVersionsResponse> {
    let req = api_versions_request_for(software_name.to_owned(), software_version.to_owned());
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;

    // ApiVersions request header is v2 (flexible) on v3+; v1 (plain) on
    // v0-2. Either way the response header is v0 — ApiVersions intentionally
    // keeps its response header at v0 across versions so v0 clients can
    // parse the error code on negotiated downgrade.
    let flexible = version >= 3;

    let frame = request_frame()
        .version(version)
        .correlation(99)
        .header(HeaderEncoding::from_wire(flexible))
        .client_id("krabka-kip-511-test")
        .body(&body)
        .maybe_capacity(Some(request_body_capacity(&body)))
        .call();

    let mut stream = TcpStream::connect(addr).await?;
    write_frame(&mut stream, &frame, None).await?;

    let resp = read_frame(&mut stream).await?;
    // ApiVersions keeps a non-flexible response header, including on flexible versions.
    decode_response_frame(&resp, version, false, "ApiVersionsResponse decode")
}

async fn scrape(addr: SocketAddr) -> String { scrape_metrics(addr).await }

struct SoftwareIdentity {
    broker: Option<BootedBroker>,
    name: String,
    version: String,
    response: Option<ApiVersionsResponse>,
    metrics: Option<String>,
}

#[fixture]
fn software() -> SoftwareIdentity {
    // Start this scenario with fresh inputs and observations.

    SoftwareIdentity {
        broker: None,
        name: String::new(),
        version: String::new(),
        response: None,
        metrics: None,
    }
}

#[given("a plaintext broker with client software metrics enabled")]
async fn metrics_broker(software: &mut SoftwareIdentity) { software.broker = Some(boot().await); }

#[given(r#"client software name is "{name}""#)]
fn software_name(software: &mut SoftwareIdentity, name: String) { software.name = name; }

#[given("client software name is empty")]
fn empty_software_name(software: &mut SoftwareIdentity) { software.name.clear(); }

#[given(r#"client software version is "{version}""#)]
fn software_version(software: &mut SoftwareIdentity, version: String) {
    software.version = version;
}

#[given("client software version is empty")]
fn empty_software_version(software: &mut SoftwareIdentity) { software.version.clear(); }

#[when("ApiVersions version {version} is sent with that client identity")]
async fn send_identity(software: &mut SoftwareIdentity, version: i16) {
    let addr = software
        .broker
        .as_ref()
        .expect("a client-metrics broker")
        .kafka_addr;
    software.response = Some(
        send_api_versions(addr, version, &software.name, &software.version)
            .await
            .expect("ApiVersions"),
    );
}

#[then("ApiVersions returns error code {code}")]
fn handshake_error(software: &SoftwareIdentity, code: i16) {
    let response = software
        .response
        .as_ref()
        .expect("a client identity was submitted");
    assert!(response.error_code == code, "{response:?}");
}

#[then("the advertised API list is nonempty")]
fn supported_apis(software: &SoftwareIdentity) {
    let response = software
        .response
        .as_ref()
        .expect("a client identity was submitted");
    assert!(!response.api_keys.is_empty(), "{response:?}");
}

#[then("the advertised API list is empty")]
fn no_supported_apis(software: &SoftwareIdentity) {
    let response = software
        .response
        .as_ref()
        .expect("a client identity was submitted");
    assert!(response.api_keys.is_empty(), "{response:?}");
}

#[derive(DataTableRow)]
struct HandshakeSeries {
    name: String,
    version: String,
    repetitions: usize,
}

#[when("these ApiVersions v3 handshakes are sent:")]
async fn send_handshakes(software: &SoftwareIdentity, #[datatable] rows: Rows<HandshakeSeries>) {
    let addr = software
        .broker
        .as_ref()
        .expect("a client-metrics broker")
        .kafka_addr;
    for row in rows {
        for _ in 0..row.repetitions {
            send_api_versions(addr, 3, &row.name, &row.version)
                .await
                .expect("ApiVersions");
        }
    }
}

#[when("the client software metrics are scraped")]
async fn read_software_metrics(software: &mut SoftwareIdentity) {
    let addr = software
        .broker
        .as_ref()
        .expect("a client-metrics broker")
        .metrics_addr;
    software.metrics = Some(scrape(addr).await);
}

#[derive(DataTableRow)]
struct ExpectedCounter {
    name: String,
    version: String,
    count: u64,
}

#[then("the exported client software family is a counter with these samples:")]
fn software_counters(software: &SoftwareIdentity, #[datatable] rows: Rows<ExpectedCounter>) {
    let body = software
        .metrics
        .as_ref()
        .expect("client software metrics were scraped");
    assert!(
        body.contains("# TYPE krabka_broker_client_software_versions counter"),
        "TYPE line missing in:\n{body}"
    );
    for row in rows {
        let needle = format!(
            "krabka_broker_client_software_versions_total{{software_name=\"{}\",software_version=\"{}\"}} {}",
            row.name, row.version, row.count
        );
        assert!(
            body.contains(&needle),
            "expected sample {needle:?} not found in:\n{body}"
        );
    }
}

#[then("a client software metric is labelled with valid-client")]
fn accepted_identity_recorded(software: &SoftwareIdentity) {
    let body = software
        .metrics
        .as_ref()
        .expect("client software metrics were scraped");
    assert!(
        body.contains("software_name=\"valid-client\""),
        "valid handshake must be recorded:\n{body}"
    );
}

#[then("no client software metric is labelled with bad client")]
fn rejected_identity_not_recorded(software: &SoftwareIdentity) {
    let body = software
        .metrics
        .as_ref()
        .expect("client software metrics were scraped");
    assert!(
        !body.contains("software_name=\"bad client\""),
        "rejected handshake must not be recorded:\n{body}"
    );
}

#[then("no client software counter sample has an empty software name")]
fn legacy_identity_not_recorded(software: &SoftwareIdentity) {
    let body = software
        .metrics
        .as_ref()
        .expect("client software metrics were scraped");
    assert!(
        !body.contains("krabka_broker_client_software_versions_total{software_name=\"\""),
        "v0 handshake must not be recorded:\n{body}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "v3 client info validation: valid"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_client_info_validation_valid(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "v3 client info validation: empty name"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_client_info_validation_empty_name(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "v3 client info validation: empty version"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_client_info_validation_empty_version(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "v3 client info validation: invalid name"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_client_info_validation_invalid_name(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "v3 client info validation: leading dash version"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_client_info_validation_leading_dash_version(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "pre v3 does not validate client info"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_does_not_validate_client_info(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "accepted v3 handshake bumps client software versions counter"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_v3_handshake_bumps_client_software_versions_counter(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "rejected v3 handshake does not bump counter"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_v3_handshake_does_not_bump_counter(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/client_software_versions.feature",
    name = "pre v3 handshake does not bump counter"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_handshake_does_not_bump_counter(software: SoftwareIdentity) {
    software
        .broker
        .expect("a client-metrics broker")
        .handle
        .shutdown()
        .await;
}
