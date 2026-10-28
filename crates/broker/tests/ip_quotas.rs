//! Broker-side integration tests for KIP-612 IP quotas.
//!
//! Tests:
//! 1. `ip_quota_alter_then_describe_round_trip`. Over SASL/PLAIN, it alters
//!    (ip=127.0.0.1) `connection_creation_rate=2.0`, describes it, and
//!    asserts.
//! 2. `connection_creation_rate_closes_the_throttled_connection`. Over
//!    PLAINTEXT with rate=1, a second connection from the same ip closes
//!    without a response, and a connection from another ip is served at once.
//! 3. `unthrottled_ip_unaffected`. Over PLAINTEXT with no quota, it opens 5
//!    connections and asserts a wall time <500ms.
#![cfg(test)]

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `ip_quotas/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "ip_quotas/cluster.rs"]
mod cluster;
mod kafka_wire;
#[path = "ip_quotas/quota_admin.rs"]
mod quota_admin;
mod support;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-ip-quota-test";

use {
    crate::{
        cluster::{
            start_single_broker_plaintext, start_single_broker_plaintext_with_conn_caps,
            start_single_broker_sasl_plaintext_with_users,
        },
        kafka_wire::{
            PlainAuthenticationSetup,
            quotas::{QuotaAlterSetup, QuotaDescribeSetup},
        },
        quota_admin::{drive_alter_client_quotas_sasl, drive_describe_client_quotas_sasl},
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::BrokerHandle,
    krabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        io::{self, ErrorKind},
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{
        net::{TcpSocket, TcpStream},
        time,
    },
};
type IpBroker = (BrokerHandle, TempDir, SocketAddr);
type QuotaEntity = Vec<(String, Option<String>)>;
type AlteredQuotas = Vec<(QuotaEntity, i16)>;
type DescribedQuotas = Vec<(QuotaEntity, Vec<(String, f64)>)>;

struct QuotaAdministration {
    broker: Option<IpBroker>,
    altered: Option<AlteredQuotas>,
    described: Option<DescribedQuotas>,
}
#[fixture]
fn quotas() -> QuotaAdministration {
    // Start this scenario with fresh inputs and observations.

    QuotaAdministration {
        broker: None,
        altered: None,
        described: None,
    }
}
#[given("a SASL PLAIN quota administrator connects as admin")]
async fn quota_admin(quotas: &mut QuotaAdministration) {
    quotas.broker = Some(
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await,
    );
}
#[when("the administrator sets loopback connection creation rate to two per second")]
async fn alter(quotas: &mut QuotaAdministration) {
    let addr = quotas.broker.as_ref().expect("quota admin broker").2;
    quotas.altered = Some(
        drive_alter_client_quotas_sasl(
            addr,
            vec![(vec![("ip".into(), Some("127.0.0.1".into()))], vec![(
                "connection_creation_rate".into(),
                2.0,
                false,
            )])],
            &QuotaAlterSetup {
                authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
                ..Default::default()
            },
        )
        .await,
    );
}
#[then("the IP quota alteration succeeds")]
fn altered(quotas: &QuotaAdministration) {
    assert!(
        quotas.altered.as_ref().expect("alter quotas completed")[0].1 == 0,
        "alter should succeed"
    );
}
#[when("the administrator describes IP entities after the quota reaches the metadata image")]
async fn describe(quotas: &mut QuotaAdministration) {
    let (handle, _, addr) = quotas.broker.as_ref().expect("quota admin broker");
    handle
        .wait_for_image(|image| {
            let key: krabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            image
                .client_quotas()
                .get(&key)
                .and_then(|settings| settings.get("connection_creation_rate"))
                == Some(&2.0)
        })
        .await;
    quotas.described = Some(
        drive_describe_client_quotas_sasl(
            *addr,
            vec![("ip".into(), 2, None)],
            &QuotaDescribeSetup {
                authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
                ..Default::default()
            },
        )
        .await,
    );
}
#[then("the sole described IP entity reports connection creation rate two")]
fn described(quotas: &QuotaAdministration) {
    let rows = quotas
        .described
        .as_ref()
        .expect("describe quotas completed");
    assert!(rows.len() == 1);
    assert!(
        rows[0]
            .1
            .iter()
            .find(|(key, _)| key == "connection_creation_rate")
            .map(|(_, value)| *value)
            == Some(2.0)
    );
}

struct ConnectionRate {
    broker: Option<IpBroker>,
    entity: String,
    first: Option<io::Result<Vec<u8>>>,
    throttled: Option<io::Result<Vec<u8>>>,
    other: Option<io::Result<Vec<u8>>>,
    other_elapsed: Option<Duration>,
}
#[fixture]
fn rate() -> ConnectionRate {
    // Start this scenario with fresh inputs and observations.

    ConnectionRate {
        broker: None,
        entity: String::new(),
        first: None,
        throttled: None,
        other: None,
        other_elapsed: None,
    }
}
fn api_versions() -> BytesMut {
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .expect("encode ApiVersions");
    body
}
async fn connect_from(addr: SocketAddr, source: [u8; 4]) -> TcpStream {
    let socket = TcpSocket::new_v4().expect("socket");
    socket
        .bind(SocketAddr::from((source, 0)))
        .expect("bind source address");
    socket.connect(addr).await.expect("connect")
}
#[given("IP entity {entity} permits one new connection per second")]
async fn rate_limit(rate: &mut ConnectionRate, entity: String) {
    let broker = start_single_broker_plaintext().await;
    let record =
        krabka_metadata::MetadataRecord::V1ClientQuota(krabka_metadata::ClientQuotaRecord {
            entity: vec![krabka_metadata::QuotaEntity {
                entity_type: "ip".into(),
                entity_name: Some(entity.clone()),
            }],
            config_key: "connection_creation_rate".into(),
            config_value: Some(1.0),
        });
    broker
        .0
        .submit_metadata_record_for_test(record)
        .await
        .expect("seed quota");
    broker
        .0
        .wait_for_image(|image| {
            let key: krabka_metadata::EntityKey = vec![("ip".into(), Some(entity.clone()))];
            image
                .client_quotas()
                .get(&key)
                .and_then(|settings| settings.get("connection_creation_rate"))
                .is_some()
        })
        .await;
    // Broker-internal connections may have spent the first token.
    time::sleep(Duration::from_millis(1_100)).await;
    rate.broker = Some(broker);
    rate.entity = entity;
}
#[when(
    "two loopback connections request API versions while another loopback IP requests them independently"
)]
async fn concurrent_ips(rate: &mut ConnectionRate) {
    let addr = rate.broker.as_ref().expect("rate limit configured").2;
    let mut first = connect_from(addr, [127, 0, 0, 1]).await;
    rate.first = Some(
        kafka_wire::round_trip()
            .stream(&mut first)
            .client_id(CLIENT_ID)
            .body(&api_versions())
            .call()
            .await,
    );
    let mut throttled = connect_from(addr, [127, 0, 0, 1]).await;
    let body = api_versions();
    let task = tokio::spawn(async move {
        kafka_wire::round_trip()
            .stream(&mut throttled)
            .correlation(2)
            .client_id(CLIENT_ID)
            .body(&body)
            .call()
            .await
    });
    // Preserve accept ordering before probing an independent address.
    time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    let mut other = connect_from(addr, [127, 0, 0, 2]).await;
    rate.other = Some(
        kafka_wire::round_trip()
            .stream(&mut other)
            .correlation(3)
            .client_id(CLIENT_ID)
            .body(&api_versions())
            .call()
            .await,
    );
    rate.other_elapsed = Some(started.elapsed());
    rate.throttled = Some(
        time::timeout(Duration::from_secs(5), task)
            .await
            .expect("throttled connection closes after throttle")
            .expect("round-trip task"),
    );
    drop((first, other));
}
#[then(
    "the first connection is served the second closes without a response and the other IP is served within five hundred milliseconds"
)]
fn rate_outcomes(rate: &ConnectionRate) {
    let first = rate
        .first
        .as_ref()
        .expect("first connection requested API versions");
    assert!(first.is_ok(), "first connection is served: {first:?}");
    let other = rate
        .other
        .as_ref()
        .expect("independent IP requested API versions");
    assert!(other.is_ok(), "another IP is served: {other:?}");
    let elapsed = rate.other_elapsed.expect("independent IP latency observed");
    assert!(
        elapsed < Duration::from_millis(500),
        "another IP waits for no throttle, took {elapsed:?}"
    );
    let refused = rate
        .throttled
        .as_ref()
        .expect("second connection completed");
    assert!(
        refused.as_ref().is_err_and(|error| matches!(
            error.kind(),
            ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset
        )),
        "throttled connection closes without a response under ip={}: {refused:?}",
        rate.entity
    );
}

struct ConnectionSlots {
    broker: Option<IpBroker>,
    held: Option<TcpStream>,
    result: Option<io::Result<Vec<u8>>>,
    elapsed: Option<Duration>,
}
#[fixture]
fn slots() -> ConnectionSlots {
    // Start this scenario with fresh inputs and observations.

    ConnectionSlots {
        broker: None,
        held: None,
        result: None,
        elapsed: None,
    }
}
#[given("a plaintext broker has no connection creation rate quota")]
async fn unlimited(slots: &mut ConnectionSlots) {
    slots.broker = Some(start_single_broker_plaintext().await);
}
#[given("a plaintext broker permits only one simultaneous connection per IP")]
async fn single_slot(slots: &mut ConnectionSlots) {
    slots.broker = Some(start_single_broker_plaintext_with_conn_caps(usize::MAX, 1).await);
}
#[when("five TCP connections open without a configured rate limit")]
async fn five_connections(slots: &mut ConnectionSlots) {
    let addr = slots.broker.as_ref().expect("unlimited broker").2;
    let started = Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        streams.push(TcpStream::connect(addr).await.expect("connect"));
    }
    slots.elapsed = Some(started.elapsed());
    drop(streams);
}
#[then("the five connections open within five hundred milliseconds")]
fn unthrottled(slots: &ConnectionSlots) {
    let elapsed = slots.elapsed.expect("five connection attempts completed");
    assert!(
        elapsed < Duration::from_millis(500),
        "expected fast unthrottled connect, got {elapsed:?}"
    );
}
#[when("a first connection requests API versions and remains open")]
async fn hold(slots: &mut ConnectionSlots) {
    let addr = slots.broker.as_ref().expect("single slot broker").2;
    let mut stream = TcpStream::connect(addr).await.expect("connect first");
    slots.result = Some(
        kafka_wire::round_trip()
            .stream(&mut stream)
            .client_id(CLIENT_ID)
            .body(&api_versions())
            .call()
            .await,
    );
    slots.held = Some(stream);
}
#[then("the connection inside the per IP cap is served")]
fn first_served(slots: &ConnectionSlots) {
    assert!(
        slots
            .result
            .as_ref()
            .expect("first round trip completed")
            .is_ok()
    );
}
#[when("a second connection from the same IP requests API versions")]
async fn exceed(slots: &mut ConnectionSlots) {
    let addr = slots.broker.as_ref().expect("single slot broker").2;
    let mut stream = TcpStream::connect(addr).await.expect("connect second");
    slots.result = Some(
        kafka_wire::round_trip()
            .stream(&mut stream)
            .client_id(CLIENT_ID)
            .body(&api_versions())
            .call()
            .await,
    );
}
#[then("the excess connection is refused while the first holds its slot")]
fn excess_refused(slots: &ConnectionSlots) {
    assert!(
        slots
            .result
            .as_ref()
            .expect("second round trip completed")
            .is_err()
    );
}
#[when("the first connection closes and a fresh connection retries for at most five seconds")]
async fn release(slots: &mut ConnectionSlots) {
    drop(slots.held.take());
    let addr = slots.broker.as_ref().expect("single slot broker").2;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut stream = TcpStream::connect(addr).await.expect("connect replacement");
        let result = kafka_wire::round_trip()
            .stream(&mut stream)
            .client_id(CLIENT_ID)
            .body(&api_versions())
            .call()
            .await;
        let done = result.is_ok() || Instant::now() >= deadline;
        slots.result = Some(result);
        if done {
            break;
        }
        // Handler-local ConnectionGuard release has no metadata awaiter.
        time::sleep(Duration::from_millis(50)).await;
    }
}
#[then("a replacement connection is served after the original slot is released")]
fn replacement_served(slots: &ConnectionSlots) {
    assert!(
        slots
            .result
            .as_ref()
            .expect("replacement retry completed")
            .is_ok(),
        "per-IP slot was not freed after first connection closed"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/ip_quotas.feature",
    name = "An administrator changes and reads the per IP creation rate"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ip_quota_alter_then_describe_round_trip(quotas: QuotaAdministration) {
    let (broker, _dir, _addr) = quotas.broker.expect("quota broker started");
    broker.shutdown().await;
}
#[scenario(
    path = "tests/features/migrated/tests/ip_quotas.feature",
    name = "The canonical loopback IP rate limits connections independently"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_closes_the_throttled_connection(rate: ConnectionRate) {
    let (broker, _dir, _addr) = rate.broker.expect("rate broker started");
    broker.shutdown().await;
}
#[scenario(
    path = "tests/features/migrated/tests/ip_quotas.feature",
    name = "Equivalent loopback address spellings share the same connection quota"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_matches_another_spelling_of_the_address(
    rate: ConnectionRate,
    entity: String,
) {
    let _ = entity;
    let (broker, _dir, _addr) = rate.broker.expect("rate broker started");
    broker.shutdown().await;
}
#[scenario(
    path = "tests/features/migrated/tests/ip_quotas.feature",
    name = "Unconfigured connection creation rates leave the baseline unthrottled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_ip_unaffected(slots: ConnectionSlots) {
    let (broker, _dir, _addr) = slots.broker.expect("unlimited broker started");
    broker.shutdown().await;
}
#[scenario(
    path = "tests/features/migrated/tests/ip_quotas.feature",
    name = "A simultaneous connection cap releases capacity when the held connection closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_connections_per_ip_refuses_excess_and_frees_on_close(slots: ConnectionSlots) {
    let (broker, _dir, _addr) = slots.broker.expect("single slot broker started");
    broker.shutdown().await;
}
