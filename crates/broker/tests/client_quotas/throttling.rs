//! Tests for quota enforcement on the request path.
//!
//! Each test sets one quota for alice, drives the request the quota governs,
//! and asserts that the broker reports the delay to the client as a non-zero
//! `throttle_time_ms` (KIP-219 throttle-then-respond) instead of silently
//! stalling the connection.

use {
    super::{
        CLIENT_ID,
        cluster::{
            create_topic_as_admin, seed_alice_write_acl, seed_compat_shim_disable_acl,
            start_single_broker_sasl_plaintext_with_users, wait_partition_exists,
        },
        data_plane::{drive_add_offsets_to_txn, drive_fetch_sasl, drive_produce_sasl},
        quota_admin::drive_alter_client_quotas_sasl,
    },
    crate::{
        kafka_wire::{
            self, PlainAuthenticationSetup, ProduceRecordCount, ProduceValueBytes,
            ResponseHeaderSelection as WireResponseHeaderSelection, ZeroFilledRecordsSetup,
            quotas::{QuotaAlterSetup, QuotaValue, alter_user_quota},
        },
        support::{
            acl::seed_topic_acl, discovery::api_versions_request_for, topics::ConfiguredTopicSetup,
            wire::HeaderEncoding as RequestHeaderEncoding,
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            api_versions_response::ApiVersionsResponse, fetch_response::FetchResponse,
            produce_response::ProduceResponse,
        },
    },
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tokio::{net::TcpStream, time},
};

/// `ApiVersions` is `api_key` 18.
const API_VERSIONS_KEY: i16 = 18;

/// The broker's default `quota_throttle_max`, in milliseconds. KIP-219 caps
/// the reported back-off at this, so no response may carry more.
const QUOTA_THROTTLE_MAX_MS: i32 = 1000;

async fn throttled_response<R>(
    stream: &mut TcpStream,
    drive: impl AsyncFn(&mut TcpStream, i32) -> R,
    throttle: impl Fn(&R) -> i32,
    failure: &str,
) -> R {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut correlation = 11;
    loop {
        let response = drive(stream, correlation).await;
        correlation += 1;
        if throttle(&response) > 0 {
            return response;
        }
        assert!(Instant::now() <= deadline, "{failure}");
    }
}

/// `ApiVersions` v3 is the version driven below: v1 is the first with a
/// `ThrottleTimeMs` at all, and v3 is the first flexible one, which is what a
/// modern client negotiates with.
const API_VERSIONS_VERSION: i16 = 3;

/// Open the fixture's Alice session without charging a second handshake.
async fn alice_quota_connection(addr: SocketAddr, context: &str) -> TcpStream {
    kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        password: b"alice-secret".to_vec(),
        ..Default::default()
    })
    .await
    .expect(context)
}

/// Drives one `ApiVersions` v3 handshake on an already-authenticated `stream`
/// and returns the decoded response.
///
/// The request body is flexible from v3, but the `ApiVersions` *response*
/// header is v0 at every version -- Kafka's one header exception, so that a
/// client which has not yet learned the broker's versions can always parse the
/// reply. The two header flexibilities therefore differ.
async fn drive_api_versions(stream: &mut TcpStream, corr_id: i32) -> ApiVersionsResponse {
    let req = api_versions_request_for("krabka-quota-test".to_owned(), "0.0.1".to_owned());
    let mut body = BytesMut::new();
    req.encode(&mut body, API_VERSIONS_VERSION)
        .expect("encode ApiVersions");
    let resp_bytes = kafka_wire::round_trip()
        .stream(stream)
        .api_key(API_VERSIONS_KEY)
        .version(API_VERSIONS_VERSION)
        .correlation(corr_id)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .response_header(WireResponseHeaderSelection::Exact(
            RequestHeaderEncoding::Legacy,
        ))
        .body(&body)
        .call()
        .await
        .expect("ApiVersions round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ApiVersionsResponse::decode(&mut cur, API_VERSIONS_VERSION).expect("decode ApiVersionsResponse")
}

async fn produce_when_acl_applied(
    addr: SocketAddr,
    records: ZeroFilledRecordsSetup,
) -> ProduceResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = drive_produce_sasl(addr, records.clone(), &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            password: (b"alice-secret").to_vec(),
            ..Default::default()
        })
        .await;
        let code = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if code != 29 {
            return r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// A SASL broker with its quota users, the directory that backs it, and its
/// listen address.
struct QuotaFixture {
    handle: krabka_broker::BrokerHandle,
    dir: tempfile::TempDir,
    addr: SocketAddr,
}

async fn quota_fixture() -> QuotaFixture {
    let (handle, dir, addr) = start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", "admin-secret"),
        ("alice", "alice-secret"),
    ])
    .await;
    QuotaFixture { handle, dir, addr }
}

async fn prepare_quota_topic(
    broker: &krabka_broker::BrokerHandle,
    addr: SocketAddr,
    topic: &str,
    operation: krabka_metadata::AclOperation,
) {
    seed_compat_shim_disable_acl(broker).await;
    create_topic_as_admin()
        .addr(addr)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: (topic).into(),
            ..Default::default()
        })
        .call()
        .await;
    wait_partition_exists(broker, topic, 0).await;
    seed_topic_acl()
        .broker(broker)
        .topic(topic)
        .operation(operation)
        .call()
        .await;
}

fn check_successful_produce(response: &ProduceResponse) {
    let part = &response.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
}

#[derive(krabka_macros::FieldDefaults)]
struct QuotaSettingSetup<'a> {
    #[default("producer_byte_rate")]
    key: &'a str,
    value: QuotaValue,
}

#[bon::builder]
async fn set_alice_quota(
    broker: &krabka_broker::BrokerHandle,
    addr: SocketAddr,
    #[builder(default = "producer_byte_rate")] key: &'_ str,
    #[builder(into, default)] value: QuotaValue,
) {
    let setup = QuotaSettingSetup { key, value };
    kafka_wire::quotas::set_user_quota()
        .broker(broker)
        .addr(addr)
        .authentication(PlainAuthenticationSetup::admin(CLIENT_ID))
        .key(setup.key)
        .value(setup.value)
        .call()
        .await;
}

use {crate::support::client::metric_value, krabka_metadata::AclOperation};
struct QuotaEnforcement {
    broker: Option<QuotaFixture>,
    topic: String,
    produced: Option<ProduceResponse>,
    fetched: Option<FetchResponse>,
    stream: Option<TcpStream>,
    offsets_baseline: Option<AddOffsetsToTxnResponse>,
    offsets_throttled: Option<AddOffsetsToTxnResponse>,
    versions_baseline: Option<ApiVersionsResponse>,
    versions_throttled: Option<ApiVersionsResponse>,
}
impl QuotaEnforcement {
    async fn shutdown(self) {
        let QuotaFixture {
            handle,
            dir: _directory,
            ..
        } = self.broker.expect("quota broker started");
        drop(self.stream);
        handle.shutdown().await;
    }
}
#[rstest::fixture]
fn quota() -> QuotaEnforcement {
    // Start this scenario with fresh inputs and observations.

    QuotaEnforcement {
        broker: None,
        topic: String::new(),
        produced: None,
        fetched: None,
        stream: None,
        offsets_baseline: None,
        offsets_throttled: None,
        versions_baseline: None,
        versions_throttled: None,
    }
}
#[rstest_bdd_macros::given("a SASL quota broker has super user admin and ordinary user alice")]
async fn quota_enforcement_broker(quota: &mut QuotaEnforcement) {
    quota.broker = Some(quota_fixture().await);
}
#[rstest_bdd_macros::given("topic {topic} permits alice to {operation}")]
async fn quota_enforcement_topic(quota: &mut QuotaEnforcement, topic: String, operation: String) {
    let QuotaFixture { handle, addr, .. } = quota.broker.as_ref().expect("quota broker started");
    let addr = *addr;
    let grant = match operation.as_str() {
        "Write" => AclOperation::Write,
        "Read" => AclOperation::Read,
        other => panic!("unsupported topic grant {other}"),
    };
    prepare_quota_topic(handle, addr, &topic, grant).await;
    quota.topic = topic;
}
#[rstest_bdd_macros::given("alice has {key} quota {rate}")]
async fn quota_enforcement_rate(quota: &mut QuotaEnforcement, key: String, rate: f64) {
    let QuotaFixture { handle, addr, .. } = quota.broker.as_ref().expect("quota broker started");
    let addr = *addr;
    set_alice_quota()
        .broker(handle)
        .addr(addr)
        .key(&key)
        .value(rate)
        .call()
        .await;
}
#[rstest_bdd_macros::when("alice produces {count} records of {value_bytes} bytes to the topic")]
async fn quota_produce_records(quota: &mut QuotaEnforcement, count: usize, value_bytes: usize) {
    let addr = quota.broker.as_ref().expect("quota broker started").addr;
    quota.produced = Some(
        produce_when_acl_applied(addr, ZeroFilledRecordsSetup {
            topic: quota.topic.clone(),
            count: ProduceRecordCount(count),
            value_bytes: ProduceValueBytes(value_bytes),
        })
        .await,
    );
}
#[rstest_bdd_macros::then("the produce partition succeeds and reports a positive throttle delay")]
fn quota_produce_throttled(quota: &QuotaEnforcement) {
    let response = quota.produced.as_ref().expect("records produced");
    check_successful_produce(response);
    assert!(
        response.throttle_time_ms > 0,
        "expected positive throttle, got {}",
        response.throttle_time_ms
    );
}
#[rstest_bdd_macros::then(
    "Produce latency and producer quota metrics cover the delay while request quota credit stays zero"
)]
async fn quota_produce_metrics(quota: &QuotaEnforcement) {
    let handle = &quota.broker.as_ref().expect("quota broker started").handle;
    let resp = quota.produced.as_ref().expect("records produced");
    let applied_seconds = f64::from(resp.throttle_time_ms) / 1000.0;
    let phase = metric_value(
        handle,
        "krabka_broker_request_throttle_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    assert!(
        phase >= applied_seconds,
        "the throttle phase must cover the delay the response reported \
         ({applied_seconds}s); got {phase}"
    );

    // The byte-rate quota is what fired, so it is what the applied-throttle
    // family credits. The request quota is unset and must stay at zero.
    let by_quota = metric_value(
        handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Produce\"}",
    )
    .await;
    assert!(
        by_quota >= applied_seconds,
        "producer_byte_rate must be credited with the applied throttle \
         ({applied_seconds}s); got {by_quota}"
    );
    let request_quota = metric_value(
        handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Request\"}",
    )
    .await;
    assert!(
        request_quota < 1.0e-9,
        "no request_percentage quota is set, so it must be credited with \
         nothing; got {request_quota}"
    );
}
#[rstest_bdd_macros::given("admin has stored eight 1024 byte records in throttle-fetch")]
async fn quota_fetch_records(quota: &mut QuotaEnforcement) {
    let QuotaFixture { handle, addr, .. } = quota.broker.as_ref().expect("quota broker started");
    let addr = *addr;
    // Produce 8 KB as admin (not subject to quota yet).
    seed_alice_write_acl(handle, "throttle-fetch").await; // give admin path a topic
    let produce_resp = drive_produce_sasl(
        addr,
        ZeroFilledRecordsSetup {
            topic: ("throttle-fetch").to_owned(),
            count: ProduceRecordCount(8),
            ..Default::default()
        },
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            user: "admin",
            password: (b"admin-secret").to_vec(),
        },
    )
    .await;
    let part = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "admin produce must succeed, error_code={}",
        part.error_code
    );
}
#[rstest_bdd_macros::when("alice fetches throttle-fetch after the Read grant applies")]
async fn quota_fetch_attempt(quota: &mut QuotaEnforcement) {
    let addr = quota.broker.as_ref().expect("quota broker started").addr;
    let deadline = Instant::now() + Duration::from_secs(15);
    let fetch_resp = loop {
        let r = drive_fetch_sasl(addr, "alice", b"alice-secret", "throttle-fetch").await;
        // If throttle_time_ms > 0 or error_code == 0, we have a real response.
        if r.error_code == 0 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "fetch error after 15s; error_code={}",
            r.error_code
        );
        // real-time wait (not a progress poll): retry cadence between network fetch attempts, deadline-guarded
        time::sleep(Duration::from_millis(100)).await;
    };

    quota.fetched = Some(fetch_resp);
}
#[rstest_bdd_macros::then(
    "the throttled fetch reports a positive delay with no records rows errors or session"
)]
fn quota_fetch_throttled(quota: &QuotaEnforcement) {
    let fetch_resp = quota.fetched.as_ref().expect("fetch attempted");
    assert!(
        fetch_resp.throttle_time_ms > 0,
        "expected consumer throttle_time_ms > 0, got {}",
        fetch_resp.throttle_time_ms
    );
    // Kafka answers a throttled consumer fetch with an empty response: no
    // rows, no error, and no session (`FetchContext.getThrottledResponse`).
    // The client did not get the records, so its position does not move.
    assert!(
        *fetch_resp
            == FetchResponse {
                throttle_time_ms: fetch_resp.throttle_time_ms,
                ..Default::default()
            }
    );
}
#[rstest_bdd_macros::given(
    "alice producer rate is 8192 but the krabka-quota-test user-client tuple rate is 128"
)]
async fn quota_tuple_precedence(quota: &mut QuotaEnforcement) {
    let QuotaFixture { handle, addr, .. } = quota.broker.as_ref().expect("quota broker started");
    let addr = *addr;
    // Set a lenient user-only quota.
    let alter_user = alter_user_quota()
        .addr(addr)
        .authentication(PlainAuthenticationSetup::admin(CLIENT_ID))
        .value(8192.0)
        .call()
        .await;
    assert!(alter_user[0].1 == 0, "alter user quota must succeed");

    // Set a tight tuple quota for `CLIENT_ID`.
    let alter_tuple = drive_alter_client_quotas_sasl(
        addr,
        vec![(
            vec![
                ("user".into(), Some("alice".into())),
                ("client-id".into(), Some("krabka-quota-test".into())),
            ],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        &QuotaAlterSetup {
            authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
            ..Default::default()
        },
    )
    .await;
    assert!(alter_tuple[0].1 == 0, "alter tuple quota must succeed");

    // Wait for both quotas to appear in the image.
    handle
        .wait_for_image(|img| {
            let user_key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            let tuple_key: krabka_metadata::EntityKey = vec![
                ("client-id".into(), Some("krabka-quota-test".into())),
                ("user".into(), Some("alice".into())),
            ];
            let user_rate = img
                .client_quotas()
                .get(&user_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            let tuple_rate = img
                .client_quotas()
                .get(&tuple_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            user_rate == Some(8192.0) && tuple_rate == Some(128.0)
        })
        .await;
}
#[rstest_bdd_macros::given("alice uses one authenticated connection for the requests")]
async fn quota_persistent_connection(quota: &mut QuotaEnforcement) {
    let addr = quota.broker.as_ref().expect("quota broker started").addr;
    quota.stream = Some(alice_quota_connection(addr, "authenticate quota session").await);
}
#[rstest_bdd_macros::when("the connection sends an unthrottled AddOffsetsToTxn request")]
async fn quota_offsets_baseline(quota: &mut QuotaEnforcement) {
    quota.offsets_baseline = Some(
        drive_add_offsets_to_txn(quota.stream.as_mut().expect("alice authenticated"), 10).await,
    );
}
#[rstest_bdd_macros::then("AddOffsetsToTxn initially reports zero throttle")]
fn quota_offsets_initially_unthrottled(quota: &QuotaEnforcement) {
    let baseline = quota
        .offsets_baseline
        .as_ref()
        .expect("baseline offsets request sent");
    assert!(
        baseline.throttle_time_ms == 0,
        "unthrottled response must report throttle_time_ms=0"
    );
}
#[rstest_bdd_macros::when(
    "the same connection drives AddOffsetsToTxn until the request quota throttles"
)]
async fn quota_offsets_exhausted(quota: &mut QuotaEnforcement) {
    quota.offsets_throttled = Some(
        throttled_response(
            quota.stream.as_mut().expect("alice authenticated"),
            drive_add_offsets_to_txn,
            |response| response.throttle_time_ms,
            "no request-quota throttle on AddOffsetsToTxn after 15s",
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the positive AddOffsetsToTxn throttle leaves every other response field unchanged"
)]
fn quota_offsets_patch(quota: &QuotaEnforcement) {
    let baseline = quota
        .offsets_baseline
        .as_ref()
        .expect("baseline offsets request sent");
    let throttled = quota
        .offsets_throttled
        .as_ref()
        .expect("throttled offsets request sent");
    assert!(throttled.throttle_time_ms > 0);
    assert!(
        *throttled
            == AddOffsetsToTxnResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                ..baseline.clone()
            }
    );
}
#[rstest_bdd_macros::when("the connection sends an unthrottled API versions v3 request")]
async fn quota_versions_baseline(quota: &mut QuotaEnforcement) {
    quota.versions_baseline =
        Some(drive_api_versions(quota.stream.as_mut().expect("alice authenticated"), 10).await);
}
#[rstest_bdd_macros::then("API versions initially succeeds with zero throttle")]
fn quota_versions_initially_unthrottled(quota: &QuotaEnforcement) {
    let baseline = quota
        .versions_baseline
        .as_ref()
        .expect("baseline API versions sent");
    assert!(
        baseline.error_code == 0,
        "ApiVersions must succeed, error_code={}",
        baseline.error_code
    );
    assert!(
        baseline.throttle_time_ms == 0,
        "unthrottled response must report throttle_time_ms=0, got {}",
        baseline.throttle_time_ms
    );
}
#[rstest_bdd_macros::when(
    "the same connection drives API versions v3 until the request quota throttles"
)]
async fn quota_versions_exhausted(quota: &mut QuotaEnforcement) {
    quota.versions_throttled = Some(
        throttled_response(
            quota.stream.as_mut().expect("alice authenticated"),
            drive_api_versions,
            |response| response.throttle_time_ms,
            "no request-quota throttle on ApiVersions after 15s",
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "API versions reports a positive capped delay and advancing epoch with all advertised APIs and features unchanged"
)]
fn quota_versions_accounted(quota: &QuotaEnforcement) {
    let baseline = quota
        .versions_baseline
        .as_ref()
        .expect("baseline API versions sent");
    let throttled = quota
        .versions_throttled
        .as_ref()
        .expect("throttled API versions sent");
    assert!(throttled.throttle_time_ms > 0);
    // The handler set the field on the typed response, so nothing else moved:
    // the advertised table and the feature rows are the baseline's. The
    // finalized features epoch is the metadata offset, as Kafka's
    // `KRaftMetadataCache.features` reports it, so the quota record written in
    // between moves it on.
    assert!(throttled.finalized_features_epoch > baseline.finalized_features_epoch);
    assert!(
        *throttled
            == ApiVersionsResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                finalized_features_epoch: throttled.finalized_features_epoch,
                ..baseline.clone()
            }
    );
    assert!(
        throttled.throttle_time_ms <= QUOTA_THROTTLE_MAX_MS,
        "reported back-off must stay inside quota_throttle_max, got {}",
        throttled.throttle_time_ms
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "producer byte rate throttles produce"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttles_produce(quota: QuotaEnforcement) { quota.shutdown().await; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "request percentage throttles produce"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttles_produce(quota: QuotaEnforcement) { quota.shutdown().await; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "producer byte rate throttle moves the throttle metrics"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttle_moves_the_throttle_metrics(quota: QuotaEnforcement) {
    quota.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "consumer byte rate throttles fetch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_byte_rate_throttles_fetch(quota: QuotaEnforcement) { quota.shutdown().await; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "user client tuple overrides user specific"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_client_tuple_overrides_user_specific(quota: QuotaEnforcement) {
    quota.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "request percentage throttle is echoed on a patched api"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttle_is_echoed_on_a_patched_api(quota: QuotaEnforcement) {
    quota.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/throttling.feature",
    name = "request percentage throttle is reported on api versions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttle_is_reported_on_api_versions(quota: QuotaEnforcement) {
    quota.shutdown().await;
}
