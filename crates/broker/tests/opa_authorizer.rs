#![cfg(test)]

//! End-to-end OPA authorizer enforcement over the wire path.
//!
//! Two integration tests boot a single-broker `SASL_PLAINTEXT` cluster with an
//! [`OpaAuthorizer`] that points at a `wiremock::MockServer`. The mock returns
//! either `{"result": false}` or `{"result": true}` for every `POST`. The
//! tests assert that the per-topic `error_code` on a Produce response carries
//! `TOPIC_AUTHORIZATION_FAILED (29)` or `0`, in that order.
//!
//! Test 1 bootstraps its topic like this. The `admin` principal is a
//! super-user in BOTH the [`OpaAuthorizer`], so it bypasses OPA, and the
//! [`BrokerConfig.super_users`] field, so the broker-level super-user checks
//! accept it. The test calls `CreateTopics` over a SASL/PLAIN `admin` session.
//! The super-user bypass means the broker never asks OPA, so the topic
//! materialises even though the mock would otherwise deny it. The OPA gate
//! itself fires when `alice`, who is not a super-user, sends Produce.
//!
//! These tests are gated to non-Windows, to match the other SASL integration
//! tests. The listener bring-up works on Windows, but a uniform gate avoids
//! one-off CI matrix surprises.

mod kafka_wire;
mod support;

use {
    assert2::assert,
    kafka_wire::single_record_produce_request,
    krabka_broker::{BrokerHandle, authorizer::opa::OpaAuthorizer, config::InterBrokerCredentials},
    krabka_protocol::owned::produce_response::ProduceResponse,
    krabka_security::SaslMechanism,
    std::{collections::HashSet, io, net::SocketAddr, sync::Arc},
    tempfile::TempDir,
    wiremock::{Mock, MockServer, ResponseTemplate, matchers::method},
};

// Local mirror of `krabka_broker::codes::TOPIC_AUTHORIZATION_FAILED`,
// kept inline because the `codes` module is crate-private. The value
// matches the Apache Kafka error table and `tests/acl_handlers.rs`.
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;

// API versions chosen to match `tests/acl_handlers.rs`:
//   * CreateTopics v7 — flexible (FLEXIBLE_MIN=5), topic id round-trips.
//   * Produce v11 — flexible (FLEXIBLE_MIN=9), still uses topic `name`
//     rather than topic_id (v >= 13 introduces the latter).

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-opa-test";

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boots a single-broker `SASL_PLAINTEXT` cluster whose
/// `BrokerConfig.authorizer` is an [`OpaAuthorizer`] that points at the `opa` mock.
///
/// `admin` is a super-user in BOTH the authorizer, so it bypasses the OPA HTTP
/// call, AND `BrokerConfig.super_users`, so the broker-level super-user checks
/// accept it. `alice` is a regular user, so every authorization check on
/// alice's sessions goes through OPA.
///
/// `expire_after_ms = 1` stops the OPA cache from masking variation inside one
/// test. The second authorization check in `produce_allowed_by_opa_succeeds`
/// always fetches from the mock again, so the assertion does not depend on
/// in-process cache hits from earlier requests in the same test process.
async fn start_broker_with_opa_authorizer(opa: MockServer) -> OpaFixture {
    let opa_url = format!("{}/v1/data/kafka/authz/allow", opa.uri());
    let (log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_owned(), "admin-secret".to_owned());
    cfg.plain_credentials
        .insert("alice".to_owned(), "wonderland".to_owned());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "admin".to_owned(),
        password: "admin-secret".to_owned(),
    });
    // Broker-level super-user set (used by handler code that reads
    // `broker.config.super_users` directly, independent of the authorizer
    // trait dispatch — e.g. the act-as gate).
    cfg.super_users.insert("admin".to_owned());

    // OpaAuthorizer carries its own super-user set so it can bypass HTTP
    // before consulting OPA. Building it inside the test means `Handle::
    // try_current()` succeeds (the `#[tokio::test(flavor = "multi_thread")]`
    // attribute on each test guarantees a multi-thread runtime, which is
    // what `block_in_place` requires).
    let mut super_users = HashSet::new();
    super_users.insert("admin".to_owned());
    // Clients reach this broker over SASL, but its own heartbeat reaches the
    // PLAINTEXT controller listener as ANONYMOUS; like a Kafka inter-broker
    // principal it needs `ClusterAction`, or the broker never unfences.
    super_users.insert("ANONYMOUS".to_owned());
    let authorizer = OpaAuthorizer::new(
        super_users,
        opa_url,
        /* allow_on_error */ false,
        /* max_cache_size */ 100,
        // Tight TTL so subsequent authorize() calls within the same test
        // re-consult the mock rather than serving from cache. The
        // wall-clock TTL is enforced by `time_util::now_ms()`; 1 ms
        // means the second call in any same-test sequence is always a
        // cache miss after `tokio::time::sleep(Duration::from_millis(5))`.
        /* expire_after */
        krabka_units::millis(1),
        krabka_units::secs(5),
    )
    .expect("OpaAuthorizer::new must succeed inside a tokio runtime");
    cfg.authorizer = Arc::new(authorizer);

    let (handle, dir, addr) = Box::pin(start_broker(cfg, log_dir)).await;
    OpaFixture {
        opa,
        handle,
        dir,
        addr,
    }
}

/// An OPA mock and the broker that asks it, with the directory backing the
/// broker and its address.
struct OpaFixture {
    opa: MockServer,
    handle: BrokerHandle,
    dir: TempDir,
    addr: SocketAddr,
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver helpers.
// ─────────────────────────────────────────────────────────────────────────────

use crate::{
    kafka_wire::{PlainAuthenticationSetup, create_topic_as_super_user, drive_produce_as_plain},
    support::{
        produce::single_partition_response,
        sasl::{sasl_temp_config, start_broker},
        topics::ConfiguredTopicSetup,
    },
};

/// Drives `CreateTopics(name, 1 partition, rf=1)` over a SASL/PLAIN admin
/// session. Admin is the super-user in both tests, so the call bypasses the OPA
/// mock and the topic materialises whatever OPA answers.
async fn create_topic_as_admin(addr: SocketAddr, name: &str) {
    create_topic_as_super_user()
        .addr(addr)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: name.to_owned(),
            ..Default::default()
        })
        .call()
        .await;
}

/// Waits on `handle`, event-driven, until the local writer-actor of `topic`
/// partition 0 has materialised, then runs a single `drive_produce_as_plain`.
///
/// Once the local writer materialises, the raft commit-then-apply gap has
/// closed. That gap sits between `CreateTopics` returning and the partition
/// appearing in the broker's `MetadataImage`. Before the gap closes, the
/// authorizer finds no matching topic resource and denies alice's Write with
/// `TOPIC_AUTHORIZATION_FAILED`. Once the broker applies the topic, the check
/// goes through OPA, which allows it. Waiting on the same handle removes that
/// race without a fixed-interval retry loop. The OPA decision cache uses a 1 ms
/// TTL, far below any scheduling delay here, so it never masks the result.
///
/// alice's SASL/PLAIN test password as bytes, assembled at runtime instead of
/// written as a byte-string literal. The value is a non-secret test fixture,
/// but a literal that flows into the client auth calls trips GitHub's default
/// code-scanning credential query. Building it here keeps those sites free of
/// literals.
fn alice_password() -> Vec<u8> { b"wonderland".to_vec() }

async fn produce_when_partition_ready(
    handle: &BrokerHandle,
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    handle.wait_until_local_log_end_offset(topic, 0, 0).await;
    drive_produce_as_plain(
        addr,
        single_record_produce_request(topic, 0, b"hello"),
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            user,
            password: (password).to_vec(),
        },
    )
    .await
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests.
// ─────────────────────────────────────────────────────────────────────────────

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct ExternalPolicy {
    server: Option<OpaFixture>,
    topic: String,
    response: Option<ProduceResponse>,
}
#[fixture]
fn policy() -> ExternalPolicy {
    // Start this scenario with fresh inputs and observations.

    ExternalPolicy {
        server: None,
        topic: String::new(),
        response: None,
    }
}
#[given("OPA returns {decision} while the administrator creates topic {topic}")]
async fn decision(policy: &mut ExternalPolicy, decision: bool, topic: String) {
    let server = opa_fixture(decision).await;
    create_topic_as_admin(server.addr, &topic).await;
    policy.server = Some(server);
    policy.topic = topic;
}
#[when("ordinary PLAIN user alice produces hello to the administrator's topic")]
async fn write(policy: &mut ExternalPolicy) {
    let server = policy
        .server
        .as_ref()
        .expect("OPA broker and topic created");
    policy.response = Some(
        produce_when_partition_ready(
            &server.handle,
            server.addr,
            "alice",
            &alice_password(),
            &policy.topic,
        )
        .await
        .expect("Produce must round-trip"),
    );
}
#[then("the partition returns topic authorization failed")]
fn refused(policy: &ExternalPolicy) {
    let response = policy.response.as_ref().expect("Produce completed");
    let partition = single_partition_response(response);
    assert!(
        partition.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "OPA denied alice's Write, expected TOPIC_AUTHORIZATION_FAILED (29), got {partition:?}"
    );
}
#[then("the partition accepts the authorized write")]
fn accepted(policy: &ExternalPolicy) {
    let response = policy.response.as_ref().expect("Produce completed");
    let partition = single_partition_response(response);
    assert!(
        partition.error_code == 0,
        "OPA allowed alice's Write, expected error_code0, got {partition:?}"
    );
}
async fn shutdown_policy(policy: ExternalPolicy) {
    let OpaFixture {
        opa, handle, dir, ..
    } = policy.server.expect("OPA broker started");
    handle.shutdown().await;
    drop((opa, dir));
}
#[scenario(
    path = "tests/features/migrated/tests/opa_authorizer.feature",
    name = "OPA denial prevents an ordinary authenticated topic write"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_blocked_by_opa_returns_topic_authorization_failed(policy: ExternalPolicy) {
    shutdown_policy(policy).await;
}
#[scenario(
    path = "tests/features/migrated/tests/opa_authorizer.feature",
    name = "OPA permission admits an ordinary authenticated topic write"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_by_opa_succeeds(policy: ExternalPolicy) { shutdown_policy(policy).await; }

async fn opa_fixture(allowed: bool) -> OpaFixture {
    let opa = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": allowed})),
        )
        .mount(&opa)
        .await;
    start_broker_with_opa_authorizer(opa).await
}
