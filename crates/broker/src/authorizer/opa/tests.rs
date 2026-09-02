//! Behavior tests for [`OpaAuthorizer`] against a `wiremock` OPA endpoint:
//! the super-user bypass, the decision cache and its TTL, and the fail-open
//! and fail-closed branches that an OPA outage takes.
//!
//! The tests for the Kafka-to-OPA vocabulary mapping live beside that mapping
//! in [`super::wire`].

use std::{
    collections::HashSet,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use assert2::assert;
use krabka_authz::{AuthorizationRequest, AuthorizationResult, Authorizer};
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_security::{AuthMethod, Principal};
use krabka_units::{millis, minutes, secs};
use qubit_clock::ManualMonotonicClock;
use uuid::Uuid;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::*;

fn test_principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::SaslPlain,
        groups: vec![],
    }
}

fn img() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

fn host() -> SocketAddr {
    "1.2.3.4:9092".parse().unwrap()
}

fn req<'a>(p: &'a Principal, h: &'a SocketAddr, topic: &'a str) -> AuthorizationRequest<'a> {
    AuthorizationRequest {
        principal: p,
        host: h,
        resource_type: ResourceType::Topic,
        resource_name: topic,
        operation: AclOperation::Read,
    }
}

fn opa_url(server: &MockServer) -> String {
    format!("{}/v1/data/kafka/authz/allow", server.uri())
}

fn supers(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn super_user_bypasses_opa_call() {
    let mock = MockServer::start().await;
    // expect(0) verifies on drop that no HTTP call landed.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})),
        )
        .expect(0)
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        supers(&["admin"]),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("admin");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "anything")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_returns_cached_decision_without_http_call() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .expect(1) // exactly one call — second authorize() must hit cache.
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_preserves_a_deny_decision() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_miss_calls_opa_and_caches_result() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .expect(1)
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "fresh-topic")) == AuthorizationResult::Allow);
    // Cache populated; introspect by asserting a second call doesn't
    // bump the mock's request count when the assertion fires on drop.
    assert!(auth.authorize(&image, &req(&p, &h, "fresh-topic")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_entry_expires_after_ttl() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .expect(2) // first call + post-expiry call.
        .mount(&mock)
        .await;

    // 10ms decision-cache TTL, driven by an injected manual clock so the entry
    // expires on a controlled timeline — deterministic, no wall-clock sleep.
    // `timeline` is the advance handle; the wall clock it hands out is anchored
    // to it, so advancing one moves the other by the same amount.
    let timeline = ManualMonotonicClock::new_shared();
    let clock = timeline.new_wall_clock(SystemTime::now());
    let auth = OpaAuthorizer::with_clock(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        millis(10),
        secs(5),
        clock,
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    // Cache miss -> HTTP call #1; caches the decision with expires_at = now+10ms.
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    // The exact deadline is stale: freshness is a strict comparison.
    timeline
        .advance(Duration::from_millis(10))
        .expect("manual time moves forward");
    // Cache entry expired -> HTTP call #2 (verified by the mock's expect(2)).
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonpositive_cache_ttl_is_rejected() {
    for ttl in [millis(0), krabka_units::Time::from_millis(-1)] {
        let result = OpaAuthorizer::with_clock(
            HashSet::new(),
            "http://opa.invalid/v1/data/kafka/authz/allow".to_string(),
            false,
            1,
            ttl,
            secs(1),
            ManualMonotonicClock::new_shared().new_wall_clock(SystemTime::now()),
        );
        assert!(matches!(result, Err(OpaConfigError::InvalidCacheTtl)));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_error_with_allow_on_error_true_returns_allow() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock)
        .await;

    // allow_on_error=true → 500 maps to Allow.
    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        true,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn http_error_with_allow_on_error_false_returns_deny() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_http_timeout_fails_closed() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(250))
                .set_body_json(serde_json::json!({"result": true})),
        )
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        millis(25),
    )
    .unwrap();
    let image = img();
    let p = test_principal("alice");
    let h = host();

    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
}

#[tokio::test(flavor = "multi_thread")]
async fn json_response_parse_error_returns_per_allow_on_error_config() {
    // 200 OK but body isn't valid OPA JSON. The shape parses as
    // serde-json but lacks the `result` field — should fall through
    // to error_decision().
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json-at-all"))
        .mount(&mock)
        .await;

    let p = test_principal("alice");
    let h = host();
    let image = img();

    let auth_open = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        true,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    assert!(auth_open.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);

    let auth_closed = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        secs(5),
    )
    .unwrap();
    assert!(auth_closed.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
}
