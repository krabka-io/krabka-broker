//! `DescribeDelegationToken` tests that drive [`super::handle`] against a live
//! single-voter controller.
//!
//! Every case here is about the KIP-48 visible set: who sees which tokens with
//! and without an owner filter, the token-authed caller's isolation, and the
//! spec §5.3 extension that a `Describe` ACL on `TOKEN:<owner>` grants.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
use krabka_protocol::owned::describe_delegation_token_request::{
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
};
use krabka_raft::ControllerHandle;
use krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism, SecretBytes};
use tempfile::TempDir;

use super::handle;
use crate::network::auth::ConnectionAuth;

/// Spin up a single-voter `Controller` for tests, wait for leader.
async fn test_controller(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
    let cfg = krabka_raft::ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(1), log_dir)
    };
    let handle = Arc::new(krabka_raft::Controller::start(cfg).await.unwrap());
    let mut rx = handle.watch_leader();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while rx.borrow().is_none() {
        assert!(std::time::Instant::now() < deadline, "no leader in 5s");
        let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
    }
    handle
}

fn authed_with_token(name: &str, via_token: bool) -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        },
        mechanism: SaslMechanism::ScramSha256,
        expires_at_ms: None,
        authenticated_via_token: via_token,
    }
}

fn authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, false)
}

fn kp(name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".into(),
        name: name.into(),
    }
}

fn peer() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// The tests below want the "real ACL" semantics. The
/// describe-via-ACL extension should add tokens if and only if the
/// caller holds a matching `Describe` ACL on `TOKEN:<owner>`. This
/// helper builds a [`SimpleAclAuthorizer`] for that. With
/// [`AllowAllAuthorizer`] every token would surface. That is correct
/// under "allow everything", but it does not exercise the ACL filter
/// these tests are written against.
fn simple_authz() -> crate::authorizer::SimpleAclAuthorizer {
    crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new())
}

async fn seed_acl(controller: &ControllerHandle, entry: krabka_metadata::AclEntry) {
    controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(entry)])
        .await
        .expect("seed acl");
}

async fn seed_token(
    controller: &ControllerHandle,
    token_id: &str,
    owner: KafkaPrincipal,
    renewers: Vec<KafkaPrincipal>,
) {
    let rec = DelegationTokenRecord {
        token_id: token_id.into(),
        owner,
        hmac: vec![0u8; 32],
        issue_timestamp_ms: 1_000,
        expiry_timestamp_ms: 2_000,
        max_timestamp_ms: 3_000,
        renewers,
    };
    controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(rec)])
        .await
        .expect("seed token");
}

#[tokio::test]
async fn returns_auth_disabled_when_no_secret_key() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let req = DescribeDelegationTokenRequest::default();
    let resp = handle(
        &req,
        &authed("alice"),
        None,
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    controller.cancel().await;
}

#[tokio::test]
async fn empty_filter_returns_all_tokens_visible_to_caller() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // alice owns t-a; bob owns t-b; alice is a renewer on t-b.
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;
    // carol owns an unrelated token — alice should not see it.
    seed_token(&controller, "t-c", kp("carol"), vec![]).await;

    let req = DescribeDelegationTokenRequest::default();
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    let ids: std::collections::HashSet<&str> =
        resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
    let expected: std::collections::HashSet<&str> = ["t-a", "t-b"].into_iter().collect();
    assert!(ids == expected);
    controller.cancel().await;
}

#[tokio::test]
async fn owner_filter_intersects_with_visibility() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    // bob's token: alice is a renewer.
    seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;
    // carol's token: alice has no relationship.
    seed_token(&controller, "t-c", kp("carol"), vec![]).await;

    // Ask for tokens owned by either bob or carol. alice can see
    // t-b (renewer) but not t-c (no relationship).
    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "bob".into(),
                ..Default::default()
            },
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "carol".into(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    let ids: std::collections::HashSet<&str> =
        resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
    assert!(ids.len() == 1);
    assert!(ids.contains("t-b"));
    controller.cancel().await;
}

#[tokio::test]
async fn token_authed_caller_sees_only_own_owned_tokens() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // alice owns t-a; bob owns t-b; alice is a renewer on t-b.
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;

    // Wire owner filter asks for bob's tokens — but a token-authed
    // alice is restricted to her own owned set regardless.
    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![DescribeDelegationTokenOwner {
            principal_type: "User".into(),
            principal_name: "bob".into(),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed_with_token("alice", true),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    let ids: std::collections::HashSet<&str> =
        resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
    assert!(ids.len() == 1);
    assert!(ids.contains("t-a"));
    controller.cancel().await;
}

/// Spec §5.3: a caller who is neither owner nor a listed renewer can
/// still see a token when the owner grants it `Describe` on
/// `TOKEN:<owner_principal_string>`. Token-authed callers do NOT
/// pick this extension up. The test
/// `token_authed_caller_acl_extension_does_not_apply` below covers
/// that.
#[tokio::test]
async fn describe_grants_visibility_via_token_acl() {
    use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // alice owns t-a; bob has no owner/renewer relationship to it.
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    // Grant bob `Describe` on `TOKEN:User:alice`.
    seed_acl(
        &controller,
        AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        },
    )
    .await;

    // bob queries with an empty filter; the ACL extension should
    // surface alice's token.
    let req = DescribeDelegationTokenRequest::default();
    let resp = handle(
        &req,
        &authed("bob"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    let ids: std::collections::HashSet<&str> =
        resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
    assert!(
        ids.contains("t-a"),
        "expected ACL Describe on TOKEN:User:alice to make t-a visible to bob; got {ids:?}"
    );
    controller.cancel().await;
}

/// Token-authenticated callers stay restricted to their own owned
/// tokens even when an ACL would otherwise extend visibility.
#[tokio::test]
async fn token_authed_caller_acl_extension_does_not_apply() {
    use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    seed_acl(
        &controller,
        AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        },
    )
    .await;

    let req = DescribeDelegationTokenRequest::default();
    let resp = handle(
        &req,
        &authed_with_token("bob", true),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    // bob owns nothing — ACL extension MUST NOT surface alice's t-a.
    assert!(
        resp.tokens.is_empty(),
        "token-authed bob must not see alice's token via ACL; got {:?}",
        resp.tokens.iter().map(|t| &t.token_id).collect::<Vec<_>>()
    );
    controller.cancel().await;
}
