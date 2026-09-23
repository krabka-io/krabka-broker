//! `DescribeDelegationToken` tests that drive [`super::handle`] against a live
//! single-voter controller.
//!
//! Every case here is about the KIP-48 visible set: `filterToken`'s
//! owner-or-renewer filter, the owner/renewer/ACL relationship it then
//! requires, the token id used as the ACL resource name, the empty-list
//! sentinel, and `allowTokenRequests`' blanket refusal of a
//! delegation-token-authenticated caller — the credential-disclosure gate
//! this suite exists to pin down.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use krabka_metadata::{
    AclEntry, AclOperation, DelegationTokenRecord, MetadataRecord, PatternType, PermissionType,
    ResourceType,
};
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

fn anonymous() -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: "ANONYMOUS".into(),
            auth_method: AuthMethod::Anonymous,
            groups: vec![],
        },
        mechanism: SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
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

/// The tests below want the "real ACL" semantics: the ACL extension should
/// add a token if and only if the caller holds a matching `Describe` ACL on
/// `DelegationToken:<token_id>`. With [`crate::authorizer::AllowAllAuthorizer`]
/// every token would surface, which is correct under "allow everything" but
/// does not exercise the ACL filter these tests are written against.
fn simple_authz() -> crate::authorizer::SimpleAclAuthorizer {
    crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new())
}

async fn seed_acl(controller: &ControllerHandle, entry: AclEntry) {
    controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(entry)])
        .await
        .expect("seed acl");
}

/// A `Describe` ACL on `DelegationToken:<token_id>` for `principal`, which is
/// the resource-name convention Kafka's own authorizer call
/// (`authHelper.authorize(..., DESCRIBE, DELEGATION_TOKEN, tokenId)`) and its
/// admin tooling use. A resource name of the owner's principal string
/// instead would grant every token of that owner from one ACL.
fn describe_token_acl(token_id: &str, principal: &str) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::DelegationToken,
        resource_name: token_id.into(),
        pattern_type: PatternType::Literal,
        principal: format!("User:{principal}"),
        host: "*".into(),
        operation: AclOperation::Describe,
        permission_type: PermissionType::Allow,
    }
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

fn token_ids(
    resp: &krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
) -> std::collections::HashSet<&str> {
    resp.tokens.iter().map(|t| t.token_id.as_str()).collect()
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
async fn anonymous_caller_is_rejected_without_exposing_token_hmacs() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;

    let resp = handle(
        &DescribeDelegationTokenRequest::default(),
        &anonymous(),
        Some(&secret),
        &*controller,
        &peer(),
        &crate::authorizer::AllowAllAuthorizer,
    );

    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert!(resp.tokens.is_empty());
    controller.cancel().await;
}

/// `allowTokenRequests`: a delegation-token-authenticated caller is refused
/// entirely, with no tokens returned, regardless of what it owns. This is
/// the fix for the credential-disclosure bug: a caller holding one token
/// must never be able to read a sibling token's HMAC by calling
/// `DescribeDelegationToken`.
#[tokio::test]
async fn token_authed_caller_is_refused_entirely() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // alice owns both t-a and t-b: a token-authed session must not be able
    // to read either one, including its own token's sibling.
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    seed_token(&controller, "t-b", kp("alice"), vec![]).await;

    let resp = handle(
        &DescribeDelegationTokenRequest::default(),
        &authed_with_token("alice", true),
        Some(&secret),
        &*controller,
        &peer(),
        &crate::authorizer::AllowAllAuthorizer,
    );

    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert!(
        resp.tokens.is_empty(),
        "token-authed caller must never see any token's HMAC, got {:?}",
        token_ids(&resp)
    );
    controller.cancel().await;
}

/// Check order: `allowTokenRequests` (64) fires before the
/// `tokenAuthEnabled` check (61), so a token-authed caller gets 64 even
/// when the broker also has no secret key configured.
#[tokio::test]
async fn token_authed_caller_gets_request_not_allowed_before_auth_disabled() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;

    let resp = handle(
        &DescribeDelegationTokenRequest::default(),
        &authed_with_token("alice", true),
        None,
        &*controller,
        &peer(),
        &crate::authorizer::AllowAllAuthorizer,
    );

    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    controller.cancel().await;
}

/// `ownersListEmpty`: a present-but-empty `owners` list returns no tokens
/// with `error_code = NONE`, distinct from a missing (null) list.
#[tokio::test]
async fn empty_owners_list_returns_no_tokens_without_error() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;

    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![]),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        &*controller,
        &peer(),
        &crate::authorizer::AllowAllAuthorizer,
    );
    assert!(resp.error_code == 0);
    assert!(resp.tokens.is_empty());
    controller.cancel().await;
}

/// Table-driven: which tokens a caller sees, varying the caller's
/// relationship to each token and the ACL state, matching
/// `DelegationTokenManager.filterToken`.
#[tokio::test]
async fn filter_token_matches_owner_renewer_or_token_id_acl() {
    struct Case {
        name: &'static str,
        acl: Option<AclEntry>,
        expected: &'static [&'static str],
    }

    let cases = [
        Case {
            name: "owner sees own token, not a sibling owned by someone else",
            acl: None,
            expected: &["t-owner"],
        },
        Case {
            name: "describe acl on the exact token id grants only that token",
            acl: Some(describe_token_acl("t-other", "alice")),
            expected: &["t-owner", "t-other"],
        },
        Case {
            name: "describe acl on a different token id grants exactly that token, nothing else",
            acl: Some(describe_token_acl("t-unrelated", "alice")),
            expected: &["t-owner", "t-unrelated"],
        },
    ];

    for Case {
        name,
        acl,
        expected,
    } in cases
    {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        seed_token(&controller, "t-owner", kp("alice"), vec![]).await;
        seed_token(&controller, "t-renewer", kp("bob"), vec![kp("carol")]).await;
        seed_token(&controller, "t-other", kp("bob"), vec![]).await;
        seed_token(&controller, "t-unrelated", kp("dave"), vec![]).await;
        if let Some(entry) = acl {
            seed_acl(&controller, entry).await;
        }

        let resp = handle(
            &DescribeDelegationTokenRequest::default(),
            &authed("alice"),
            Some(&secret),
            &*controller,
            &peer(),
            &simple_authz(),
        );
        assert!(resp.error_code == 0, "{name}");
        let expected: std::collections::HashSet<&str> = expected.iter().copied().collect();
        assert!(token_ids(&resp) == expected, "{name}");
        controller.cancel().await;
    }
}

/// A caller who is a listed renewer (not the owner) sees the token too, and
/// the owner filter matches on owner-OR-renewer, not owner alone.
#[tokio::test]
async fn renewer_sees_the_token_and_owner_filter_matches_renewer_too() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // bob's token: carol is a listed renewer.
    seed_token(&controller, "t-b", kp("bob"), vec![kp("carol")]).await;
    // dave's token: carol has no relationship.
    seed_token(&controller, "t-d", kp("dave"), vec![]).await;

    // Filter asks for tokens whose owner-or-renewer is bob or dave. carol
    // is a renewer of t-b (owned by bob), so the filter matches it, and
    // carol's renewer relationship then makes it visible; t-d's owner
    // (dave) matches the filter too, but carol has no owner/renewer/ACL
    // relationship to it, so it stays hidden.
    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "bob".into(),
                ..Default::default()
            },
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "dave".into(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("carol"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    assert!(token_ids(&resp) == std::collections::HashSet::from(["t-b"]));
    controller.cancel().await;
}

/// A caller with a cluster-wide `Describe` ACL on the token's own id sees
/// it even without an owner filter naming that owner, and a `Describe` ACL
/// on one token id does not leak a second token owned by the same
/// principal — the resource-name fix this issue is about.
#[tokio::test]
async fn describe_acl_on_token_id_grants_exactly_that_token() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // alice owns two tokens; bob has no owner/renewer relationship to
    // either.
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;
    seed_token(&controller, "t-b", kp("alice"), vec![]).await;
    seed_acl(&controller, describe_token_acl("t-a", "bob")).await;

    let resp = handle(
        &DescribeDelegationTokenRequest::default(),
        &authed("bob"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    assert!(
        token_ids(&resp) == std::collections::HashSet::from(["t-a"]),
        "an ACL on t-a must not also surface t-b, same owner or not; got {:?}",
        token_ids(&resp)
    );
    controller.cancel().await;
}

/// A caller with no owner/renewer relationship and no matching ACL sees
/// nothing — pure token possession (proven by nothing here, since the
/// handler never inspects HMACs) grants no visibility on its own.
#[tokio::test]
async fn unrelated_caller_sees_nothing() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    seed_token(&controller, "t-a", kp("alice"), vec![]).await;

    let resp = handle(
        &DescribeDelegationTokenRequest::default(),
        &authed("eve"),
        Some(&secret),
        &*controller,
        &peer(),
        &simple_authz(),
    );
    assert!(resp.error_code == 0);
    assert!(resp.tokens.is_empty());
    controller.cancel().await;
}
