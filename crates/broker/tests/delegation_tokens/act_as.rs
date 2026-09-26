//! Act-as wire-path tests for spec §3.1: the `owner_principal_type` and
//! `owner_principal_name` request fields that let a super-user mint a token
//! owned by another principal, and the authorization gate that stops everyone
//! else from doing the same.

use assert2::{assert, check};
use base64::Engine;
use krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;

use crate::{
    DELEGATION_TOKEN_AUTHORIZATION_FAILED, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
    cluster::{start_broker_with_super_users, wait_for_token},
    rpc::send_create_delegation_token,
    wire::{sasl_plain_authenticate, sasl_scram_sha256_authenticate},
};

// ─────────────────────────────────────────────────────────────────────────────
// Act-as wire-path tests (spec §3.1). These exercise the
// `owner_principal_type` + `owner_principal_name` request fields on
// `CreateDelegationTokenRequest` (v3+), which let a super-user mint a token
// owned by *another* principal. Implemented in
// `handlers/create_delegation_token.rs`; these are the integration-level
// oracles for that wire path.
// ─────────────────────────────────────────────────────────────────────────────

/// Spec §3.1 test 1.
///
/// Super-user `admin` mints a delegation token owned by `alice`, through
/// act-as. The test verifies that:
///   - the request succeeds (`error_code = 0`)
///   - the response `principal_*` holds the OWNER, `User:alice`
///   - the response `token_requester_*` holds the CALLER, `User:admin`
///   - a second SCRAM-token-authed connection carries
///     `authenticated_via_token = true`. A second Create proves it by
///     returning `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64), which fires
///     only on token-authed sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_super_user_mints_token_owned_by_target() {
    let (handle, _dir, addr) =
        start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"])
            .await;

    let result: Result<(), String> = async {
        // (1) admin authenticates via SASL/PLAIN.
        let mut admin = sasl_plain_authenticate(addr, "admin", b"admin-pw")
            .await
            .map_err(|e| format!("admin PLAIN auth: {e}"))?;

        // (2) admin mints a token owned by alice. owner_principal_type=User,
        // owner_principal_name=alice, empty renewers, broker-chosen lifetime.
        let create_req = CreateDelegationTokenRequest {
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("alice".to_string()),
            max_lifetime_ms: -1,
            renewers: vec![],
            ..Default::default()
        };
        let create_resp = send_create_delegation_token(&mut admin, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(admin act-as alice): {e}"))?;
        if create_resp.error_code != 0 {
            return Err(format!(
                "act-as Create must succeed; got code={} principal={}:{} requester={}:{}",
                create_resp.error_code,
                create_resp.principal_type,
                create_resp.principal_name,
                create_resp.token_requester_principal_type,
                create_resp.token_requester_principal_name,
            ));
        }
        check!(create_resp.principal_type == "User");
        check!(create_resp.principal_name == "alice");
        check!(create_resp.token_requester_principal_type == "User");
        check!(create_resp.token_requester_principal_name == "admin");
        check!(!create_resp.token_id.is_empty(), "token_id must be set");
        check!(create_resp.hmac.len() == 32, "HMAC length must be 32 bytes");

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();

        // Wait for the V1DelegationToken record to replicate to this node's
        // image. Belt-and-suspenders — the SCRAM token-fallback lookup in
        // step (3) reads from the same image.
        let img_token = wait_for_token(&handle, &token_id).await;
        assert!(img_token.owner.principal_type == "User");
        assert!(img_token.owner.name == "alice");

        // (3) Open a second connection; SASL/SCRAM-SHA-256 with username =
        // token_id, password = base64(hmac). The token-fallback path
        // authenticates this session as the token's OWNER — alice.
        let token_password = base64::engine::general_purpose::STANDARD.encode(&hmac_bytes);
        let mut tokenuser = sasl_scram_sha256_authenticate(addr, &token_id, &token_password)
            .await
            .map_err(|e| format!("token SCRAM auth: {e}"))?;

        // (4) Re-Create from the token-authed connection MUST return 64
        // (DELEGATION_TOKEN_REQUEST_NOT_ALLOWED). This is the unambiguous
        // oracle that the broker tagged this session as
        // `authenticated_via_token = true` AND set the principal back to the
        // token's owner. If either flag/override regressed, the request
        // would either succeed (wrong) or fail with a different error.
        let create_via_token = send_create_delegation_token(
            &mut tokenuser,
            200,
            &CreateDelegationTokenRequest {
                max_lifetime_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("CreateDelegationToken(token-auth): {e}"))?;
        assert!(
            create_via_token.error_code == DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); \
             got {}",
            create_via_token.error_code
        );

        drop(admin);
        drop(tokenuser);
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("act-as super-user mint test failed: {msg}");
    }
}

/// Spec §3.1 test 2.
///
/// Non-super-user `alice` tries to act as another user and requests a token
/// owned by `bob`. The broker must reject it with
/// `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65). This is the authorization
/// gate for act-as. Without it, any authenticated user could mint tokens that
/// impersonate any other user.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_non_super_user_rejected_with_authorization_failed() {
    let (handle, _dir, addr) = start_broker_with_super_users(&[("alice", "alice-pw")], &[]).await;

    let result: Result<(), String> = async {
        let mut alice = sasl_plain_authenticate(addr, "alice", b"alice-pw")
            .await
            .map_err(|e| format!("alice PLAIN auth: {e}"))?;

        let create_req = CreateDelegationTokenRequest {
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("bob".to_string()),
            max_lifetime_ms: -1,
            renewers: vec![],
            ..Default::default()
        };
        let resp = send_create_delegation_token(&mut alice, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(alice act-as bob): {e}"))?;
        assert!(
            resp.error_code == DELEGATION_TOKEN_AUTHORIZATION_FAILED,
            "non-super-user act-as must be rejected with \
             DELEGATION_TOKEN_AUTHORIZATION_FAILED (65); got {}",
            resp.error_code
        );

        drop(alice);
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("act-as non-super-user reject test failed: {msg}");
    }
}
