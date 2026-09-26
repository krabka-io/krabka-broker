//! The full KIP-48 lifecycle walk in one test: mint a token, authenticate
//! with it over SASL/SCRAM-SHA-256 and SASL/SCRAM-SHA-512, renew it as a
//! listed renewer, describe it, expire it, and prove that the expired
//! credentials no longer authenticate.
//!
//! This is the file that covers spec §8.2 step by step. The steps are
//! lettered (a) to (h) in the test body and in the suite-level documentation
//! on the crate root.

use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use base64::Engine;
use krabka_protocol::owned::{
    create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
    describe_delegation_token_request::{
        DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    },
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    renew_delegation_token_request::RenewDelegationTokenRequest,
};
use krabka_security::SaslMechanism;
use tokio::net::TcpStream;

use crate::{
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
    cluster::{start_broker, wait_for_token, wait_for_token_gone},
    rpc::{
        send_create_delegation_token, send_describe_delegation_token, send_expire_delegation_token,
        send_renew_delegation_token,
    },
    wire::{sasl_plain_authenticate, sasl_scram_authenticate, sasl_scram_token_authenticate},
};

/// The broker's `delegation_token_default_renew_period` in `cluster.rs`.
const DEFAULT_RENEW_PERIOD_MS: i64 = 24 * 60 * 60 * 1_000;

/// Step (c): the token logs in under both SCRAM mechanisms with
/// `tokenauth=true`, and not at all without it. Returns the SCRAM-SHA-256
/// session.
async fn token_logins(
    addr: SocketAddr,
    token_id: &str,
    token_password: &str,
) -> Result<TcpStream, String> {
    let without_tokenauth = sasl_scram_authenticate(
        addr,
        SaslMechanism::ScramSha256,
        token_id,
        token_password,
        false,
    )
    .await;
    assert!(
        without_tokenauth.is_err(),
        "token credentials without tokenauth=true must not authenticate"
    );
    let sha512 =
        sasl_scram_token_authenticate(addr, SaslMechanism::ScramSha512, token_id, token_password)
            .await
            .map_err(|e| format!("token SCRAM-SHA-512 auth: {e}"))?;
    drop(sha512);
    sasl_scram_token_authenticate(addr, SaslMechanism::ScramSha256, token_id, token_password)
        .await
        .map_err(|e| format!("token SCRAM-SHA-256 auth: {e}"))
}

fn wall_clock_ms() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock is after the epoch");
    i64::try_from(since_epoch.as_millis()).expect("epoch millis fit in i64")
}

// ─────────────────────────────────────────────────────────────────────────────
// The lifecycle test.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegation_token_lifecycle_end_to_end() {
    let (handle, _dir, addr) = start_broker().await;

    let result: Result<(), String> = async {
        // ── (a) alice authenticates over SASL/PLAIN.
        let mut alice = sasl_plain_authenticate(addr, "alice", b"wonderland")
            .await
            .map_err(|e| format!("alice PLAIN auth: {e}"))?;

        // ── (b) alice mints a delegation token, with bob as a renewer.
        //         `max_lifetime_ms = -1` → broker uses its ceiling.
        let create_req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            renewers: vec![CreatableRenewers {
                principal_type: "User".to_string(),
                principal_name: "bob".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let create_resp = send_create_delegation_token(&mut alice, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(alice): {e}"))?;
        if create_resp.error_code != 0 {
            return Err(format!(
                "Create failed: code={} principal={}:{} requester={}:{}",
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
        check!(create_resp.token_requester_principal_name == "alice");
        check!(!create_resp.token_id.is_empty(), "token_id must be set");
        // HMAC-SHA-256 → 32 raw bytes.
        check!(create_resp.hmac.len() == 32, "HMAC length must be 32 bytes");
        check!(create_resp.expiry_timestamp_ms > create_resp.issue_timestamp_ms);

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();
        // Capture both timestamps: create sets the expiry one default renew
        // period out and the max at the lifetime ceiling, as separate values.
        let initial_expiry_ms = create_resp.expiry_timestamp_ms;
        let max_timestamp_ms = create_resp.max_timestamp_ms;
        assert!(
            initial_expiry_ms < max_timestamp_ms,
            "KIP-48 separation invariant: initial expiry ({initial_expiry_ms}) must be strictly \
             less than max ({max_timestamp_ms}) when default_renew_period < max_lifetime",
        );

        // Wait briefly for the V1DelegationToken record to apply on this
        // node's image — every subsequent step reads it back via the same
        // controller, so the visibility window is tiny but non-zero.
        let img_token = wait_for_token(&handle, &token_id).await;
        check!(img_token.owner.principal_type == "User");
        check!(img_token.owner.name == "alice");
        assert!(
            img_token.renewers.len() == 1,
            "renewers must carry exactly the requested entry"
        );
        check!(img_token.renewers[0].principal_type == "User");
        check!(img_token.renewers[0].name == "bob");

        // ── (c) Open a second connection and SASL/SCRAM authenticate with
        //         username=token_id, password=base64(hmac) and the
        //         `tokenauth=true` extension. Kafka prepares a token
        //         credential for every SCRAM mechanism, so SHA-512 works as
        //         well as SHA-256. Without the extension the token id is an
        //         ordinary username the credential store does not hold.
        let token_password = base64::engine::general_purpose::STANDARD.encode(&hmac_bytes);
        let mut tokenuser = token_logins(addr, &token_id, &token_password).await?;

        // ── (d) From the token-authed connection, Create must fail with
        //         DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). This is the
        //         load-bearing oracle for the principal-override check —
        //         that error is only reachable when the broker sees this
        //         session as `authenticated_via_token = true`, which is set
        //         in the same branch that overrides the principal back to
        //         the token's owner (here, alice). If the override regressed
        //         and the principal stayed as the token_id, the request
        //         would fail with INVALID_REQUEST (or be authorized as a
        //         brand-new user). 64 is the unambiguous proof.
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
             got {} — principal override may have regressed",
            create_via_token.error_code
        );

        // ── (e) Third connection: bob (a listed renewer) calls Renew.
        //         Renew authorization (owner OR renewer) is load-bearing
        //         here. Kafka's `renewDelegationToken` sets the expiry to
        //         `min(max, now + min(default renew period, requested))`,
        //         so a 30-day request under the 24-hour default lands at
        //         `now + 24h`, not at the 7-day max.
        let mut bob = sasl_plain_authenticate(addr, "bob", b"builder")
            .await
            .map_err(|e| format!("bob PLAIN auth: {e}"))?;
        // The broker reads its clock while the request is in flight, so its
        // `now` lies between these two readings.
        let before_renew_ms = wall_clock_ms();
        let renew_resp = send_renew_delegation_token(
            &mut bob,
            300,
            &RenewDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                renew_period_ms: 30 * 24 * 60 * 60 * 1_000, // 30d (> 7d ceiling)
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(bob): {e}"))?;
        let after_renew_ms = wall_clock_ms();
        check!(
            renew_resp.error_code == 0,
            "Renew by listed renewer must succeed; got {}",
            renew_resp.error_code
        );
        let renewed_ms = renew_resp.expiry_timestamp_ms;
        check!(
            (before_renew_ms + DEFAULT_RENEW_PERIOD_MS..=after_renew_ms + DEFAULT_RENEW_PERIOD_MS)
                .contains(&renewed_ms),
            "Renew must land one default renew period after the broker's now: \
             renewed={renewed_ms} window=[{before_renew_ms}, {after_renew_ms}] + 24h",
        );
        check!(
            renewed_ms >= initial_expiry_ms,
            "renew happens after create, so it cannot move the expiry earlier: \
             renewed={renewed_ms} initial={initial_expiry_ms}",
        );
        check!(
            renewed_ms < max_timestamp_ms,
            "a 30-day request is capped by the 24h default, not by the 7-day max: \
             renewed={renewed_ms} max={max_timestamp_ms}",
        );

        // ── (f) alice describes with an explicit owner filter — should see
        //         exactly the one token she owns.
        let describe_resp = send_describe_delegation_token(
            &mut alice,
            400,
            &DescribeDelegationTokenRequest {
                owners: Some(vec![DescribeDelegationTokenOwner {
                    principal_type: "User".to_string(),
                    principal_name: "alice".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("DescribeDelegationToken(alice): {e}"))?;
        check!(
            describe_resp.error_code == 0,
            "Describe must succeed; got {}",
            describe_resp.error_code
        );
        assert!(
            describe_resp.tokens.len() == 1,
            "alice must see exactly her one token; got {} entries",
            describe_resp.tokens.len()
        );
        check!(describe_resp.tokens[0].token_id == token_id);
        check!(describe_resp.tokens[0].principal_type == "User");
        check!(describe_resp.tokens[0].principal_name == "alice");

        // ── (g) alice expires the token (negative period = immediate delete).
        let expire_resp = send_expire_delegation_token(
            &mut alice,
            500,
            &ExpireDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(alice): {e}"))?;
        assert!(
            expire_resp.error_code == 0,
            "Expire must succeed; got {}",
            expire_resp.error_code
        );

        // Drop the still-open connections we used for the wire dance —
        // they'd otherwise sit around until the test ends.
        drop(alice);
        drop(bob);
        drop(tokenuser);

        // Wait for the tombstone to apply, so the SCRAM credential lookup
        // in step (h) sees a fully-removed token.
        wait_for_token_gone(&handle, &token_id).await;

        // ── (h) Fourth connection: SCRAM auth with the same token creds
        //         must now fail (the token is gone). The driver surfaces
        //         the failure either as a non-zero error_code on round 1
        //         (the token-store lookup misses) or as an EOF / connection
        //         close.
        let fresh_attempt = sasl_scram_token_authenticate(
            addr,
            SaslMechanism::ScramSha256,
            &token_id,
            &token_password,
        )
        .await;
        assert!(
            fresh_attempt.is_err(),
            "SCRAM with the expired token's creds must fail; got Ok"
        );

        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("delegation-token lifecycle failed: {msg}");
    }
}
