//! The full KIP-48 lifecycle walk in one test: mint a token, authenticate
//! with it over SASL/SCRAM-SHA-256, renew it as a listed renewer, describe
//! it, expire it, and prove that the expired credentials no longer
//! authenticate.
//!
//! This is the file that covers spec §8.2 step by step. The steps are
//! lettered (a) to (h) in the test body and in the suite-level documentation
//! on the crate root.

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

use crate::{
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
    cluster::{start_broker, wait_for_token, wait_for_token_gone},
    rpc::{
        send_create_delegation_token, send_describe_delegation_token, send_expire_delegation_token,
        send_renew_delegation_token,
    },
    wire::{sasl_plain_authenticate, sasl_scram_sha256_authenticate},
};

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
        // Capture both timestamps: with the KIP-48 fix, Renew must extend
        // `expiry_timestamp_ms` strictly past `create_resp.expiry_timestamp_ms`
        // but never push it past `create_resp.max_timestamp_ms`.
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

        // ── (c) Open a second connection and SASL/SCRAM-SHA-256 authenticate
        //         with username=token_id, password=base64(hmac). KIP-48
        //         token-fallback in `handle_authenticate_scram` is what makes
        //         this succeed; without it the broker would respond
        //         "unknown user" at round 1.
        let token_password = base64::engine::general_purpose::STANDARD.encode(&hmac_bytes);
        let mut tokenuser = sasl_scram_sha256_authenticate(addr, &token_id, &token_password)
            .await
            .map_err(|e| format!("token SCRAM auth: {e}"))?;

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
        //         Renew authorization (owner OR renewer) is what's load-bearing
        //         here. With the KIP-48 fix, Create sets
        //         `expiry_timestamp_ms = issue + 24h` and
        //         `max_timestamp_ms = issue + 7d` as SEPARATE values, so
        //         `min(now + renew_period_ms, max_timestamp_ms)` actually
        //         advances the expiry — bounded above by `max_timestamp_ms`.
        let mut bob = sasl_plain_authenticate(addr, "bob", b"builder")
            .await
            .map_err(|e| format!("bob PLAIN auth: {e}"))?;
        // Use a huge renew period so the clamp lands at `max_timestamp_ms`
        // regardless of wall-clock drift between Create and Renew.
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
        check!(
            renew_resp.error_code == 0,
            "Renew by listed renewer must succeed; got {}",
            renew_resp.error_code
        );
        // KIP-48: with the fix, Renew strictly extends the expiry past
        // its initial value, capped at `max_timestamp_ms`.
        check!(
            renew_resp.expiry_timestamp_ms > initial_expiry_ms,
            "Renew must strictly extend expiry past initial value: \
             renewed={} initial={}",
            renew_resp.expiry_timestamp_ms,
            initial_expiry_ms,
        );
        check!(
            renew_resp.expiry_timestamp_ms <= max_timestamp_ms,
            "Renew must never push expiry past max_timestamp_ms: \
             renewed={} max={}",
            renew_resp.expiry_timestamp_ms,
            max_timestamp_ms,
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
        //         must now fail (the token is gone). `sasl_scram_sha256_authenticate`
        //         surfaces the failure either as a non-zero error_code on
        //         round 1 (the credential lookup misses → "unknown user")
        //         or as an EOF / connection close.
        let fresh_attempt = sasl_scram_sha256_authenticate(addr, &token_id, &token_password).await;
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
