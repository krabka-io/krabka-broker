//! Super-user bypass on Renew and Expire, spec §1.3 and §1.4.
//!
//! An operator that mints a token through act-as is neither the owner nor a
//! listed renewer, so the owner-or-renewer gate alone locks it out of its own
//! tokens. This file holds the end-to-end oracle for the super-user fast path
//! that Kafka's `DelegationTokenManager` applies before that gate.

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_delegation_token_request::CreateDelegationTokenRequest,
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    renew_delegation_token_request::RenewDelegationTokenRequest,
};

use crate::{
    cluster::{start_broker_with_super_users, wait_for_token, wait_for_token_gone},
    rpc::{
        send_create_delegation_token, send_expire_delegation_token, send_renew_delegation_token,
    },
    wire::sasl_plain_authenticate,
};

// ─────────────────────────────────────────────────────────────────────────────
// Super-user bypass (Renew + Expire).
//
// The Renew/Expire handlers originally gated on `caller == owner || caller in
// renewers` only. With operator-driven token issuance, the operator (a
// super-user) was unable to renew/expire tokens it minted via act-as on
// behalf of `KafkaUser` principals, because it was neither owner nor
// renewer. The super-user fast path that Kafka's
// `DelegationTokenManager.isAuthorizedToOperateOnToken` includes fixes this.
//
// This integration test exercises the wire path end-to-end: admin act-as
// mints a token owned by alice, then admin Renews and Expires it — both
// must succeed (no err 63 / 65).
// ─────────────────────────────────────────────────────────────────────────────

/// Super-user-bypass regression, spec §1.3 and §1.4.
///
/// Super-user `admin` mints a token owned by `alice` through act-as, then
/// renews and expires it over the wire. Both must succeed even though admin
/// is neither the owner nor a renewer.
///
/// This mirrors the kind-kafkauser-delegation-token e2e flow, which failed
/// with `RenewDelegationToken: UNKNOWN (63)` when the super-user bypass was
/// missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn super_user_can_renew_other_owners_token() {
    let (handle, _dir, addr) =
        start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"])
            .await;

    let result: Result<(), String> = async {
        // (1) admin authenticates via SASL/PLAIN.
        let mut admin = sasl_plain_authenticate(addr, "admin", b"admin-pw")
            .await
            .map_err(|e| format!("admin PLAIN auth: {e}"))?;

        // (2) admin act-as mints a token owned by alice — no renewers.
        // This is exactly what the operator does for a
        // delegation-token `KafkaUser`.
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
                "act-as Create must succeed; got code={}",
                create_resp.error_code,
            ));
        }
        assert!(create_resp.principal_name == "alice");

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();
        let initial_expiry_ms = create_resp.expiry_timestamp_ms;
        let max_timestamp_ms = create_resp.max_timestamp_ms;
        assert!(
            initial_expiry_ms < max_timestamp_ms,
            "KIP-48 separation invariant must hold so Renew has room to extend"
        );

        // Wait for the V1DelegationToken record to apply on this node's image.
        let img_token = wait_for_token(&handle, &token_id).await;
        assert!(img_token.owner.name == "alice");
        assert!(
            img_token.renewers.is_empty(),
            "no renewers were specified, so admin is neither owner NOR renewer"
        );

        // (3) admin Renews — this is the operator's renewal
        // path. Without the super-user bypass, this returned err 63
        // (DELEGATION_TOKEN_OWNER_MISMATCH); with the super-user bypass,
        // it must succeed and strictly extend the expiry.
        let renew_resp = send_renew_delegation_token(
            &mut admin,
            200,
            &RenewDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                renew_period_ms: 30 * 24 * 60 * 60 * 1_000, // 30d (> 7d ceiling → clamps to max)
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(admin super-user): {e}"))?;
        check!(
            renew_resp.error_code == 0,
            "super-user Renew of another owner's token must succeed; got {} \
             (super-user bypass regressed)",
            renew_resp.error_code
        );
        check!(
            renew_resp.expiry_timestamp_ms > initial_expiry_ms,
            "Renew must strictly extend expiry: renewed={} initial={}",
            renew_resp.expiry_timestamp_ms,
            initial_expiry_ms,
        );
        check!(
            renew_resp.expiry_timestamp_ms <= max_timestamp_ms,
            "Renew must never push expiry past max_timestamp_ms",
        );

        // (4) admin Expires (tombstone path) — this is the
        // operator's finalizer path on KafkaUser delete.
        let expire_resp = send_expire_delegation_token(
            &mut admin,
            300,
            &ExpireDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(admin super-user): {e}"))?;
        assert!(
            expire_resp.error_code == 0,
            "super-user Expire of another owner's token must succeed; got {} \
             (super-user bypass regressed)",
            expire_resp.error_code
        );

        // Tombstone should propagate.
        drop(admin);
        wait_for_token_gone(&handle, &token_id).await;
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("super-user renew/expire bypass test failed: {msg}");
    }
}
