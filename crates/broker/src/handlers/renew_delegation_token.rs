//! KIP-48: `RenewDelegationToken` (`api_key` 39).
//!
//! Matches Kafka trunk's `KafkaApis.handleRenewTokenRequest` and
//! `DelegationTokenControlManager.renewDelegationToken`, in their error
//! order: `allowTokenRequests` (`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, with
//! the `-1` error timestamp), token support
//! (`DELEGATION_TOKEN_AUTH_DISABLED`), the metadata version
//! (`UNSUPPORTED_VERSION`), the HMAC lookup (`DELEGATION_TOKEN_NOT_FOUND`),
//! expiry (`DELEGATION_TOKEN_EXPIRED`), and finally the owner/renewer gate
//! (`DELEGATION_TOKEN_OWNER_MISMATCH`). The new expiry is
//! `min(max_timestamp_ms, now + period)`, where a positive requested period
//! is capped at the configured default and any other request selects it
//! (see [`renew_token_expiry`]). It replaces the old expiry outright, so a
//! short period shortens it. The handler appends a fresh `V1DelegationToken`
//! record with the same `token_id`; the image semantics are replace.
//!
//! A configured super-user also passes the owner/renewer gate. Kafka's
//! `KRaft` controller has no such bypass; krabka keeps it because the operator
//! is a super-user that mints tokens on behalf of `KafkaUser` principals with
//! act-as, then must be able to renew them though it is neither the owner nor
//! a listed renewer.

use std::{collections::HashSet, hash::BuildHasher};

use krabka_metadata::{DelegationToken, DelegationTokenRecord};
use krabka_protocol::owned::{
    renew_delegation_token_request::RenewDelegationTokenRequest,
    renew_delegation_token_response::RenewDelegationTokenResponse,
};
use krabka_raft::DelegationTokenMutation;
use krabka_security::SecretBytes;
use krabka_verified::{
    TokenRenewDecision,
    delegation_token::{TokenApi, TokenApiAdmission},
    renew_token_expiry,
};

use crate::{network::auth::ConnectionAuth, time_util::now_ms};

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry a refused
/// `allowTokenRequests` response carries.
const ERROR_TIMESTAMP: i64 = -1;

#[tracing::instrument(
    name = "handle_renew_delegation_token",
    level = "info",
    skip_all,
    fields(api = "RenewDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> RenewDelegationTokenResponse {
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return not_allowed_response();
    };
    if auth.token_api_admission(TokenApi::Renew) == TokenApiAdmission::Reject {
        return not_allowed_response();
    }
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    let caller = principal.to_kafka();

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        krabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return err_response(crate::codes::UNSUPPORTED_VERSION);
    }
    let Some(token) = image.delegation_token_by_hmac(req.hmac.as_ref()).cloned() else {
        return err_response(crate::codes::DELEGATION_TOKEN_NOT_FOUND);
    };

    // The configured default renew period is positive: config validation
    // enforces it, as Kafka's `atLeast(1)` validator does.
    let new_expiry = match renew_token_expiry(
        now_ms(),
        req.renew_period_ms,
        default_renew_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    ) {
        TokenRenewDecision::Renew(expiry) => expiry,
        TokenRenewDecision::Expired => {
            return err_response(crate::codes::DELEGATION_TOKEN_EXPIRED);
        }
    };

    // Kafka's `allowedToRenew`, plus the krabka super-user bypass described
    // in the module docs.
    let is_super_user = super_users.contains(&principal.name);
    if !is_super_user && token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let expected = token_to_record(&token);
    let replacement = DelegationTokenRecord {
        expiry_timestamp_ms: new_expiry,
        ..expected.clone()
    };
    if let Err(e) = controller
        .submit_delegation_token_mutations(vec![DelegationTokenMutation::Renew {
            expected,
            replacement,
        }])
        .await
    {
        tracing::warn!(error = %e, "RenewDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    RenewDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

fn token_to_record(token: &DelegationToken) -> DelegationTokenRecord {
    DelegationTokenRecord {
        token_id: token.token_id.clone(),
        owner: token.owner.clone(),
        hmac: token.hmac.clone(),
        issue_timestamp_ms: token.issue_timestamp_ms,
        expiry_timestamp_ms: token.expiry_timestamp_ms,
        max_timestamp_ms: token.max_timestamp_ms,
        renewers: token.renewers.clone(),
    }
}

fn err_response(code: i16) -> RenewDelegationTokenResponse {
    RenewDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

fn not_allowed_response() -> RenewDelegationTokenResponse {
    RenewDelegationTokenResponse {
        expiry_timestamp_ms: ERROR_TIMESTAMP,
        ..err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_metadata::MetadataRecord;
    use krabka_raft::ControllerHandle;
    use krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use tempfile::TempDir;

    use super::*;

    /// Helper: empty super-users set for the tests that all exercise the
    /// owner/renewer path.
    fn empty_super_users() -> HashSet<String> {
        HashSet::new()
    }

    /// Helper: super-users set containing the given names, for the
    /// super-user-bypass tests.
    fn super_users_with(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

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

    async fn seed_token(
        target: (&ControllerHandle, &str),
        hmac: Vec<u8>,
        owner: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
        issue_ms: i64,
        expiry_ms: i64,
        max_ms: i64,
    ) {
        let (controller, token_id) = target;
        let rec = DelegationTokenRecord {
            token_id: token_id.into(),
            owner,
            hmac,
            issue_timestamp_ms: issue_ms,
            expiry_timestamp_ms: expiry_ms,
            max_timestamp_ms: max_ms,
            renewers,
        };
        controller
            .submit_change(vec![MetadataRecord::V1DelegationToken(rec)])
            .await
            .expect("seed token");
    }

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        // Auth-disabled gate fires before anything else (no controller needed).
        let req = RenewDelegationTokenRequest::default();
        let auth = authed("alice");
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let resp = handle(&req, &auth, None, 1_000, &*controller, &empty_super_users()).await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn token_authenticated_caller_is_rejected_before_lookup_or_mutation() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xAB; 32];
        let now = now_ms();
        let original_expiry = now + 60_000;
        seed_token(
            (&controller, "tok-token-auth"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            original_expiry,
            now + 120_000,
        )
        .await;
        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 90_000,
            ..Default::default()
        };

        let resp = handle(
            &req,
            &authed_with_token("alice", true),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;

        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
        assert!(
            controller
                .current_image()
                .delegation_token_by_id("tok-token-auth")
                .unwrap()
                .expiry_timestamp_ms
                == original_expiry
        );
        controller.cancel().await;
    }

    /// `DelegationTokenControlManager.renewDelegationToken`: a positive
    /// period is capped at the configured default, any other period selects
    /// the default, the token's max timestamp caps the sum, and the result
    /// replaces the old expiry even when it is earlier.
    #[tokio::test]
    async fn renewal_follows_kafkas_period_rule() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hour = 60 * 60 * 1_000;
        let day = 24 * hour;

        // (token id, hmac byte, requested period, current expiry offset,
        //  max offset, expected expiry offset from the handler's now)
        for (token_id, hmac_byte, requested, current, max, delta) in [
            ("hour", 0xA0, hour, 60_000, 7 * day, hour),
            ("minus-one", 0xA1, -1, 60_000, 7 * day, day),
            ("zero", 0xA2, 0, 60_000, 7 * day, day),
            ("negative", 0xA3, -7, 60_000, 7 * day, day),
            ("above-default", 0xA4, 30 * day, 60_000, 7 * day, day),
            ("shortens", 0xA5, 1_000, 12 * hour, 7 * day, 1_000),
            ("max-caps", 0xA6, -1, 60_000, 2 * hour, day),
        ] {
            let hmac = vec![hmac_byte; 32];
            let seeded_at = now_ms();
            seed_token(
                (&controller, token_id),
                hmac.clone(),
                kp("alice"),
                vec![],
                seeded_at - 1_000,
                seeded_at + current,
                seeded_at + max,
            )
            .await;
            let before = now_ms();
            let resp = handle(
                &RenewDelegationTokenRequest {
                    hmac: hmac.into(),
                    renew_period_ms: requested,
                    ..Default::default()
                },
                &authed("alice"),
                Some(&secret),
                day,
                &*controller,
                &empty_super_users(),
            )
            .await;
            let after = now_ms();
            check!(resp.error_code == 0, "{token_id}");
            let expected_low = (before + delta).min(seeded_at + max);
            let expected_high = (after + delta).min(seeded_at + max);
            check!(
                (expected_low..=expected_high).contains(&resp.expiry_timestamp_ms),
                "{token_id}: expiry {} outside [{expected_low}, {expected_high}]",
                resp.expiry_timestamp_ms
            );
            check!(
                controller
                    .current_image()
                    .delegation_token_by_id(token_id)
                    .expect("token remains")
                    .expiry_timestamp_ms
                    == resp.expiry_timestamp_ms,
                "{token_id}"
            );
        }
        controller.cancel().await;
    }

    /// Kafka checks expiry before the owner/renewer gate, and never mutates
    /// an expired token.
    #[tokio::test]
    async fn expired_token_reports_expired_before_owner_mismatch() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let now = now_ms();

        for (token_id, hmac_byte, expiry, max, caller) in [
            ("expired", 0xE1, now - 1, now + 120_000, "alice"),
            ("past-max", 0xE2, now - 1, now - 1, "alice"),
            ("expired-stranger", 0xE3, now - 1, now + 120_000, "eve"),
        ] {
            let hmac = vec![hmac_byte; 32];
            seed_token(
                (&controller, token_id),
                hmac.clone(),
                kp("alice"),
                vec![],
                now - 1_000,
                expiry,
                max,
            )
            .await;
            let resp = handle(
                &RenewDelegationTokenRequest {
                    hmac: hmac.into(),
                    renew_period_ms: 1_000,
                    ..Default::default()
                },
                &authed(caller),
                Some(&secret),
                1_000,
                &*controller,
                &empty_super_users(),
            )
            .await;
            check!(
                resp == err_response(crate::codes::DELEGATION_TOKEN_EXPIRED),
                "{token_id}"
            );
            check!(
                controller
                    .current_image()
                    .delegation_token_by_id(token_id)
                    .expect("token remains")
                    .expiry_timestamp_ms
                    == expiry,
                "{token_id}"
            );
        }

        controller.cancel().await;
    }

    /// `allowTokenRequests` runs on the broker before the controller checks
    /// token support, and its refusal carries Kafka's `-1` error timestamp.
    #[tokio::test]
    async fn admission_is_checked_before_token_support() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        for auth in [authed_with_token("alice", true), ConnectionAuth::Anonymous] {
            let resp = handle(
                &RenewDelegationTokenRequest::default(),
                &auth,
                None,
                1_000,
                &*controller,
                &empty_super_users(),
            )
            .await;
            check!(
                resp == RenewDelegationTokenResponse {
                    error_code: crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                    expiry_timestamp_ms: -1,
                    ..Default::default()
                }
            );
        }
        controller.cancel().await;
    }

    #[tokio::test]
    async fn success_as_renewer_extends_expiry() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xBB; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-2"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000, // +1m, below the 24h default
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("bob"),
            Some(&secret),
            24 * 60 * 60 * 1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(resp.expiry_timestamp_ms > now + 30_000);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn unknown_hmac_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = RenewDelegationTokenRequest {
            hmac: vec![0xFF; 32].into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_NOT_FOUND);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn non_owner_non_renewer_returns_owner_mismatch() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xCC; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-3"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        controller.cancel().await;
    }

    /// A super-user caller may renew a token they
    /// neither own nor are listed as a renewer on. This is krabka's bypass
    /// (Kafka's `KRaft` controller has none) and is the
    /// load-bearing gate for the operator flow. The operator
    /// is a super-user that act-as-mints tokens on behalf of `KafkaUser`
    /// principals, then must be able to renew them before expiry.
    #[tokio::test]
    async fn super_user_can_renew_any_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xDD; 32];
        let now = now_ms();
        // Token owned by `alice`, no renewers — operator (`admin`) is
        // neither, but it IS in `super_users`, so renew must succeed.
        seed_token(
            (&controller, "tok-super"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 3_600_000, // +1h
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            1_000,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(
            resp.error_code == 0,
            "super-user must be able to renew any token regardless of owner/renewers"
        );
        // Persisted in image with the new expiry.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-super").expect("present");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    /// The handler must still reject a non-super-user caller who is also not
    /// the owner and not a listed renewer, with
    /// `DELEGATION_TOKEN_OWNER_MISMATCH`. This test guards against accidentally
    /// widening the bypass beyond `super_users`.
    #[tokio::test]
    async fn non_super_user_non_owner_non_renewer_still_rejected() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xEE; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-eve"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        // `eve` is not in the super-users set (only `admin` is) and is
        // neither owner nor renewer — must still get the mismatch error.
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            1_000,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        controller.cancel().await;
    }
}
