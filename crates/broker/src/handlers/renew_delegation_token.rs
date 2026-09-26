//! KIP-48: `RenewDelegationToken` (`api_key` 39).
//!
//! Matches Kafka trunk's `KafkaApis.handleRenewTokenRequest` and
//! `DelegationTokenControlManager.renewDelegationToken`, in their order:
//!
//! 1. `allowTokenRequests` refuses a caller that is not securely
//!    authenticated, or that authenticated with a delegation token, with
//!    `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) and an expiry of `-1`
//!    (`DelegationTokenManager.ERROR_TIMESTAMP`).
//! 2. The controller answers `DELEGATION_TOKEN_AUTH_DISABLED` (61) when no
//!    secret key is configured, `UNSUPPORTED_VERSION` (35) below the
//!    delegation-token `metadata.version`, `DELEGATION_TOKEN_NOT_FOUND` (62)
//!    for an unknown `hmac`, and `DELEGATION_TOKEN_EXPIRED` (66) for a token
//!    whose expiry or maximum timestamp is before now.
//! 3. `allowedToRenew`: only the owner or a listed renewer may renew. Anyone
//!    else, a super user included, gets `DELEGATION_TOKEN_OWNER_MISMATCH`
//!    (63).
//!
//! The new expiry is `now + min(delegation.token.expiry.time.ms,
//! renew_period_ms)` for a positive period and `now +
//! delegation.token.expiry.time.ms` otherwise, capped at the token's
//! `max_timestamp_ms`. It replaces the current expiry even when it is
//! earlier. The handler appends a fresh `V1DelegationToken` record with the
//! same `token_id`; the image semantics are replace.

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

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry the broker
/// answers with when it refuses the request before forwarding it.
const ERROR_TIMESTAMP: i64 = -1;

#[tracing::instrument(
    name = "handle_renew_delegation_token",
    level = "info",
    skip_all,
    fields(api = "RenewDelegationToken")
)]
pub(crate) async fn handle(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &dyn crate::metadata_source::MetadataSource,
) -> RenewDelegationTokenResponse {
    if auth.token_api_admission(TokenApi::Renew) == TokenApiAdmission::Reject {
        return RenewDelegationTokenResponse {
            expiry_timestamp_ms: ERROR_TIMESTAMP,
            ..err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
        };
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return RenewDelegationTokenResponse {
            expiry_timestamp_ms: ERROR_TIMESTAMP,
            ..err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
        };
    };
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

    let decision = renew_token_expiry(
        now_ms(),
        req.renew_period_ms,
        default_renew_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    );
    if decision == TokenRenewDecision::Expired {
        return err_response(crate::codes::DELEGATION_TOKEN_EXPIRED);
    }
    if token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }
    let TokenRenewDecision::Renew(new_expiry) = decision else {
        return err_response(crate::codes::INVALID_REQUEST);
    };

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

/// A controller-side refusal: `code`, and the schema-default expiry of 0.
fn err_response(code: i16) -> RenewDelegationTokenResponse {
    RenewDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use krabka_metadata::MetadataRecord;
    use krabka_raft::ControllerHandle;
    use krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use tempfile::TempDir;

    use super::*;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

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

    /// Seeds a token owned by `alice` with renewer `bob`.
    async fn seed_token(
        controller: &ControllerHandle,
        token_id: &str,
        hmac: Vec<u8>,
        expiry_ms: i64,
        max_ms: i64,
    ) {
        let rec = DelegationTokenRecord {
            token_id: token_id.into(),
            owner: kp("alice"),
            hmac,
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: expiry_ms,
            max_timestamp_ms: max_ms,
            renewers: vec![kp("bob")],
        };
        controller
            .submit_change(vec![MetadataRecord::V1DelegationToken(rec)])
            .await
            .expect("seed token");
    }

    fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> i64 {
        controller
            .current_image()
            .delegation_token_by_id(token_id)
            .expect("token remains")
            .expiry_timestamp_ms
    }

    /// Refusals come in Kafka's order, carry Kafka's expiry, and leave the
    /// stored expiry alone. A super user that is neither owner nor renewer
    /// gets `DELEGATION_TOKEN_OWNER_MISMATCH`, as `allowedToRenew` gives it,
    /// and an expired token answers `DELEGATION_TOKEN_EXPIRED` before the
    /// owner check.
    #[tokio::test]
    async fn refusals_follow_kafka_order() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let now = now_ms();
        let live = (vec![0xA1; 32], now + 60_000);
        let expired = (vec![0xA2; 32], now - 1);
        for (token_id, (hmac, expiry)) in [("live", &live), ("expired", &expired)] {
            seed_token(&controller, token_id, hmac.clone(), *expiry, now + DAY_MS).await;
        }

        // (case, caller, secret configured, hmac, error code, expiry)
        let cases = [
            (
                "token-authenticated owner",
                authed_with_token("alice", true),
                true,
                &live.0,
                crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                -1,
            ),
            (
                "token-authenticated owner, tokens disabled",
                authed_with_token("alice", true),
                false,
                &live.0,
                crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                -1,
            ),
            (
                "tokens disabled",
                authed("alice"),
                false,
                &live.0,
                crate::codes::DELEGATION_TOKEN_AUTH_DISABLED,
                0,
            ),
            (
                "unknown hmac",
                authed("alice"),
                true,
                &vec![0xFF; 32],
                crate::codes::DELEGATION_TOKEN_NOT_FOUND,
                0,
            ),
            (
                "expired token, foreign caller",
                authed("eve"),
                true,
                &expired.0,
                crate::codes::DELEGATION_TOKEN_EXPIRED,
                0,
            ),
            (
                "expired token, owner",
                authed("alice"),
                true,
                &expired.0,
                crate::codes::DELEGATION_TOKEN_EXPIRED,
                0,
            ),
            (
                "foreign caller",
                authed("eve"),
                true,
                &live.0,
                crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                0,
            ),
            (
                "super user",
                authed("admin"),
                true,
                &live.0,
                crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                0,
            ),
        ];
        for (case, auth, enabled, hmac, error_code, expiry_timestamp_ms) in cases {
            let resp = handle(
                &RenewDelegationTokenRequest {
                    hmac: hmac.clone().into(),
                    renew_period_ms: 1_000,
                    ..Default::default()
                },
                &auth,
                enabled.then_some(&secret),
                DAY_MS,
                &*controller,
            )
            .await;
            let expected = RenewDelegationTokenResponse {
                error_code,
                expiry_timestamp_ms,
                ..Default::default()
            };
            assert!(resp == expected, "{case}");
        }
        assert!(
            (
                stored_expiry(&controller, "live"),
                stored_expiry(&controller, "expired")
            ) == (live.1, expired.1)
        );
        controller.cancel().await;
    }

    /// The owner or a renewer renews to `min(max, now + min(default,
    /// period))` for a positive period and `min(max, now + default)`
    /// otherwise, even when that is earlier than the current expiry.
    #[tokio::test]
    async fn renews_to_kafka_capped_expiry() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hour: i64 = 60 * 60 * 1_000;
        let default_period = 2 * hour;

        // (case, caller, renew_period_ms, current expiry delta, max delta,
        //  expected expiry delta, where a delta of `None` means the max)
        let cases = [
            (
                "owner, shorter period",
                "alice",
                hour,
                hour,
                DAY_MS,
                Some(hour),
            ),
            (
                "renewer, shorter period",
                "bob",
                hour,
                hour,
                DAY_MS,
                Some(hour),
            ),
            (
                "period above the default is capped",
                "alice",
                10 * hour,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "-1 takes the default",
                "alice",
                -1,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "0 takes the default",
                "alice",
                0,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "-5 takes the default",
                "alice",
                -5,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "shortens a later expiry",
                "alice",
                60_000,
                20 * hour,
                DAY_MS,
                Some(60_000),
            ),
            (
                "capped at the max timestamp",
                "alice",
                -1,
                60_000,
                30 * 60_000,
                None,
            ),
            (
                "period near i64::MAX",
                "alice",
                i64::MAX,
                60_000,
                30 * 60_000,
                None,
            ),
        ];
        for (index, (case, caller, period, expiry_delta, max_delta, expected_delta)) in
            cases.into_iter().enumerate()
        {
            let hmac = vec![u8::try_from(index).unwrap(); 32];
            let token_id = format!("tok-{index}");
            let seeded_at = now_ms();
            let max_timestamp_ms = seeded_at + max_delta;
            seed_token(
                &controller,
                &token_id,
                hmac.clone(),
                seeded_at + expiry_delta,
                max_timestamp_ms,
            )
            .await;

            let before = now_ms();
            let resp = handle(
                &RenewDelegationTokenRequest {
                    hmac: hmac.into(),
                    renew_period_ms: period,
                    ..Default::default()
                },
                &authed(caller),
                Some(&secret),
                default_period,
                &*controller,
            )
            .await;
            let after = now_ms();

            assert!(resp.error_code == crate::codes::NONE, "{case}");
            match expected_delta {
                Some(delta) => assert!(
                    (before + delta..=after + delta).contains(&resp.expiry_timestamp_ms),
                    "{case}: {} not in [{}, {}]",
                    resp.expiry_timestamp_ms,
                    before + delta,
                    after + delta
                ),
                None => assert!(resp.expiry_timestamp_ms == max_timestamp_ms, "{case}"),
            }
            assert!(
                stored_expiry(&controller, &token_id) == resp.expiry_timestamp_ms,
                "{case}"
            );
        }
        controller.cancel().await;
    }
}
