//! KIP-48: `ExpireDelegationToken` (`api_key` 40).
//!
//! Matches Kafka trunk's `KafkaApis.handleExpireTokenRequest` and
//! `DelegationTokenControlManager.expireDelegationToken`, in their order:
//!
//! 1. `allowTokenRequests` refuses a caller that is not securely
//!    authenticated, or that authenticated with a delegation token, with
//!    `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) and an expiry of `-1`
//!    (`DelegationTokenManager.ERROR_TIMESTAMP`).
//! 2. The controller answers `DELEGATION_TOKEN_AUTH_DISABLED` (61) when no
//!    secret key is configured, `UNSUPPORTED_VERSION` (35) below the
//!    delegation-token `metadata.version`, and `DELEGATION_TOKEN_NOT_FOUND`
//!    (62) for an unknown `hmac`.
//! 3. `allowedToRenew`: only the owner or a listed renewer may expire the
//!    token. Anyone else, a super user included, gets
//!    `DELEGATION_TOKEN_OWNER_MISMATCH` (63).
//!
//! Then `expiry_time_period_ms` decides:
//!   - Below 0: the token is deleted, whatever its deadlines, and the response
//!     carries `expiry_timestamp_ms = now`.
//!   - Otherwise a token whose expiry or maximum timestamp is before now gets
//!     `DELEGATION_TOKEN_EXPIRED` (66), and a live one gets `now + period`,
//!     saturated at `i64::MAX` and capped at its `max_timestamp_ms`.

use krabka_metadata::{DelegationToken, DelegationTokenRecord};
use krabka_protocol::owned::{
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    expire_delegation_token_response::ExpireDelegationTokenResponse,
};
use krabka_raft::DelegationTokenMutation;
use krabka_security::SecretBytes;
use krabka_verified::{
    TokenExpireDecision,
    delegation_token::{TokenApi, TokenApiAdmission},
    expire_token_deadline,
};

use crate::{network::auth::ConnectionAuth, time_util::now_ms};

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry the broker
/// answers with when it refuses the request before forwarding it.
const ERROR_TIMESTAMP: i64 = -1;

#[tracing::instrument(
    name = "handle_expire_delegation_token",
    level = "info",
    skip_all,
    fields(api = "ExpireDelegationToken")
)]
pub(crate) async fn handle(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn crate::metadata_source::MetadataSource,
) -> ExpireDelegationTokenResponse {
    if auth.token_api_admission(TokenApi::Expire) == TokenApiAdmission::Reject {
        return ExpireDelegationTokenResponse {
            expiry_timestamp_ms: ERROR_TIMESTAMP,
            ..err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
        };
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return ExpireDelegationTokenResponse {
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

    if token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let now = now_ms();
    let expected = token_to_record(&token);
    let (mutation, new_expiry) = match expire_token_deadline(
        now,
        req.expiry_time_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    ) {
        TokenExpireDecision::Expired => {
            return err_response(crate::codes::DELEGATION_TOKEN_EXPIRED);
        }
        TokenExpireDecision::Delete => (DelegationTokenMutation::Delete { expected }, now),
        TokenExpireDecision::Update(new_expiry) => {
            let replacement = DelegationTokenRecord {
                expiry_timestamp_ms: new_expiry,
                ..expected.clone()
            };
            (
                DelegationTokenMutation::Expire {
                    expected,
                    replacement,
                },
                new_expiry,
            )
        }
    };
    if let Err(e) = controller
        .submit_delegation_token_mutations(vec![mutation])
        .await
    {
        tracing::warn!(error = %e, "ExpireDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    ExpireDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

/// A controller-side refusal: `code`, and the schema-default expiry of 0.
fn err_response(code: i16) -> ExpireDelegationTokenResponse {
    ExpireDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

/// Projects an image-level [`DelegationToken`] back into a
/// [`DelegationTokenRecord`], so that struct-update syntax can express a
/// partial update in which only the `expiry_*` fields change.
fn token_to_record(t: &DelegationToken) -> DelegationTokenRecord {
    DelegationTokenRecord {
        token_id: t.token_id.clone(),
        owner: t.owner.clone(),
        hmac: t.hmac.clone(),
        issue_timestamp_ms: t.issue_timestamp_ms,
        expiry_timestamp_ms: t.expiry_timestamp_ms,
        max_timestamp_ms: t.max_timestamp_ms,
        renewers: t.renewers.clone(),
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

    fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> Option<i64> {
        controller
            .current_image()
            .delegation_token_by_id(token_id)
            .map(|token| token.expiry_timestamp_ms)
    }

    /// Refusals come in Kafka's order, carry Kafka's expiry, and leave the
    /// token alone. A caller that is neither owner nor renewer gets
    /// `DELEGATION_TOKEN_OWNER_MISMATCH`, a super user included and before
    /// the expiry check.
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

        // (case, caller, secret configured, hmac, period, error code, expiry)
        let cases = [
            (
                "token-authenticated owner",
                authed_with_token("alice", true),
                true,
                &live.0,
                -1,
                crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                -1,
            ),
            (
                "token-authenticated owner, tokens disabled",
                authed_with_token("alice", true),
                false,
                &live.0,
                -1,
                crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                -1,
            ),
            (
                "tokens disabled",
                authed("alice"),
                false,
                &live.0,
                -1,
                crate::codes::DELEGATION_TOKEN_AUTH_DISABLED,
                0,
            ),
            (
                "unknown hmac",
                authed("alice"),
                true,
                &vec![0xFF; 32],
                -1,
                crate::codes::DELEGATION_TOKEN_NOT_FOUND,
                0,
            ),
            (
                "foreign caller",
                authed("eve"),
                true,
                &live.0,
                -1,
                crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                0,
            ),
            (
                "super user",
                authed("admin"),
                true,
                &live.0,
                0,
                crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                0,
            ),
            (
                "foreign caller, expired token",
                authed("eve"),
                true,
                &expired.0,
                0,
                crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                0,
            ),
            (
                "owner, expired token",
                authed("alice"),
                true,
                &expired.0,
                0,
                crate::codes::DELEGATION_TOKEN_EXPIRED,
                0,
            ),
            (
                "renewer, expired token",
                authed("bob"),
                true,
                &expired.0,
                1_000,
                crate::codes::DELEGATION_TOKEN_EXPIRED,
                0,
            ),
        ];
        for (case, auth, enabled, hmac, period, error_code, expiry_timestamp_ms) in cases {
            let resp = handle(
                &ExpireDelegationTokenRequest {
                    hmac: hmac.clone().into(),
                    expiry_time_period_ms: period,
                    ..Default::default()
                },
                &auth,
                enabled.then_some(&secret),
                &*controller,
            )
            .await;
            let expected = ExpireDelegationTokenResponse {
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
            ) == (Some(live.1), Some(expired.1))
        );
        controller.cancel().await;
    }

    /// The owner or a renewer deletes the token for a negative period, even an
    /// expired one, and answers `now`; a non-negative period sets
    /// `min(max, now + period)`.
    #[tokio::test]
    async fn expires_to_kafka_deadline() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());

        // (case, caller, period, current expiry delta, max delta, expected
        //  expiry delta; `None` means the max timestamp; whether the token
        //  is deleted)
        let cases = [
            ("owner deletes", "alice", -1, 60_000, DAY_MS, Some(0), true),
            ("renewer deletes", "bob", -5, 60_000, DAY_MS, Some(0), true),
            (
                "owner deletes an expired token",
                "alice",
                -1,
                -1_000,
                DAY_MS,
                Some(0),
                true,
            ),
            (
                "zero expires now",
                "alice",
                0,
                60_000,
                DAY_MS,
                Some(0),
                false,
            ),
            (
                "renewer shortens",
                "bob",
                30_000,
                60_000,
                DAY_MS,
                Some(30_000),
                false,
            ),
            (
                "owner lengthens",
                "alice",
                120_000,
                60_000,
                DAY_MS,
                Some(120_000),
                false,
            ),
            (
                "capped at the max timestamp",
                "alice",
                2 * DAY_MS,
                60_000,
                DAY_MS,
                None,
                false,
            ),
            (
                "period near i64::MAX saturates",
                "alice",
                i64::MAX,
                60_000,
                DAY_MS,
                None,
                false,
            ),
        ];
        for (index, (case, caller, period, expiry_delta, max_delta, expected_delta, deleted)) in
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
                &ExpireDelegationTokenRequest {
                    hmac: hmac.into(),
                    expiry_time_period_ms: period,
                    ..Default::default()
                },
                &authed(caller),
                Some(&secret),
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
            let stored = stored_expiry(&controller, &token_id);
            let want = (!deleted).then_some(resp.expiry_timestamp_ms);
            assert!(stored == want, "{case}");
        }
        controller.cancel().await;
    }
}
