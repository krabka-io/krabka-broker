//! KIP-48: `ExpireDelegationToken` (`api_key` 40).
//!
//! Matches Kafka trunk's `KafkaApis.handleExpireTokenRequest` and
//! `DelegationTokenControlManager.expireDelegationToken`, in their error
//! order: `allowTokenRequests` (`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, with
//! the `-1` error timestamp), token support
//! (`DELEGATION_TOKEN_AUTH_DISABLED`), the metadata version
//! (`UNSUPPORTED_VERSION`), the HMAC lookup (`DELEGATION_TOKEN_NOT_FOUND`),
//! and the owner/renewer gate (`DELEGATION_TOKEN_OWNER_MISMATCH`).
//!
//! The handler then decides on `expiry_time_period_ms` (see
//! [`expire_token_deadline`]):
//!   - Below 0: it appends a `V1DeleteDelegationToken` tombstone, even for an
//!     already expired token, and responds with `expiry_timestamp_ms = now`.
//!   - Otherwise an expired token gets `DELEGATION_TOKEN_EXPIRED`, and a live
//!     one gets `now + period` (saturating), clamped to its
//!     `max_timestamp_ms`, in a replacement record. Zero expires it at `now`.
//!
//! A configured super-user also passes the owner/renewer gate. Kafka's
//! `KRaft` controller has no such bypass; krabka keeps it because the operator
//! tombstones the tokens that it created through act-as on behalf of
//! `KafkaUser` principals.

use std::{collections::HashSet, hash::BuildHasher};

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

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry a refused
/// `allowTokenRequests` response carries.
const ERROR_TIMESTAMP: i64 = -1;

#[tracing::instrument(
    name = "handle_expire_delegation_token",
    level = "info",
    skip_all,
    fields(api = "ExpireDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> ExpireDelegationTokenResponse {
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return not_allowed_response();
    };
    if auth.token_api_admission(TokenApi::Expire) == TokenApiAdmission::Reject {
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

    // Kafka's `allowedToRenew`, plus the krabka super-user bypass described
    // in the module docs.
    let is_super_user = super_users.contains(&principal.name);
    if !is_super_user && token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let now = now_ms();
    let expected = token_to_record(&token);
    let (mutation, expiry_timestamp_ms) = match expire_token_deadline(
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
        expiry_timestamp_ms,
        ..Default::default()
    }
}

fn err_response(code: i16) -> ExpireDelegationTokenResponse {
    ExpireDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

fn not_allowed_response() -> ExpireDelegationTokenResponse {
    ExpireDelegationTokenResponse {
        expiry_timestamp_ms: ERROR_TIMESTAMP,
        ..err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
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
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_metadata::MetadataRecord;
    use krabka_raft::ControllerHandle;
    use krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use tempfile::TempDir;

    use super::*;

    /// Helper that gives an empty super-users set, for the older tests. They
    /// all exercise the owner and renewer path.
    fn empty_super_users() -> HashSet<String> {
        HashSet::new()
    }

    /// Helper that gives a super-users set with the given names, for the new
    /// super-user-bypass tests.
    fn super_users_with(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

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
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let req = ExpireDelegationTokenRequest::default();
        let resp = handle(
            &req,
            &authed("alice"),
            None,
            &*controller,
            &empty_super_users(),
        )
        .await;
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
        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: -1,
            ..Default::default()
        };

        let resp = handle(
            &req,
            &authed_with_token("alice", true),
            Some(&secret),
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

    #[tokio::test]
    async fn future_expiry_period_updates_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xAA; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-1"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 30_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        let target = now_ms() + 30_000;
        let slop = 60_000;
        assert!(
            (resp.expiry_timestamp_ms - target).abs() < slop,
            "expiry {} far from {target}",
            resp.expiry_timestamp_ms
        );
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-1").expect("present");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    /// A negative period deletes the token, expired or not, and reports
    /// `now` as the expiry, as `expireDelegationToken` does.
    #[tokio::test]
    async fn negative_period_tombstones_live_and_expired_tokens() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let now = now_ms();

        for (token_id, hmac_byte, expiry, max) in [
            ("live", 0xB0, now + 60_000, now + 120_000),
            ("expired", 0xB1, now - 1, now + 120_000),
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
            let before = now_ms();
            let resp = handle(
                &ExpireDelegationTokenRequest {
                    hmac: hmac.into(),
                    expiry_time_period_ms: -1,
                    ..Default::default()
                },
                &authed("alice"),
                Some(&secret),
                &*controller,
                &empty_super_users(),
            )
            .await;
            let after = now_ms();
            check!(resp.error_code == 0, "{token_id}");
            check!(
                (before..=after).contains(&resp.expiry_timestamp_ms),
                "{token_id}"
            );
            check!(
                controller
                    .current_image()
                    .delegation_token_by_id(token_id)
                    .is_none(),
                "{token_id}"
            );
        }
        controller.cancel().await;
    }

    /// A non-negative period sets `min(max, now + period)`, saturating the
    /// sum as Kafka's `sum` does instead of failing.
    #[tokio::test]
    async fn non_negative_period_is_clamped_to_max_timestamp() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let now = now_ms();

        // (token id, hmac byte, period, max offset, expected offset from now
        //  or `None` for the max timestamp)
        for (token_id, hmac_byte, period, max_offset, delta) in [
            ("zero", 0xC0, 0, 120_000, Some(0)),
            ("shorter", 0xC1, 1_000, 120_000, Some(1_000)),
            ("past-max", 0xC2, 600_000, 120_000, None),
            ("overflow", 0xC3, i64::MAX, 120_000, None),
        ] {
            let hmac = vec![hmac_byte; 32];
            seed_token(
                (&controller, token_id),
                hmac.clone(),
                kp("alice"),
                vec![],
                now - 1_000,
                now + 60_000,
                now + max_offset,
            )
            .await;
            let before = now_ms();
            let resp = handle(
                &ExpireDelegationTokenRequest {
                    hmac: hmac.into(),
                    expiry_time_period_ms: period,
                    ..Default::default()
                },
                &authed("alice"),
                Some(&secret),
                &*controller,
                &empty_super_users(),
            )
            .await;
            let after = now_ms();
            check!(resp.error_code == 0, "{token_id}");
            let (low, high) = delta.map_or((now + max_offset, now + max_offset), |delta| {
                (before + delta, after + delta)
            });
            check!(
                (low..=high).contains(&resp.expiry_timestamp_ms),
                "{token_id}: {}",
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

    #[tokio::test]
    async fn positive_period_does_not_resurrect_expired_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xBD; 32];
        let now = now_ms();
        seed_token(
            (&controller, "expired"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 2_000,
            now - 1_000,
            now + 120_000,
        )
        .await;

        let resp = handle(
            &ExpireDelegationTokenRequest {
                hmac: hmac.into(),
                expiry_time_period_ms: 60_000,
                ..Default::default()
            },
            &authed("alice"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;

        assert!(resp == err_response(crate::codes::DELEGATION_TOKEN_EXPIRED));
        assert!(
            controller
                .current_image()
                .delegation_token_by_id("expired")
                .expect("token remains unchanged")
                .expiry_timestamp_ms
                == now - 1_000
        );
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
                &ExpireDelegationTokenRequest::default(),
                &auth,
                None,
                &*controller,
                &empty_super_users(),
            )
            .await;
            check!(
                resp == ExpireDelegationTokenResponse {
                    error_code: crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                    expiry_timestamp_ms: -1,
                    ..Default::default()
                }
            );
        }
        controller.cancel().await;
    }

    #[tokio::test]
    async fn unauthorized_caller_returns_owner_mismatch() {
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

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 1_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        // Token unchanged.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-3").expect("present");
        assert!(stored.expiry_timestamp_ms == now + 60_000);
        controller.cancel().await;
    }

    /// A super-user caller can expire a token that it does not own and is not
    /// a renewer on. This is krabka's bypass (Kafka's `KRaft` controller has
    /// none), and it is the gate that the operator's finalizer needs. On a `KafkaUser` delete, the
    /// operator tombstones the act-as token by a call to
    /// `ExpireDelegationToken` with period -1.
    #[tokio::test]
    async fn super_user_can_expire_any_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xDD; 32];
        let now = now_ms();
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

        // Period = -1 → tombstone path; same code path the operator's
        // finalizer hits.
        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(
            resp.error_code == 0,
            "super-user must be able to expire any token regardless of owner/renewers"
        );
        // Kafka reports `now` for a deletion; the token is tombstoned.
        assert!(resp.expiry_timestamp_ms <= now_ms());
        let img = controller.current_image();
        assert!(img.delegation_token_by_id("tok-super").is_none());
        controller.cancel().await;
    }

    /// A caller that is not a super-user, not the owner, and not a listed
    /// renewer must still get `DELEGATION_TOKEN_OWNER_MISMATCH`. This
    /// test guards against a bypass that reaches past `super_users`.
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

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 1_000,
            ..Default::default()
        };
        // `eve` is not in the super-users set (only `admin` is) and is
        // neither owner nor renewer — must still get the owner mismatch.
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        // Token unchanged.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-eve").expect("present");
        assert!(stored.expiry_timestamp_ms == now + 60_000);
        controller.cancel().await;
    }
}
