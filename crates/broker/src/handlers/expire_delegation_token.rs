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

use krabka_metadata::DelegationTokenRecord;
use krabka_protocol::owned::{
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    expire_delegation_token_response::ExpireDelegationTokenResponse,
};
use krabka_raft::DelegationTokenMutation;
use krabka_security::SecretBytes;
use krabka_verified::{TokenExpireDecision, delegation_token::TokenApi, expire_token_deadline};

use crate::{
    handlers::{ErrorCodeResponse as _, renew_delegation_token::admit_token_request},
    network::auth::ConnectionAuth,
    time_util::now_ms,
};

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
    let (principal, token) = match admit_token_request(
        TokenApi::Expire,
        auth,
        secret_key,
        req.hmac.as_ref(),
        controller,
    ) {
        Ok(admitted) => admitted,
        Err((error_code, expiry_timestamp_ms)) => {
            return ExpireDelegationTokenResponse {
                expiry_timestamp_ms,
                ..ExpireDelegationTokenResponse::error(error_code)
            };
        }
    };
    let caller = principal.to_kafka();

    if token.owner != caller && !token.renewers.contains(&caller) {
        return ExpireDelegationTokenResponse::error(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let now = now_ms();
    let expected = token.to_record();
    let (mutation, new_expiry) = match expire_token_deadline(
        now,
        req.expiry_time_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    ) {
        TokenExpireDecision::Expired => {
            return ExpireDelegationTokenResponse::error(crate::codes::DELEGATION_TOKEN_EXPIRED);
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
        return ExpireDelegationTokenResponse::error(crate::codes::INVALID_REQUEST);
    }

    ExpireDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::TempDir;

    use super::*;
    use crate::handlers::delegation_token_test_support::{
        DAY_MS, OwnershipRefusalSetup, TokenMutationRefusalSetup, TokenPeriodMillis, authed,
        mutation_credential_refusals, ownership_refusal, stored_expiry, test_controller,
    };

    /// Refusals come in Kafka's order, carry Kafka's expiry, and leave the
    /// token alone. A caller that is neither owner nor renewer gets
    /// `DELEGATION_TOKEN_OWNER_MISMATCH`, a super user included and before
    /// the expiry check.
    #[tokio::test]
    async fn refusals_follow_kafka_order() {
        refusal_token_fixture!((_dir, controller, secret, live, expired));

        let cases = mutation_credential_refusals(&live.0).into_iter().chain([
            ownership_refusal(&live.0, OwnershipRefusalSetup::default()),
            ownership_refusal(
                &live.0,
                OwnershipRefusalSetup {
                    name: "super user",
                    caller: "admin",
                    period: TokenPeriodMillis(0),
                },
            ),
            // `allowedToRenew` is the owner or a renewer. Unlike `filterToken`'s
            // `ownerOrRenewer`, it leaves out the requester that created the
            // token for another owner.
            ownership_refusal(
                &live.0,
                OwnershipRefusalSetup {
                    name: "requester of a token created for another owner",
                    caller: "minter",
                    period: TokenPeriodMillis(0),
                },
            ),
            TokenMutationRefusalSetup {
                name: "foreign caller, expired token",
                authentication: authed("eve"),
                hmac: expired.0.clone(),
                period: TokenPeriodMillis(0),
                error: crate::test_support::KafkaErrorCode(
                    crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH,
                ),
                ..Default::default()
            },
            TokenMutationRefusalSetup {
                name: "owner, expired token",
                hmac: expired.0.clone(),
                period: TokenPeriodMillis(0),
                error: crate::test_support::KafkaErrorCode(crate::codes::DELEGATION_TOKEN_EXPIRED),
                ..Default::default()
            },
            TokenMutationRefusalSetup {
                name: "renewer, expired token",
                authentication: authed("bob"),
                hmac: expired.0.clone(),
                period: TokenPeriodMillis(1_000),
                error: crate::test_support::KafkaErrorCode(crate::codes::DELEGATION_TOKEN_EXPIRED),
                ..Default::default()
            },
        ]);
        for case in cases {
            let resp = handle(
                &ExpireDelegationTokenRequest {
                    hmac: case.hmac.into(),
                    expiry_time_period_ms: case.period.0,
                    ..Default::default()
                },
                &case.authentication,
                case.secret.get(&secret),
                &*controller,
            )
            .await;
            check_token_mutation_refusal!(resp, case, ExpireDelegationTokenResponse);
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
        token_fixture!(dir, controller, secret);

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
            seeded_token_fixture!(
                (token_id, hmac, seeded_at, max_timestamp_ms),
                &controller,
                index,
                expiry_delta,
                max_delta
            );

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

            crate::handlers::delegation_token_test_support::check_expiry_window(
                case,
                (resp.error_code, resp.expiry_timestamp_ms),
                (before, after),
                expected_delta,
                max_timestamp_ms,
            );
            let stored = stored_expiry(&controller, &token_id);
            let want = (!deleted).then_some(resp.expiry_timestamp_ms);
            assert!(stored == want, "{case}");
        }
        controller.cancel().await;
    }
}
