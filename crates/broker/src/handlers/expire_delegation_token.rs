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

use {
    crate::{
        codes::{DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_OWNER_MISMATCH, INVALID_REQUEST},
        handlers::{ErrorCodeResponse as _, renew_delegation_token::admit_token_request},
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
        time_util::now_ms,
    },
    krabka_metadata::DelegationTokenRecord,
    krabka_protocol::owned::{
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
    },
    krabka_raft::DelegationTokenMutation,
    krabka_security::SecretBytes,
    krabka_verified::{TokenExpireDecision, delegation_token::TokenApi, expire_token_deadline},
};

#[tracing::instrument(
    name = "handle_expire_delegation_token",
    level = "info",
    skip_all,
    fields(api = "ExpireDelegationToken")
)]
pub async fn handle(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn MetadataSource,
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
        return ExpireDelegationTokenResponse::error(DELEGATION_TOKEN_OWNER_MISMATCH);
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
            return ExpireDelegationTokenResponse::error(DELEGATION_TOKEN_EXPIRED);
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
        return ExpireDelegationTokenResponse::error(INVALID_REQUEST);
    }

    ExpireDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::{
            delegation_token_test_support as token_fixtures,
            delegation_token_test_support::{in_mutation_window, stored_expiry},
        },
        assert2::assert,
    };

    type ExpireTokenScenario = token_fixtures::TokenMutationContext<ExpireDelegationTokenResponse>;
    #[rstest::fixture]
    fn expire_token() -> ExpireTokenScenario {
        let refusals = None;
        token_fixtures::TokenMutationContext {
            refusals,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "the expire request names a {token} token using {caller} {credential} authentication with secret {secret} and period {period}"
    )]
    async fn expire_refusal_inputs(
        expire_token: &mut ExpireTokenScenario,
        token: String,
        caller: String,
        credential: String,
        secret: String,
        period: i64,
    ) {
        *expire_token = token_fixtures::TokenMutationContext::refusal_request(
            &token,
            &caller,
            &credential,
            &secret,
            period,
        )
        .await;
    }
    #[rstest_bdd_macros::given(
        "{caller} may expire a token expiring in {expiry} milliseconds with maximum lifetime {maximum} milliseconds using period {period}"
    )]
    async fn expire_mutation_inputs(
        expire_token: &mut ExpireTokenScenario,
        caller: String,
        expiry: i64,
        maximum: i64,
        period: i64,
    ) {
        *expire_token =
            token_fixtures::TokenMutationContext::allowed_request(&caller, expiry, maximum, period)
                .await;
    }
    #[rstest_bdd_macros::when("the controller handles the expire token request")]
    async fn expire_requested_token(expire_token: &mut ExpireTokenScenario) {
        let resources = expire_token.resources();
        let request = ExpireDelegationTokenRequest {
            hmac: expire_token.hmac.clone().into(),
            expiry_time_period_ms: expire_token.period,
            ..Default::default()
        };
        expire_token.execution = Some(
            in_mutation_window(handle(
                &request,
                expire_token.authentication.as_ref().unwrap(),
                expire_token.secret.get(&resources.secret),
                &*resources.controller,
            ))
            .await,
        );
    }
    #[rstest_bdd_macros::then(
        "the expire refusal has error {code} expiry {expiry} and leaves both token deadlines unchanged"
    )]
    async fn check_expire_refusal(expire_token: &mut ExpireTokenScenario, code: i16, expiry: i64) {
        let response = &expire_token.execution.as_ref().unwrap().response;
        let observation = token_fixtures::MutationResponse {
            actual: response.clone(),
            expected: ExpireDelegationTokenResponse {
                error_code: code,
                expiry_timestamp_ms: expiry,
                ..Default::default()
            },
            case: "expire token refusal",
        };
        assert!(
            observation.actual == observation.expected,
            "{}",
            observation.case
        );
        expire_token.check_refusal_deadlines().await;
    }
    #[rstest_bdd_macros::then(
        "the expire expiry follows {deadline} milliseconds and the token is {persistence}"
    )]
    async fn check_expire_mutation(
        expire_token: &mut ExpireTokenScenario,
        deadline: String,
        persistence: String,
    ) {
        let execution = expire_token.execution.as_ref().unwrap();
        let seeded = expire_token.seeded.as_ref().unwrap();
        let resources = expire_token.fixture.as_ref().unwrap();
        expire_token.expiry_setup(&deadline).check(
            "expire token expiry",
            token_fixtures::TokenMutationOutcome::from_wire(
                execution.response.error_code,
                execution.response.expiry_timestamp_ms,
            ),
        );
        let expected = match persistence.as_str() {
            "stored" => Some(execution.response.expiry_timestamp_ms),
            "deleted" => None,
            _ => panic!("unknown token persistence: {persistence}"),
        };
        assert!(stored_expiry(&resources.controller, &seeded.token_id) == expected);
        resources.controller.cancel().await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (1)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (2)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_2(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=password (3)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_3(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=missing, caller=alice, credential=password (4)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_4(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=wrong-secret, caller=alice, credential=password (5)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_5(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=eve, credential=password (6)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_6(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=admin, credential=password (7)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_7(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=minter, credential=password (8)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_8(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=eve, credential=password (9)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_9(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=alice, credential=password (10)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_10(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=bob, credential=password (11)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_11(expire_token: ExpireTokenScenario) {
        let _ = (expire_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (1)"
    )]
    #[tokio::test]
    async fn expire_deadlines(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=60000, maximum=86400000 (2)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_2(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=-1000, maximum=86400000 (3)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_3(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (4)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_4(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=60000, maximum=86400000 (5)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_5(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (6)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_6(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (7)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_7(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_expire_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=86400000 (8)"
    )]
    #[tokio::test]
    async fn expire_deadlines_case_8(expire_token: ExpireTokenScenario) { let _ = (expire_token,); }
}
