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

use {
    crate::{
        codes::{
            DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_OWNER_MISMATCH, INVALID_REQUEST,
            NOT_CONTROLLER, UNKNOWN_SERVER_ERROR,
        },
        handlers::{
            ErrorCodeResponse as _,
            delegation_token_admission::{admit_token_request, may_renew_or_expire},
            submit_failure_code,
        },
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
        time_util::now_ms,
    },
    krabka_metadata::DelegationTokenRecord,
    krabka_protocol::owned::{
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    },
    krabka_raft::{DelegationTokenMutation, RaftError},
    krabka_security::SecretBytes,
    krabka_verified::{TokenRenewDecision, delegation_token::TokenApi, renew_token_expiry},
};

#[tracing::instrument(
    name = "handle_renew_delegation_token",
    level = "info",
    skip_all,
    fields(api = "RenewDelegationToken")
)]
pub async fn handle(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &dyn MetadataSource,
) -> RenewDelegationTokenResponse {
    let (principal, token) = match admit_token_request(
        TokenApi::Renew,
        auth,
        secret_key,
        req.hmac.as_ref(),
        controller,
    ) {
        Ok(admitted) => admitted,
        Err((error_code, expiry_timestamp_ms)) => {
            return RenewDelegationTokenResponse {
                expiry_timestamp_ms,
                ..RenewDelegationTokenResponse::error(error_code)
            };
        }
    };
    let caller = principal.to_kafka();

    let decision = renew_token_expiry(
        now_ms(),
        req.renew_period_ms,
        default_renew_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    );
    if decision == TokenRenewDecision::Expired {
        return RenewDelegationTokenResponse::error(DELEGATION_TOKEN_EXPIRED);
    }
    if !may_renew_or_expire(&token, &caller) {
        return RenewDelegationTokenResponse::error(DELEGATION_TOKEN_OWNER_MISMATCH);
    }
    let TokenRenewDecision::Renew(new_expiry) = decision else {
        return RenewDelegationTokenResponse::error(INVALID_REQUEST);
    };

    let expected = token.to_record();
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
        // Kafka fails the request with the controller's exception and
        // answers `getErrorResponse` of it: `NOT_CONTROLLER` when there is no
        // active controller, which the admin client retries, and
        // `UNKNOWN_SERVER_ERROR` for anything else.
        let code = match e {
            RaftError::NotLeader { .. } | RaftError::LeaderUnknown => NOT_CONTROLLER,
            other => submit_failure_code(&other, UNKNOWN_SERVER_ERROR),
        };
        return RenewDelegationTokenResponse::error(code);
    }

    RenewDelegationTokenResponse {
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
            delegation_token_test_support::{
                in_mutation_window, stored_expiry as stored_expiry_fixture,
            },
        },
        assert2::assert,
        krabka_raft::ControllerHandle,
    };

    fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> i64 {
        stored_expiry_fixture(controller, token_id).expect("token remains")
    }

    type RenewTokenScenario = token_fixtures::TokenMutationContext<RenewDelegationTokenResponse>;
    #[rstest::fixture]
    fn renew_token() -> RenewTokenScenario {
        let refusals = None;
        token_fixtures::TokenMutationContext {
            refusals,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "the renew request names a {token} token using {caller} {credential} authentication with secret {secret} and period {period}"
    )]
    async fn renew_refusal_inputs(
        renew_token: &mut RenewTokenScenario,
        token: String,
        caller: String,
        credential: String,
        secret: String,
        period: i64,
    ) {
        *renew_token = token_fixtures::TokenMutationContext::refusal_request(
            &token,
            &caller,
            &credential,
            &secret,
            period,
        )
        .await;
    }
    #[rstest_bdd_macros::given(
        "{caller} may renew a token expiring in {expiry} milliseconds with maximum lifetime {maximum} milliseconds using period {period}"
    )]
    async fn renew_mutation_inputs(
        renew_token: &mut RenewTokenScenario,
        caller: String,
        expiry: i64,
        maximum: i64,
        period: i64,
    ) {
        *renew_token =
            token_fixtures::TokenMutationContext::allowed_request(&caller, expiry, maximum, period)
                .await;
    }
    #[rstest_bdd_macros::when("the controller handles the renew token request")]
    async fn renew_requested_token(renew_token: &mut RenewTokenScenario) {
        let resources = renew_token.resources();
        let request = RenewDelegationTokenRequest {
            hmac: renew_token.hmac.clone().into(),
            renew_period_ms: renew_token.period,
            ..Default::default()
        };
        renew_token.execution = Some(
            in_mutation_window(handle(
                &request,
                renew_token.authentication.as_ref().unwrap(),
                renew_token.secret.get(&resources.secret),
                2 * 60 * 60 * 1000,
                &*resources.controller,
            ))
            .await,
        );
    }
    #[rstest_bdd_macros::then(
        "the renew refusal has error {code} expiry {expiry} and leaves both token deadlines unchanged"
    )]
    async fn check_renew_refusal(renew_token: &mut RenewTokenScenario, code: i16, expiry: i64) {
        let response = &renew_token.execution.as_ref().unwrap().response;
        let observation = token_fixtures::MutationResponse {
            actual: response.clone(),
            expected: RenewDelegationTokenResponse {
                error_code: code,
                expiry_timestamp_ms: expiry,
                ..Default::default()
            },
            case: "renew token refusal",
        };
        assert!(
            observation.actual == observation.expected,
            "{}",
            observation.case
        );
        renew_token.check_refusal_deadlines().await;
    }
    #[rstest_bdd_macros::then(
        "the renew expiry follows {deadline} milliseconds and the token is {persistence}"
    )]
    async fn check_renew_mutation(
        renew_token: &mut RenewTokenScenario,
        deadline: String,
        persistence: String,
    ) {
        let execution = renew_token.execution.as_ref().unwrap();
        let seeded = renew_token.seeded.as_ref().unwrap();
        let resources = renew_token.fixture.as_ref().unwrap();
        renew_token.expiry_setup(&deadline).check(
            "renew token expiry",
            token_fixtures::TokenMutationOutcome::from_wire(
                execution.response.error_code,
                execution.response.expiry_timestamp_ms,
            ),
        );
        assert!(persistence == "stored");
        assert!(
            stored_expiry(&resources.controller, &seeded.token_id)
                == execution.response.expiry_timestamp_ms
        );
        resources.controller.cancel().await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (1)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=delegation-token (2)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_2(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=alice, credential=password (3)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_3(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=missing, caller=alice, credential=password (4)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_4(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=wrong-secret, caller=alice, credential=password (5)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_5(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=eve, credential=password (6)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_6(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=admin, credential=password (7)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_7(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=live, caller=minter, credential=password (8)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_8(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=eve, credential=password (9)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_9(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Credential ownership and token expiry checks preserve Kafka refusal ordering: token=expired, caller=alice, credential=password (10)"
    )]
    #[tokio::test]
    async fn refusals_follow_kafka_order_case_10(renew_token: RenewTokenScenario) {
        let _ = (renew_token,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (1)"
    )]
    #[tokio::test]
    async fn renew_deadlines(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=bob, expiry=3600000, maximum=86400000 (2)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_2(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (3)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_3(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (4)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_4(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (5)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_5(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=3600000, maximum=86400000 (6)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_6(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=72000000, maximum=86400000 (7)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_7(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=1800000 (8)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_8(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_renew_token/tokens.feature",
        name = "Token mutation deadlines follow the requested period and lifetime cap: caller=alice, expiry=60000, maximum=1800000 (9)"
    )]
    #[tokio::test]
    async fn renew_deadlines_case_9(renew_token: RenewTokenScenario) { let _ = (renew_token,); }

    /// A renewal whose controller write fails answers Kafka's error response
    /// for the controller's exception, not `INVALID_REQUEST`: a failover
    /// answers the retriable `NOT_CONTROLLER`.
    #[tokio::test]
    async fn failed_controller_write_answers_kafka_error_code() {
        let secret = SecretBytes::new(b"k".to_vec());
        let req = RenewDelegationTokenRequest {
            hmac: token_fixtures::hmac_for(token_fixtures::FAILING_WRITE_TOKEN_ID).into(),
            renew_period_ms: 30_000,
            ..Default::default()
        };
        for (case, failure, code) in token_fixtures::failed_write_cases() {
            let controller = token_fixtures::failing_writes_source(failure);
            let resp = handle(
                &req,
                &token_fixtures::authed("alice"),
                Some(&secret),
                token_fixtures::DAY_MS,
                &controller,
            )
            .await;
            assert!(resp == RenewDelegationTokenResponse::error(code), "{case}");
        }
    }
}
