use super::*;
#[cfg(test)]
use std::vec;

#[yare::parameterized(
    no_filter_even_owner = { false, true, false, false, false, false },
    no_filter_even_requester = { false, false, true, false, false, false },
    owner_with_filter = { true, true, false, false, false, true },
    requester_with_filter = { true, false, true, false, false, true },
    renewer_with_filter = { true, false, false, true, false, true },
    acl_with_filter = { true, false, false, false, true, true },
    unrelated_with_filter = { true, false, false, false, false, false },
)]
fn token_visibility_requires_owner_filter_plus_a_relationship(
    filter: bool,
    owner: bool,
    requester: bool,
    renewer: bool,
    acl: bool,
    visible: bool,
) {
    check!(token_describe_visible(filter, owner, requester, renewer, acl) == visible);
}

#[yare::parameterized(
    regular_credential_wins = { true, false, true, ScramCredentialSource::Regular },
    token_credential = { false, true, true, ScramCredentialSource::DelegationToken },
    non_token_mechanism = { false, false, true, ScramCredentialSource::Unknown },
    no_token_credential = { false, true, false, ScramCredentialSource::Unknown },
)]
fn scram_source_prefers_regular_and_limits_token_fallback(
    regular: bool,
    token_mechanism: bool,
    token: bool,
    expected: ScramCredentialSource,
) {
    check!(scram_credential_source(regular, token_mechanism, token) == expected);
}

#[yare::parameterized(
    create = { TokenApi::Create },
    renew = { TokenApi::Renew },
    expire = { TokenApi::Expire },
    describe = { TokenApi::Describe },
)]
fn token_api_admission_requires_identity_and_blocks_every_token_authed_call(api: TokenApi) {
    check!(token_api_admission(false, false, api) == TokenApiAdmission::Reject);
    check!(token_api_admission(false, true, api) == TokenApiAdmission::Reject);
    check!(token_api_admission(true, false, api) == TokenApiAdmission::Allow);
    // KafkaApis.allowTokenRequests refuses every delegation-token API,
    // Describe included, to a token-authenticated caller.
    check!(token_api_admission(true, true, api) == TokenApiAdmission::Reject);
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
};

struct TokenDeadlinesScenario {
    creation_policy: Option<(i64, i64)>,
    renewal_context: Option<(i64, i64, i64)>,
    creation: Option<TokenCreateDecision>,
    renewal: Option<TokenRenewDecision>,
}

#[fixture]
fn token() -> TokenDeadlinesScenario {
    // Start this scenario with fresh inputs and observations.

    TokenDeadlinesScenario {
        creation_policy: None,
        renewal_context: None,
        creation: None,
        renewal: None,
    }
}

#[given("a token policy ceiling of {ceiling} ms and renewal interval of {renewal} ms")]
fn token_policy(token: &mut TokenDeadlinesScenario, ceiling: String, renewal: String) {
    token.creation_policy = Some((token_millis(&ceiling), token_millis(&renewal)));
}

#[when("a token is created at {now} ms with requested lifetime {requested} ms")]
fn create_token(token: &mut TokenDeadlinesScenario, now: String, requested: String) {
    let (ceiling, renewal) = token.creation_policy.expect("a token policy was given");
    token.creation = Some(create_token_deadlines(
        token_millis(&now),
        token_millis(&requested),
        ceiling,
        renewal,
    ));
}

#[then(
    "token creation is {outcome} with maximum timestamp {maximum} ms and initial expiry {expiry} ms"
)]
fn created_token(token: &TokenDeadlinesScenario, outcome: String, maximum: String, expiry: String) {
    let expected = match outcome.as_str() {
        "create" => created(token_millis(&maximum), token_millis(&expiry)),
        "invalid" => {
            check!(maximum == "none" && expiry == "none");
            TokenCreateDecision::Invalid
        }
        _ => panic!("unknown token creation outcome: {outcome}"),
    };
    check!(token.creation == Some(expected));
}

#[given(
    "a token expiring at {current} ms with maximum timestamp {maximum} ms and default renewal interval {default} ms"
)]
fn renewable_token(
    token: &mut TokenDeadlinesScenario,
    current: String,
    maximum: String,
    default: String,
) {
    token.renewal_context = Some((
        token_millis(&current),
        token_millis(&maximum),
        token_millis(&default),
    ));
}

#[when("renewal is requested at {now} ms for a period of {requested} ms")]
fn renew_token(token: &mut TokenDeadlinesScenario, now: String, requested: String) {
    let (current, maximum, default) = token.renewal_context.expect("a renewable token was given");
    token.renewal = Some(renew_token_expiry(
        token_millis(&now),
        token_millis(&requested),
        default,
        current,
        maximum,
    ));
}

#[then("token renewal is {outcome} with expiry {expiry} ms")]
fn renewed_token(token: &TokenDeadlinesScenario, outcome: String, expiry: String) {
    let expected = match outcome.as_str() {
        "renewed" => TokenRenewDecision::Renew(token_millis(&expiry)),
        "expired" => {
            check!(expiry == "none");
            TokenRenewDecision::Expired
        }
        "invalid" => {
            check!(expiry == "none");
            TokenRenewDecision::Invalid
        }
        _ => panic!("unknown token renewal outcome: {outcome}"),
    };
    check!(token.renewal == Some(expected));
}

scenarios!("tests/features/migrated/src/delegation_token/tests/token_visibility_requires_owner_filter_plus_a_relationship.feature", fixtures = [token: TokenDeadlinesScenario]);
