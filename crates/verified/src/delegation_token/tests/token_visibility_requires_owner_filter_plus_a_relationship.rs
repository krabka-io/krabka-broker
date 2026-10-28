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

#[cfg(test)]
#[rstest_bdd_macros::then(
    "delegation token tests token visibility requires owner filter plus a relationship: create matches kafka lifetime and saturation for {example}"
)]
fn bdd_verify_create_matches_kafka_lifetime_and_saturation(example: String) {
    type Example = (i64, i64, i64, i64, TokenCreateDecision);
    let (now, requested, ceiling, renew, expected): Example = match example.as_str() {
        "negative request uses ceiling" => (0, -1, 1_000, 100, created(1_000, 100)),
        "later time adds deadlines" => (100, -1, 1_000, 100, created(1_100, 200)),
        "zero request uses ceiling" => (100, 0, 1_000, 100, created(1_100, 200)),
        "other negative request uses ceiling" => (100, -2, 1_000, 100, created(1_100, 200)),
        "minimum request uses ceiling" => (100, i64::MIN, 1_000, 100, created(1_100, 200)),
        "positive request caps lifetime" => (100, 50, 1_000, 100, created(150, 150)),
        "oversized request uses ceiling" => (100, 5_000, 1_000, 100, created(1_100, 200)),
        "renewal period can extend expiry" => (100, -1, 100, 101, created(200, 200)),
        "maximum expiry saturates" => (100, -1, i64::MAX - 100, 100, created(i64::MAX, 200)),
        "both deadlines saturate" => (i64::MAX, -1, 1, 1, created(i64::MAX, i64::MAX)),
        "only expiry saturates" => (1, -1, i64::MAX, 1, created(i64::MAX, 2)),
        "expiry and renewal saturate" => (1, -1, i64::MAX, i64::MAX, created(i64::MAX, i64::MAX)),
        "zero ceiling is invalid" => (100, -1, 0, 100, TokenCreateDecision::Invalid),
        "zero renewal is invalid" => (100, -1, 1_000, 0, TokenCreateDecision::Invalid),
        "negative ceiling is invalid" => (100, -1, -1, 100, TokenCreateDecision::Invalid),
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(create_token_deadlines(now, requested, ceiling, renew) == expected);
}

/// `DelegationTokenControlManager.createDelegationToken`: a request of 0
/// or less takes the configured lifetime, a positive one the smaller of
/// the two, and both sums saturate at `i64::MAX`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/delegation_token/tests/token_visibility_requires_owner_filter_plus_a_relationship.feature",
    name = "create matches kafka lifetime and saturation"
)]
fn create_matches_kafka_lifetime_and_saturation(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "delegation token tests token visibility requires owner filter plus a relationship: renew matches kafka period cap and expiry for {example}"
)]
fn bdd_verify_renew_matches_kafka_period_cap_and_expiry(example: String) {
    type Example = (i64, i64, i64, i64, i64, TokenRenewDecision);
    let (now, requested, default, current, max, expected): Example = match example.as_str() {
        "requested shorter period" => (100, 25, 50, 150, 1_000, TokenRenewDecision::Renew(125)),
        "requested longer period" => (100, 75, 50, 150, 1_000, TokenRenewDecision::Renew(150)),
        "requested much longer period" => {
            (100, 500, 50, 150, 1_000, TokenRenewDecision::Renew(150))
        }
        "negative period uses default" => (100, -1, 50, 150, 1_000, TokenRenewDecision::Renew(150)),
        "zero period uses default" => (100, 0, 50, 150, 1_000, TokenRenewDecision::Renew(150)),
        "other negative period uses default" => {
            (100, -5, 50, 150, 1_000, TokenRenewDecision::Renew(150))
        }
        "short period can shorten expiry" => {
            (100, 10, 50, 900, 1_000, TokenRenewDecision::Renew(110))
        }
        "capped by maximum" => (100, 500, 1_000, 150, 200, TokenRenewDecision::Renew(200)),
        "saturated expiry capped by maximum" => (
            100,
            i64::MAX,
            i64::MAX,
            150,
            200,
            TokenRenewDecision::Renew(200),
        ),
        "saturated expiry" => (
            1,
            i64::MAX,
            i64::MAX,
            150,
            i64::MAX,
            TokenRenewDecision::Renew(i64::MAX),
        ),
        "deadline equal to now is live" => {
            (100, 25, 50, 100, 1_000, TokenRenewDecision::Renew(125))
        }
        "max equal to now caps renewal" => (100, 25, 50, 100, 100, TokenRenewDecision::Renew(100)),
        "expired deadline" => (100, 25, 50, 99, 1_000, TokenRenewDecision::Expired),
        "max before now is expired" => (100, 25, 50, 150, 99, TokenRenewDecision::Expired),
        "zero default cannot renew expired" => (100, 25, 0, 99, 1_000, TokenRenewDecision::Expired),
        "zero default is invalid while live" => {
            (100, 25, 0, 150, 1_000, TokenRenewDecision::Invalid)
        }
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(renew_token_expiry(now, requested, default, current, max) == expected);
}

/// `DelegationTokenControlManager.renewDelegationToken`: `Expired` only
/// for a deadline strictly before `now`; the expiry is
/// `min(max, now + min(default, period))` for a positive period and
/// `min(max, now + default)` otherwise, even when that shortens it.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/delegation_token/tests/token_visibility_requires_owner_filter_plus_a_relationship.feature",
    name = "renew matches kafka period cap and expiry"
)]
fn renew_matches_kafka_period_cap_and_expiry(example: String) { let _ = example; }
