use super::*;

#[test]
fn token_visibility_requires_owner_filter_plus_a_relationship() {
    for (filter, owner, requester, renewer, acl, visible) in [
        (false, true, false, false, false, false),
        (false, false, true, false, false, false),
        (true, true, false, false, false, true),
        (true, false, true, false, false, true),
        (true, false, false, true, false, true),
        (true, false, false, false, true, true),
        (true, false, false, false, false, false),
    ] {
        check!(token_describe_visible(filter, owner, requester, renewer, acl) == visible);
    }
}

#[test]
fn scram_source_prefers_regular_and_limits_token_fallback() {
    use ScramCredentialSource::{DelegationToken, ExpiredDelegationToken, Regular, Unknown};

    for (regular, token_mechanism, token, active, expected) in [
        (true, false, true, false, Regular),
        (false, true, true, true, DelegationToken),
        (false, true, true, false, ExpiredDelegationToken),
        (false, false, true, true, Unknown),
        (false, true, false, true, Unknown),
    ] {
        check!(scram_credential_source(regular, token_mechanism, token, active) == expected);
    }
}

#[test]
fn token_api_admission_requires_identity_and_blocks_every_token_authed_call() {
    for api in [
        TokenApi::Create,
        TokenApi::Renew,
        TokenApi::Expire,
        TokenApi::Describe,
    ] {
        check!(token_api_admission(false, false, api) == TokenApiAdmission::Reject);
        check!(token_api_admission(false, true, api) == TokenApiAdmission::Reject);
        check!(token_api_admission(true, false, api) == TokenApiAdmission::Allow);
        // KafkaApis.allowTokenRequests refuses every delegation-token API,
        // Describe included, to a token-authenticated caller.
        check!(token_api_admission(true, true, api) == TokenApiAdmission::Reject);
    }
}

/// `DelegationTokenControlManager.createDelegationToken`: a request of 0
/// or less takes the configured lifetime, a positive one the smaller of
/// the two, and both sums saturate at `i64::MAX`.
#[test]
fn create_matches_kafka_lifetime_and_saturation() {
    for (now, requested, ceiling, renew, expected) in [
        (0, -1, 1_000, 100, created(1_000, 100)),
        (100, -1, 1_000, 100, created(1_100, 200)),
        (100, 0, 1_000, 100, created(1_100, 200)),
        (100, -2, 1_000, 100, created(1_100, 200)),
        (100, i64::MIN, 1_000, 100, created(1_100, 200)),
        (100, 50, 1_000, 100, created(150, 150)),
        (100, 5_000, 1_000, 100, created(1_100, 200)),
        (100, -1, 100, 101, created(200, 200)),
        (100, -1, i64::MAX - 100, 100, created(i64::MAX, 200)),
        (i64::MAX, -1, 1, 1, created(i64::MAX, i64::MAX)),
        (1, -1, i64::MAX, 1, created(i64::MAX, 2)),
        (1, -1, i64::MAX, i64::MAX, created(i64::MAX, i64::MAX)),
        (100, -1, 0, 100, TokenCreateDecision::Invalid),
        (100, -1, 1_000, 0, TokenCreateDecision::Invalid),
        (100, -1, -1, 100, TokenCreateDecision::Invalid),
    ] {
        check!(
            create_token_deadlines(now, requested, ceiling, renew) == expected,
            "now={now} requested={requested} ceiling={ceiling} renew={renew}"
        );
    }
}

/// `DelegationTokenControlManager.renewDelegationToken`: `Expired` only
/// for a deadline strictly before `now`; the expiry is
/// `min(max, now + min(default, period))` for a positive period and
/// `min(max, now + default)` otherwise, even when that shortens it.
#[test]
fn renew_matches_kafka_period_cap_and_expiry() {
    use TokenRenewDecision::{Expired, Invalid, Renew};

    // (now, requested, default, current expiry, max, expected)
    for (now, requested, default, current, max, expected) in [
        (100, 25, 50, 150, 1_000, Renew(125)),
        (100, 75, 50, 150, 1_000, Renew(150)),
        (100, 500, 50, 150, 1_000, Renew(150)),
        (100, -1, 50, 150, 1_000, Renew(150)),
        (100, 0, 50, 150, 1_000, Renew(150)),
        (100, -5, 50, 150, 1_000, Renew(150)),
        // A shorter period than the remaining lifetime shortens the expiry.
        (100, 10, 50, 900, 1_000, Renew(110)),
        (100, 500, 1_000, 150, 200, Renew(200)),
        (100, i64::MAX, i64::MAX, 150, 200, Renew(200)),
        (1, i64::MAX, i64::MAX, 150, i64::MAX, Renew(i64::MAX)),
        // A deadline equal to `now` is still live in Kafka.
        (100, 25, 50, 100, 1_000, Renew(125)),
        (100, 25, 50, 100, 100, Renew(100)),
        (100, 25, 50, 99, 1_000, Expired),
        (100, 25, 50, 150, 99, Expired),
        (100, 25, 0, 99, 1_000, Expired),
        (100, 25, 0, 150, 1_000, Invalid),
    ] {
        check!(
            renew_token_expiry(now, requested, default, current, max) == expected,
            "now={now} requested={requested} default={default} current={current} max={max}"
        );
    }
}
