use super::*;
#[cfg(test)]
use std::vec;

const RENEW_EXPECTED: TokenMutationFacts = TokenMutationFacts {
    kind: TokenMutationKind::Renew,
    state: TokenMutationState::Expected,
    now_ms: 100,
    expected_expiry_ms: 150,
    incoming_expiry_ms: 175,
    max_timestamp_ms: 200,
    uncommitted_tail: false,
};

const EXPIRE_EXPECTED: TokenMutationFacts = TokenMutationFacts {
    kind: TokenMutationKind::Expire,
    ..RENEW_EXPECTED
};

#[yare::parameterized(
    renew_append_current = { RENEW_EXPECTED, TokenMutationDecision::Append },
    renew_append_earlier_deadline = { TokenMutationFacts {
        now_ms: 0, expected_expiry_ms: 50, incoming_expiry_ms: 75, max_timestamp_ms: 100,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_may_shorten_expiry = { TokenMutationFacts {
        incoming_expiry_ms: 149, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_deadline_equal_to_now = { TokenMutationFacts {
        incoming_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_expected_deadline_equal_to_now = { TokenMutationFacts {
        expected_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_max_timestamp_equal_to_expiry = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 110, max_timestamp_ms: 110,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_retry_at_expected_expiry = { TokenMutationFacts {
        incoming_expiry_ms: 150, ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    renew_retry_at_now = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 100, max_timestamp_ms: 100,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    renew_rejects_negative_now = { TokenMutationFacts {
        now_ms: -1, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_stale_state = { TokenMutationFacts {
        state: TokenMutationState::Stale, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_deadline_before_now = { TokenMutationFacts {
        incoming_expiry_ms: 99, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_deadline_after_max_timestamp = { TokenMutationFacts {
        incoming_expiry_ms: i64::MAX, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expired_expected_state = { TokenMutationFacts {
        expected_expiry_ms: 99, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expiry_before_max_timestamp = { TokenMutationFacts {
        max_timestamp_ms: 99, expected_expiry_ms: 99, incoming_expiry_ms: 99,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expected_expiry_after_max = { TokenMutationFacts {
        expected_expiry_ms: 201, max_timestamp_ms: 200, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_uncommitted_tail = { TokenMutationFacts {
        uncommitted_tail: true, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    delete_missing_retries = { TokenMutationFacts {
        kind: TokenMutationKind::Delete, state: TokenMutationState::Missing, ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    delete_expected_appends = { TokenMutationFacts {
        kind: TokenMutationKind::Delete, expected_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    expire_append_current = { EXPIRE_EXPECTED, TokenMutationDecision::Append },
    expire_append_earlier_deadline = { TokenMutationFacts {
        now_ms: 0, expected_expiry_ms: 50, incoming_expiry_ms: 0, max_timestamp_ms: 100,
        ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_deadline_equal_to_now = { TokenMutationFacts {
        expected_expiry_ms: 100, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_max_timestamp_equal_to_expiry = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 100, max_timestamp_ms: 100,
        ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_rejects_negative_now = { TokenMutationFacts {
        now_ms: -1, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_expired_expected_state = { TokenMutationFacts {
        expected_expiry_ms: 99, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_max_before_expected = { TokenMutationFacts {
        max_timestamp_ms: 99, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_expected_expiry_after_max = { TokenMutationFacts {
        expected_expiry_ms: 201, max_timestamp_ms: 200, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_negative_incoming_expiry = { TokenMutationFacts {
        incoming_expiry_ms: -1, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_incoming_expiry_after_max = { TokenMutationFacts {
        incoming_expiry_ms: 201, max_timestamp_ms: 200, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
)]
fn token_mutations_are_generation_bound_live_and_idempotent(
    facts: TokenMutationFacts,
    expected: TokenMutationDecision,
) {
    check!(token_mutation_decision(facts) == expected, "{facts:?}");
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "delegation token tests token mutations are generation bound live and idempotent: expire matches kafka delete expiry and saturation for {example}"
)]
fn bdd_verify_expire_matches_kafka_delete_expiry_and_saturation(example: String) {
    type Example = (i64, i64, i64, i64, TokenExpireDecision);
    let (now, period, current, max, expected): Example = match example.as_str() {
        "zero period at zero" => (0, 0, 150, 200, TokenExpireDecision::Update(0)),
        "zero period" => (100, 0, 150, 200, TokenExpireDecision::Update(100)),
        "positive period" => (100, 25, 150, 200, TokenExpireDecision::Update(125)),
        "period capped at max" => (100, 500, 150, 200, TokenExpireDecision::Update(200)),
        "near overflow is capped" => (
            100,
            i64::MAX - 100,
            150,
            200,
            TokenExpireDecision::Update(200),
        ),
        "saturating overflow" => (
            100,
            i64::MAX,
            150,
            i64::MAX,
            TokenExpireDecision::Update(i64::MAX),
        ),
        "zero period at expiry" => (100, 0, 100, 200, TokenExpireDecision::Update(100)),
        "zero period at max" => (100, 0, 100, 100, TokenExpireDecision::Update(100)),
        "negative period deletes live" => (100, -1, 150, 200, TokenExpireDecision::Delete),
        "negative period deletes expired" => (100, -1, 50, 60, TokenExpireDecision::Delete),
        "minimum period deletes" => (100, i64::MIN, 150, 200, TokenExpireDecision::Delete),
        "current expiry is past" => (100, 0, 99, 200, TokenExpireDecision::Expired),
        "max timestamp is past" => (100, 0, 150, 99, TokenExpireDecision::Expired),
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(expire_token_deadline(now, period, current, max) == expected);
}

/// `DelegationTokenControlManager.expireDelegationToken`: a negative period
/// deletes before the expiry check, and a live token gets
/// `min(max, now + period)` with a saturating sum.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/delegation_token/tests/token_mutations_are_generation_bound_live_and_idempotent.feature",
    name = "expire matches kafka delete expiry and saturation"
)]
fn expire_matches_kafka_delete_expiry_and_saturation(example: String) { let _ = example; }

#[test]
fn expiry_and_renewal_stay_within_the_token_maximum() {
    check!(expire_token_deadline(100, 50, 200, 200) == TokenExpireDecision::Update(150));
    check!(renew_token_expiry(100, 50, 50, 200, 200) == TokenRenewDecision::Renew(150));
    let renew_at_max = TokenMutationFacts {
        state: TokenMutationState::Expected,
        kind: TokenMutationKind::Renew,
        now_ms: 100,
        expected_expiry_ms: 200,
        incoming_expiry_ms: 200,
        max_timestamp_ms: 200,
        uncommitted_tail: false,
    };
    check!(token_mutation_decision(renew_at_max) == TokenMutationDecision::Retry);
    let renew_over_max = TokenMutationFacts {
        expected_expiry_ms: 250,
        incoming_expiry_ms: 250,
        ..renew_at_max
    };
    check!(token_mutation_decision(renew_over_max) == TokenMutationDecision::Reject);
}
