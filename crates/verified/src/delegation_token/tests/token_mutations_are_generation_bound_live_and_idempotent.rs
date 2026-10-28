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

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
};

struct TokenExpiration {
    deadlines: Option<(i64, i64)>,
    outcome: Option<TokenExpireDecision>,
}

#[fixture]
fn expiration() -> TokenExpiration {
    // Start this scenario with fresh inputs and observations.

    TokenExpiration {
        deadlines: None,
        outcome: None,
    }
}

#[given("an expirable token with current deadline {current} ms and maximum timestamp {maximum} ms")]
fn expirable_token(expiration: &mut TokenExpiration, current: String, maximum: String) {
    expiration.deadlines = Some((token_millis(&current), token_millis(&maximum)));
}

#[when("token expiration is requested at {now} ms for a period of {period} ms")]
fn expire_token(expiration: &mut TokenExpiration, now: String, period: String) {
    let (current, maximum) = expiration.deadlines.expect("an expirable token was given");
    expiration.outcome = Some(expire_token_deadline(
        token_millis(&now),
        token_millis(&period),
        current,
        maximum,
    ));
}

#[then("token expiration is {outcome} with deadline {deadline} ms")]
fn expired_token(expiration: &TokenExpiration, outcome: String, deadline: String) {
    let expected = match outcome.as_str() {
        "updated" => TokenExpireDecision::Update(token_millis(&deadline)),
        "delete" => {
            check!(deadline == "none");
            TokenExpireDecision::Delete
        }
        "expired" => {
            check!(deadline == "none");
            TokenExpireDecision::Expired
        }
        _ => panic!("unknown token expiration outcome: {outcome}"),
    };
    check!(expiration.outcome == Some(expected));
}

scenarios!("tests/features/migrated/src/delegation_token/tests/token_mutations_are_generation_bound_live_and_idempotent.feature", fixtures = [expiration: TokenExpiration]);

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
