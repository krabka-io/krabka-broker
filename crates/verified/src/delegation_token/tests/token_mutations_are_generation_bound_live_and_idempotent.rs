use super::*;

#[test]
fn token_mutations_are_generation_bound_live_and_idempotent() {
    let expected = TokenMutationFacts {
        kind: TokenMutationKind::Renew,
        state: TokenMutationState::Expected,
        now_ms: 100,
        expected_expiry_ms: 150,
        incoming_expiry_ms: 175,
        max_timestamp_ms: 200,
        uncommitted_tail: false,
    };
    for facts in [
        expected,
        TokenMutationFacts {
            now_ms: 0,
            expected_expiry_ms: 50,
            incoming_expiry_ms: 75,
            max_timestamp_ms: 100,
            ..expected
        },
        // Kafka's renew may shorten the expiry, down to `now`.
        TokenMutationFacts {
            incoming_expiry_ms: 149,
            ..expected
        },
        TokenMutationFacts {
            incoming_expiry_ms: 100,
            ..expected
        },
        // A deadline equal to `now` is still live.
        TokenMutationFacts {
            expected_expiry_ms: 100,
            ..expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 100,
            incoming_expiry_ms: 110,
            max_timestamp_ms: 110,
            ..expected
        },
    ] {
        check!(
            token_mutation_decision(facts) == TokenMutationDecision::Append,
            "{facts:?}"
        );
    }
    for facts in [
        TokenMutationFacts {
            incoming_expiry_ms: 150,
            ..expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 100,
            incoming_expiry_ms: 100,
            max_timestamp_ms: 100,
            ..expected
        },
    ] {
        check!(
            token_mutation_decision(facts) == TokenMutationDecision::Retry,
            "{facts:?}"
        );
    }
    for facts in [
        TokenMutationFacts {
            now_ms: -1,
            ..expected
        },
        TokenMutationFacts {
            state: TokenMutationState::Stale,
            ..expected
        },
        TokenMutationFacts {
            incoming_expiry_ms: 99,
            ..expected
        },
        TokenMutationFacts {
            incoming_expiry_ms: i64::MAX,
            ..expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 99,
            ..expected
        },
        TokenMutationFacts {
            max_timestamp_ms: 99,
            expected_expiry_ms: 99,
            incoming_expiry_ms: 99,
            ..expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 201,
            max_timestamp_ms: 200,
            ..expected
        },
        TokenMutationFacts {
            uncommitted_tail: true,
            ..expected
        },
    ] {
        check!(token_mutation_decision(facts) == TokenMutationDecision::Reject);
    }
    check!(
        token_mutation_decision(TokenMutationFacts {
            kind: TokenMutationKind::Delete,
            state: TokenMutationState::Missing,
            ..expected
        }) == TokenMutationDecision::Retry
    );
    check!(
        token_mutation_decision(TokenMutationFacts {
            kind: TokenMutationKind::Delete,
            expected_expiry_ms: 100,
            ..expected
        }) == TokenMutationDecision::Append
    );

    let expire_expected = TokenMutationFacts {
        kind: TokenMutationKind::Expire,
        state: TokenMutationState::Expected,
        now_ms: 100,
        expected_expiry_ms: 150,
        incoming_expiry_ms: 175,
        max_timestamp_ms: 200,
        uncommitted_tail: false,
    };
    for facts in [
        expire_expected,
        TokenMutationFacts {
            now_ms: 0,
            expected_expiry_ms: 50,
            incoming_expiry_ms: 0,
            max_timestamp_ms: 100,
            ..expire_expected
        },
        // A deadline equal to `now` is still live.
        TokenMutationFacts {
            expected_expiry_ms: 100,
            ..expire_expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 100,
            incoming_expiry_ms: 100,
            max_timestamp_ms: 100,
            ..expire_expected
        },
    ] {
        check!(
            token_mutation_decision(facts) == TokenMutationDecision::Append,
            "{facts:?}"
        );
    }
    for facts in [
        TokenMutationFacts {
            now_ms: -1,
            ..expire_expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 99,
            ..expire_expected
        },
        TokenMutationFacts {
            max_timestamp_ms: 99,
            ..expire_expected
        },
        TokenMutationFacts {
            expected_expiry_ms: 201,
            max_timestamp_ms: 200,
            ..expire_expected
        },
        TokenMutationFacts {
            incoming_expiry_ms: -1,
            ..expire_expected
        },
        TokenMutationFacts {
            incoming_expiry_ms: 201,
            max_timestamp_ms: 200,
            ..expire_expected
        },
    ] {
        check!(token_mutation_decision(facts) == TokenMutationDecision::Reject);
    }
}

/// `DelegationTokenControlManager.expireDelegationToken`: a negative period
/// deletes before the expiry check, and a live token gets
/// `min(max, now + period)` with a saturating sum.
#[test]
fn expire_matches_kafka_delete_expiry_and_saturation() {
    use TokenExpireDecision::{Delete, Expired, Update};

    // (now, period, current expiry, max, expected)
    for (now, period, current, max, expected) in [
        (0, 0, 150, 200, Update(0)),
        (100, 0, 150, 200, Update(100)),
        (100, 25, 150, 200, Update(125)),
        (100, 500, 150, 200, Update(200)),
        (100, i64::MAX - 100, 150, 200, Update(200)),
        (100, i64::MAX, 150, i64::MAX, Update(i64::MAX)),
        (100, 0, 100, 200, Update(100)),
        (100, 0, 100, 100, Update(100)),
        (100, -1, 150, 200, Delete),
        (100, -1, 50, 60, Delete),
        (100, i64::MIN, 150, 200, Delete),
        (100, 0, 99, 200, Expired),
        (100, 0, 150, 99, Expired),
    ] {
        check!(
            expire_token_deadline(now, period, current, max) == expected,
            "now={now} period={period} current={current} max={max}"
        );
    }
}

#[test]
fn active_tokens_require_both_live_ordered_deadlines() {
    check!(token_is_active(0, 50, 100));
    check!(!token_is_active(-1, 50, 100));
    check!(token_is_active(100, 150, 200));
    check!(!token_is_active(100, 100, 200));
    check!(!token_is_active(100, 150, 100));
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
    check!(!token_is_active(100, 201, 200));
}
