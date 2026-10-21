use {super::*, proptest::prelude::*};

fn saturated(value: i128) -> i64 { i64::try_from(value.min(i128::from(i64::MAX))).unwrap() }

// Wide closed-form deadlines and a symbolic extension/no-op schedule. No
// production deadline, mutation, credential or request policy is called.
fn expected(
    issued: i64,
    periods: (i64, i64, i64, i64, i64),
    delays: (i64, i64),
    cap: i64,
    tail: bool,
    api: i16,
) -> RenewalTrace {
    let lifetime = if periods.0 > 0 {
        periods.0.min(periods.1)
    } else {
        periods.1
    };
    let maximum = saturated(i128::from(issued) + i128::from(lifetime));
    let original = saturated(i128::from(issued) + i128::from(periods.2)).min(maximum);
    let renewal_period = if periods.3 > 0 {
        periods.3.min(periods.4)
    } else {
        periods.4
    };
    let renewed = saturated(i128::from(original) + i128::from(renewal_period)).min(maximum);
    let extends = original < maximum;
    let renewal = if tail {
        TokenMutationDecision::Reject
    } else if extends {
        TokenMutationDecision::Append
    } else {
        TokenMutationDecision::Retry
    };
    let cleanup = if tail || extends {
        TokenMutationDecision::Reject
    } else {
        TokenMutationDecision::Append
    };
    // The cleanup removes the token only at its maximum; a stored token
    // authenticates whether or not it has expired.
    let session = if cleanup == TokenMutationDecision::Append {
        None
    } else {
        let stored = if tail { original } else { renewed };
        let completed = saturated(i128::from(original) + i128::from(delays.0));
        let at = if cap > 0 {
            saturated(i128::from(completed) + i128::from(cap)).min(stored)
        } else {
            stored
        };
        let lifetime = (at - completed).max(0);
        let admitted = api == 17 || saturated(i128::from(original) + i128::from(delays.1)) <= at;
        Some((at, lifetime, admitted))
    };
    RenewalTrace {
        original_expiry: original,
        maximum,
        renewed_expiry: renewed,
        renewal,
        cleanup,
        session,
    }
}

#[test]
fn renewal_at_exact_expiry_preserves_service_against_old_cleanup() {
    let periods = (0, 2000, 1000, 500, 500);
    let trace = renewed_token_survives_captured_cleanup(0, periods, (250, 499), 0, false, 0);
    assert2::assert!(
        trace
            == RenewalTrace {
                original_expiry: 1000,
                maximum: 2000,
                renewed_expiry: 1500,
                renewal: TokenMutationDecision::Append,
                cleanup: TokenMutationDecision::Reject,
                session: Some((1500, 250, true)),
            }
    );
    // (delays, api) -> session: the request at the deadline is served, the
    // one after it only when it begins a re-authentication.
    for (delays, api, session) in [
        ((250, 500), 0, Some((1500, 250, true))),
        ((250, 501), 0, Some((1500, 250, false))),
        ((250, 501), 36, Some((1500, 250, false))),
        ((250, 501), 17, Some((1500, 250, true))),
        // Round two at or after the renewed expiry still authenticates, with
        // a lifetime of 0.
        ((500, 500), 36, Some((1500, 0, true))),
        ((500, 501), 36, Some((1500, 0, false))),
        ((900, 900), 36, Some((1500, 0, false))),
    ] {
        let actual = renewed_token_survives_captured_cleanup(0, periods, delays, 0, false, api);
        assert2::assert!(actual.session == session, "delays={delays:?} api={api}");
    }
    // An uncommitted tail refuses both mutations, so the original expiry
    // stays stored and the session is already past it.
    let tail = renewed_token_survives_captured_cleanup(0, periods, (250, 499), 0, true, 0);
    assert2::assert!(tail.renewal == TokenMutationDecision::Reject);
    assert2::assert!(tail.session == Some((1000, 0, false)));
    // At the maximum the cleanup removes the token, so nothing authenticates.
    let at_maximum =
        renewed_token_survives_captured_cleanup(0, (0, 1000, 1000, 500, 500), (0, 0), 0, false, 0);
    assert2::assert!(at_maximum.cleanup == TokenMutationDecision::Append);
    assert2::assert!(at_maximum.session == None);
}

#[test]
fn token_renewal_schedules_match_independent_deadline_oracle() {
    for issued in [0, 100, i64::MAX - 4, i64::MAX] {
        for periods in [
            (0, 2000, 1000, 500, 500),
            (0, 2000, 3000, 1, 500),
            (1, 2000, 1000, 0, 500),
            (-1, i64::MAX, 1, 1, i64::MAX),
            (i64::MAX, i64::MAX, i64::MAX, i64::MAX, i64::MAX),
        ] {
            let oracle = expected(issued, periods, (0, 0), 0, false, 0);
            let width = oracle.renewed_expiry - oracle.original_expiry;
            for delays in [
                (0, 0),
                (0, 1),
                (1, i64::MAX),
                (width, width),
                (width.saturating_sub(1).max(0), width),
                (width.saturating_add(1), width.saturating_add(2)),
            ] {
                for cap in [-1, 0, 1, 100, i64::MAX] {
                    for tail in [false, true] {
                        for api in [i16::MIN, 0, 17, 18, 36, 55, i16::MAX] {
                            let actual = renewed_token_survives_captured_cleanup(
                                issued, periods, delays, cap, tail, api,
                            );
                            assert2::assert!(
                                actual == expected(issued, periods, delays, cap, tail, api)
                            );
                        }
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn arbitrary_renewal_schedules_preserve_generation_and_session_bounds(
        issued in 0_i64..=i64::MAX, ceiling in 1_i64..=i64::MAX,
        initial in 1_i64..=i64::MAX, default in 1_i64..=i64::MAX,
        requested in any::<i64>(), renewal in any::<i64>(), cap in any::<i64>(),
        d0 in 0_i64..=i64::MAX, d1 in 0_i64..=i64::MAX,
        tail in any::<bool>(), api in any::<i16>(),
    ) {
        let delays = (d0.min(d1), d0.max(d1));
        let periods = (requested, ceiling, initial, renewal, default);
        prop_assert_eq!(renewed_token_survives_captured_cleanup(issued, periods, delays, cap, tail, api),
            expected(issued, periods, delays, cap, tail, api));
    }
}
