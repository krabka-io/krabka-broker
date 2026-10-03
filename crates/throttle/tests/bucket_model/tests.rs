use super::*;

#[test]
fn bucket_basic() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 1,
            consumers: 2,
            configs: vec![Config { rate: 1, burst: 2 }, Config { rate: 1, burst: 1 }],
            max_resets: 1,
            max_time: 2,
            max_req: 2,
        },
        "bucket_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn bucket_wide() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 1,
            consumers: 2,
            configs: vec![
                Config { rate: 1, burst: 3 },
                Config { rate: 2, burst: 1 },
                Config { rate: 0, burst: 0 },
                Config { rate: 1, burst: 0 },
            ],
            max_resets: 2,
            max_time: 2,
            max_req: 2,
        },
        "bucket_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

/// Half-token rates and bursts, as a fractional quota rate gives production:
/// at two storage units to a token, `rate: 1` is half a token per second and
/// `burst: 1` holds half a token, so no whole token is ever granted under it.
/// The bucket still conserves every claimed refill, stays within its burst,
/// and grants only whole tokens.
#[test]
fn bucket_fractional() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 2,
            consumers: 2,
            configs: vec![Config { rate: 1, burst: 3 }, Config { rate: 1, burst: 1 }],
            max_resets: 1,
            max_time: 2,
            max_req: 2,
        },
        "bucket_fractional",
        PINNED_UNIQUE_STATES_FRACTIONAL,
    );
}

/// RED witness: the former seqlock lets a whole reset run between a
/// consumer's generation re-check and its `available` commit. The reset stores
/// the value the commit expects, so the stale commit succeeds and leaves
/// `available` above the new burst.
#[test]
fn seqlock_cas_lets_a_straddled_reset_exceed_burst() {
    let checker = run(straddle_config(Algorithm::SeqlockCas));
    let found = checker.assert_any_discovery("available_within_burst");
    eprintln!("straddled reset counterexample: {:?}", found.into_actions());
}

/// RED witness: the former seqlock claims a refill on `last_refill` before it
/// commits `available`, and a commit that loses its race restarts without the
/// refill it claimed. Without any reset, `available` stays within the burst,
/// yet the claimed tokens vanish.
#[test]
fn seqlock_cas_drops_a_claimed_refill() {
    let checker = run(contention_config(Algorithm::SeqlockCas));
    checker.assert_no_discovery("available_within_burst");
    let found = checker.assert_any_discovery("claimed_refill_conserved");
    eprintln!("dropped refill counterexample: {:?}", found.into_actions());
}

/// GREEN counterpart of both RED witnesses: production's lock holds both
/// properties in the very searches where the seqlock breaks them.
#[test]
fn locked_holds_where_seqlock_cas_fails() {
    for model in [
        straddle_config(Algorithm::Locked),
        contention_config(Algorithm::Locked),
    ] {
        let checker = run(model);
        checker.assert_no_discovery("available_within_burst");
        checker.assert_no_discovery("claimed_refill_conserved");
    }
}

/// Replays the review's concrete straddle, `burst = 10`, `available = 3`, a
/// refill of 7, and a request of 1, against both algorithms.
///
/// Under the seqlock, the consume passes its generation re-check, a whole
/// reset to `(rate 3, burst 3)` runs and stores `available = 3`, and the
/// consume's commit, which expects 3, then stores 9 under a burst of 3. Under
/// the lock, the reset cannot take its first step while the consume holds the
/// lock across its claim and commit; it runs after, and the bucket ends at the
/// new burst.
#[test]
fn straddled_reset_schedule() {
    let model = |algorithm| BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 1,
        configs: vec![Config { rate: 1, burst: 10 }, Config { rate: 3, burst: 3 }],
        max_resets: 1,
        max_time: 7,
        max_req: 7,
    };
    let ticks = vec![Act::Tick; 7];

    // Drain 7 of 10 at no refill (9 steps), let 7 seconds pass, run the consume
    // of 1 up to its commit, run the whole reset, then commit.
    let last = replay(
        &model(Algorithm::SeqlockCas),
        &[
            consume(0, 7, 9),
            ticks.clone(),
            consume(0, 1, 9),
            reset(6),
            vec![Act::StepConsumer(0)],
        ],
    )
    .expect("the straddle is enabled step by step under the seqlock");
    assert2::assert!((last.available, last.burst, available_within_burst(&last)) == (9, 3, false));

    let locked = model(Algorithm::Locked);
    let straddle = [consume(0, 7, 6), ticks.clone(), consume(0, 1, 3), reset(1)];
    assert2::assert!(replay(&locked, &straddle[..3]).is_some());
    assert2::assert!(
        replay(&locked, &straddle).is_none(),
        "the reset waits for the lock"
    );
    let last = replay(
        &locked,
        &[consume(0, 7, 6), ticks, consume(0, 1, 6), reset(4)],
    )
    .expect("the reset runs once the consume releases the lock");
    assert2::assert!((last.available, last.burst, available_within_burst(&last)) == (3, 3, true));
}

/// Replays a dropped refill whose loss a later consume can observe.
///
/// The bucket holds 2 of 3 tokens when one second passes. Consumer 0 claims
/// that second's token and reaches its commit expecting 2; consumer 1 then
/// takes a token, so consumer 0's commit fails and it restarts with nothing
/// left to claim. Both grants of 1 succeed, but the bucket ends empty, where a
/// serial run of the two consumes leaves `min(2 + 1, 3) - 2 = 1`. Under the
/// lock, consumer 1 cannot take its lock step while consumer 0 holds it, and
/// the bucket ends at 1.
#[test]
fn dropped_refill_schedule() {
    let model = |algorithm| BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 2,
        configs: vec![Config { rate: 1, burst: 3 }],
        max_resets: 0,
        max_time: 1,
        max_req: 1,
    };
    let tick = vec![Act::Tick];

    let last = replay(
        &model(Algorithm::SeqlockCas),
        &[
            consume(0, 1, 9),
            tick.clone(),
            consume(0, 1, 9),
            consume(1, 1, 9),
            vec![Act::StepConsumer(0); 9],
        ],
    )
    .expect("the contention is enabled step by step under the seqlock");
    assert2::assert!(
        (
            last.available,
            last.granted,
            claimed_refill_conserved(&last)
        ) == (0, 3, false)
    );

    let locked = model(Algorithm::Locked);
    let contention = [
        consume(0, 1, 6),
        tick.clone(),
        consume(0, 1, 3),
        consume(1, 1, 2),
    ];
    assert2::assert!(replay(&locked, &contention[..3]).is_some());
    assert2::assert!(
        replay(&locked, &contention).is_none(),
        "consumer 1 waits for the lock"
    );
    let last = replay(
        &locked,
        &[
            consume(0, 1, 6),
            tick,
            consume(0, 1, 3),
            consume(1, 1, 1),
            vec![Act::StepConsumer(0); 3],
            vec![Act::StepConsumer(1); 5],
        ],
    )
    .expect("consumer 1 runs once consumer 0 releases the lock");
    assert2::assert!(
        (
            last.available,
            last.granted,
            claimed_refill_conserved(&last)
        ) == (1, 3, true)
    );
}
