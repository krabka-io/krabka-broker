//! [`TokenBucket::try_consume`]: turn the time elapsed since the last refill
//! into tokens, cap them at the burst, and grant from the result, all in one
//! critical section.
//!
//! The arithmetic that caps and grants is the verified [`plan_consume`]
//! kernel. [`BucketState::consume`] is the whole step on the locked group, so
//! it can be tested without threads.

use krabka_verified::throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};

use super::{BucketState, TokenBucket};

const NANOS_PER_SEC: u128 = 1_000_000_000;

impl TokenBucket {
    /// Tries to consume up to `requested` tokens.
    ///
    /// This method returns the amount actually granted. Rate-0 grants the full
    /// request.
    ///
    /// The refill, the grant, and the new balance are one critical section on
    /// the bucket's group, so a concurrent
    /// [`Self::set_token_rate_with_burst`] runs wholly before or wholly after
    /// this call, and concurrent consumes never lose each other's refill. See
    /// the module documentation of the runtime for the design.
    ///
    /// # Panics
    /// Panics if the injected clock reads more than `u64::MAX` nanoseconds
    /// since its origin, about 584 years.
    pub fn try_consume(&self, requested: u64) -> u64 {
        // Unthrottled fast path: a `0` here orders this consume before any
        // reset still in flight, so it may grant without the lock.
        if self.fast_path_rate() == 0 {
            return requested;
        }
        let mut state = self.lock_state();
        let now = self.now_nanos();
        state.consume(now, requested)
    }
}

impl BucketState {
    /// Refills the bucket for the time elapsed up to `now`, then grants up to
    /// `requested` from it, and returns the grant.
    ///
    /// Only the time the whole refilled tokens account for is claimed. The
    /// remainder stays unclaimed for the next call, so a caller that retries
    /// faster than one token per interval still sees the bucket refill at
    /// `rate`. Claiming the whole gap would drop that remainder on every call,
    /// and a bucket polled often enough would never refill at all.
    ///
    /// Every value is computed before the first store, so a panic cannot leave
    /// the group half updated.
    fn consume(&mut self, now: u64, requested: u64) -> u64 {
        let rate = self.rate_per_sec;
        if rate == 0 {
            return requested;
        }
        let elapsed = now.saturating_sub(self.last_refill_nanos);
        let refill = u128::from(elapsed) * u128::from(rate) / NANOS_PER_SEC;
        let refill =
            u64::try_from(refill.min(u128::from(u64::MAX))).expect("refill is capped at u64::MAX");
        let claimed = u64::try_from(
            (u128::from(refill) * NANOS_PER_SEC / u128::from(rate)).min(u128::from(elapsed)),
        )
        .expect("the claimed time is at most the elapsed time");
        let (grant, new_available) = plan_consume(
            AvailableTokens(self.available),
            RefillTokens(refill),
            BurstCapacity(self.burst),
            RequestedTokens(requested),
        );
        self.last_refill_nanos += claimed;
        self.available = new_available.0;
        grant.0
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering::Relaxed},
            mpsc::RecvTimeoutError,
        },
        time::Duration,
    };

    use assert2::check;
    use krabka_units::prelude::{bytes, bytes_per_sec};
    use qubit_clock::ManualMonotonicClock;

    use super::*;

    /// Builds a bucket whose refill clock is a manual timeline starting at its
    /// own zero-duration origin, which is what the refill differences measure
    /// from.
    ///
    /// The function returns the bucket with the [`ManualMonotonicClock`]
    /// handle, so the test can advance logical time with `clock.advance(..)`
    /// instead of sleeping.
    fn manual_bucket() -> (Arc<TokenBucket>, Arc<ManualMonotonicClock>) {
        let clock = ManualMonotonicClock::new_shared();
        let bucket = Arc::new(TokenBucket::with_clock(clock.clone()));
        (bucket, clock)
    }

    const TRY_CONSUME_TIMEOUT: Duration = Duration::from_secs(2);

    fn try_consume_with_timeout(bucket: &Arc<TokenBucket>, requested: u64) -> u64 {
        let bucket = Arc::clone(bucket);
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let granted = bucket.try_consume(requested);
            let _ = tx.send(granted);
        });

        match rx.recv_timeout(TRY_CONSUME_TIMEOUT) {
            Ok(granted) => {
                handle.join().expect("try_consume worker panicked");
                granted
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(handle);
                panic!("try_consume({requested}) did not complete within {TRY_CONSUME_TIMEOUT:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                handle.join().expect("try_consume worker panicked");
                panic!("try_consume worker exited without sending a result");
            }
        }
    }

    #[test]
    fn zero_rate_grants_full_request() {
        let b = TokenBucket::new();
        assert2::assert!(b.try_consume(1024) == 1024);
    }

    #[test]
    fn first_consume_under_rate_succeeds() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate(bytes_per_sec(1024));
        assert2::assert!(try_consume_with_timeout(&b, 512) == 512);
    }

    #[test]
    fn independent_burst_can_exceed_rate() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate_with_burst(bytes_per_sec(100), bytes(1000));
        check!(
            (
                b.byte_rate(),
                b.byte_burst(),
                try_consume_with_timeout(&b, 500)
            ) == (bytes_per_sec(100), bytes(1000), 500)
        );
    }

    #[test]
    fn consume_drains_bucket() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate(bytes_per_sec(1024));
        assert2::assert!(try_consume_with_timeout(&b, 1024) == 1024);
        let g = try_consume_with_timeout(&b, 1024);
        assert2::assert!(g < 100);
    }

    #[test]
    fn bucket_refills_at_rate_after_elapsed_time() {
        let (b, clock) = manual_bucket();
        b.set_byte_rate(bytes_per_sec(1024));
        try_consume_with_timeout(&b, 1024);
        // 500ms at 1024 tokens/s refills exactly 512 tokens — deterministic,
        // where a real 500ms sleep only gets "roughly" 512 under scheduler jitter.
        clock
            .advance(Duration::from_millis(500))
            .expect("manual time moves forward");
        let g = try_consume_with_timeout(&b, 1024);
        assert2::assert!(g == 512);
    }

    /// A bucket polled faster than one token per interval still refills at its
    /// rate: the part of an interval that did not make a whole token carries
    /// over to the next call instead of being dropped.
    #[test]
    fn frequent_empty_polls_keep_the_partial_refill() {
        let (b, clock) = manual_bucket();
        b.set_token_rate_with_burst(1, 1);
        check!(try_consume_with_timeout(&b, 1) == 1);

        let mut grants = Vec::new();
        for _ in 0..10 {
            clock
                .advance(Duration::from_millis(100))
                .expect("manual time moves forward");
            grants.push(try_consume_with_timeout(&b, 1));
        }

        check!(grants == vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn bucket_caps_at_burst_capacity() {
        let (b, clock) = manual_bucket();
        b.set_byte_rate_with_burst(bytes_per_sec(1024), bytes(2048));
        try_consume_with_timeout(&b, 2048);
        // 2.5s at 1024 tokens/s would refill 2560 tokens, but the 2048 burst cap
        // clamps it; advancing logical time makes this exact and instant.
        clock
            .advance(Duration::from_millis(2500))
            .expect("manual time moves forward");
        let g = try_consume_with_timeout(&b, 4096);
        assert2::assert!(g == 2048);
    }

    #[test]
    fn set_rate_resets_available() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate(bytes_per_sec(1024));
        try_consume_with_timeout(&b, 1024);
        b.set_byte_rate(bytes_per_sec(2048));
        assert2::assert!(try_consume_with_timeout(&b, 2048) == 2048);
    }

    #[test]
    fn positive_rate_zero_burst_grants_zero() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));

        assert2::assert!(try_consume_with_timeout(&b, 1) == 0);
    }

    /// One whole consume step on the locked group, table-driven over the
    /// cases that differ only in the group, the clock, and the request.
    /// Each row pins the grant and the whole group after the step.
    #[test]
    fn consume_step_refills_caps_and_grants() {
        const SEC: u64 = 1_000_000_000;
        let group = |rate_per_sec, burst, available, last_refill_nanos| BucketState {
            rate_per_sec,
            burst,
            available,
            last_refill_nanos,
        };
        // (label, group before, now, requested, grant, group after)
        let cases = [
            (
                "rate 0 grants the request and leaves the group alone",
                group(0, 0, 0, 0),
                5 * SEC,
                7,
                7,
                group(0, 0, 0, 0),
            ),
            (
                "the whole elapsed second becomes tokens",
                group(10, 20, 0, 0),
                SEC,
                4,
                4,
                group(10, 20, 6, SEC),
            ),
            (
                "the refill is capped at the burst, and the time is still claimed",
                group(10, 20, 15, 0),
                SEC,
                0,
                0,
                group(10, 20, 20, SEC),
            ),
            (
                "a part-token remainder of the gap stays unclaimed",
                group(4, 10, 0, 0),
                SEC / 2 + SEC / 8,
                10,
                2,
                group(4, 10, 0, SEC / 2),
            ),
            (
                "a positive rate with a zero burst grants nothing",
                group(10, 0, 0, 0),
                SEC,
                3,
                0,
                group(10, 0, 0, SEC),
            ),
            (
                "a clock reading behind last_refill refills nothing",
                group(10, 20, 3, SEC),
                0,
                5,
                3,
                group(10, 20, 0, SEC),
            ),
        ];

        for (label, before, now, requested, grant, after) in cases {
            let mut state = before;
            let granted = state.consume(now, requested);
            check!((granted, state) == (grant, after), "{label}");
        }
    }

    /// A consume that arrives while a reset holds the group waits for it and
    /// then grants against the new configuration, never against a value it
    /// read before the reset.
    #[test]
    fn try_consume_waits_for_an_in_flight_reset() {
        let (b, clock) = manual_bucket();
        b.set_token_rate_with_burst(10, 10);
        clock
            .advance(Duration::from_secs(1))
            .expect("manual time moves forward");

        // Stand in for a reset part-way through its critical section: the lock
        // is held and the group is rewritten to a smaller burst.
        let mut in_flight_reset = b.lock_state();
        *in_flight_reset = BucketState {
            rate_per_sec: 10,
            burst: 3,
            available: 3,
            last_refill_nanos: 1_000_000_000,
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_bucket = Arc::clone(&b);
        let handle = std::thread::spawn(move || {
            let _ = tx.send(worker_bucket.try_consume(10));
        });
        match rx.recv_timeout(Duration::from_millis(50)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(granted) => panic!("try_consume granted {granted} during a reset"),
            Err(RecvTimeoutError::Disconnected) => {
                panic!("try_consume worker exited during a reset")
            }
        }

        drop(in_flight_reset);
        let granted = rx
            .recv_timeout(TRY_CONSUME_TIMEOUT)
            .expect("try_consume completes once the reset releases the group");
        handle.join().expect("try_consume worker panicked");
        check!(granted == 3);
    }

    /// Consumers racing each other never lose a refill one of them claimed.
    ///
    /// The bucket starts empty, and the manual clock advances by exactly one
    /// token's worth of time per step while the consumers poll. The burst is
    /// larger than the whole refill, so no token is capped away. Every refilled
    /// token must therefore end up granted: a consume that claimed time and
    /// then dropped its tokens would leave the total short.
    #[test]
    fn concurrent_consumers_never_lose_a_claimed_refill() {
        const STEPS: u64 = 2_000;
        const RATE: u64 = 1_000;
        let (b, clock) = manual_bucket();
        b.set_token_rate_with_burst(RATE, STEPS + 1);
        check!(try_consume_with_timeout(&b, STEPS + 1) == STEPS + 1);

        let stop = Arc::new(AtomicBool::new(false));
        let consumers: Vec<_> = (0..3)
            .map(|_| {
                let b = Arc::clone(&b);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut granted = 0;
                    while !stop.load(Relaxed) {
                        granted += b.try_consume(1);
                    }
                    granted
                })
            })
            .collect();

        for _ in 0..STEPS {
            clock
                .advance(Duration::from_millis(1))
                .expect("manual time moves forward");
            std::thread::yield_now();
        }
        stop.store(true, Relaxed);
        let raced: u64 = consumers
            .into_iter()
            .map(|h| h.join().expect("consumer panicked"))
            .sum();
        let leftover = try_consume_with_timeout(&b, u64::MAX);

        check!(raced + leftover == STEPS);
    }

    // Stress the reset path: consumers racing a stream of resets that shrink
    // and grow the burst must never see `available` above the current burst.
    // A reset straddled by a consume's read and commit would let that commit
    // store a balance computed under the old, larger burst. The real clock
    // runs, so consumes refill between resets, which the straddle needs.
    #[test]
    fn concurrent_set_rate_never_over_grants_past_burst() {
        // (rate, burst) pairs the resetter cycles through, largest burst first.
        const CONFIGS: [(u64, u64); 4] = [
            (1_000_000, 4_096),
            (1_000_000, 1_024),
            (3_000_000, 128),
            (500_000, 3),
        ];
        const MAX_BURST: u64 = CONFIGS[0].1;
        let b = Arc::new(TokenBucket::new());
        b.set_token_rate_with_burst(CONFIGS[0].0, CONFIGS[0].1);
        let stop = Arc::new(AtomicBool::new(false));

        let resetter = {
            let b = Arc::clone(&b);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                for (rate, burst) in CONFIGS.iter().cycle() {
                    if stop.load(Relaxed) {
                        break;
                    }
                    b.set_token_rate_with_burst(*rate, *burst);
                    std::thread::yield_now();
                }
            })
        };

        // The group's own invariant, sampled under its lock throughout.
        let observer = {
            let b = Arc::clone(&b);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut over_burst = None;
                while !stop.load(Relaxed) && over_burst.is_none() {
                    let state = *b.lock_state();
                    if state.available > state.burst {
                        over_burst = Some(state);
                    }
                }
                over_burst
            })
        };

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut consumer_handles = Vec::new();
        for requested in [0, 1, 128] {
            let b = Arc::clone(&b);
            let done_tx = done_tx.clone();
            consumer_handles.push(std::thread::spawn(move || {
                for _ in 0..5_000 {
                    let g = b.try_consume(requested);
                    if g > requested.min(MAX_BURST) {
                        let _ = done_tx.send(Err(g));
                        return;
                    }
                }
                let _ = done_tx.send(Ok(()));
            }));
        }
        drop(done_tx);

        let mut over_grant = None;
        let mut timed_out = false;
        for _ in 0..consumer_handles.len() {
            match done_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => {}
                Ok(Err(g)) => {
                    over_grant = Some(g);
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    timed_out = true;
                    break;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        stop.store(true, Relaxed);
        resetter.join().expect("resetter panicked");
        let over_burst = observer.join().expect("observer panicked");
        for h in consumer_handles {
            h.join().expect("consumer panicked");
        }

        check!(let None = over_grant, "a grant exceeded the request or the burst");
        check!(let None = over_burst, "available exceeded the burst");
        check!(!timed_out);
        let state = *b.lock_state();
        check!(state.available <= state.burst);
    }
}
