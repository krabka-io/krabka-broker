//! [`TokenBucket::try_consume`]: turn the time elapsed since the last refill
//! into micro-tokens, cap them at the burst, and grant from the result, all in
//! one critical section.
//!
//! The arithmetic that caps and grants is the verified [`plan_consume`]
//! kernel, run over micro-token counts. [`BucketState::consume`] is the whole
//! step on the locked group, so it can be tested without threads.

use krabka_verified::throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};

use super::{BucketState, MICROS_PER_TOKEN, TokenBucket};

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// How much of a part token a consume may grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grain {
    /// Whole tokens only. A part token stays in the bucket.
    WholeTokens,
    /// Micro-tokens: when the bucket holds less than the request, the grant
    /// is everything it holds, part token included.
    MicroTokens,
}

/// The micro-token request a whole-token consume hands to [`plan_consume`].
///
/// `requested` is in whole tokens and `total` is the capped balance in
/// storage units, `units_per_token` to a token. The result is the request in
/// storage units, cut down to the whole tokens `total` holds, so the grant
/// [`plan_consume`] makes from it is a whole number of tokens and a part
/// token stays in the bucket.
///
/// Production passes [`MICROS_PER_TOKEN`]. The stateright model in
/// `tests/bucket_model.rs` drives this same function with a small
/// `units_per_token`, which keeps its search small.
///
/// # Panics
/// Panics if `units_per_token` is zero.
#[must_use]
pub fn whole_token_request(requested: u64, total: u64, units_per_token: u64) -> u64 {
    requested
        .saturating_mul(units_per_token)
        .min(total - total % units_per_token)
}

impl TokenBucket {
    /// Tries to consume up to `requested` whole tokens.
    ///
    /// This method returns the whole tokens actually granted. A part token in
    /// the bucket is not granted; it stays for a later consume. Rate-0 grants
    /// the full request.
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
        state
            .consume(now, requested, Grain::WholeTokens)
            .map_or(requested, |micros| micros / MICROS_PER_TOKEN)
    }

    /// Tries to consume up to `requested` whole tokens and returns the grant
    /// in micro-tokens, [`MICROS_PER_TOKEN`] to a token.
    ///
    /// Unlike [`Self::try_consume`], a bucket that holds less than the request
    /// grants everything it holds, part token included. A quota caller turns
    /// the rest of the request into a throttle delay at the configured rate,
    /// so that delay is the exact shortfall even under a rate of a fraction
    /// of a token per second. Rate-0 grants the full request.
    ///
    /// # Panics
    /// Panics if the injected clock reads more than `u64::MAX` nanoseconds
    /// since its origin, about 584 years.
    pub fn try_consume_micros(&self, requested: u64) -> u64 {
        let unlimited = requested.saturating_mul(MICROS_PER_TOKEN);
        if self.fast_path_rate() == 0 {
            return unlimited;
        }
        let mut state = self.lock_state();
        let now = self.now_nanos();
        state
            .consume(now, requested, Grain::MicroTokens)
            .unwrap_or(unlimited)
    }

    /// Gives back `tokens` that an earlier [`Self::try_consume`] granted and
    /// the caller did not use, capped at the burst.
    ///
    /// This is Kafka's `ClientQuotaManager.unrecordQuotaSensor`: a fetch that
    /// is throttled sends no records, so the bytes it was charged come off
    /// the quota again. A rate-0 bucket holds no balance to give back to.
    pub fn refund(&self, tokens: u64) {
        self.refund_micros(tokens.saturating_mul(MICROS_PER_TOKEN));
    }

    /// Gives back `micros` micro-tokens that an earlier
    /// [`Self::try_consume_micros`] granted, capped at the burst.
    pub fn refund_micros(&self, micros: u64) {
        if micros == 0 {
            return;
        }
        self.lock_state().refund(micros);
    }
}

impl BucketState {
    /// Refills the bucket for the time elapsed up to `now`, then grants up to
    /// `requested` whole tokens from it at `grain`, and returns the grant in
    /// micro-tokens, or `None` when the bucket has no limit.
    ///
    /// Only the time the whole refilled micro-tokens account for is claimed.
    /// The remainder stays unclaimed for the next call, so a caller that
    /// retries faster than one micro-token per interval still sees the bucket
    /// refill at `rate`. Claiming the whole gap would drop that remainder on
    /// every call, and a bucket polled often enough would never refill at all.
    ///
    /// Every value is computed before the first store, so a panic cannot leave
    /// the group half updated.
    fn consume(&mut self, now: u64, requested: u64, grain: Grain) -> Option<u64> {
        let rate = self.micro_rate_per_sec;
        if rate == 0 {
            return None;
        }
        let elapsed = now.saturating_sub(self.last_refill_nanos);
        let refill = u128::from(elapsed) * u128::from(rate) / NANOS_PER_SEC;
        let refill =
            u64::try_from(refill.min(u128::from(u64::MAX))).expect("refill is capped at u64::MAX");
        let claimed = u64::try_from(
            (u128::from(refill) * NANOS_PER_SEC / u128::from(rate)).min(u128::from(elapsed)),
        )
        .expect("the claimed time is at most the elapsed time");
        let burst = BurstCapacity(self.micro_burst);
        let (_, total) = plan_consume(
            AvailableTokens(self.micro_available),
            RefillTokens(refill),
            burst,
            RequestedTokens(0),
        );
        let request = match grain {
            Grain::WholeTokens => whole_token_request(requested, total.0, MICROS_PER_TOKEN),
            Grain::MicroTokens => requested.saturating_mul(MICROS_PER_TOKEN),
        };
        let (grant, new_available) = plan_consume(
            AvailableTokens(total.0),
            RefillTokens(0),
            burst,
            RequestedTokens(request),
        );
        self.last_refill_nanos += claimed;
        self.micro_available = new_available.0;
        Some(grant.0)
    }

    /// Adds `micros` back to the balance, capped at the burst. A rate-0
    /// bucket is unthrottled and keeps no balance, so it is left alone.
    fn refund(&mut self, micros: u64) {
        if self.micro_rate_per_sec != 0 {
            self.micro_available = self
                .micro_available
                .saturating_add(micros)
                .min(self.micro_burst);
        }
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
    use krabka_units::prelude::{
        ByteRate, ByteRateExt as _, ByteSize, ByteSizeExt as _, bytes, bytes_per_sec,
    };
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

    /// A refund gives back what a consume took, and never more than the
    /// burst: consuming 600 of a 1000-token burst and refunding it leaves the
    /// whole burst; a second refund cannot push past it.
    #[test]
    fn refund_returns_granted_tokens_up_to_the_burst() {
        let (b, _clock) = manual_bucket();
        b.set_byte_rate_with_burst(bytes_per_sec(100), bytes(1000));
        let granted = try_consume_with_timeout(&b, 600);
        b.refund(granted);
        let after_one_refund = try_consume_with_timeout(&b, 1000);
        b.refund(after_one_refund);
        b.refund(500);
        let after_overflowing_refund = try_consume_with_timeout(&b, 2000);
        check!((granted, after_one_refund, after_overflowing_refund) == (600, 1000, 1000));
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
    /// cases that differ only in the group, the clock, the request and the
    /// grain. Each row pins the grant and the whole group after the step.
    /// The group is in micro-tokens and the request in whole tokens.
    #[test]
    fn consume_step_refills_caps_and_grants() {
        const SEC: u64 = 1_000_000_000;
        const M: u64 = MICROS_PER_TOKEN;
        let group =
            |micro_rate_per_sec, micro_burst, micro_available, last_refill_nanos| BucketState {
                micro_rate_per_sec,
                micro_burst,
                micro_available,
                last_refill_nanos,
            };
        let whole = Grain::WholeTokens;
        let micro = Grain::MicroTokens;
        // (label, group before, now, requested, grain, grant, group after)
        let cases = [
            (
                "rate 0 grants the request and leaves the group alone",
                group(0, 0, 0, 0),
                5 * SEC,
                7,
                whole,
                None,
                group(0, 0, 0, 0),
            ),
            (
                "the whole elapsed second becomes tokens",
                group(10 * M, 20 * M, 0, 0),
                SEC,
                4,
                whole,
                Some(4 * M),
                group(10 * M, 20 * M, 6 * M, SEC),
            ),
            (
                "the refill is capped at the burst, and the time is still claimed",
                group(10 * M, 20 * M, 15 * M, 0),
                SEC,
                0,
                whole,
                Some(0),
                group(10 * M, 20 * M, 20 * M, SEC),
            ),
            (
                "a part-micro-token remainder of the gap stays unclaimed",
                group(4, 10, 0, 0),
                SEC / 2 + SEC / 8,
                10,
                micro,
                Some(2),
                group(4, 10, 0, SEC / 2),
            ),
            (
                "a positive rate with a zero burst grants nothing",
                group(10 * M, 0, 0, 0),
                SEC,
                3,
                whole,
                Some(0),
                group(10 * M, 0, 0, SEC),
            ),
            (
                "a clock reading behind last_refill refills nothing",
                group(10 * M, 20 * M, 3 * M, SEC),
                0,
                5,
                whole,
                Some(3 * M),
                group(10 * M, 20 * M, 0, SEC),
            ),
            (
                "half a token per second refills half a token in a second",
                group(M / 2, M, 0, 0),
                SEC,
                1,
                micro,
                Some(M / 2),
                group(M / 2, M, 0, SEC),
            ),
            (
                "a whole-token consume leaves half a token in the bucket",
                group(M / 2, M, 0, 0),
                SEC,
                1,
                whole,
                Some(0),
                group(M / 2, M, M / 2, SEC),
            ),
            (
                "two seconds at half a token per second make one whole token",
                group(M / 2, M, 0, 0),
                2 * SEC,
                1,
                whole,
                Some(M),
                group(M / 2, M, 0, 2 * SEC),
            ),
            (
                "a whole-token consume grants the whole tokens of a part balance",
                group(M, 3 * M, 2 * M + M / 4, 0),
                0,
                5,
                whole,
                Some(2 * M),
                group(M, 3 * M, M / 4, 0),
            ),
            (
                "a micro-token consume within the balance grants the request",
                group(M, 3 * M, 2 * M + M / 4, 0),
                0,
                1,
                micro,
                Some(M),
                group(M, 3 * M, M + M / 4, 0),
            ),
        ];

        for (label, before, now, requested, grain, grant, after) in cases {
            let mut state = before;
            let granted = state.consume(now, requested, grain);
            check!((granted, state) == (grant, after), "{label}");
        }
    }

    /// A bucket at half a token per second admits half a token per second:
    /// a caller asking for one token every second is granted one every other
    /// second, and the micro-token grant sees the half token in between.
    #[test]
    fn a_fractional_rate_is_enforced_at_its_rate() {
        let (b, clock) = manual_bucket();
        b.set_byte_rate_with_burst(ByteRate::from_bytes_per_sec_f64(0.5), bytes(1));
        let mut whole = Vec::new();
        for _ in 0..5 {
            whole.push(try_consume_with_timeout(&b, 1));
            clock
                .advance(Duration::from_secs(1))
                .expect("manual time moves forward");
        }
        let micros = b.try_consume_micros(1);

        check!((whole, micros) == (vec![1, 0, 1, 0, 1], MICROS_PER_TOKEN / 2));
    }

    /// A micro-token grant given back is available again, capped at the
    /// burst, as a whole-token refund is.
    #[test]
    fn refund_micros_returns_a_part_token() {
        let (b, _clock) = manual_bucket();
        b.set_byte_rate_with_burst(
            ByteRate::from_bytes_per_sec_f64(0.25),
            ByteSize::from_bytes_f64(0.75),
        );
        let granted = b.try_consume_micros(1);
        b.refund_micros(granted);
        let again = b.try_consume_micros(1);
        b.refund_micros(u64::MAX);
        let capped = b.try_consume_micros(5);

        check!(
            (granted, again, capped)
                == (
                    MICROS_PER_TOKEN * 3 / 4,
                    MICROS_PER_TOKEN * 3 / 4,
                    MICROS_PER_TOKEN * 3 / 4
                )
        );
    }

    #[test]
    fn whole_token_request_cuts_to_the_whole_tokens_held() {
        // (requested tokens, total units, units per token, request in units)
        let cases = [
            (0, 5, 2, 0),
            (1, 1, 2, 0),
            (1, 3, 2, 2),
            (3, 5, 2, 4),
            (1, 5, 2, 2),
            (u64::MAX, 7, 1, 7),
            (u64::MAX, 2_500_000, MICROS_PER_TOKEN, 2_000_000),
        ];
        for (requested, total, units, want) in cases {
            check!(
                whole_token_request(requested, total, units) == want,
                "{requested} of {total} at {units}"
            );
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
            micro_rate_per_sec: 10 * MICROS_PER_TOKEN,
            micro_burst: 3 * MICROS_PER_TOKEN,
            micro_available: 3 * MICROS_PER_TOKEN,
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
                    if state.micro_available > state.micro_burst {
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
        check!(state.micro_available <= state.micro_burst);
    }
}
