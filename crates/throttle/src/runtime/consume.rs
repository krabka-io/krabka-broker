//! [`TokenBucket::try_consume`]: turn the time elapsed since the last refill
//! into micro-tokens, cap them at the burst, and grant from the result, all in
//! one critical section. [`TokenBucket::record`] charges the same bucket
//! without a grant limit and leaves what the balance could not cover as debt.
//!
//! The arithmetic that caps and grants is the verified [`plan_consume`]
//! kernel, run over micro-token counts. [`BucketState::consume`] and
//! [`BucketState::record`] are the whole step on the locked group, so they can
//! be tested without threads.

use krabka_verified::throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};

use super::{BucketState, MICROS_PER_TOKEN, TokenBucket};

const NANOS_PER_SEC: u128 = 1_000_000_000;

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
    /// the bucket is not granted; it stays for a later consume. A bucket in
    /// debt from [`Self::record`] grants nothing. Rate-0 grants the full
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
        state
            .consume(now, requested)
            .map_or(requested, |micros| micros / MICROS_PER_TOKEN)
    }

    /// Charges `tokens` whole tokens whatever the bucket holds, and returns
    /// the debt the charge leaves, in micro-tokens, [`MICROS_PER_TOKEN`] to a
    /// token. The debt is `0` while the charge fits the balance.
    ///
    /// This is Kafka's `Sensor.record(value, timeMs, true)` on a quota's
    /// `Rate`: the value is recorded first and the quota is checked after, so
    /// an over-quota request is charged in full. The part the balance could
    /// not cover is debt, and the refill repays it before it adds to the
    /// balance, so a client that keeps sending above its quota is throttled by
    /// everything it has sent above the bound and not only by its latest
    /// request. A caller turns the debt into the throttle time `debt / rate`,
    /// which is Kafka's `QuotaUtils.throttleTime`.
    ///
    /// A rate-0 bucket has no limit, records nothing and reports no debt.
    ///
    /// # Panics
    /// Panics if the injected clock reads more than `u64::MAX` nanoseconds
    /// since its origin, about 584 years.
    pub fn record(&self, tokens: u64) -> u64 {
        if self.fast_path_rate() == 0 {
            return 0;
        }
        let mut state = self.lock_state();
        let now = self.now_nanos();
        state.record(now, tokens).unwrap_or(0)
    }

    /// Gives back `tokens` that an earlier charge took and the caller did not
    /// use, capped at the burst.
    ///
    /// This is Kafka's `ClientQuotaManager.unrecordQuotaSensor`: a fetch that
    /// is throttled sends no records, so the bytes it was charged come off
    /// the quota again. A rate-0 bucket holds no balance to give back to.
    pub fn refund(&self, tokens: u64) {
        self.refund_micros(tokens.saturating_mul(MICROS_PER_TOKEN));
    }

    /// Gives back `micros` micro-tokens that an earlier charge took, capped at
    /// the burst. Debt is repaid first, so refunding a [`Self::record`] in
    /// full restores the balance it started from.
    pub fn refund_micros(&self, micros: u64) {
        if micros == 0 {
            return;
        }
        self.lock_state().refund(micros);
    }
}

/// What the time up to a clock reading makes of a bucket: the balance after
/// the refill, and the elapsed time the refill accounts for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Refilled {
    /// Micro-tokens the bucket can grant, refill added and capped at the
    /// burst.
    pub(super) available: u64,
    /// Debt the refill did not repay.
    pub(super) debt: u64,
    /// The elapsed time, in nanoseconds, that became micro-tokens.
    claimed_nanos: u64,
}

impl BucketState {
    /// Turns the time elapsed up to `now` into micro-tokens: the refill
    /// repays the debt first, then adds to the balance, capped at the burst.
    ///
    /// Only the time the whole refilled micro-tokens account for is claimed.
    /// The remainder stays unclaimed for the next call, so a caller that
    /// retries faster than one micro-token per interval still sees the bucket
    /// refill at `rate`. Claiming the whole gap would drop that remainder on
    /// every call, and a bucket polled often enough would never refill at all.
    ///
    /// The bucket must have a rate. Nothing is stored: the caller stores the
    /// result together with the rest of its step.
    pub(super) fn refilled(&self, now: u64) -> Refilled {
        let rate = self.micro_rate_per_sec;
        let elapsed = now.saturating_sub(self.last_refill_nanos);
        let refill = u128::from(elapsed) * u128::from(rate) / NANOS_PER_SEC;
        let refill =
            u64::try_from(refill.min(u128::from(u64::MAX))).expect("refill is capped at u64::MAX");
        let claimed_nanos = u64::try_from(
            (u128::from(refill) * NANOS_PER_SEC / u128::from(rate)).min(u128::from(elapsed)),
        )
        .expect("the claimed time is at most the elapsed time");
        let repaid = refill.min(self.micro_debt);
        let (_, available) = plan_consume(
            AvailableTokens(self.micro_available),
            RefillTokens(refill - repaid),
            BurstCapacity(self.micro_burst),
            RequestedTokens(0),
        );
        Refilled {
            available: available.0,
            debt: self.micro_debt - repaid,
            claimed_nanos,
        }
    }

    /// Stores a refilled balance, the balance left after the step took its
    /// grant, and the debt the step leaves.
    fn store(&mut self, refilled: Refilled, new_available: u64, debt: u64) {
        self.last_refill_nanos += refilled.claimed_nanos;
        self.micro_available = new_available;
        self.micro_debt = debt;
    }

    /// Refills the bucket for the time elapsed up to `now`, then grants up to
    /// `requested` whole tokens from it, and returns the grant in
    /// micro-tokens, or `None` when the bucket has no limit.
    ///
    /// Every value is computed before the first store, so a panic cannot leave
    /// the group half updated.
    fn consume(&mut self, now: u64, requested: u64) -> Option<u64> {
        if self.micro_rate_per_sec == 0 {
            return None;
        }
        let refilled = self.refilled(now);
        let (grant, new_available) = plan_consume(
            AvailableTokens(refilled.available),
            RefillTokens(0),
            BurstCapacity(self.micro_burst),
            RequestedTokens(whole_token_request(
                requested,
                refilled.available,
                MICROS_PER_TOKEN,
            )),
        );
        self.store(refilled, new_available.0, refilled.debt);
        Some(grant.0)
    }

    /// Refills the bucket for the time elapsed up to `now`, then charges
    /// `tokens` whole tokens: the balance pays what it can, and the rest is
    /// debt. Returns the debt after the charge in micro-tokens, or `None` when
    /// the bucket has no limit.
    fn record(&mut self, now: u64, tokens: u64) -> Option<u64> {
        if self.micro_rate_per_sec == 0 {
            return None;
        }
        let refilled = self.refilled(now);
        let request = tokens.saturating_mul(MICROS_PER_TOKEN);
        let (grant, new_available) = plan_consume(
            AvailableTokens(refilled.available),
            RefillTokens(0),
            BurstCapacity(self.micro_burst),
            RequestedTokens(request),
        );
        let debt = refilled.debt.saturating_add(request - grant.0);
        self.store(refilled, new_available.0, debt);
        Some(debt)
    }

    /// Adds `micros` back to the balance, capped at the burst, after it has
    /// repaid the debt. A rate-0 bucket is unthrottled and keeps no balance,
    /// so it is left alone.
    fn refund(&mut self, micros: u64) {
        if self.micro_rate_per_sec != 0 {
            let repaid = micros.min(self.micro_debt);
            self.micro_debt -= repaid;
            self.micro_available = self
                .micro_available
                .saturating_add(micros - repaid)
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

    /// A rate change keeps the balance, as Kafka's
    /// `ClientQuotaManager.updateQuotaMetricConfigs` swaps a metric's bound
    /// and leaves what it recorded (#1241). Each row drives one bucket
    /// through a consume, an optional wait, a change of rate and burst, and a
    /// consume that asks for more than the bucket can hold. The last grant is
    /// the balance that survived the change.
    #[test]
    fn a_rate_change_keeps_the_balance() {
        // (label, first rate/burst, spent, wait in ms, second rate/burst,
        // grant after the change)
        let cases = [
            (
                "a drained bucket stays drained when its rate goes up",
                (1_000, 1_000),
                1_000,
                0,
                (2_000, 2_000),
                0,
            ),
            (
                "a wait refills at the old rate and the balance is kept",
                (1_000, 1_000),
                1_000,
                500,
                (2_000, 2_000),
                500,
            ),
            (
                "the balance is capped at the new burst",
                (1_000, 1_000),
                0,
                0,
                (2_000, 300),
                300,
            ),
            (
                "a balance under the new burst is kept as it is",
                (1_000, 1_000),
                600,
                0,
                (2_000, 2_000),
                400,
            ),
            (
                "the first rate of a bucket starts it full",
                (0, 0),
                0,
                0,
                (1_000, 1_000),
                1_000,
            ),
            (
                "a bucket that was unlimited starts full at its next rate",
                (0, 0),
                5_000,
                0,
                (1_000, 700),
                700,
            ),
        ];
        for (label, (rate, burst), spent, wait_ms, (new_rate, new_burst), want) in cases {
            let (b, clock) = manual_bucket();
            b.set_token_rate_with_burst(rate, burst);
            b.try_consume(spent);
            clock
                .advance(Duration::from_millis(wait_ms))
                .expect("manual time moves forward");
            b.set_token_rate_with_burst(new_rate, new_burst);
            check!(b.try_consume(u64::MAX) == want, "{label}");
        }
    }

    /// A bucket in debt stays in debt across a change of its rate: the client
    /// is still throttled, now by `debt / new rate`.
    #[test]
    fn a_rate_change_keeps_the_debt() {
        let (b, _clock) = manual_bucket();
        b.set_token_rate_with_burst(1_000, 1_000);
        check!(b.record(3_000) == 2_000 * MICROS_PER_TOKEN);

        b.set_token_rate_with_burst(2_000, 2_000);

        check!((b.try_consume(1), b.record(0)) == (0, 2_000 * MICROS_PER_TOKEN));
    }

    #[test]
    fn positive_rate_zero_burst_grants_zero() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));

        assert2::assert!(try_consume_with_timeout(&b, 1) == 0);
    }

    /// One whole-token consume step on the locked group, table-driven over the
    /// cases that differ only in the group, the clock and the request. Each
    /// row pins the grant and the whole group after the step. The group is in
    /// micro-tokens and the request in whole tokens.
    #[test]
    fn consume_step_refills_caps_and_grants() {
        const SEC: u64 = 1_000_000_000;
        const M: u64 = MICROS_PER_TOKEN;
        let group = |rate, burst, available, debt, last_refill| BucketState {
            micro_rate_per_sec: rate,
            micro_burst: burst,
            micro_available: available,
            micro_debt: debt,
            last_refill_nanos: last_refill,
        };
        // (label, group before, now, requested, grant, group after)
        let cases = [
            (
                "rate 0 grants the request and leaves the group alone",
                group(0, 0, 0, 0, 0),
                5 * SEC,
                7,
                None,
                group(0, 0, 0, 0, 0),
            ),
            (
                "the whole elapsed second becomes tokens",
                group(10 * M, 20 * M, 0, 0, 0),
                SEC,
                4,
                Some(4 * M),
                group(10 * M, 20 * M, 6 * M, 0, SEC),
            ),
            (
                "the refill is capped at the burst, and the time is still claimed",
                group(10 * M, 20 * M, 15 * M, 0, 0),
                SEC,
                0,
                Some(0),
                group(10 * M, 20 * M, 20 * M, 0, SEC),
            ),
            (
                "a part-micro-token remainder of the gap stays unclaimed",
                group(4, 10 * M, 0, 0, 0),
                SEC / 2 + SEC / 8,
                10,
                Some(0),
                group(4, 10 * M, 2, 0, SEC / 2),
            ),
            (
                "a positive rate with a zero burst grants nothing",
                group(10 * M, 0, 0, 0, 0),
                SEC,
                3,
                Some(0),
                group(10 * M, 0, 0, 0, SEC),
            ),
            (
                "a clock reading behind last_refill refills nothing",
                group(10 * M, 20 * M, 3 * M, 0, SEC),
                0,
                5,
                Some(3 * M),
                group(10 * M, 20 * M, 0, 0, SEC),
            ),
            (
                "a whole-token consume leaves half a token in the bucket",
                group(M / 2, M, 0, 0, 0),
                SEC,
                1,
                Some(0),
                group(M / 2, M, M / 2, 0, SEC),
            ),
            (
                "two seconds at half a token per second make one whole token",
                group(M / 2, M, 0, 0, 0),
                2 * SEC,
                1,
                Some(M),
                group(M / 2, M, 0, 0, 2 * SEC),
            ),
            (
                "a whole-token consume grants the whole tokens of a part balance",
                group(M, 3 * M, 2 * M + M / 4, 0, 0),
                0,
                5,
                Some(2 * M),
                group(M, 3 * M, M / 4, 0, 0),
            ),
            (
                "a bucket in debt grants nothing and repays from the refill",
                group(10 * M, 20 * M, 0, 8 * M, 0),
                SEC / 2,
                3,
                Some(0),
                group(10 * M, 20 * M, 0, 3 * M, SEC / 2),
            ),
            (
                "a refill past the debt goes to the balance",
                group(10 * M, 20 * M, 0, 4 * M, 0),
                SEC,
                3,
                Some(3 * M),
                group(10 * M, 20 * M, 3 * M, 0, SEC),
            ),
        ];

        for (label, before, now, requested, grant, after) in cases {
            let mut state = before;
            let granted = state.consume(now, requested);
            check!((granted, state) == (grant, after), "{label}");
        }
    }

    /// One `record` step on the locked group: the charge is taken in full, the
    /// balance pays what it can, and the debt is what it could not. Each row
    /// pins the debt reported and the whole group after the step.
    #[test]
    fn record_step_charges_in_full_and_leaves_the_rest_as_debt() {
        const SEC: u64 = 1_000_000_000;
        const M: u64 = MICROS_PER_TOKEN;
        let group = |rate, burst, available, debt, last_refill| BucketState {
            micro_rate_per_sec: rate,
            micro_burst: burst,
            micro_available: available,
            micro_debt: debt,
            last_refill_nanos: last_refill,
        };
        // (label, group before, now, tokens charged, debt reported, group after)
        let cases = [
            (
                "rate 0 records nothing",
                group(0, 0, 0, 0, 0),
                5 * SEC,
                7,
                None,
                group(0, 0, 0, 0, 0),
            ),
            (
                "a charge inside the balance leaves no debt",
                group(10 * M, 20 * M, 20 * M, 0, 0),
                0,
                12,
                Some(0),
                group(10 * M, 20 * M, 8 * M, 0, 0),
            ),
            (
                "a charge past the balance is debt",
                group(10 * M, 20 * M, 5 * M, 0, 0),
                0,
                12,
                Some(7 * M),
                group(10 * M, 20 * M, 0, 7 * M, 0),
            ),
            (
                "a second charge adds to the debt the first left",
                group(10 * M, 20 * M, 0, 7 * M, 0),
                0,
                3,
                Some(10 * M),
                group(10 * M, 20 * M, 0, 10 * M, 0),
            ),
            (
                "the refill repays debt before the charge",
                group(10 * M, 20 * M, 0, 7 * M, 0),
                SEC,
                4,
                Some(M),
                group(10 * M, 20 * M, 0, M, SEC),
            ),
            (
                "a part-micro-token remainder of the gap stays unclaimed",
                group(4, 10 * M, 0, 0, 0),
                SEC / 2 + SEC / 8,
                10,
                Some(10 * M - 2),
                group(4, 10 * M, 0, 10 * M - 2, SEC / 2),
            ),
            (
                "half a token per second owes half a token for a token",
                group(M / 2, M, M / 2, 0, 0),
                0,
                1,
                Some(M / 2),
                group(M / 2, M, 0, M / 2, 0),
            ),
            (
                "the debt saturates instead of wrapping",
                group(M, M, 0, u64::MAX - 5, 0),
                0,
                1,
                Some(u64::MAX),
                group(M, M, 0, u64::MAX, 0),
            ),
        ];

        for (label, before, now, tokens, debt, after) in cases {
            let mut state = before;
            let reported = state.record(now, tokens);
            check!((reported, state) == (debt, after), "{label}");
        }
    }

    /// A bucket at half a token per second admits half a token per second:
    /// a caller asking for one token every second is granted one every other
    /// second, and a charge in between owes the half token it lacks.
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
        let debt = b.record(1);

        check!((whole, debt) == (vec![1, 0, 1, 0, 1], MICROS_PER_TOKEN / 2));
    }

    /// The overage of a charge is kept, not credited back: a bucket driven at
    /// twice its rate by charges of a constant size owes more with every
    /// charge, and a consume finds it empty however long the last mute was.
    /// Before #1212 the bucket granted only what it held, so the refill of
    /// the mute paid the overage and the client sent about twice its quota.
    #[test]
    fn charges_above_the_rate_accumulate_as_debt() {
        let (b, clock) = manual_bucket();
        b.set_token_rate_with_burst(1_000, 1_000);
        let mut debts = Vec::new();
        for _ in 0..4 {
            debts.push(b.record(2_000) / MICROS_PER_TOKEN);
            clock
                .advance(Duration::from_secs(1))
                .expect("manual time moves forward");
        }

        // The first charge is 1000 over the balance. Each later second's
        // refill repays 1000 of the debt, and the next charge adds 2000.
        check!(debts == vec![1_000, 2_000, 3_000, 4_000]);
        check!(b.try_consume(1) == 0);
    }

    /// A debt is repaid by the refill and by a refund, and both come before
    /// the balance.
    #[test]
    fn refill_and_refund_repay_the_debt_before_the_balance() {
        let (b, clock) = manual_bucket();
        b.set_token_rate_with_burst(100, 100);
        check!(b.record(160) == 60 * MICROS_PER_TOKEN);

        // Refunding the whole charge gives back the balance it started from.
        b.refund(160);
        check!(b.record(0) == 0);
        check!(b.try_consume(100) == 100);

        // A second's refill of 100 repays a debt of 30 and leaves 70.
        check!(b.record(30) == 30 * MICROS_PER_TOKEN);
        clock
            .advance(Duration::from_secs(1))
            .expect("manual time moves forward");
        check!(b.try_consume(1_000) == 70);
    }

    /// A charge on an unlimited bucket records nothing, whatever its size.
    #[test]
    fn an_unlimited_bucket_records_no_debt() {
        let b = TokenBucket::new();
        check!((b.record(u64::MAX), b.try_consume(5)) == (0, 5));
    }

    /// A part-token charge given back is available again, capped at the
    /// burst, as a whole-token refund is.
    #[test]
    fn refund_micros_returns_a_part_token() {
        let (b, _clock) = manual_bucket();
        b.set_byte_rate_with_burst(
            ByteRate::from_bytes_per_sec_f64(0.25),
            ByteSize::from_bytes_f64(0.75),
        );
        let debt = b.record(1);
        b.refund_micros(MICROS_PER_TOKEN);
        let after_refund = b.record(0);
        b.refund_micros(u64::MAX);
        let capped = b.record(5);

        check!(
            (debt, after_refund, capped)
                == (
                    MICROS_PER_TOKEN / 4,
                    0,
                    5 * MICROS_PER_TOKEN - MICROS_PER_TOKEN * 3 / 4
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
            micro_debt: 0,
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
