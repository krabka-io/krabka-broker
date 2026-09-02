//! [`TokenBucket::try_consume`]: the read side of the seqlock, which claims a
//! refill for the elapsed window and commits the new balance with a compare
//! and exchange.
//!
//! The loop re-reads the rate and the burst under a generation check, so a
//! concurrent reset from [`TokenBucket::set_token_rate_with_burst`] can never
//! apply non-atomically and a stale commit can never clobber it. The
//! arithmetic itself is the verified [`plan_consume`] kernel.

use std::sync::atomic::{Ordering, Ordering::Relaxed};

use krabka_verified::throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};

use super::TokenBucket;

impl TokenBucket {
    /// Tries to consume up to `requested` tokens.
    ///
    /// This method returns the amount actually granted. Rate-0 grants the full
    /// request.
    ///
    /// The method re-reads `rate` and `burst` inside the CAS loop under a
    /// seqlock generation check. A concurrent
    /// [`Self::set_token_rate_with_burst`] reset that straddles this call's
    /// refill-claim and CAS commit can thus never apply non-atomically. An odd
    /// or mismatched generation forces a retry. On retry, the method claims the
    /// refill gap again against the post-reset `last_refill`.
    /// # Panics
    /// Panics if validated compression or rate-limit state contains an impossible size or time value.
    pub fn try_consume(&self, requested: u64) -> u64 {
        if self.rate_per_sec.load(Relaxed) == 0 {
            return requested;
        }

        loop {
            // Read the seqlock generation; an odd value means a reset is in
            // flight, so spin until it is quiescent before sampling the group.
            let gen_before = self.generation.load(Relaxed);
            if gen_before & 1 != 0 {
                continue;
            }
            std::sync::atomic::fence(Ordering::Acquire);

            let rate = self.rate_per_sec.load(Relaxed);
            if rate == 0 {
                return requested;
            }
            let burst = self.burst.load(Relaxed);
            if burst == 0 {
                // Re-validate against a straddling reset before committing 0.
                if self.generation.load(Relaxed) != gen_before {
                    continue;
                }
                return 0;
            }

            let now = self.now_nanos();
            let last = self.last_refill_nanos.swap(now, Relaxed);
            let elapsed = now.saturating_sub(last);
            let refill = (u128::from(elapsed) * u128::from(rate)) / 1_000_000_000;
            let refill = u64::try_from(refill.min(u128::from(u64::MAX)))
                .expect("refill is capped at u64::MAX");

            let cur = self.available.load(Relaxed);
            let (grant, new_avail) = plan_consume(
                AvailableTokens(cur),
                RefillTokens(refill),
                BurstCapacity(burst),
                RequestedTokens(requested),
            );

            // Only commit if no reset straddled the read-compute window; the CAS
            // itself guards against a concurrent consumer mutating `available`.
            if self.generation.load(Relaxed) != gen_before {
                continue;
            }
            if self
                .available
                .compare_exchange_weak(cur, new_avail.0, Relaxed, Relaxed)
                .is_ok()
            {
                return grant.0;
            }
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
    use krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes, bytes_per_sec};
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

    #[test]
    fn try_consume_waits_while_generation_is_odd() {
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate(bytes_per_sec(4));
        b.generation.store(1, Relaxed);

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_bucket = Arc::clone(&b);
        let handle = std::thread::spawn(move || {
            let granted = worker_bucket.try_consume(1);
            let _ = tx.send(granted);
        });

        match rx.recv_timeout(Duration::from_millis(50)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(granted) => panic!("try_consume granted {granted} while generation was odd"),
            Err(RecvTimeoutError::Disconnected) => {
                handle.join().expect("try_consume worker panicked");
                panic!("try_consume worker exited while generation was odd");
            }
        }

        b.generation.store(2, Relaxed);
        let granted = rx
            .recv_timeout(TRY_CONSUME_TIMEOUT)
            .expect("try_consume should complete after generation becomes even");
        handle.join().expect("try_consume worker panicked");
        assert2::assert!(granted == 1);
    }

    // Stress the seqlock: many consumers racing a stream of set_rate resets must
    // never leave `available` above `burst` (the rate-change race the stateright
    // model in tests/bucket_model.rs proves bounded). A straddled reset that was
    // clobbered by a stale CAS would let `available` exceed the new burst here.
    #[test]
    fn concurrent_set_rate_never_over_grants_past_burst() {
        const BURST: ByteSize = bytes(4096);
        let b = Arc::new(TokenBucket::new());
        b.set_byte_rate_with_burst(bytes_per_sec(1024), BURST);
        let stop = Arc::new(AtomicBool::new(false));

        // Resetter: hammer set_rate_with_burst with the same burst cap.
        let resetter = {
            let b = Arc::clone(&b);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Relaxed) {
                    b.set_byte_rate_with_burst(bytes_per_sec(1024), BURST);
                    std::thread::yield_now();
                }
            })
        };

        // Consumers: drain small amounts and assert the grant never exceeds the
        // burst cap (an over-grant would mean a clobbered reset).
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut consumer_handles = Vec::new();
        for _ in 0..3 {
            let b = Arc::clone(&b);
            let done_tx = done_tx.clone();
            consumer_handles.push(std::thread::spawn(move || {
                for _ in 0..5_000 {
                    let g = b.try_consume(128);
                    if g > BURST.bytes_u64() {
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
        resetter.join().unwrap();

        if let Some(g) = over_grant {
            panic!("over-grant past burst: {g}");
        }
        assert2::assert!(!timed_out);
        for h in consumer_handles {
            h.join().unwrap();
        }

        // Invariant after the storm: available is within the burst cap.
        assert2::assert!(try_consume_with_timeout(&b, 0) == 0);
    }
}
