//! The rate and burst side of [`TokenBucket`]: the typed setters, the
//! accessors that read the configuration back, and the seqlock write section
//! that publishes the `{rate, burst, available, last_refill}` group as one
//! unit.
//!
//! The bucket stores raw tokens, so every dimensioned quantity narrows here.
//! The byte pair and the event pair stay separate because a token means a
//! different thing in each.

use std::sync::atomic::{Ordering, Ordering::Relaxed};

use krabka_units::prelude::{
    ByteRate, ByteRateExt as _, ByteSize, ByteSizeExt as _, Frequency, FrequencyExt as _, Time,
    secs,
};

use super::TokenBucket;

/// The time window that [`TokenBucket::set_byte_rate`] uses for the burst
/// capacity when the caller does not give one. The burst is the throughput of
/// this window.
const DEFAULT_BURST_WINDOW: Time = secs(1);

/// A throughput in the bucket's raw storage unit: whole bytes per second.
///
/// The bucket stores its rate in an `AtomicU64` because the refill arithmetic is
/// verified over integers. Every rate that crosses the accessor boundary
/// narrows here. A negative rate is not a throughput, so it becomes `0`, the
/// bucket's "no limit configured" sentinel.
fn rate_to_bytes_per_sec(rate: ByteRate) -> u64 {
    u64::try_from(rate.bytes_per_sec_i64()).unwrap_or(0)
}

/// The inverse of [`rate_to_bytes_per_sec`].
///
/// This is exact for every value the bucket can hold. A stored rate came
/// through [`rate_to_bytes_per_sec`], which saturates at `i64::MAX`.
fn rate_from_bytes_per_sec(raw: u64) -> ByteRate {
    ByteRate::from_bytes_per_sec(i64::try_from(raw).unwrap_or(i64::MAX))
}

impl TokenBucket {
    /// Updates the rate in raw tokens per second.
    ///
    /// This method resets `available` to a one-second burst at the new rate.
    ///
    /// This is the primitive. The bucket counts tokens and does not know what a
    /// token means. Callers that meter a dimensioned quantity should use the
    /// typed pair that names the dimension: [`Self::set_byte_rate`] or
    /// [`Self::set_event_rate`].
    pub fn set_token_rate(&self, tokens_per_sec: u64) {
        self.set_token_rate_with_burst(tokens_per_sec, tokens_per_sec);
    }

    /// Updates the rate and the independent burst capacity, both in raw tokens.
    ///
    /// This method publishes the `{rate, burst, available, last_refill}` group
    /// as one seqlock critical section. It moves `generation` to an odd value
    /// before the stores and to the next even value after them. A concurrent
    /// `try_consume` that straddles the reset must thus try again, and it
    /// cannot clobber the new `available` with a stale CAS.
    pub fn set_token_rate_with_burst(&self, new_rate: u64, burst: u64) {
        let _writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Enter the write section (generation becomes odd).
        let gen_start = self.generation.fetch_add(1, Relaxed);
        // Release fence so the group stores below cannot be reordered before the
        // odd-generation publish (pairs with the consumer's Acquire fence).
        std::sync::atomic::fence(Ordering::Release);
        self.rate_per_sec.store(new_rate, Relaxed);
        self.burst.store(burst, Relaxed);
        self.available.store(burst, Relaxed);
        self.last_refill_nanos.store(self.now_nanos(), Relaxed);
        std::sync::atomic::fence(Ordering::Release);
        // Leave the write section (generation becomes even again, advanced by 2
        // total so any straddling reader sees a changed generation).
        self.generation.store(gen_start.wrapping_add(2), Relaxed);
    }

    /// The configured rate in raw tokens per second. `0` means no limit.
    #[must_use]
    pub fn token_rate(&self) -> u64 {
        self.rate_per_sec.load(Relaxed)
    }

    /// The configured burst capacity in raw tokens. This is the most the bucket
    /// holds.
    #[must_use]
    pub fn token_burst(&self) -> u64 {
        self.burst.load(Relaxed)
    }

    /// Updates a byte throughput and bursts one second's worth.
    ///
    /// The burst is `rate * DEFAULT_BURST_WINDOW`. `uom` type-checks this as a
    /// [`ByteRate`] times a [`Time`], which gives a [`ByteSize`].
    pub fn set_byte_rate(&self, new_rate: ByteRate) {
        self.set_byte_rate_with_burst(new_rate, (new_rate * DEFAULT_BURST_WINDOW).into());
    }

    /// Updates a byte throughput and an independent byte burst capacity.
    pub fn set_byte_rate_with_burst(&self, new_rate: ByteRate, burst: ByteSize) {
        self.set_token_rate_with_burst(rate_to_bytes_per_sec(new_rate), burst.bytes_u64());
    }

    /// The configured byte throughput.
    /// [`krabka_units::prelude::ByteRateExt::ZERO`] means no limit.
    #[must_use]
    pub fn byte_rate(&self) -> ByteRate {
        rate_from_bytes_per_sec(self.token_rate())
    }

    /// The configured byte burst capacity.
    #[must_use]
    pub fn byte_burst(&self) -> ByteSize {
        ByteSize::from_bytes(self.token_burst())
    }

    /// Updates an event throughput, such as samples, records, or requests, and
    /// bursts one second's worth.
    ///
    /// A token here is one event, not one byte. If you meter events with the
    /// byte pair above, the code compiles but the result is wrong. This is why
    /// the two pairs are separate.
    pub fn set_event_rate(&self, new_rate: Frequency) {
        let per_sec = new_rate.per_sec_u64();
        self.set_token_rate_with_burst(per_sec, per_sec);
    }

    /// Updates an event throughput and an independent burst, in whole events.
    pub fn set_event_rate_with_burst(&self, new_rate: Frequency, burst: u64) {
        self.set_token_rate_with_burst(new_rate.per_sec_u64(), burst);
    }

    /// The configured event throughput. Zero means no limit.
    #[must_use]
    pub fn event_rate(&self) -> Frequency {
        Frequency::from_per_sec_u64(self.token_rate())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, bytes_per_sec, kibibytes, kibibytes_per_sec, mebibytes};

    use super::*;

    // The bucket narrows every rate and burst to raw bytes and bytes-per-second
    // for the verified integer kernel, so the accessors are the only place a
    // scale factor could go missing. Each case pairs a value written in one unit
    // with the byte-denominated magnitude it must read back as: a dropped or
    // doubled 1024 in either direction fails here.
    #[test]
    fn rate_and_burst_round_trip_through_the_accessors() {
        let cases = [
            (bytes_per_sec(0), bytes(0)),
            (bytes_per_sec(1), bytes(1)),
            (kibibytes_per_sec(1), bytes(1024)),
            (bytes_per_sec(1024), kibibytes(1)),
            (bytes_per_sec(3_000_000), mebibytes(2)),
            (kibibytes_per_sec(64), mebibytes(1)),
        ];

        for (rate, burst) in cases {
            let b = TokenBucket::new();
            b.set_byte_rate_with_burst(rate, burst);
            check!((b.byte_rate(), b.byte_burst()) == (rate, burst));
        }
    }

    /// The event pair exists separately from the byte pair because a token
    /// means a different thing in each -- the API's own warning is that mixing
    /// them "compiles but the result is wrong". Only the byte pair was round
    /// tripped, so nothing checked that an event rate reads back as one, nor
    /// that the raw token primitive underneath both publishes what it is given.
    #[test]
    fn event_and_token_rates_round_trip_through_the_accessors() {
        // (rate per second, independent burst in whole events)
        let cases = [(0u64, 0u64), (1, 1), (10, 100), (1_000, 250)];

        for (per_sec, burst) in cases {
            let rate = Frequency::from_per_sec_u64(per_sec);

            let b = TokenBucket::new();
            b.set_event_rate_with_burst(rate, burst);
            check!((b.event_rate(), b.token_burst()) == (rate, burst));

            // `set_event_rate` bursts one second of the rate, as the byte pair does.
            let b = TokenBucket::new();
            b.set_event_rate(rate);
            check!((b.event_rate(), b.token_burst()) == (rate, per_sec));

            // The untyped primitive both pairs delegate to.
            let b = TokenBucket::new();
            b.set_token_rate(per_sec);
            check!((b.token_rate(), b.token_burst()) == (per_sec, per_sec));
        }
    }

    // `set_rate` derives the burst from one second's worth of the rate, so the
    // burst it publishes must be the byte count the rate delivers in that
    // second — not the rate's bare number in some other unit.
    #[test]
    fn set_rate_bursts_one_second_of_throughput() {
        let b = TokenBucket::new();
        b.set_byte_rate(kibibytes_per_sec(64));
        check!((b.byte_rate(), b.byte_burst()) == (kibibytes_per_sec(64), kibibytes(64)));
    }
}
