use creusot_std::prelude::*;

/// Greatest representable whole micro-token debt earned during this wait.
/// The caller converts its time bound to the clock's nanosecond resolution;
/// carried fractional credit can only repay this conservative cap sooner.
#[ensures(result@ == (wait_nanos@ * rate@ / 1_000_000_000).min(u64::MAX@))]
#[must_use]
pub fn quota_debt_cap(wait_nanos: u128, rate: u64) -> u64 {
    let Some(numerator) = wait_nanos.checked_mul(u128::from(rate)) else {
        return u64::MAX;
    };
    (numerator / 1_000_000_000).min(18_446_744_073_709_551_615) as u64
}
