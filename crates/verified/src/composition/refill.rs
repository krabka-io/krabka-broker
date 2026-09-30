use creusot_std::prelude::*;

use crate::{
    quota::quota_refill,
    throttle::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume},
};

/// Splitting an elapsed interval cannot create or lose a consume budget,
/// including fractional credit, debt repayment, and burst saturation.
/// The host must claim each interval once and retain the returned fraction;
/// the rate and burst stay fixed, with no charge or consume between refills.
#[requires(initial.0@ <= burst@ && (initial.0@ == 0 || initial.1@ == 0))]
#[requires(initial.2@ < 1_000_000_000)]
#[requires(elapsed.0@ + elapsed.1@ <= u64::MAX@)]
#[ensures(result.0@ <= requested@ && result.1@ <= burst@)]
#[ensures({
    let earned = ((elapsed.0@ + elapsed.1@) * rate@ + initial.2@) / 1_000_000_000;
    let balance = initial.0@ - initial.1@ + earned;
    let budget = if balance <= 0 { 0 } else if balance <= burst@ { balance } else { burst@ };
    result.0@ == if requested@ <= budget { requested@ } else { budget }
        && result.0@ + result.1@ == budget
        && result.2@ == if balance < 0 { -balance } else { 0 }
})]
#[ensures(result.3@ < 1_000_000_000)]
#[ensures({
    let numerator = (elapsed.0@ + elapsed.1@) * rate@ + initial.2@;
    let balance = initial.0@ - initial.1@ + numerator / 1_000_000_000;
    result.3@ == if balance >= burst@ { 0 } else { numerator % 1_000_000_000 }
})]
pub(super) fn refill_partition_preserves_consume_budget(
    initial: (u64, u64, u64), // available, debt, fractional numerator
    elapsed: (u64, u64),
    rate: u64,
    burst: u64,
    requested: u64,
) -> (u64, u64, u64, u64) {
    let first = quota_refill(initial.0, initial.1, initial.2, elapsed.0, rate, burst);
    let split = quota_refill(first.0, first.1, first.2, elapsed.1, rate, burst);
    let single = quota_refill(
        initial.0,
        initial.1,
        initial.2,
        elapsed.0 + elapsed.1,
        rate,
        burst,
    );
    proof_assert!((elapsed.0@ + elapsed.1@) * rate@
        == elapsed.0@ * rate@ + elapsed.1@ * rate@);
    proof_assert!({
        let balance = (initial.0@ - initial.1@) * 1_000_000_000 + initial.2@
            + (elapsed.0@ + elapsed.1@) * rate@;
        (split.0@ - split.1@) * 1_000_000_000 + split.2@
            == if balance <= burst@ * 1_000_000_000 { balance } else { burst@ * 1_000_000_000 }
    });
    proof_assert!((split.0@ - split.1@) * 1_000_000_000 + split.2@
        == (single.0@ - single.1@) * 1_000_000_000 + single.2@);
    proof_assert!(split.0 == single.0 && split.1 == single.1 && split.2 == single.2);
    // Keep the comparison executable so native tests exercise both paths.
    #[cfg(not(creusot))]
    assert2::assert!(split == single);
    let (grant, left) = plan_consume(
        AvailableTokens(split.0),
        RefillTokens(0),
        BurstCapacity(burst),
        RequestedTokens(requested),
    );
    (grant.0, left.0, split.1, split.2)
}
