use creusot_std::prelude::*;

use super::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume, quota_charge,
    refill_partition_preserves_consume_budget,
};
use crate::quota::{quota_debt_cap, quota_whole_request};

type DeadlineQuota = (u64, u64, u64, u64, u64, u64);

/// Derive the retained debt from the actual wait, earn repayment from two
/// elapsed intervals, and grant only complete tokens from that same ledger.
/// A completed wait clears bounded debt; enough additional time grants the
/// full probe. Rate, burst and token quantum stay fixed, with no intervening
/// charge/refund/consume. Clock claims, time conversion and publication are
/// host obligations. Counts in the ledger are effective micro-tokens.
#[requires(initial.0@ <= burst@ && (initial.0@ == 0 || initial.1@ == 0))]
#[requires(initial.2@ < 1_000_000_000 && rate@ > 0 && units@ > 0)]
#[requires(elapsed.0@ + elapsed.1@ <= u64::MAX@)]
#[ensures(result.0@ == (charge.1@ * rate@ / 1_000_000_000).min(u64::MAX@))]
#[ensures(result.1@ == (initial.1@ + charge.0@ - charge.0@.min(initial.0@)).min(result.0@))]
#[ensures({
    let earned = ((elapsed.0@ + elapsed.1@) * rate@ + initial.2@) / 1_000_000_000;
    let paid_available = if charge.0@ <= initial.0@ { initial.0@ - charge.0@ } else { 0 };
    let balance = paid_available - result.1@ + earned;
    let budget = balance.max(0).min(burst@);
    result.2@ == probe@.min(budget / units@)
        && result.2@ * units@ + result.3@ == budget
        && result.4@ == (result.1@ - earned).max(0)
        && result.5@ == if balance >= burst@ { 0 }
            else { ((elapsed.0@ + elapsed.1@) * rate@ + initial.2@) % 1_000_000_000 }
})]
#[ensures(result.2@ <= probe@ && result.3@ <= burst@ && result.5@ < 1_000_000_000)]
#[ensures(charge.1@ <= elapsed.0@ + elapsed.1@ ==> result.4@ == 0)]
#[ensures(probe@ * units@ <= burst@
    && (elapsed.0@ + elapsed.1@) * rate@ + initial.2@
        >= (result.0@ + probe@ * units@) * 1_000_000_000
    ==> result.2 == probe)]
pub(super) fn capped_charge_is_repaid_by_elapsed_time(
    initial: (u64, u64, u64), // available, debt, fractional numerator at charge
    charge: (u64, u128),      // effective request, wait in nanoseconds
    elapsed: (u64, u64),
    rate: u64,
    burst: u64,
    probe: u64, // requested whole tokens
    units: u64,
) -> DeadlineQuota {
    let cap = quota_debt_cap(charge.1, rate);
    let charged = quota_charge(initial.0, initial.1, charge.0, burst, cap);
    let (_, available, debt, fraction) = refill_partition_preserves_consume_budget(
        (charged.0, charged.1, initial.2),
        elapsed,
        rate,
        burst,
        0,
    );
    let request = quota_whole_request(probe, available, units);
    let (grant, left) = plan_consume(
        AvailableTokens(available),
        RefillTokens(0),
        BurstCapacity(burst),
        RequestedTokens(request),
    );
    (cap, charged.1, grant.0 / units, left.0, debt, fraction)
}
