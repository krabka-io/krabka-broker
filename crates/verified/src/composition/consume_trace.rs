use creusot_std::prelude::*;

use crate::{
    quota::{quota_refill, quota_whole_request},
    throttle::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume},
};

/// Arbitrarily many whole-token consumes conserve elapsed-time credit,
/// including debt, fractional credit, and credit discarded at the burst.
/// Repeated/backward clocks cannot mint another interval. The host serializes
/// these steps with a fixed positive rate and burst; requests are in whole
/// tokens. The runtime's rate-0 unlimited fast path is outside this theorem.
#[requires(initial.0@ <= burst@ && (initial.0@ == 0 || initial.1@ == 0))]
#[requires(initial.2@ < 1_000_000_000 && units_per_token@ > 0)]
#[requires(rate@ > 0)]
#[ensures(result.1.0@ <= burst@ && (result.1.0@ == 0 || result.1.1@ == 0))]
#[ensures(result.1.1@ <= initial.1@ && result.1.2@ < 1_000_000_000)]
#[ensures(result.1.3@ >= start@)]
#[ensures(forall<i: Int> 0 <= i && i < steps@.len() ==> result.1.3@ >= steps@[i].0@)]
#[ensures(result.1.3@ == start@ || exists<i: Int>
    0 <= i && i < steps@.len() && result.1.3@ == steps@[i].0@)]
#[ensures((result.0@ * units_per_token@ + result.1.0@ - result.1.1@) * 1_000_000_000
    + result.1.2@ + result.2@
    == (initial.0@ - initial.1@) * 1_000_000_000 + initial.2@
        + (result.1.3@ - start@) * rate@)]
#[ensures(result.0@ * units_per_token@ + result.1.0@ <= initial.0@
    + (initial.2@ + (result.1.3@ - start@) * rate@) / 1_000_000_000)]
#[ensures(steps@.len() > 0
    && steps@[steps@.len() - 1].1@ >= burst@ / units_per_token@
    ==> result.1.0@ < units_per_token@)]
pub(super) fn metered_consumes_conserve_elapsed_credit(
    initial: (u64, u64, u64), // available, debt, fractional numerator
    start: u64,
    steps: &[(u64, u64)], // clock, requested whole tokens
    rate: u64,
    burst: u64,
    units_per_token: u64,
) -> (u128, (u64, u64, u64, u64), u128) {
    // whole tokens, final state, discarded numerator
    let (mut available, mut debt, mut fraction) = initial;
    let mut last_clock = start;
    let mut granted = 0_u128;
    let mut lost = 0_u128;
    let mut i: usize = 0;
    #[cfg_attr(creusot, invariant(i@ <= steps@.len()))]
    #[cfg_attr(creusot, invariant(available@ <= burst@ && (available@ == 0 || debt@ == 0)))]
    #[cfg_attr(creusot, invariant(debt@ <= initial.1@ && fraction@ < 1_000_000_000))]
    #[cfg_attr(creusot, invariant(last_clock@ >= start@))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@ ==> last_clock@ >= steps@[k].0@))]
    #[cfg_attr(creusot, invariant(last_clock@ == start@ || exists<k: Int>
        0 <= k && k < i@ && last_clock@ == steps@[k].0@))]
    #[cfg_attr(creusot, invariant(granted@ * units_per_token@ + available@ >= initial.0@))]
    #[cfg_attr(creusot, invariant(lost@ <= initial.2@ + (last_clock@ - start@) * rate@))]
    #[cfg_attr(creusot, invariant((granted@ * units_per_token@ + available@ - debt@) * 1_000_000_000
        + fraction@ + lost@
        == (initial.0@ - initial.1@) * 1_000_000_000 + initial.2@ + (last_clock@ - start@) * rate@))]
    #[cfg_attr(creusot, invariant(granted@ * units_per_token@ + available@ <= initial.0@
        + (initial.2@ + (last_clock@ - start@) * rate@) / 1_000_000_000))]
    #[cfg_attr(creusot, invariant(i@ > 0
        && steps@[i@ - 1].1@ >= burst@ / units_per_token@ ==> available@ < units_per_token@))]
    #[cfg_attr(creusot, variant(steps@.len() - i@))]
    while i < steps.len() {
        let next = last_clock.max(steps[i].0);
        let elapsed = next - last_clock;
        proof_assert!((next@ - start@) * rate@
            == (last_clock@ - start@) * rate@ + elapsed@ * rate@);
        let refilled = quota_refill(available, debt, fraction, elapsed, rate, burst);
        let numerator = u128::from(elapsed) * u128::from(rate) + u128::from(fraction);
        let discarded = if refilled.0 == burst && refilled.1 == 0 {
            let credited = u128::from(debt - refilled.1) + u128::from(refilled.0 - available);
            numerator - credited * 1_000_000_000
        } else {
            0
        };
        proof_assert!((refilled.0@ - refilled.1@) * 1_000_000_000 + refilled.2@ + discarded@
            == (available@ - debt@) * 1_000_000_000 + fraction@ + elapsed@ * rate@);
        let request = quota_whole_request(steps[i].1, refilled.0, units_per_token);
        proof_assert!({
            crate::stretch::lemma_div_monotone(refilled.0@, burst@, units_per_token@);
            refilled.0@ / units_per_token@ <= burst@ / units_per_token@
        });
        let (grant, left) = plan_consume(
            AvailableTokens(refilled.0),
            RefillTokens(0),
            BurstCapacity(burst),
            RequestedTokens(request),
        );
        proof_assert!(grant.0@ == request@);
        proof_assert!((grant.0@ / units_per_token@) * units_per_token@ == grant.0@);
        proof_assert!((granted@ + grant.0@ / units_per_token@) * units_per_token@
            == granted@ * units_per_token@ + grant.0@);
        // Expand the whole-token update before composing the ledger equality.
        proof_assert!((granted@ * units_per_token@ + available@ - debt@) * 1_000_000_000
            == granted@ * units_per_token@ * 1_000_000_000
                + (available@ - debt@) * 1_000_000_000);
        proof_assert!(((granted@ + grant.0@ / units_per_token@) * units_per_token@
            + left.0@ - refilled.1@) * 1_000_000_000
            == granted@ * units_per_token@ * 1_000_000_000
                + (grant.0@ + left.0@ - refilled.1@) * 1_000_000_000);
        proof_assert!((grant.0@ + left.0@ - refilled.1@) * 1_000_000_000
            + refilled.2@ + discarded@
            == (available@ - debt@) * 1_000_000_000 + fraction@ + elapsed@ * rate@);
        granted += u128::from(grant.0 / units_per_token);
        lost += discarded;
        available = left.0;
        debt = refilled.1;
        fraction = refilled.2;
        last_clock = next;
        i += 1;
    }
    (granted, (available, debt, fraction, last_clock), lost)
}
