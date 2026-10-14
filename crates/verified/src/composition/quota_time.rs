use super::*;

type DeadlineQuota = (u64, u64, u64, u64, u64, u64);

#[derive(Clone, Copy)]
pub(super) struct DeadlineInputs {
    /// Available balance, retained debt and fractional refill credit.
    pub(super) initial: (u64, u64, u64),
    /// Effective request and wait duration in nanoseconds.
    pub(super) charge: (u64, u128),
    pub(super) elapsed: (u64, u64),
    pub(super) rate: u64,
    pub(super) burst: u64,
    /// Requested whole tokens and their micro-token quantum.
    pub(super) probe: u64,
    pub(super) units: u64,
}

impl DeadlineInputs {
    #[must_use]
    pub(super) const fn new(
        initial: (u64, u64, u64),
        charge: (u64, u128),
        elapsed: (u64, u64),
        rate: u64,
        burst: u64,
        probe: u64,
        units: u64,
    ) -> Self {
        Self {
            initial,
            charge,
            elapsed,
            rate,
            burst,
            probe,
            units,
        }
    }
}

/// Derive the retained debt from the actual wait, earn repayment from two
/// elapsed intervals, and grant only complete tokens from that same ledger.
/// A completed wait clears bounded debt; enough additional time grants the
/// full probe. Rate, burst and token quantum stay fixed, with no intervening
/// charge/refund/consume. Clock claims, time conversion and publication are
/// host obligations. Counts in the ledger are effective micro-tokens.
#[requires(crate::quota::balance_within_burst(inputs.initial.0@, inputs.initial.1@, inputs.burst@))]
#[requires(inputs.initial.2@ < 1_000_000_000 && inputs.rate@ > 0 && inputs.units@ > 0)]
#[requires(inputs.elapsed.0@ + inputs.elapsed.1@ <= u64::MAX@)]
#[ensures(result.0@ == (inputs.charge.1@ * inputs.rate@ / 1_000_000_000).min(u64::MAX@))]
#[ensures(
    result.1@ == (inputs.initial.1@ + inputs.charge.0@ - inputs.charge.0@.min(inputs.initial.0@))
        .min(result.0@)
)]
#[ensures({
    let earned =
        ((inputs.elapsed.0@ + inputs.elapsed.1@) * inputs.rate@ + inputs.initial.2@)
            / 1_000_000_000;
    let paid_available = if inputs.charge.0@ <= inputs.initial.0@ {
        inputs.initial.0@ - inputs.charge.0@
    } else {
        0
    };
    let balance = paid_available - result.1@ + earned;
    let budget = balance.max(0).min(inputs.burst@);
    result.2@ == inputs.probe@.min(budget / inputs.units@)
        && result.2@ * inputs.units@ + result.3@ == budget
        && result.4@ == (result.1@ - earned).max(0)
        && result.5@ == if balance >= inputs.burst@ {
            0
        } else {
            ((inputs.elapsed.0@ + inputs.elapsed.1@) * inputs.rate@ + inputs.initial.2@)
                % 1_000_000_000
        }
})]
#[ensures(result.2@ <= inputs.probe@ && result.3@ <= inputs.burst@ && result.5@ < 1_000_000_000)]
#[ensures(inputs.charge.1@ <= inputs.elapsed.0@ + inputs.elapsed.1@ ==> result.4@ == 0)]
#[ensures(inputs.probe@ * inputs.units@ <= inputs.burst@
    && (inputs.elapsed.0@ + inputs.elapsed.1@) * inputs.rate@ + inputs.initial.2@
        >= (result.0@ + inputs.probe@ * inputs.units@) * 1_000_000_000
    ==> result.2 == inputs.probe)]
pub(super) fn capped_charge_is_repaid_by_elapsed_time(inputs: DeadlineInputs) -> DeadlineQuota {
    let DeadlineInputs {
        initial,
        charge,
        elapsed,
        rate,
        burst,
        probe,
        units,
    } = inputs;
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
