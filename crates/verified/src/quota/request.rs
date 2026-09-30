use creusot_std::prelude::*;

/// Convert a whole-token request to storage units, limited by the whole
/// tokens the balance holds. Saturating the multiplication cannot grant a
/// fractional token or reduce the largest representable whole-token grant.
///
/// # Panics
/// Panics if `units_per_token` is zero.
#[requires(units_per_token@ > 0)]
#[ensures(result@ <= total@ && result@ % units_per_token@ == 0)]
#[ensures(result@ / units_per_token@ == if requested@ <= total@ / units_per_token@ {
    requested@
} else { total@ / units_per_token@ })]
#[ensures(result@ == (if requested@ <= total@ / units_per_token@ {
    requested@
} else { total@ / units_per_token@ }) * units_per_token@)]
#[must_use]
pub fn quota_whole_request(requested: u64, total: u64, units_per_token: u64) -> u64 {
    requested
        .saturating_mul(units_per_token)
        .min(total - total % units_per_token)
}
