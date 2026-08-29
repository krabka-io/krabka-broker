//! Validating the requested token lifetime and turning it into the two
//! absolute instants KIP-48 stores on a delegation token.
//!
//! Kafka keeps the renewal deadline and the hard ceiling apart, and the two
//! are computed from the same clamped lifetime, so both the clamp and the
//! arithmetic that follows it live in one module where they cannot drift.

use super::DurationMs;

/// Wire sentinel: `CreateDelegationToken.max_lifetime_ms == -1` defers to the
/// broker's configured lifetime ceiling (`delegation.token.max.lifetime.ms`).
const USE_BROKER_LIFETIME_CEILING: i64 = -1;

/// The two absolute epoch-millisecond instants a freshly minted token carries.
///
/// See [`token_deadlines`] for how each is derived and why they are separate.
pub(super) struct TokenDeadlines {
    /// The absolute upper bound on the token's lifetime. Renew may never push
    /// expiry past it.
    pub(super) max_timestamp_ms: i64,
    /// The initial "next renewal due" instant.
    pub(super) initial_expiry_ms: i64,
}

/// Validates and clamps the requested `max_lifetime_ms`.
///
/// `-1` defers to the broker ceiling; a positive value is clamped to the
/// ceiling; anything else is invalid (zero or non-`-1` negatives) and returns
/// `None`, which the handler reports as `INVALID_REQUEST`.
pub(super) fn chosen_lifetime_ms(
    requested_ms: DurationMs,
    ceiling_ms: DurationMs,
) -> Option<DurationMs> {
    match requested_ms {
        USE_BROKER_LIFETIME_CEILING => Some(ceiling_ms),
        n if n > 0 => Some(n.min(ceiling_ms)),
        _ => None,
    }
}

/// Derives the token's deadlines from the clamped lifetime.
///
/// KIP-48 (matches `org.apache.kafka.metadata.security.DelegationTokenManager`):
/// `max_timestamp_ms` is the absolute upper bound on the token's lifetime
/// — `Renew` may never push expiry past it. `expiry_timestamp_ms` is the
/// initial "next renewal due" instant, computed as `now + default_renew_period`
/// clamped down so a tiny `chosen_lifetime` never produces an `expiry >
/// max`. The two values are deliberately separate so that the typical
/// case (7-day ceiling, 24h renew window) leaves room for `Renew` to
/// actually extend `expiry_timestamp_ms` up to `max_timestamp_ms`.
pub(super) fn token_deadlines(
    now_ms: i64,
    chosen_lifetime: DurationMs,
    default_renew_period_ms: DurationMs,
) -> TokenDeadlines {
    TokenDeadlines {
        max_timestamp_ms: now_ms + chosen_lifetime,
        initial_expiry_ms: now_ms + default_renew_period_ms.min(chosen_lifetime),
    }
}
