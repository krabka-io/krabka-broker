//! KIP-48 delegation-token deadline decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Absolute deadlines stored on a freshly created delegation token.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenDeadlines {
    pub max_timestamp_ms: i64,
    pub initial_expiry_ms: i64,
}

/// Whether a create request has safe, valid deadline arithmetic.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenCreateDecision {
    Invalid,
    Create(TokenDeadlines),
}

/// Whether a token can be renewed and, if so, its next expiry.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenRenewDecision {
    Invalid,
    Expired,
    Renew(i64),
}

/// Mutation selected by `ExpireDelegationToken`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenExpireDecision {
    Invalid,
    Expired,
    Delete,
    Update(i64),
}

#[cfg(creusot)]
#[logic]
fn chosen_lifetime_model(requested_ms: i64, ceiling_ms: i64) -> Int {
    pearlite! {
        if requested_ms@ == -1 {
            ceiling_ms@
        } else if requested_ms@ < ceiling_ms@ {
            requested_ms@
        } else {
            ceiling_ms@
        }
    }
}

#[cfg(creusot)]
#[logic]
fn renew_period_model(requested_ms: i64, default_renew_period_ms: i64) -> Int {
    pearlite! {
        if requested_ms@ == -1 { default_renew_period_ms@ } else { requested_ms@ }
    }
}

/// Validate a create request and derive both stored deadlines without wrap.
///
/// `requested_ms == -1` selects the configured ceiling. Other requests must
/// be positive and are clamped to that ceiling. Invalid host configuration and
/// any sum outside the `i64` timestamp domain fail closed.
#[ensures((result == TokenCreateDecision::Invalid) == (
    now_ms@ < 0
        || ceiling_ms@ <= 0
        || default_renew_period_ms@ <= 0
        || (requested_ms@ != -1 && requested_ms@ <= 0)
        || chosen_lifetime_model(requested_ms, ceiling_ms) > i64::MAX@ - now_ms@
))]
#[ensures(match result {
    TokenCreateDecision::Invalid => true,
    TokenCreateDecision::Create(deadlines) =>
        now_ms@ >= 0
            && deadlines.max_timestamp_ms@ ==
                now_ms@ + chosen_lifetime_model(requested_ms, ceiling_ms)
            && deadlines.initial_expiry_ms@ == now_ms@
                + (if default_renew_period_ms@ < chosen_lifetime_model(requested_ms, ceiling_ms) {
                    default_renew_period_ms@
                } else {
                    chosen_lifetime_model(requested_ms, ceiling_ms)
                })
            && deadlines.initial_expiry_ms@ > now_ms@
            && deadlines.initial_expiry_ms@ <= deadlines.max_timestamp_ms@
            && deadlines.max_timestamp_ms@ <= i64::MAX@
            && deadlines.max_timestamp_ms@ > now_ms@,
})]
#[must_use]
pub fn create_token_deadlines(
    now_ms: i64,
    requested_ms: i64,
    ceiling_ms: i64,
    default_renew_period_ms: i64,
) -> TokenCreateDecision {
    if now_ms < 0 || ceiling_ms <= 0 || default_renew_period_ms <= 0 {
        return TokenCreateDecision::Invalid;
    }

    let chosen_lifetime = if requested_ms == -1 {
        ceiling_ms
    } else if requested_ms > 0 {
        requested_ms.min(ceiling_ms)
    } else {
        return TokenCreateDecision::Invalid;
    };

    if chosen_lifetime > i64::MAX - now_ms {
        return TokenCreateDecision::Invalid;
    }
    let initial_period = default_renew_period_ms.min(chosen_lifetime);
    TokenCreateDecision::Create(TokenDeadlines {
        max_timestamp_ms: now_ms + chosen_lifetime,
        initial_expiry_ms: now_ms + initial_period,
    })
}

/// Derive a renewed expiry without reviving an expired token or wrapping.
///
/// `requested_ms == -1` selects the configured default. Every other accepted
/// period is positive. The resulting expiry is strictly in the future and no
/// later than the token's immutable maximum timestamp.
#[ensures((result == TokenRenewDecision::Expired) ==
    (current_expiry_ms@ <= now_ms@ || max_timestamp_ms@ <= now_ms@))]
#[ensures((result == TokenRenewDecision::Invalid) == (
    current_expiry_ms@ > now_ms@
        && max_timestamp_ms@ > now_ms@
        && (now_ms@ < 0
            || current_expiry_ms@ > max_timestamp_ms@
            || default_renew_period_ms@ <= 0
            || (requested_ms@ != -1 && requested_ms@ <= 0)
            || renew_period_model(requested_ms, default_renew_period_ms) > i64::MAX@ - now_ms@)
))]
#[ensures(match result {
    TokenRenewDecision::Renew(expiry) =>
        expiry@ > now_ms@
            && expiry@ <= max_timestamp_ms@
            && expiry@ == if now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
                    < max_timestamp_ms@ {
                now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
            } else {
                max_timestamp_ms@
            },
    _ => true,
})]
#[must_use]
pub fn renew_token_expiry(
    now_ms: i64,
    requested_ms: i64,
    default_renew_period_ms: i64,
    current_expiry_ms: i64,
    max_timestamp_ms: i64,
) -> TokenRenewDecision {
    if current_expiry_ms <= now_ms || max_timestamp_ms <= now_ms {
        return TokenRenewDecision::Expired;
    }
    if now_ms < 0 || current_expiry_ms > max_timestamp_ms || default_renew_period_ms <= 0 {
        return TokenRenewDecision::Invalid;
    }

    let period = if requested_ms == -1 {
        default_renew_period_ms
    } else if requested_ms > 0 {
        requested_ms
    } else {
        return TokenRenewDecision::Invalid;
    };
    if period > i64::MAX - now_ms {
        return TokenRenewDecision::Invalid;
    }

    TokenRenewDecision::Renew((now_ms + period).min(max_timestamp_ms))
}

/// Whether a stored delegation token may authenticate at `now_ms`.
#[ensures(result == (
    now_ms@ >= 0
        && expiry_timestamp_ms@ > now_ms@
        && max_timestamp_ms@ > now_ms@
        && expiry_timestamp_ms@ <= max_timestamp_ms@
))]
#[must_use]
pub fn token_is_active(now_ms: i64, expiry_timestamp_ms: i64, max_timestamp_ms: i64) -> bool {
    now_ms >= 0
        && expiry_timestamp_ms > now_ms
        && max_timestamp_ms > now_ms
        && expiry_timestamp_ms <= max_timestamp_ms
}

/// Select deletion or a bounded expiry update without wrapping.
///
/// Kafka assigns deletion semantics to every negative period. Zero means
/// `now`; a positive period is added only when the sum fits in `i64`.
#[ensures((result == TokenExpireDecision::Delete) == (period_ms@ < 0))]
#[ensures((result == TokenExpireDecision::Expired) ==
    (period_ms@ >= 0
        && (current_expiry_ms@ <= now_ms@ || max_timestamp_ms@ <= now_ms@)))]
#[ensures((result == TokenExpireDecision::Invalid) == (
    period_ms@ >= 0
        && current_expiry_ms@ > now_ms@
        && max_timestamp_ms@ > now_ms@
        && (now_ms@ < 0
            || current_expiry_ms@ > max_timestamp_ms@
            || period_ms@ > i64::MAX@ - now_ms@)
))]
#[ensures(match result {
    TokenExpireDecision::Update(expiry) =>
        expiry@ <= max_timestamp_ms@
            && expiry@ == if now_ms@ + period_ms@ < max_timestamp_ms@ {
                now_ms@ + period_ms@
            } else {
                max_timestamp_ms@
            },
    _ => true,
})]
#[must_use]
pub fn expire_token_deadline(
    now_ms: i64,
    period_ms: i64,
    current_expiry_ms: i64,
    max_timestamp_ms: i64,
) -> TokenExpireDecision {
    if period_ms < 0 {
        return TokenExpireDecision::Delete;
    }
    if current_expiry_ms <= now_ms || max_timestamp_ms <= now_ms {
        return TokenExpireDecision::Expired;
    }
    if now_ms < 0 || current_expiry_ms > max_timestamp_ms {
        return TokenExpireDecision::Invalid;
    }
    if period_ms > i64::MAX - now_ms {
        return TokenExpireDecision::Invalid;
    }

    TokenExpireDecision::Update((now_ms + period_ms).min(max_timestamp_ms))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn create_clamps_and_rejects_invalid_or_overflowing_deadlines() {
        check!(
            create_token_deadlines(100, -1, 1_000, 100)
                == TokenCreateDecision::Create(TokenDeadlines {
                    max_timestamp_ms: 1_100,
                    initial_expiry_ms: 200,
                })
        );
        check!(
            create_token_deadlines(100, 50, 1_000, 100)
                == TokenCreateDecision::Create(TokenDeadlines {
                    max_timestamp_ms: 150,
                    initial_expiry_ms: 150,
                })
        );
        check!(
            create_token_deadlines(100, -1, 100, 101)
                == TokenCreateDecision::Create(TokenDeadlines {
                    max_timestamp_ms: 200,
                    initial_expiry_ms: 200,
                })
        );
        for decision in [
            create_token_deadlines(100, 0, 1_000, 100),
            create_token_deadlines(100, -2, 1_000, 100),
            create_token_deadlines(100, -1, 0, 100),
            create_token_deadlines(100, -1, 1_000, 0),
            create_token_deadlines(i64::MAX, -1, 1, 1),
            create_token_deadlines(1, -1, i64::MAX, 1),
        ] {
            check!(decision == TokenCreateDecision::Invalid);
        }
    }

    #[test]
    fn renew_never_resurrects_or_wraps() {
        check!(renew_token_expiry(100, 50, 10, 150, 200) == TokenRenewDecision::Renew(150));
        check!(renew_token_expiry(100, 500, 10, 150, 200) == TokenRenewDecision::Renew(200));
        check!(renew_token_expiry(100, -1, 25, 150, 200) == TokenRenewDecision::Renew(125));
        check!(renew_token_expiry(100, 1, 10, 100, 200) == TokenRenewDecision::Expired);
        check!(renew_token_expiry(100, 1, 10, 150, 100) == TokenRenewDecision::Expired);
        check!(renew_token_expiry(100, -2, 10, 150, 200) == TokenRenewDecision::Invalid);
        check!(renew_token_expiry(100, i64::MAX, 10, 150, 200) == TokenRenewDecision::Invalid);
    }

    #[test]
    fn expire_deletes_negative_and_rejects_overflow() {
        check!(expire_token_deadline(100, -1, 100, 100) == TokenExpireDecision::Delete);
        check!(expire_token_deadline(100, 0, 150, 200) == TokenExpireDecision::Update(100));
        check!(expire_token_deadline(100, 500, 150, 200) == TokenExpireDecision::Update(200));
        check!(expire_token_deadline(100, 0, 100, 200) == TokenExpireDecision::Expired);
        check!(expire_token_deadline(100, 0, 150, 100) == TokenExpireDecision::Expired);
        check!(expire_token_deadline(100, i64::MAX, 150, i64::MAX) == TokenExpireDecision::Invalid);
    }

    #[test]
    fn active_tokens_require_both_live_ordered_deadlines() {
        check!(token_is_active(100, 150, 200));
        check!(!token_is_active(100, 100, 200));
        check!(!token_is_active(100, 150, 100));
        check!(!token_is_active(100, 201, 200));
    }
}
