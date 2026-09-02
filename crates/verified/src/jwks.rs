//! JWKS cache freshness and on-demand refresh decisions.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Whether a validator may use one observed JWKS cache generation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum JwksCacheDecision {
    Reject,
    Admit,
}

/// Values read around one key validation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct JwksCacheFacts {
    pub generation_before: u64,
    pub generation_after: u64,
    pub last_successful_fetch_ms: i64,
    pub now_ms: i64,
    pub expiry_enabled: bool,
    pub expiry_ms: i64,
}

/// Admit only a stable, fully published cache generation that is not stale.
#[ensures((result == JwksCacheDecision::Admit) == (
    facts.generation_before@ % 2 == 0
        && facts.generation_before@ == facts.generation_after@
        && (!facts.expiry_enabled || (
            facts.expiry_ms@ >= 0
                && facts.last_successful_fetch_ms@ > 0
                && facts.now_ms@ >= facts.last_successful_fetch_ms@
                && facts.now_ms@ - facts.last_successful_fetch_ms@ <= facts.expiry_ms@
        ))
))]
#[must_use]
pub fn jwks_cache_admission(facts: JwksCacheFacts) -> JwksCacheDecision {
    if !facts.generation_before.is_multiple_of(2)
        || facts.generation_before != facts.generation_after
    {
        return JwksCacheDecision::Reject;
    }
    if !facts.expiry_enabled {
        return JwksCacheDecision::Admit;
    }
    if facts.expiry_ms < 0
        || facts.last_successful_fetch_ms <= 0
        || facts.now_ms < facts.last_successful_fetch_ms
    {
        return JwksCacheDecision::Reject;
    }
    if facts.now_ms - facts.last_successful_fetch_ms > facts.expiry_ms {
        JwksCacheDecision::Reject
    } else {
        JwksCacheDecision::Admit
    }
}

/// Whether an on-demand signal may start a fetch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum JwksOnDemandDecision {
    RateLimited,
    Refresh { next_refresh_ms: i64 },
}

/// Keep the limiter monotonic and fail closed across wall-clock rollback.
#[ensures(match result {
    JwksOnDemandDecision::RateLimited => true,
    JwksOnDemandDecision::Refresh { next_refresh_ms } => {
        next_refresh_ms@ == now_ms@
            && now_ms@ >= 0
            && min_pause_ms@ >= 0
            && now_ms@ >= last_refresh_ms@
            && (last_refresh_ms@ <= 0 || now_ms@ - last_refresh_ms@ >= min_pause_ms@)
    }
})]
#[must_use]
pub fn jwks_on_demand_refresh_decision(
    now_ms: i64,
    last_refresh_ms: i64,
    min_pause_ms: i64,
) -> JwksOnDemandDecision {
    if now_ms < 0 || min_pause_ms < 0 || now_ms < last_refresh_ms {
        return JwksOnDemandDecision::RateLimited;
    }
    if last_refresh_ms > 0 && now_ms - last_refresh_ms < min_pause_ms {
        return JwksOnDemandDecision::RateLimited;
    }
    JwksOnDemandDecision::Refresh {
        next_refresh_ms: now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JwksCacheDecision, JwksCacheFacts, JwksOnDemandDecision, jwks_cache_admission,
        jwks_on_demand_refresh_decision,
    };

    #[test]
    fn cache_requires_one_fresh_stable_generation() {
        let fresh = JwksCacheFacts {
            generation_before: 2,
            generation_after: 2,
            last_successful_fetch_ms: 100,
            now_ms: 200,
            expiry_enabled: true,
            expiry_ms: 100,
        };
        assert2::check!(jwks_cache_admission(fresh) == JwksCacheDecision::Admit);
        for rejected in [
            JwksCacheFacts {
                generation_before: 1,
                ..fresh
            },
            JwksCacheFacts {
                generation_after: 4,
                ..fresh
            },
            JwksCacheFacts {
                last_successful_fetch_ms: 0,
                ..fresh
            },
            JwksCacheFacts {
                now_ms: 99,
                ..fresh
            },
            JwksCacheFacts {
                now_ms: 201,
                ..fresh
            },
            JwksCacheFacts {
                expiry_ms: -1,
                ..fresh
            },
        ] {
            assert2::check!(jwks_cache_admission(rejected) == JwksCacheDecision::Reject);
        }
    }

    #[test]
    fn on_demand_limit_is_monotonic_and_overflow_safe() {
        assert2::check!(
            jwks_on_demand_refresh_decision(100, 0, 10)
                == JwksOnDemandDecision::Refresh {
                    next_refresh_ms: 100
                }
        );
        assert2::check!(
            jwks_on_demand_refresh_decision(99, 100, 0) == JwksOnDemandDecision::RateLimited
        );
        assert2::check!(
            jwks_on_demand_refresh_decision(i64::MAX, 1, i64::MAX)
                == JwksOnDemandDecision::RateLimited
        );
        assert2::check!(
            jwks_on_demand_refresh_decision(i64::MAX, 0, i64::MAX)
                == JwksOnDemandDecision::Refresh {
                    next_refresh_ms: i64::MAX
                }
        );
    }
}
