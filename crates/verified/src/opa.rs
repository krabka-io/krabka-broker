//! OPA authorization-cache admission and error policy.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Binary authorization result used at the OPA proof boundary.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OpaAuthorizationDecision {
    Allow,
    Deny,
}

/// Result of checking one exact-key cache entry.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OpaCacheAdmission {
    Miss,
    Hit(OpaAuthorizationDecision),
}

/// Result of computing a monotonic cache deadline.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OpaCacheExpiry {
    DoNotCache,
    CacheUntil { expires_at_ms: i128 },
}

/// Reuse only an unexpired entry for the complete authorization context.
#[ensures(match result {
    OpaCacheAdmission::Miss => !complete_key_match || expires_at_ms@ <= now_ms@,
    OpaCacheAdmission::Hit(decision) => complete_key_match
        && expires_at_ms@ > now_ms@
        && decision == cached_decision,
})]
#[must_use]
pub fn opa_cache_admission(
    complete_key_match: bool,
    now_ms: i128,
    expires_at_ms: i128,
    cached_decision: OpaAuthorizationDecision,
) -> OpaCacheAdmission {
    if complete_key_match && expires_at_ms > now_ms {
        OpaCacheAdmission::Hit(cached_decision)
    } else {
        OpaCacheAdmission::Miss
    }
}

/// Compute a positive, exactly representable deadline on a monotonic clock.
#[ensures(match result {
    OpaCacheExpiry::DoNotCache => true,
    OpaCacheExpiry::CacheUntil { expires_at_ms } => ttl_ms@ > 0
        && expires_at_ms@ == now_ms@ + ttl_ms@,
})]
#[must_use]
pub fn opa_cache_expiry(now_ms: i128, ttl_ms: i64) -> OpaCacheExpiry {
    if ttl_ms <= 0 {
        return OpaCacheExpiry::DoNotCache;
    }
    match now_ms.checked_add(i128::from(ttl_ms)) {
        Some(expires_at_ms) => OpaCacheExpiry::CacheUntil { expires_at_ms },
        None => OpaCacheExpiry::DoNotCache,
    }
}

/// Map the explicit outage policy without changing successful OPA decisions.
#[ensures((result == OpaAuthorizationDecision::Allow) == allow_on_error)]
#[must_use]
pub fn opa_error_decision(allow_on_error: bool) -> OpaAuthorizationDecision {
    if allow_on_error {
        OpaAuthorizationDecision::Allow
    } else {
        OpaAuthorizationDecision::Deny
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OpaAuthorizationDecision, OpaCacheAdmission, OpaCacheExpiry, opa_cache_admission,
        opa_cache_expiry, opa_error_decision,
    };

    #[test]
    fn cache_requires_the_complete_key_and_strict_freshness() {
        let allow = OpaAuthorizationDecision::Allow;
        let deny = OpaAuthorizationDecision::Deny;
        assert2::check!(opa_cache_admission(true, 9, 10, allow) == OpaCacheAdmission::Hit(allow));
        assert2::check!(opa_cache_admission(true, 9, 10, deny) == OpaCacheAdmission::Hit(deny));
        assert2::check!(opa_cache_admission(false, 9, 10, allow) == OpaCacheAdmission::Miss);
        assert2::check!(opa_cache_admission(true, 10, 10, allow) == OpaCacheAdmission::Miss);
        assert2::check!(opa_cache_admission(true, 11, 10, allow) == OpaCacheAdmission::Miss);
    }

    #[test]
    fn expiry_and_error_mapping_fail_safely() {
        assert2::check!(
            opa_cache_expiry(10, 5) == OpaCacheExpiry::CacheUntil { expires_at_ms: 15 }
        );
        assert2::check!(opa_cache_expiry(10, 0) == OpaCacheExpiry::DoNotCache);
        assert2::check!(opa_cache_expiry(10, -1) == OpaCacheExpiry::DoNotCache);
        assert2::check!(opa_cache_expiry(i128::MAX, 1) == OpaCacheExpiry::DoNotCache);
        assert2::check!(
            opa_error_decision(false) == OpaAuthorizationDecision::Deny
                && opa_error_decision(true) == OpaAuthorizationDecision::Allow
        );
    }
}
