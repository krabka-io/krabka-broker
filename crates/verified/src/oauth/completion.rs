use creusot_std::prelude::*;

use crate::jwks::{JwksCacheDecision, JwksCacheFacts, jwks_cache_admission};

/// Bind validation completion to a forward clock observation and, when supplied,
/// the still-current and fresh JWKS generation. The supplied cache
/// timestamp is a start observation; freshness is always recomputed at finish.
#[ensures(result == (started_ms@ >= 0 && completed_ms@ >= started_ms@ && match cache {
    None => true,
    Some(facts) => facts.generation_before@ % 2 == 0
        && facts.generation_before@ == facts.generation_after@
        && (!facts.expiry_enabled || (facts.expiry_ms@ >= 0
            && facts.last_successful_fetch_ms@ > 0
            && completed_ms@ >= facts.last_successful_fetch_ms@
            && completed_ms@ - facts.last_successful_fetch_ms@ <= facts.expiry_ms@)),
}))]
#[must_use]
pub fn oauth_validation_admission(
    started_ms: i64,
    completed_ms: i64,
    cache: Option<JwksCacheFacts>,
) -> bool {
    if started_ms < 0 || completed_ms < started_ms {
        return false;
    }
    match cache {
        None => true,
        Some(mut facts) => {
            facts.now_ms = completed_ms;
            matches!(jwks_cache_admission(facts), JwksCacheDecision::Admit)
        }
    }
}
