//! KIP-48 delegation-token deadline decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Delegation-token API whose admission policy is being evaluated.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenApi {
    Create,
    Renew,
    Expire,
    Describe,
}

/// Whether a connection may invoke a delegation-token API.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenApiAdmission {
    Reject,
    Allow,
}

/// Credential source selected for the first SCRAM round.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ScramCredentialSource {
    Regular,
    DelegationToken,
    ExpiredDelegationToken,
    Unknown,
}

/// Classify the credential for SCRAM round 1.
///
/// `token_mechanism` is whether the delegation-token store may be read: the
/// broker passes the client-first `tokenauth` extension there, and leaves
/// `has_regular_credential` false when it is set, so exactly one store is
/// consulted, as in Kafka's `ScramSaslServer`.
#[ensures((result == ScramCredentialSource::Regular) == has_regular_credential)]
#[ensures((result == ScramCredentialSource::DelegationToken) ==
    (!has_regular_credential && token_mechanism && has_token && token_active))]
#[ensures((result == ScramCredentialSource::ExpiredDelegationToken) ==
    (!has_regular_credential && token_mechanism && has_token && !token_active))]
#[ensures((result == ScramCredentialSource::Unknown) ==
    (!has_regular_credential && (!token_mechanism || !has_token)))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies four independent credential lookup facts"
)]
#[must_use]
pub fn scram_credential_source(
    has_regular_credential: bool,
    token_mechanism: bool,
    has_token: bool,
    token_active: bool,
) -> ScramCredentialSource {
    if has_regular_credential {
        ScramCredentialSource::Regular
    } else if !token_mechanism || !has_token {
        ScramCredentialSource::Unknown
    } else if token_active {
        ScramCredentialSource::DelegationToken
    } else {
        ScramCredentialSource::ExpiredDelegationToken
    }
}

/// Whether one delegation token is visible to a Describe caller.
///
/// Matches `DelegationTokenManager.filterToken` in Kafka trunk: a caller sees
/// a token only when the (possibly absent) owner filter matches it, and even
/// then only as the token's owner, a listed renewer, or the holder of a
/// `Describe` ACL on that exact token. The caller is never a delegation-token-
/// authenticated identity here — [`token_api_admission`] refuses every such
/// caller before the host ever builds this predicate, so this kernel does not
/// re-derive that isolation; folding it into the owner/renewer/ACL relation
/// would let a filter-matching owner or ACL grant leak a token-authed
/// caller's sibling tokens back in.
#[ensures(result == (owner_filter_matches && (caller_is_owner || caller_is_renewer || acl_allows)))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies independent token visibility relationships"
)]
#[must_use]
pub fn token_describe_visible(
    owner_filter_matches: bool,
    caller_is_owner: bool,
    caller_is_renewer: bool,
    acl_allows: bool,
) -> bool {
    owner_filter_matches && (caller_is_owner || caller_is_renewer || acl_allows)
}

/// Admit delegation-token APIs only for a securely authenticated, non-token
/// identity.
///
/// Matches `KafkaApis.allowTokenRequests`: a delegation-token-authenticated
/// identity may not invoke ANY delegation-token API, including
/// `DescribeDelegationToken`. KIP-48 forbids a token-issued session from
/// bootstrapping further token access; letting it describe would leak the
/// HMACs of every other token the same owner holds. The host adapter is
/// responsible for treating an anonymous listener principal as
/// unauthenticated.
#[ensures((result == TokenApiAdmission::Allow) == (
    has_authenticated_identity && !authenticated_via_token
))]
#[ensures((result == TokenApiAdmission::Reject) == (
    !has_authenticated_identity || authenticated_via_token
))]
#[must_use]
pub fn token_api_admission(
    has_authenticated_identity: bool,
    authenticated_via_token: bool,
    _api: TokenApi,
) -> TokenApiAdmission {
    if has_authenticated_identity && !authenticated_via_token {
        TokenApiAdmission::Allow
    } else {
        TokenApiAdmission::Reject
    }
}

/// Absolute deadlines stored on a freshly created delegation token.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenDeadlines {
    pub max_timestamp_ms: i64,
    pub initial_expiry_ms: i64,
}

/// Whether the host configuration admits a create and, if so, its deadlines.
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
    Expired,
    Delete,
    Update(i64),
}

/// Delegation-token mutation whose committed-state precondition is checked.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationKind {
    Renew,
    Expire,
    Delete,
}

/// Relationship between the committed token and a guarded mutation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationState {
    Missing,
    Expected,
    Applied,
    Stale,
}

/// Controller action for a generation-bound delegation-token mutation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationDecision {
    Append,
    Retry,
    Reject,
}

/// Scalar facts projected by the controller before it mutates token state.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenMutationFacts {
    pub kind: TokenMutationKind,
    pub state: TokenMutationState,
    pub now_ms: i64,
    pub expected_expiry_ms: i64,
    pub incoming_expiry_ms: i64,
    pub max_timestamp_ms: i64,
    pub uncommitted_tail: bool,
}

/// Fence a token mutation against its exact committed generation.
///
/// A retained log tail wins over retry classification. Exact already-applied
/// updates and already-missing deletes are idempotent. No update may revive a
/// token that has expired, which Kafka's `DelegationTokenControlManager`
/// defines as a deadline strictly before `now`, or cross the token's immutable
/// maximum timestamp. A renewal lands at or after `now` but, as in Kafka's
/// `renewDelegationToken`, may shorten the current expiry.
#[ensures(result == TokenMutationDecision::Append ==>
    !facts.uncommitted_tail
        && facts.state == TokenMutationState::Expected
        && match facts.kind {
            TokenMutationKind::Delete => true,
            TokenMutationKind::Renew =>
                facts.now_ms@ >= 0
                    && facts.expected_expiry_ms@ >= facts.now_ms@
                    && facts.max_timestamp_ms@ >= facts.now_ms@
                    && facts.expected_expiry_ms@ <= facts.max_timestamp_ms@
                    && facts.incoming_expiry_ms@ != facts.expected_expiry_ms@
                    && facts.incoming_expiry_ms@ >= facts.now_ms@
                    && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@,
            TokenMutationKind::Expire =>
                facts.now_ms@ >= 0
                    && facts.expected_expiry_ms@ >= facts.now_ms@
                    && facts.max_timestamp_ms@ >= facts.now_ms@
                    && facts.expected_expiry_ms@ <= facts.max_timestamp_ms@
                    && facts.incoming_expiry_ms@ >= 0
                    && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@,
        })]
#[ensures(result == TokenMutationDecision::Retry ==>
    !facts.uncommitted_tail
        && (facts.state == TokenMutationState::Applied
            || (facts.kind == TokenMutationKind::Delete
                && facts.state == TokenMutationState::Missing)
            || (facts.kind == TokenMutationKind::Renew
                && facts.state == TokenMutationState::Expected
                && facts.incoming_expiry_ms@ == facts.expected_expiry_ms@)))]
#[must_use]
pub fn token_mutation_decision(facts: TokenMutationFacts) -> TokenMutationDecision {
    if facts.uncommitted_tail {
        return TokenMutationDecision::Reject;
    }
    match facts.state {
        TokenMutationState::Applied => return TokenMutationDecision::Retry,
        TokenMutationState::Missing => {
            return match facts.kind {
                TokenMutationKind::Delete => TokenMutationDecision::Retry,
                TokenMutationKind::Renew | TokenMutationKind::Expire => {
                    TokenMutationDecision::Reject
                }
            };
        }
        TokenMutationState::Stale => return TokenMutationDecision::Reject,
        TokenMutationState::Expected => {}
    }

    match facts.kind {
        TokenMutationKind::Delete => TokenMutationDecision::Append,
        TokenMutationKind::Renew => {
            if facts.now_ms < 0
                || facts.expected_expiry_ms < facts.now_ms
                || facts.max_timestamp_ms < facts.now_ms
                || facts.expected_expiry_ms > facts.max_timestamp_ms
            {
                return TokenMutationDecision::Reject;
            }
            if facts.incoming_expiry_ms == facts.expected_expiry_ms {
                TokenMutationDecision::Retry
            } else if facts.incoming_expiry_ms >= facts.now_ms
                && facts.incoming_expiry_ms <= facts.max_timestamp_ms
            {
                TokenMutationDecision::Append
            } else {
                TokenMutationDecision::Reject
            }
        }
        TokenMutationKind::Expire => {
            if facts.now_ms >= 0
                && facts.expected_expiry_ms >= facts.now_ms
                && facts.max_timestamp_ms >= facts.now_ms
                && facts.expected_expiry_ms <= facts.max_timestamp_ms
                && facts.incoming_expiry_ms >= 0
                && facts.incoming_expiry_ms <= facts.max_timestamp_ms
            {
                TokenMutationDecision::Append
            } else {
                TokenMutationDecision::Reject
            }
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn deadline_model(now_ms: i64, duration_ms: i64) -> Int {
    pearlite! {
        if now_ms@ + duration_ms@ > i64::MAX@ { i64::MAX@ } else { now_ms@ + duration_ms@ }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn bounded_period_model(requested_ms: i64, default_ms: i64) -> Int {
    pearlite! {
        if requested_ms@ > 0 && requested_ms@ < default_ms@ { requested_ms@ } else { default_ms@ }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn min_model(left: Int, right: Int) -> Int {
    pearlite! {
        if left < right { left } else { right }
    }
}

/// Kafka's `DelegationTokenControlManager.sum`: `now + duration`, saturated
/// at `i64::MAX` instead of wrapping.
#[requires(duration_ms@ >= 0)]
#[ensures(result@ == deadline_model(now_ms, duration_ms))]
fn token_deadline(now_ms: i64, duration_ms: i64) -> i64 {
    now_ms.checked_add(duration_ms).unwrap_or(i64::MAX)
}

/// A requested period, bounded by the configured one.
///
/// Kafka's `createDelegationToken` and `renewDelegationToken` both use the
/// configured value for a request of 0 or less, and the smaller of the two
/// for a positive request.
#[requires(default_ms@ > 0)]
#[ensures(result@ == bounded_period_model(requested_ms, default_ms))]
#[ensures(result@ > 0)]
fn bounded_period(requested_ms: i64, default_ms: i64) -> i64 {
    if requested_ms > 0 {
        requested_ms.min(default_ms)
    } else {
        default_ms
    }
}

/// Derive both stored deadlines of a new token.
///
/// Matches `DelegationTokenControlManager.createDelegationToken` in Kafka
/// trunk: the lifetime is `delegation.token.max.lifetime.ms` (`ceiling_ms`)
/// for a request of 0 or less and the smaller of the two otherwise, the
/// maximum timestamp is `now` plus that lifetime, and the first expiry is
/// `now` plus `delegation.token.expiry.time.ms` (`default_renew_period_ms`),
/// capped at the maximum timestamp. Both sums saturate at `i64::MAX`. Kafka's
/// configuration validation rejects a period below 1, so a non-positive host
/// setting is `Invalid` here.
#[ensures((result == TokenCreateDecision::Invalid) ==
    (ceiling_ms@ <= 0 || default_renew_period_ms@ <= 0))]
#[ensures(match result {
    TokenCreateDecision::Invalid => true,
    TokenCreateDecision::Create(deadlines) =>
        deadlines.max_timestamp_ms@ ==
            deadline_model(now_ms, bounded_period_model(requested_ms, ceiling_ms))
            && deadlines.initial_expiry_ms@ == min_model(
                deadlines.max_timestamp_ms@,
                deadline_model(now_ms, default_renew_period_ms),
            ),
})]
#[must_use]
pub fn create_token_deadlines(
    now_ms: i64,
    requested_ms: i64,
    ceiling_ms: i64,
    default_renew_period_ms: i64,
) -> TokenCreateDecision {
    if ceiling_ms <= 0 || default_renew_period_ms <= 0 {
        return TokenCreateDecision::Invalid;
    }

    let max_timestamp_ms = token_deadline(now_ms, bounded_period(requested_ms, ceiling_ms));
    let renew_deadline_ms = token_deadline(now_ms, default_renew_period_ms);
    TokenCreateDecision::Create(TokenDeadlines {
        max_timestamp_ms,
        initial_expiry_ms: renew_deadline_ms.min(max_timestamp_ms),
    })
}

/// Derive a renewed expiry.
///
/// Matches `DelegationTokenControlManager.renewDelegationToken` in Kafka
/// trunk: a token whose expiry or maximum timestamp is strictly before `now`
/// is `Expired`. Otherwise the renew period is the configured
/// `delegation.token.expiry.time.ms` for a request of 0 or less and the
/// smaller of the two for a positive request, and the new expiry is `now`
/// plus that period, capped at the maximum timestamp. The new expiry replaces
/// the current one even when it is earlier. A non-positive configured period
/// is `Invalid`.
#[ensures((result == TokenRenewDecision::Expired) ==
    (current_expiry_ms@ < now_ms@ || max_timestamp_ms@ < now_ms@))]
#[ensures((result == TokenRenewDecision::Invalid) == (
    current_expiry_ms@ >= now_ms@
        && max_timestamp_ms@ >= now_ms@
        && default_renew_period_ms@ <= 0
))]
#[ensures(match result {
    TokenRenewDecision::Renew(expiry) =>
        expiry@ == min_model(
            max_timestamp_ms@,
            deadline_model(now_ms, bounded_period_model(requested_ms, default_renew_period_ms)),
        ),
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
    if current_expiry_ms < now_ms || max_timestamp_ms < now_ms {
        return TokenRenewDecision::Expired;
    }
    if default_renew_period_ms <= 0 {
        return TokenRenewDecision::Invalid;
    }

    let period = bounded_period(requested_ms, default_renew_period_ms);
    TokenRenewDecision::Renew(token_deadline(now_ms, period).min(max_timestamp_ms))
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

/// Select deletion or a bounded expiry update.
///
/// Matches `DelegationTokenControlManager.expireDelegationToken` in Kafka
/// trunk: every negative period deletes the token, whatever its deadlines.
/// Otherwise a token whose expiry or maximum timestamp is strictly before
/// `now` is `Expired`, and a live token gets `now + period`, saturated at
/// `i64::MAX` and capped at its maximum timestamp.
#[ensures((result == TokenExpireDecision::Delete) == (period_ms@ < 0))]
#[ensures((result == TokenExpireDecision::Expired) ==
    (period_ms@ >= 0
        && (current_expiry_ms@ < now_ms@ || max_timestamp_ms@ < now_ms@)))]
#[ensures(match result {
    TokenExpireDecision::Update(expiry) =>
        expiry@ == min_model(max_timestamp_ms@, deadline_model(now_ms, period_ms)),
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
    if current_expiry_ms < now_ms || max_timestamp_ms < now_ms {
        return TokenExpireDecision::Expired;
    }

    TokenExpireDecision::Update(token_deadline(now_ms, period_ms).min(max_timestamp_ms))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn token_visibility_requires_owner_filter_plus_a_relationship() {
        for (filter, owner, renewer, acl, visible) in [
            (false, true, false, false, false),
            (true, true, false, false, true),
            (true, false, true, false, true),
            (true, false, false, true, true),
            (true, false, false, false, false),
        ] {
            check!(token_describe_visible(filter, owner, renewer, acl) == visible);
        }
    }

    #[test]
    fn scram_source_prefers_regular_and_limits_token_fallback() {
        use ScramCredentialSource::{DelegationToken, ExpiredDelegationToken, Regular, Unknown};

        for (regular, token_mechanism, token, active, expected) in [
            (true, false, true, false, Regular),
            (false, true, true, true, DelegationToken),
            (false, true, true, false, ExpiredDelegationToken),
            (false, false, true, true, Unknown),
            (false, true, false, true, Unknown),
        ] {
            check!(scram_credential_source(regular, token_mechanism, token, active) == expected);
        }
    }

    #[test]
    fn token_api_admission_requires_identity_and_blocks_every_token_authed_call() {
        for api in [
            TokenApi::Create,
            TokenApi::Renew,
            TokenApi::Expire,
            TokenApi::Describe,
        ] {
            check!(token_api_admission(false, false, api) == TokenApiAdmission::Reject);
            check!(token_api_admission(false, true, api) == TokenApiAdmission::Reject);
            check!(token_api_admission(true, false, api) == TokenApiAdmission::Allow);
            // KafkaApis.allowTokenRequests refuses every delegation-token API,
            // Describe included, to a token-authenticated caller.
            check!(token_api_admission(true, true, api) == TokenApiAdmission::Reject);
        }
    }

    fn created(max_timestamp_ms: i64, initial_expiry_ms: i64) -> TokenCreateDecision {
        TokenCreateDecision::Create(TokenDeadlines {
            max_timestamp_ms,
            initial_expiry_ms,
        })
    }

    /// `DelegationTokenControlManager.createDelegationToken`: a request of 0
    /// or less takes the configured lifetime, a positive one the smaller of
    /// the two, and both sums saturate at `i64::MAX`.
    #[test]
    fn create_matches_kafka_lifetime_and_saturation() {
        for (now, requested, ceiling, renew, expected) in [
            (0, -1, 1_000, 100, created(1_000, 100)),
            (100, -1, 1_000, 100, created(1_100, 200)),
            (100, 0, 1_000, 100, created(1_100, 200)),
            (100, -2, 1_000, 100, created(1_100, 200)),
            (100, i64::MIN, 1_000, 100, created(1_100, 200)),
            (100, 50, 1_000, 100, created(150, 150)),
            (100, 5_000, 1_000, 100, created(1_100, 200)),
            (100, -1, 100, 101, created(200, 200)),
            (100, -1, i64::MAX - 100, 100, created(i64::MAX, 200)),
            (i64::MAX, -1, 1, 1, created(i64::MAX, i64::MAX)),
            (1, -1, i64::MAX, 1, created(i64::MAX, 2)),
            (1, -1, i64::MAX, i64::MAX, created(i64::MAX, i64::MAX)),
            (100, -1, 0, 100, TokenCreateDecision::Invalid),
            (100, -1, 1_000, 0, TokenCreateDecision::Invalid),
            (100, -1, -1, 100, TokenCreateDecision::Invalid),
        ] {
            check!(
                create_token_deadlines(now, requested, ceiling, renew) == expected,
                "now={now} requested={requested} ceiling={ceiling} renew={renew}"
            );
        }
    }

    /// `DelegationTokenControlManager.renewDelegationToken`: `Expired` only
    /// for a deadline strictly before `now`; the expiry is
    /// `min(max, now + min(default, period))` for a positive period and
    /// `min(max, now + default)` otherwise, even when that shortens it.
    #[test]
    fn renew_matches_kafka_period_cap_and_expiry() {
        use TokenRenewDecision::{Expired, Invalid, Renew};

        // (now, requested, default, current expiry, max, expected)
        for (now, requested, default, current, max, expected) in [
            (100, 25, 50, 150, 1_000, Renew(125)),
            (100, 75, 50, 150, 1_000, Renew(150)),
            (100, 500, 50, 150, 1_000, Renew(150)),
            (100, -1, 50, 150, 1_000, Renew(150)),
            (100, 0, 50, 150, 1_000, Renew(150)),
            (100, -5, 50, 150, 1_000, Renew(150)),
            // A shorter period than the remaining lifetime shortens the expiry.
            (100, 10, 50, 900, 1_000, Renew(110)),
            (100, 500, 1_000, 150, 200, Renew(200)),
            (100, i64::MAX, i64::MAX, 150, 200, Renew(200)),
            (1, i64::MAX, i64::MAX, 150, i64::MAX, Renew(i64::MAX)),
            // A deadline equal to `now` is still live in Kafka.
            (100, 25, 50, 100, 1_000, Renew(125)),
            (100, 25, 50, 100, 100, Renew(100)),
            (100, 25, 50, 99, 1_000, Expired),
            (100, 25, 50, 150, 99, Expired),
            (100, 25, 0, 99, 1_000, Expired),
            (100, 25, 0, 150, 1_000, Invalid),
        ] {
            check!(
                renew_token_expiry(now, requested, default, current, max) == expected,
                "now={now} requested={requested} default={default} current={current} max={max}"
            );
        }
    }

    #[test]
    fn token_mutations_are_generation_bound_live_and_idempotent() {
        let expected = TokenMutationFacts {
            kind: TokenMutationKind::Renew,
            state: TokenMutationState::Expected,
            now_ms: 100,
            expected_expiry_ms: 150,
            incoming_expiry_ms: 175,
            max_timestamp_ms: 200,
            uncommitted_tail: false,
        };
        for facts in [
            expected,
            TokenMutationFacts {
                now_ms: 0,
                expected_expiry_ms: 50,
                incoming_expiry_ms: 75,
                max_timestamp_ms: 100,
                ..expected
            },
            // Kafka's renew may shorten the expiry, down to `now`.
            TokenMutationFacts {
                incoming_expiry_ms: 149,
                ..expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: 100,
                ..expected
            },
            // A deadline equal to `now` is still live.
            TokenMutationFacts {
                expected_expiry_ms: 100,
                ..expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 100,
                incoming_expiry_ms: 110,
                max_timestamp_ms: 110,
                ..expected
            },
        ] {
            check!(
                token_mutation_decision(facts) == TokenMutationDecision::Append,
                "{facts:?}"
            );
        }
        for facts in [
            TokenMutationFacts {
                incoming_expiry_ms: 150,
                ..expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 100,
                incoming_expiry_ms: 100,
                max_timestamp_ms: 100,
                ..expected
            },
        ] {
            check!(
                token_mutation_decision(facts) == TokenMutationDecision::Retry,
                "{facts:?}"
            );
        }
        for facts in [
            TokenMutationFacts {
                now_ms: -1,
                ..expected
            },
            TokenMutationFacts {
                state: TokenMutationState::Stale,
                ..expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: 99,
                ..expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: i64::MAX,
                ..expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 99,
                ..expected
            },
            TokenMutationFacts {
                max_timestamp_ms: 99,
                expected_expiry_ms: 99,
                incoming_expiry_ms: 99,
                ..expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 201,
                max_timestamp_ms: 200,
                ..expected
            },
            TokenMutationFacts {
                uncommitted_tail: true,
                ..expected
            },
        ] {
            check!(token_mutation_decision(facts) == TokenMutationDecision::Reject);
        }
        check!(
            token_mutation_decision(TokenMutationFacts {
                kind: TokenMutationKind::Delete,
                state: TokenMutationState::Missing,
                ..expected
            }) == TokenMutationDecision::Retry
        );
        check!(
            token_mutation_decision(TokenMutationFacts {
                kind: TokenMutationKind::Delete,
                expected_expiry_ms: 100,
                ..expected
            }) == TokenMutationDecision::Append
        );

        let expire_expected = TokenMutationFacts {
            kind: TokenMutationKind::Expire,
            state: TokenMutationState::Expected,
            now_ms: 100,
            expected_expiry_ms: 150,
            incoming_expiry_ms: 175,
            max_timestamp_ms: 200,
            uncommitted_tail: false,
        };
        for facts in [
            expire_expected,
            TokenMutationFacts {
                now_ms: 0,
                expected_expiry_ms: 50,
                incoming_expiry_ms: 0,
                max_timestamp_ms: 100,
                ..expire_expected
            },
            // A deadline equal to `now` is still live.
            TokenMutationFacts {
                expected_expiry_ms: 100,
                ..expire_expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 100,
                incoming_expiry_ms: 100,
                max_timestamp_ms: 100,
                ..expire_expected
            },
        ] {
            check!(
                token_mutation_decision(facts) == TokenMutationDecision::Append,
                "{facts:?}"
            );
        }
        for facts in [
            TokenMutationFacts {
                now_ms: -1,
                ..expire_expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 99,
                ..expire_expected
            },
            TokenMutationFacts {
                max_timestamp_ms: 99,
                ..expire_expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 201,
                max_timestamp_ms: 200,
                ..expire_expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: -1,
                ..expire_expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: 201,
                max_timestamp_ms: 200,
                ..expire_expected
            },
        ] {
            check!(token_mutation_decision(facts) == TokenMutationDecision::Reject);
        }
    }

    /// `DelegationTokenControlManager.expireDelegationToken`: a negative period
    /// deletes before the expiry check, and a live token gets
    /// `min(max, now + period)` with a saturating sum.
    #[test]
    fn expire_matches_kafka_delete_expiry_and_saturation() {
        use TokenExpireDecision::{Delete, Expired, Update};

        // (now, period, current expiry, max, expected)
        for (now, period, current, max, expected) in [
            (0, 0, 150, 200, Update(0)),
            (100, 0, 150, 200, Update(100)),
            (100, 25, 150, 200, Update(125)),
            (100, 500, 150, 200, Update(200)),
            (100, i64::MAX - 100, 150, 200, Update(200)),
            (100, i64::MAX, 150, i64::MAX, Update(i64::MAX)),
            (100, 0, 100, 200, Update(100)),
            (100, 0, 100, 100, Update(100)),
            (100, -1, 150, 200, Delete),
            (100, -1, 50, 60, Delete),
            (100, i64::MIN, 150, 200, Delete),
            (100, 0, 99, 200, Expired),
            (100, 0, 150, 99, Expired),
        ] {
            check!(
                expire_token_deadline(now, period, current, max) == expected,
                "now={now} period={period} current={current} max={max}"
            );
        }
    }

    #[test]
    fn active_tokens_require_both_live_ordered_deadlines() {
        check!(token_is_active(0, 50, 100));
        check!(!token_is_active(-1, 50, 100));
        check!(token_is_active(100, 150, 200));
        check!(!token_is_active(100, 100, 200));
        check!(!token_is_active(100, 150, 100));
        check!(expire_token_deadline(100, 50, 200, 200) == TokenExpireDecision::Update(150));
        check!(renew_token_expiry(100, 50, 50, 200, 200) == TokenRenewDecision::Renew(150));
        let renew_at_max = TokenMutationFacts {
            state: TokenMutationState::Expected,
            kind: TokenMutationKind::Renew,
            now_ms: 100,
            expected_expiry_ms: 200,
            incoming_expiry_ms: 200,
            max_timestamp_ms: 200,
            uncommitted_tail: false,
        };
        check!(token_mutation_decision(renew_at_max) == TokenMutationDecision::Retry);
        let renew_over_max = TokenMutationFacts {
            expected_expiry_ms: 250,
            incoming_expiry_ms: 250,
            ..renew_at_max
        };
        check!(token_mutation_decision(renew_over_max) == TokenMutationDecision::Reject);
        check!(!token_is_active(100, 201, 200));
    }
}
