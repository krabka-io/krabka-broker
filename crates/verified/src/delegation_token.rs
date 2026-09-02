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

/// Preserve regular-credential precedence and restrict KIP-48 fallback.
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
#[ensures(result == if authenticated_via_token {
    caller_is_owner
} else {
    owner_filter_matches && (caller_is_owner || caller_is_renewer || acl_allows)
})]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies independent token visibility relationships"
)]
#[must_use]
pub fn token_describe_visible(
    authenticated_via_token: bool,
    owner_filter_matches: bool,
    caller_is_owner: bool,
    caller_is_renewer: bool,
    acl_allows: bool,
) -> bool {
    if authenticated_via_token {
        caller_is_owner
    } else {
        owner_filter_matches && (caller_is_owner || caller_is_renewer || acl_allows)
    }
}

/// Admit delegation-token APIs only for a securely authenticated identity.
///
/// A delegation-token-authenticated identity may describe its own tokens, but
/// may not create, renew, or expire tokens. The host adapter is responsible
/// for treating an anonymous listener principal as unauthenticated.
#[ensures((result == TokenApiAdmission::Allow) == (
    has_authenticated_identity
        && (!authenticated_via_token || api == TokenApi::Describe)
))]
#[ensures((result == TokenApiAdmission::Reject) == (
    !has_authenticated_identity
        || (authenticated_via_token && api != TokenApi::Describe)
))]
#[must_use]
pub fn token_api_admission(
    has_authenticated_identity: bool,
    authenticated_via_token: bool,
    api: TokenApi,
) -> TokenApiAdmission {
    if !has_authenticated_identity {
        return TokenApiAdmission::Reject;
    }

    match (authenticated_via_token, api) {
        (true, TokenApi::Create | TokenApi::Renew | TokenApi::Expire) => TokenApiAdmission::Reject,
        _ => TokenApiAdmission::Allow,
    }
}

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
/// updates and already-missing deletes are idempotent. A renewal must preserve
/// or extend the expiry, and no update may revive an expired token or cross its
/// immutable maximum timestamp.
#[ensures(result == TokenMutationDecision::Append ==>
    !facts.uncommitted_tail
        && facts.state == TokenMutationState::Expected
        && match facts.kind {
            TokenMutationKind::Delete => true,
            TokenMutationKind::Renew =>
                facts.now_ms@ >= 0
                    && facts.expected_expiry_ms@ > facts.now_ms@
                    && facts.max_timestamp_ms@ > facts.now_ms@
                    && facts.expected_expiry_ms@ <= facts.max_timestamp_ms@
                    && facts.incoming_expiry_ms@ > facts.expected_expiry_ms@
                    && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@,
            TokenMutationKind::Expire =>
                facts.now_ms@ >= 0
                    && facts.expected_expiry_ms@ > facts.now_ms@
                    && facts.max_timestamp_ms@ > facts.now_ms@
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
                || facts.expected_expiry_ms <= facts.now_ms
                || facts.max_timestamp_ms <= facts.now_ms
                || facts.expected_expiry_ms > facts.max_timestamp_ms
            {
                return TokenMutationDecision::Reject;
            }
            if facts.incoming_expiry_ms == facts.expected_expiry_ms {
                TokenMutationDecision::Retry
            } else if facts.incoming_expiry_ms > facts.expected_expiry_ms
                && facts.incoming_expiry_ms <= facts.max_timestamp_ms
            {
                TokenMutationDecision::Append
            } else {
                TokenMutationDecision::Reject
            }
        }
        TokenMutationKind::Expire => {
            if facts.now_ms >= 0
                && facts.expected_expiry_ms > facts.now_ms
                && facts.max_timestamp_ms > facts.now_ms
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

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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
            && expiry@ == if current_expiry_ms@ >
                    (if now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
                            < max_timestamp_ms@ {
                        now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
                    } else {
                        max_timestamp_ms@
                    }) {
                current_expiry_ms@
            } else {
                if now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
                        < max_timestamp_ms@ {
                    now_ms@ + renew_period_model(requested_ms, default_renew_period_ms)
                } else {
                    max_timestamp_ms@
                }
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

    TokenRenewDecision::Renew(
        (now_ms + period)
            .min(max_timestamp_ms)
            .max(current_expiry_ms),
    )
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
    fn token_visibility_respects_token_isolation_filter_and_acl() {
        for (token_auth, filter, owner, renewer, acl, visible) in [
            (true, false, true, false, false, true),
            (true, true, false, true, true, false),
            (false, false, true, false, false, false),
            (false, true, true, false, false, true),
            (false, true, false, true, false, true),
            (false, true, false, false, true, true),
            (false, true, false, false, false, false),
        ] {
            check!(token_describe_visible(token_auth, filter, owner, renewer, acl) == visible);
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
    fn token_api_admission_requires_identity_and_blocks_token_mutations() {
        for api in [
            TokenApi::Create,
            TokenApi::Renew,
            TokenApi::Expire,
            TokenApi::Describe,
        ] {
            check!(token_api_admission(false, false, api) == TokenApiAdmission::Reject);
            check!(token_api_admission(false, true, api) == TokenApiAdmission::Reject);
            check!(token_api_admission(true, false, api) == TokenApiAdmission::Allow);
        }

        for api in [TokenApi::Create, TokenApi::Renew, TokenApi::Expire] {
            check!(token_api_admission(true, true, api) == TokenApiAdmission::Reject);
        }
        check!(token_api_admission(true, true, TokenApi::Describe) == TokenApiAdmission::Allow);
    }

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
        check!(renew_token_expiry(100, 25, 10, 150, 200) == TokenRenewDecision::Renew(150));
        check!(renew_token_expiry(100, 75, 10, 150, 200) == TokenRenewDecision::Renew(175));
        check!(renew_token_expiry(100, 500, 10, 150, 200) == TokenRenewDecision::Renew(200));
        check!(renew_token_expiry(100, -1, 25, 150, 200) == TokenRenewDecision::Renew(150));
        check!(renew_token_expiry(100, 1, 10, 100, 200) == TokenRenewDecision::Expired);
        check!(renew_token_expiry(100, 1, 10, 150, 100) == TokenRenewDecision::Expired);
        check!(renew_token_expiry(100, -2, 10, 150, 200) == TokenRenewDecision::Invalid);
        check!(renew_token_expiry(100, i64::MAX, 10, 150, 200) == TokenRenewDecision::Invalid);
    }

    #[test]
    fn token_mutations_are_generation_bound_monotonic_and_idempotent() {
        let expected = TokenMutationFacts {
            kind: TokenMutationKind::Renew,
            state: TokenMutationState::Expected,
            now_ms: 100,
            expected_expiry_ms: 150,
            incoming_expiry_ms: 175,
            max_timestamp_ms: 200,
            uncommitted_tail: false,
        };
        check!(token_mutation_decision(expected) == TokenMutationDecision::Append);
        check!(
            token_mutation_decision(TokenMutationFacts {
                incoming_expiry_ms: 150,
                ..expected
            }) == TokenMutationDecision::Retry
        );
        for facts in [
            TokenMutationFacts {
                state: TokenMutationState::Stale,
                ..expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: 149,
                ..expected
            },
            TokenMutationFacts {
                incoming_expiry_ms: i64::MAX,
                ..expected
            },
            TokenMutationFacts {
                expected_expiry_ms: 100,
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
