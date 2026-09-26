//! KIP-48 delegation-token decisions.
//!
//! The deadline arithmetic follows Kafka trunk's
//! `DelegationTokenControlManager` (`createDelegationToken`,
//! `renewDelegationToken`, `expireDelegationToken`), the SCRAM credential
//! selection follows `ScramSaslServer` and `ScramServerCallbackHandler`, and
//! the API admission and describe visibility follow `KafkaApis` /
//! `ControllerApis.allowTokenRequests` and `DelegationTokenManager.filterToken`.

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

/// What the delegation-token cache holds for a SCRAM username.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenLookup {
    /// No token has this id.
    Missing,
    /// A token has this id, but [`token_is_active`] is false for it.
    Expired,
    /// A token has this id and [`token_is_active`] is true for it.
    Live,
}

/// Facts the SCRAM round-1 handler gathers before it picks a credential.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ScramCredentialFacts {
    /// The client-first message carries the `tokenauth=true` SCRAM extension
    /// (Kafka's `ScramExtensions.tokenAuthenticated`).
    pub token_auth_requested: bool,
    /// The SCRAM credential store holds a credential for this username and
    /// the negotiated mechanism.
    pub has_regular_credential: bool,
    /// The delegation-token cache entry for this username, read as a token id.
    pub token: TokenLookup,
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

/// Pick the credential store that authenticates a SCRAM login.
///
/// Matches `ScramSaslServer.evaluateResponse`: the client's `tokenauth`
/// extension alone selects the store. A token login reads only the
/// delegation-token cache (`DelegationTokenCredentialCallback`) and a regular
/// login reads only the SCRAM credential cache (`ScramCredentialCallback`), so
/// neither can fall through to the other and a token id that collides with a
/// username is checked against whichever store the client asked for. The
/// mechanism does not matter: Kafka's `DelegationTokenManager` prepares a
/// token credential for every `ScramMechanism`.
///
/// Kafka's SASL server authenticates any token still in its cache and leaves
/// removal to the expiry sweep; krabka also refuses a cached token that
/// [`token_is_active`] calls expired, closing the window before the sweep.
#[ensures((result == ScramCredentialSource::Regular) ==
    (!facts.token_auth_requested && facts.has_regular_credential))]
#[ensures((result == ScramCredentialSource::DelegationToken) ==
    (facts.token_auth_requested && facts.token == TokenLookup::Live))]
#[ensures((result == ScramCredentialSource::ExpiredDelegationToken) ==
    (facts.token_auth_requested && facts.token == TokenLookup::Expired))]
#[ensures((result == ScramCredentialSource::Unknown) ==
    (if facts.token_auth_requested {
        facts.token == TokenLookup::Missing
    } else {
        !facts.has_regular_credential
    }))]
#[must_use]
pub fn scram_credential_source(facts: ScramCredentialFacts) -> ScramCredentialSource {
    if !facts.token_auth_requested {
        return if facts.has_regular_credential {
            ScramCredentialSource::Regular
        } else {
            ScramCredentialSource::Unknown
        };
    }
    match facts.token {
        TokenLookup::Missing => ScramCredentialSource::Unknown,
        TokenLookup::Expired => ScramCredentialSource::ExpiredDelegationToken,
        TokenLookup::Live => ScramCredentialSource::DelegationToken,
    }
}

/// Facts the Describe handler gathers for one candidate token.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenDescribeFacts {
    /// The request's `owners` filter is absent, or one of its entries is the
    /// token's owner or a listed renewer (`TokenInformation.ownerOrRenewer`).
    pub selected_by_owner_filter: bool,
    /// The caller is the token's owner or a listed renewer.
    pub caller_is_owner_or_renewer: bool,
    /// The authorizer allows the caller `Describe` on this token.
    pub describe_authorized: bool,
}

/// Whether one delegation token is visible to a Describe caller.
///
/// Matches `DelegationTokenManager.filterToken` in Kafka trunk: a caller sees
/// a token only when the (possibly absent) owner filter selects it, and even
/// then only as the token's owner, a listed renewer, or a caller the
/// authorizer allows to describe it. The caller is never a delegation-token-
/// authenticated identity here — [`token_api_admission`] refuses every such
/// caller before the host ever builds these facts, so this kernel does not
/// re-derive that isolation.
#[ensures(result == (facts.selected_by_owner_filter
    && (facts.caller_is_owner_or_renewer || facts.describe_authorized)))]
#[must_use]
pub fn token_describe_visible(facts: TokenDescribeFacts) -> bool {
    facts.selected_by_owner_filter
        && (facts.caller_is_owner_or_renewer || facts.describe_authorized)
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

/// Whether a token can be renewed and, if so, its next expiry.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenRenewDecision {
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

/// The smaller of two integers.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn min_i(a: Int, b: Int) -> Int {
    pearlite! { if a <= b { a } else { b } }
}

/// `DelegationTokenControlManager.sum`: `now + duration`, saturating at
/// `Long.MAX_VALUE`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn kafka_sum(now: Int, duration: Int) -> Int {
    pearlite! { if now > i64::MAX@ - duration { i64::MAX@ } else { now + duration } }
}

/// A token is live at `now` unless Kafka's expiry test
/// `maxTimestamp < now || expiryTimestamp < now` holds.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn live(now: Int, expiry: Int, max_timestamp: Int) -> bool {
    pearlite! { expiry >= now && max_timestamp >= now }
}

/// Kafka's request-period rule, shared by create (`maxLifetimeMs`) and renew
/// (`renewPeriodMs`): a positive request is capped at the configured value;
/// any other request selects it.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn requested_period(requested: Int, configured: Int) -> Int {
    pearlite! { if requested > 0 { min_i(configured, requested) } else { configured } }
}

/// A controller-side update keeps the token inside its lifetime: the committed
/// token is still live, and the new expiry does not cross its immutable
/// maximum timestamp.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn update_admissible(facts: TokenMutationFacts) -> bool {
    pearlite! {
        live(facts.now_ms@, facts.expected_expiry_ms@, facts.max_timestamp_ms@)
            && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@
    }
}

/// A renewal whose new expiry equals the committed one changes nothing.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn renew_is_noop(facts: TokenMutationFacts) -> bool {
    pearlite! {
        facts.kind == TokenMutationKind::Renew
            && facts.incoming_expiry_ms@ == facts.expected_expiry_ms@
    }
}

/// Fence a token mutation against its exact committed generation.
///
/// A retained log tail and a stale generation always reject. An already-
/// applied mutation, and a delete of an already-missing token, are idempotent
/// retries. On the expected generation, a delete always appends — Kafka's
/// `expireDelegationToken` removes a token with a negative period whether or
/// not it has expired — and a renew or expire appends only when
/// `update_admissible` holds: the committed token is still live by Kafka's
/// `maxTimestamp < now || expiryTimestamp < now` test, and the new expiry does
/// not cross the immutable maximum. A renewal may shorten the expiry, because
/// Kafka's `renewDelegationToken` sets `min(maxTimestamp, now + renewPeriod)`
/// with no floor at the current expiry; a renewal that leaves it unchanged is
/// a retry with nothing to append.
#[ensures((result == TokenMutationDecision::Reject) == (
    facts.uncommitted_tail
        || facts.state == TokenMutationState::Stale
        || (facts.state == TokenMutationState::Missing
            && facts.kind != TokenMutationKind::Delete)
        || (facts.state == TokenMutationState::Expected
            && facts.kind != TokenMutationKind::Delete
            && !update_admissible(facts))
))]
#[ensures((result == TokenMutationDecision::Retry) == (
    !facts.uncommitted_tail
        && (facts.state == TokenMutationState::Applied
            || (facts.state == TokenMutationState::Missing
                && facts.kind == TokenMutationKind::Delete)
            || (facts.state == TokenMutationState::Expected
                && update_admissible(facts)
                && renew_is_noop(facts)))
))]
#[ensures((result == TokenMutationDecision::Append) == (
    !facts.uncommitted_tail
        && facts.state == TokenMutationState::Expected
        && (facts.kind == TokenMutationKind::Delete
            || (update_admissible(facts) && !renew_is_noop(facts)))
))]
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

    let is_renew = match facts.kind {
        TokenMutationKind::Delete => return TokenMutationDecision::Append,
        TokenMutationKind::Renew => true,
        TokenMutationKind::Expire => false,
    };
    let admissible = token_is_active(
        facts.now_ms,
        facts.expected_expiry_ms,
        facts.max_timestamp_ms,
    ) && facts.incoming_expiry_ms <= facts.max_timestamp_ms;
    if !admissible {
        TokenMutationDecision::Reject
    } else if is_renew && facts.incoming_expiry_ms == facts.expected_expiry_ms {
        TokenMutationDecision::Retry
    } else {
        TokenMutationDecision::Append
    }
}

/// `DelegationTokenControlManager.sum` for a non-negative duration.
#[requires(duration_ms@ >= 0)]
#[ensures(result@ == kafka_sum(now_ms@, duration_ms@))]
fn saturating_sum(now_ms: i64, duration_ms: i64) -> i64 {
    if now_ms > i64::MAX - duration_ms {
        i64::MAX
    } else {
        now_ms + duration_ms
    }
}

/// Kafka's request-period rule for a positive configured period.
#[requires(configured_ms@ > 0)]
#[ensures(result@ == requested_period(requested_ms@, configured_ms@))]
#[ensures(result@ > 0)]
fn effective_period(requested_ms: i64, configured_ms: i64) -> i64 {
    if requested_ms > 0 {
        configured_ms.min(requested_ms)
    } else {
        configured_ms
    }
}

/// Derive both stored deadlines of a new token, as
/// `DelegationTokenControlManager.createDelegationToken` does.
///
/// A positive `requested_max_lifetime_ms` is capped at the configured
/// `delegation.token.max.lifetime.ms`; zero, `-1`, and every other
/// non-positive request select it. The maximum timestamp is `now` plus that
/// lifetime and the first expiry is `now` plus the configured
/// `delegation.token.expiry.time.ms`, capped at the maximum; both sums
/// saturate at `i64::MAX` as Kafka's `sum` does. Keeping both configured
/// periods positive is the host's config validation, as Kafka's `atLeast(1)`
/// validators do.
#[requires(max_lifetime_ms@ > 0 && renew_period_ms@ > 0)]
#[ensures(result.max_timestamp_ms@
    == kafka_sum(now_ms@, requested_period(requested_max_lifetime_ms@, max_lifetime_ms@)))]
#[ensures(result.initial_expiry_ms@
    == min_i(result.max_timestamp_ms@, kafka_sum(now_ms@, renew_period_ms@)))]
#[must_use]
pub fn create_token_deadlines(
    now_ms: i64,
    requested_max_lifetime_ms: i64,
    max_lifetime_ms: i64,
    renew_period_ms: i64,
) -> TokenDeadlines {
    let max_timestamp_ms = saturating_sum(
        now_ms,
        effective_period(requested_max_lifetime_ms, max_lifetime_ms),
    );
    TokenDeadlines {
        max_timestamp_ms,
        initial_expiry_ms: max_timestamp_ms.min(saturating_sum(now_ms, renew_period_ms)),
    }
}

/// Renew a token, as `DelegationTokenControlManager.renewDelegationToken`
/// does.
///
/// An expired token (`maxTimestamp < now || expiryTimestamp < now`) is
/// refused. Otherwise the new expiry is `now` plus the renew period, capped at
/// the token's maximum timestamp: a positive `requested_ms` is capped at the
/// configured `delegation.token.expiry.time.ms`, and zero, `-1`, and every
/// other non-positive request select it. The result replaces the current
/// expiry outright, so a short renew period shortens it. Kafka adds `now` and
/// the period without a check; this kernel saturates at `i64::MAX` as Kafka's
/// `sum` does elsewhere, which differs only when the configured period is
/// within `now` of `i64::MAX`, where Kafka would wrap to a past expiry.
/// Keeping the configured period positive is the host's config validation.
#[requires(default_renew_period_ms@ > 0)]
#[ensures(match result {
    TokenRenewDecision::Expired => !live(now_ms@, current_expiry_ms@, max_timestamp_ms@),
    TokenRenewDecision::Renew(expiry) =>
        live(now_ms@, current_expiry_ms@, max_timestamp_ms@)
            && expiry@ == min_i(
                max_timestamp_ms@,
                kafka_sum(now_ms@, requested_period(requested_ms@, default_renew_period_ms@)),
            ),
})]
#[must_use]
pub fn renew_token_expiry(
    now_ms: i64,
    requested_ms: i64,
    default_renew_period_ms: i64,
    current_expiry_ms: i64,
    max_timestamp_ms: i64,
) -> TokenRenewDecision {
    if !token_is_active(now_ms, current_expiry_ms, max_timestamp_ms) {
        return TokenRenewDecision::Expired;
    }
    let period = effective_period(requested_ms, default_renew_period_ms);
    TokenRenewDecision::Renew(max_timestamp_ms.min(saturating_sum(now_ms, period)))
}

/// Whether a stored delegation token is live at `now_ms`: neither its expiry
/// nor its maximum timestamp is before `now_ms`. This is the negation of the
/// test Kafka's renew, expire, and expiry-sweep paths apply.
#[ensures(result == live(now_ms@, expiry_timestamp_ms@, max_timestamp_ms@))]
#[must_use]
pub fn token_is_active(now_ms: i64, expiry_timestamp_ms: i64, max_timestamp_ms: i64) -> bool {
    expiry_timestamp_ms >= now_ms && max_timestamp_ms >= now_ms
}

/// Whether a token session's recorded expiry is not yet before `now_ms`.
///
/// The SCRAM handler rechecks this at round 2 for a token that
/// [`token_is_active`] admitted at round 1. That round-1 check covered the
/// token's immutable maximum timestamp; that the committed expiry never
/// exceeds the maximum is what makes the expiry the binding deadline here.
/// Every expiry the create, renew, and expire kernels produce is capped at
/// the maximum, and [`token_mutation_decision`] appends no update past it,
/// but that the image only ever holds such expiries is a host invariant.
#[ensures(result == (expiry_timestamp_ms@ >= now_ms@))]
#[must_use]
pub fn token_expiry_not_passed(now_ms: i64, expiry_timestamp_ms: i64) -> bool {
    expiry_timestamp_ms >= now_ms
}

/// Choose deletion, refusal, or a new expiry, as
/// `DelegationTokenControlManager.expireDelegationToken` does.
///
/// Any negative period deletes the token, expired or not. Otherwise an
/// expired token is refused, and a live one gets `now + period` (saturating
/// as Kafka's `sum` does) capped at its maximum timestamp; a zero period
/// expires it at `now`.
#[ensures(match result {
    TokenExpireDecision::Delete => period_ms@ < 0,
    TokenExpireDecision::Expired =>
        period_ms@ >= 0 && !live(now_ms@, current_expiry_ms@, max_timestamp_ms@),
    TokenExpireDecision::Update(expiry) =>
        period_ms@ >= 0
            && live(now_ms@, current_expiry_ms@, max_timestamp_ms@)
            && expiry@ == min_i(max_timestamp_ms@, kafka_sum(now_ms@, period_ms@)),
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
    if !token_is_active(now_ms, current_expiry_ms, max_timestamp_ms) {
        return TokenExpireDecision::Expired;
    }
    TokenExpireDecision::Update(max_timestamp_ms.min(saturating_sum(now_ms, period_ms)))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    const HOUR: i64 = 60 * 60 * 1_000;
    const DAY: i64 = 24 * HOUR;
    /// A wall-clock `now` in the Kafka era, far from both `i64` bounds.
    const NOW: i64 = 1_700_000_000_000;

    #[test]
    fn scram_store_is_chosen_by_the_tokenauth_extension_alone() {
        use ScramCredentialSource::{DelegationToken, ExpiredDelegationToken, Regular, Unknown};
        use TokenLookup::{Expired, Live, Missing};

        // (tokenauth, regular credential exists, token lookup, source)
        for (token_auth_requested, has_regular_credential, token, expected) in [
            // A plain SCRAM login reads only the credential store: a live
            // token with the same id as the username is never consulted.
            (false, true, Missing, Regular),
            (false, true, Live, Regular),
            (false, false, Live, Unknown),
            (false, false, Expired, Unknown),
            (false, false, Missing, Unknown),
            // A tokenauth login reads only the token cache, even when a user
            // with the token's id has a SCRAM credential.
            (true, true, Live, DelegationToken),
            (true, false, Live, DelegationToken),
            (true, true, Expired, ExpiredDelegationToken),
            (true, true, Missing, Unknown),
            (true, false, Missing, Unknown),
        ] {
            let facts = ScramCredentialFacts {
                token_auth_requested,
                has_regular_credential,
                token,
            };
            check!(scram_credential_source(facts) == expected, "{facts:?}");
        }
    }

    #[test]
    fn describe_visibility_follows_kafka_filter_token() {
        // (owner filter selects the token, caller owns or renews it,
        //  authorizer allows Describe, visible)
        for (selected_by_owner_filter, caller_is_owner_or_renewer, describe_authorized, visible) in [
            // A filter that names neither owner nor renewer hides the token
            // even from its owner or an ACL holder.
            (false, true, true, false),
            (true, true, false, true),
            (true, false, true, true),
            (true, false, false, false),
        ] {
            let facts = TokenDescribeFacts {
                selected_by_owner_filter,
                caller_is_owner_or_renewer,
                describe_authorized,
            };
            check!(token_describe_visible(facts) == visible, "{facts:?}");
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

    #[test]
    fn create_deadlines_match_kafka_create_delegation_token() {
        // (requested maxLifetimeMs, max lifetime config, expiry time config,
        //  max timestamp, first expiry), all at NOW.
        for (requested, max_lifetime, renew_period, max_timestamp, initial_expiry) in [
            // Kafka defaults (7 d, 24 h) with the CLI's -1.
            (-1, 7 * DAY, DAY, NOW + 7 * DAY, NOW + DAY),
            // Every non-positive request selects the configured lifetime.
            (0, 7 * DAY, DAY, NOW + 7 * DAY, NOW + DAY),
            (-2, 7 * DAY, DAY, NOW + 7 * DAY, NOW + DAY),
            (i64::MIN, 7 * DAY, DAY, NOW + 7 * DAY, NOW + DAY),
            // A positive request below the ceiling is honoured.
            (2 * DAY, 7 * DAY, DAY, NOW + 2 * DAY, NOW + DAY),
            // A request above the ceiling is capped at it.
            (30 * DAY, 7 * DAY, DAY, NOW + 7 * DAY, NOW + DAY),
            // A lifetime shorter than the renew period caps the first expiry.
            (HOUR, 7 * DAY, DAY, NOW + HOUR, NOW + HOUR),
            // Both sums saturate at Long.MAX_VALUE instead of failing.
            (-1, i64::MAX, DAY, i64::MAX, NOW + DAY),
            (-1, i64::MAX, i64::MAX, i64::MAX, i64::MAX),
        ] {
            check!(
                create_token_deadlines(NOW, requested, max_lifetime, renew_period)
                    == TokenDeadlines {
                        max_timestamp_ms: max_timestamp,
                        initial_expiry_ms: initial_expiry,
                    },
                "requested {requested}, max lifetime {max_lifetime}, renew {renew_period}"
            );
        }
    }

    #[test]
    fn renew_matches_kafka_renew_delegation_token() {
        use TokenRenewDecision::{Expired, Renew};

        // (requested renewPeriodMs, expiry time config, current expiry,
        //  max timestamp, decision), all at NOW.
        for (requested, default_period, current, max_timestamp, expected) in [
            // -1 and every other non-positive period select the default.
            (-1, DAY, NOW + HOUR, NOW + 7 * DAY, Renew(NOW + DAY)),
            (0, DAY, NOW + HOUR, NOW + 7 * DAY, Renew(NOW + DAY)),
            (-7, DAY, NOW + HOUR, NOW + 7 * DAY, Renew(NOW + DAY)),
            // A positive period is capped at the default, not at max.
            (30 * DAY, DAY, NOW + HOUR, NOW + 7 * DAY, Renew(NOW + DAY)),
            (
                2 * HOUR,
                DAY,
                NOW + HOUR,
                NOW + 7 * DAY,
                Renew(NOW + 2 * HOUR),
            ),
            // A short period shortens a later expiry: the result replaces it.
            (1, DAY, NOW + 12 * HOUR, NOW + 7 * DAY, Renew(NOW + 1)),
            // The token's max timestamp caps the renewed expiry.
            (-1, DAY, NOW + HOUR, NOW + 2 * HOUR, Renew(NOW + 2 * HOUR)),
            // An expiry or max exactly at now is not yet expired.
            (-1, DAY, NOW, NOW + 7 * DAY, Renew(NOW + DAY)),
            (-1, DAY, NOW, NOW, Renew(NOW)),
            // The sum saturates at i64::MAX.
            (-1, i64::MAX, NOW + HOUR, i64::MAX, Renew(i64::MAX)),
            // Either deadline before now means expired.
            (-1, DAY, NOW - 1, NOW + 7 * DAY, Expired),
            (-1, DAY, NOW + HOUR, NOW - 1, Expired),
        ] {
            check!(
                renew_token_expiry(NOW, requested, default_period, current, max_timestamp)
                    == expected,
                "requested {requested}, default {default_period}, current {current}, max {max_timestamp}"
            );
        }
    }

    #[test]
    fn expire_matches_kafka_expire_delegation_token() {
        use TokenExpireDecision::{Delete, Expired, Update};

        // (expiryTimePeriodMs, current expiry, max timestamp, decision)
        for (period, current, max_timestamp, expected) in [
            // Any negative period deletes, even an already expired token.
            (-1, NOW + HOUR, NOW + DAY, Delete),
            (i64::MIN, NOW - 1, NOW - 1, Delete),
            // Zero expires at now.
            (0, NOW + HOUR, NOW + DAY, Update(NOW)),
            // A positive period may lengthen or shorten, up to max.
            (2 * HOUR, NOW + HOUR, NOW + DAY, Update(NOW + 2 * HOUR)),
            (1, NOW + HOUR, NOW + DAY, Update(NOW + 1)),
            (2 * DAY, NOW + HOUR, NOW + DAY, Update(NOW + DAY)),
            // The sum saturates, then max caps it.
            (i64::MAX, NOW + HOUR, NOW + DAY, Update(NOW + DAY)),
            (i64::MAX, NOW + HOUR, i64::MAX, Update(i64::MAX)),
            // Deadlines exactly at now are live; before now are expired.
            (0, NOW, NOW, Update(NOW)),
            (0, NOW - 1, NOW + DAY, Expired),
            (0, NOW + HOUR, NOW - 1, Expired),
        ] {
            check!(
                expire_token_deadline(NOW, period, current, max_timestamp) == expected,
                "period {period}, current {current}, max {max_timestamp}"
            );
        }
    }

    #[test]
    fn liveness_is_the_negation_of_kafkas_expiry_test() {
        // (expiry, max timestamp, live) at NOW.
        for (expiry, max_timestamp, active) in [
            (NOW + 1, NOW + 1, true),
            (NOW, NOW, true),
            (NOW - 1, NOW + 1, false),
            (NOW + 1, NOW - 1, false),
        ] {
            check!(token_is_active(NOW, expiry, max_timestamp) == active);
        }
        for (expiry, not_passed) in [(NOW + 1, true), (NOW, true), (NOW - 1, false)] {
            check!(token_expiry_not_passed(NOW, expiry) == not_passed);
        }
    }

    #[test]
    fn token_mutations_are_generation_bound_and_idempotent() {
        use TokenMutationDecision::{Append, Reject, Retry};
        use TokenMutationKind::{Delete, Expire, Renew};
        use TokenMutationState::{Applied, Expected, Missing, Stale};

        let live = TokenMutationFacts {
            kind: Renew,
            state: Expected,
            now_ms: NOW,
            expected_expiry_ms: NOW + HOUR,
            incoming_expiry_ms: NOW + DAY,
            max_timestamp_ms: NOW + 7 * DAY,
            uncommitted_tail: false,
        };
        let expired = TokenMutationFacts {
            expected_expiry_ms: NOW - 1,
            ..live
        };
        let past_max = TokenMutationFacts {
            max_timestamp_ms: NOW - 1,
            expected_expiry_ms: NOW - 1,
            ..live
        };
        let cases = [
            // Renew on the expected generation.
            (live, Append),
            // A renewal may shorten the expiry, as Kafka's does.
            (
                TokenMutationFacts {
                    incoming_expiry_ms: NOW + 1,
                    ..live
                },
                Append,
            ),
            (
                TokenMutationFacts {
                    incoming_expiry_ms: NOW + 7 * DAY,
                    ..live
                },
                Append,
            ),
            (
                TokenMutationFacts {
                    incoming_expiry_ms: NOW + HOUR,
                    ..live
                },
                Retry,
            ),
            (
                TokenMutationFacts {
                    incoming_expiry_ms: NOW + 7 * DAY + 1,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    expected_expiry_ms: NOW,
                    max_timestamp_ms: NOW,
                    incoming_expiry_ms: NOW,
                    ..live
                },
                Retry,
            ),
            (expired, Reject),
            (
                TokenMutationFacts {
                    incoming_expiry_ms: NOW - 1,
                    ..expired
                },
                Reject,
            ),
            (past_max, Reject),
            // Expire on the expected generation, including a no-op value.
            (
                TokenMutationFacts {
                    kind: Expire,
                    ..live
                },
                Append,
            ),
            (
                TokenMutationFacts {
                    kind: Expire,
                    incoming_expiry_ms: NOW + HOUR,
                    ..live
                },
                Append,
            ),
            (
                TokenMutationFacts {
                    kind: Expire,
                    incoming_expiry_ms: NOW + 7 * DAY + 1,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    kind: Expire,
                    ..expired
                },
                Reject,
            ),
            // Delete appends even for an expired token.
            (
                TokenMutationFacts {
                    kind: Delete,
                    ..expired
                },
                Append,
            ),
            (
                TokenMutationFacts {
                    kind: Delete,
                    ..past_max
                },
                Append,
            ),
            // Generation outcomes.
            (
                TokenMutationFacts {
                    state: Applied,
                    ..expired
                },
                Retry,
            ),
            (
                TokenMutationFacts {
                    kind: Delete,
                    state: Missing,
                    ..live
                },
                Retry,
            ),
            (
                TokenMutationFacts {
                    state: Missing,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    kind: Expire,
                    state: Missing,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    state: Stale,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    kind: Delete,
                    state: Stale,
                    ..live
                },
                Reject,
            ),
            // A retained uncommitted tail rejects everything.
            (
                TokenMutationFacts {
                    uncommitted_tail: true,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    state: Applied,
                    uncommitted_tail: true,
                    ..live
                },
                Reject,
            ),
            (
                TokenMutationFacts {
                    kind: Delete,
                    state: Missing,
                    uncommitted_tail: true,
                    ..live
                },
                Reject,
            ),
        ];
        for (facts, expected) in cases {
            check!(token_mutation_decision(facts) == expected, "{facts:?}");
        }
    }

    /// The renew kernel's output always passes the controller fence for the
    /// same generation: a renewal the handler computes is never refused as a
    /// regression, whether it lengthens, keeps, or shortens the expiry.
    #[test]
    fn every_renewal_passes_the_controller_fence() {
        for (requested, current) in [
            (-1, NOW + HOUR),
            (1, NOW + 12 * HOUR),
            (DAY, NOW + DAY),
            (30 * DAY, NOW),
        ] {
            let max_timestamp = NOW + 7 * DAY;
            let TokenRenewDecision::Renew(incoming) =
                renew_token_expiry(NOW, requested, DAY, current, max_timestamp)
            else {
                panic!("live token must renew");
            };
            let decision = token_mutation_decision(TokenMutationFacts {
                kind: TokenMutationKind::Renew,
                state: TokenMutationState::Expected,
                now_ms: NOW,
                expected_expiry_ms: current,
                incoming_expiry_ms: incoming,
                max_timestamp_ms: max_timestamp,
                uncommitted_tail: false,
            });
            check!(
                decision != TokenMutationDecision::Reject,
                "requested {requested}, current {current}"
            );
        }
    }
}
