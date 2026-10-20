use {
    crate::{
        authz::{
            RequestAuthState, request_auth_admission, sasl_session_expiry,
            session_expired_for_request,
        },
        delegation_token::{
            ScramCredentialSource, TokenCreateDecision, TokenMutationDecision, TokenMutationFacts,
            TokenMutationKind, TokenMutationState, TokenRenewDecision, create_token_deadlines,
            renew_token_expiry, scram_credential_source, token_mutation_decision,
        },
    },
    creusot_std::prelude::*,
};

#[krabka_macros::model_type(proof(DeepModel))]
pub struct RenewalTrace {
    pub original_expiry: i64,
    pub maximum: i64,
    pub renewed_expiry: i64,
    pub renewal: TokenMutationDecision,
    pub cleanup: TokenMutationDecision,
    // Session deadline, advertised lifetime and request admission.
    pub session: Option<(i64, i64, bool)>,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn saturated_sum(left: Int, right: Int) -> Int {
    pearlite! {
        if left + right > i64::MAX@ { i64::MAX@ } else { left + right }
    }
}

/// Renew exactly at the expiry captured by a cleanup worker, then evaluate
/// that delayed cleanup against the resulting committed expiry. A token the
/// cleanup leaves in the store authenticates at any later SCRAM round, as
/// Kafka's token cache does without an expiry check, and its session is bound
/// to the stored expiry: the renewed one when the renewal committed, the
/// original one when an uncommitted tail refused both mutations. At the
/// immutable maximum, renewal is a no-op and cleanup removes the token, so
/// nothing authenticates. One identity and its immutable fields stay fixed;
/// only expiry changes. Commit ordering, faithful metadata projection and
/// cryptographic success remain host obligations. Delays model nondecreasing
/// clock observations, and each clock saturates at `i64::MAX`, so a session
/// whose deadline is `i64::MAX` is never past it. No re-authentication has
/// started on the connection, so past the deadline only a `SaslHandshake`
/// (17) is admitted.
#[requires(issued_ms@ >= 0 && periods.1@ > 0 && periods.2@ > 0 && periods.4@ > 0)]
#[requires(0 <= delays.0@ && delays.0@ <= delays.1@)]
#[ensures(issued_ms@ <= result.original_expiry@ && result.original_expiry@ <= result.maximum@)]
#[ensures(result.original_expiry@ <= result.renewed_expiry@ && result.renewed_expiry@ <= result.maximum@
    && (result.original_expiry@ < result.renewed_expiry@) == (result.original_expiry@ < result.maximum@))]
#[ensures((result.renewal == TokenMutationDecision::Reject) == uncommitted_tail)]
#[ensures((result.renewal == TokenMutationDecision::Append) == (!uncommitted_tail
    && result.original_expiry@ < result.maximum@))]
#[ensures((result.renewal == TokenMutationDecision::Retry) == (!uncommitted_tail
    && result.original_expiry == result.maximum))]
#[ensures((result.cleanup == TokenMutationDecision::Reject) == (uncommitted_tail
    || result.original_expiry@ < result.maximum@))]
#[ensures((result.cleanup == TokenMutationDecision::Append) == (!uncommitted_tail
    && result.original_expiry == result.maximum))]
#[ensures((result.session == None) == (result.cleanup == TokenMutationDecision::Append))]
#[ensures(match result.session {
    None => true,
    Some((at, lifetime, admitted)) =>
        (uncommitted_tail ==> at@ <= result.original_expiry@)
        && (!uncommitted_tail ==> at@ <= result.renewed_expiry@)
        && (cap_ms@ > 0 ==> at@ <= saturated_sum(result.original_expiry@, delays.0@) + cap_ms@)
        && ((uncommitted_tail && at == result.original_expiry)
            || (!uncommitted_tail && at == result.renewed_expiry)
            || (cap_ms@ > 0
                && (at@ == saturated_sum(result.original_expiry@, delays.0@) + cap_ms@
                    || at@ == i64::MAX@)))
        && lifetime@ == (if at@ <= saturated_sum(result.original_expiry@, delays.0@) { 0 }
            else { at@ - saturated_sum(result.original_expiry@, delays.0@) })
        && admitted == (api_key@ == 17
            || saturated_sum(result.original_expiry@, delays.1@) <= at@),
})]
pub(super) fn renewed_token_survives_captured_cleanup(
    issued_ms: i64,
    periods: (i64, i64, i64, i64, i64), // requested lifetime, ceiling, initial, requested/default renewal
    delays: (i64, i64),                 // after original expiry: SCRAM round two and request
    cap_ms: i64,
    uncommitted_tail: bool,
    api_key: i16,
) -> RenewalTrace {
    let TokenCreateDecision::Create(created) =
        create_token_deadlines(issued_ms, periods.0, periods.1, periods.2)
    else {
        unreachable!();
    };
    let old = created.initial_expiry_ms;
    let maximum = created.max_timestamp_ms;
    let TokenRenewDecision::Renew(renewed) =
        renew_token_expiry(old, periods.3, periods.4, old, maximum)
    else {
        unreachable!();
    };
    let renewal = token_mutation_decision(TokenMutationFacts {
        kind: TokenMutationKind::Renew,
        state: TokenMutationState::Expected,
        now_ms: old,
        expected_expiry_ms: old,
        incoming_expiry_ms: renewed,
        max_timestamp_ms: maximum,
        uncommitted_tail,
    });
    let stored = if matches!(renewal, TokenMutationDecision::Append) {
        renewed
    } else {
        old
    };
    let cleanup = token_mutation_decision(TokenMutationFacts {
        kind: TokenMutationKind::Delete,
        state: if stored == old {
            TokenMutationState::Expected
        } else {
            TokenMutationState::Stale
        },
        now_ms: old,
        expected_expiry_ms: old,
        incoming_expiry_ms: old,
        max_timestamp_ms: maximum,
        uncommitted_tail,
    });
    let mut trace = RenewalTrace {
        original_expiry: old,
        maximum,
        renewed_expiry: renewed,
        renewal,
        cleanup,
        session: None,
    };
    if matches!(cleanup, TokenMutationDecision::Append) {
        return trace;
    }
    if !matches!(
        scram_credential_source(false, true, true),
        ScramCredentialSource::DelegationToken
    ) {
        return trace;
    }
    let completed = old.saturating_add(delays.0);
    let (Some(at), lifetime) = sasl_session_expiry(completed, Some(stored), Some(cap_ms)) else {
        unreachable!();
    };
    let request = old.saturating_add(delays.1);
    let admitted = request_auth_admission(RequestAuthState::Authenticated, api_key)
        && !session_expired_for_request(Some(at), api_key, request, None);
    trace.session = Some((at, lifetime, admitted));
    trace
}

#[cfg(test)]
mod tests;
