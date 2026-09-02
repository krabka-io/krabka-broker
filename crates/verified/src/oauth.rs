//! OAuth session-lifetime and reauthentication admission.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Whether token validation supplied an absolute expiry.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OAuthExpiryPresence {
    Missing,
    Present,
}

/// Whether the broker applies a maximum session lifetime.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OAuthSessionCap {
    Disabled,
    Enabled,
}

/// Authentication phase being admitted.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OAuthAuthenticationKind {
    Initial,
    Reauthentication,
}

/// Relationship between the prior and validated principals.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OAuthPrincipalMatch {
    Matches,
    Differs,
}

/// Inputs that bind a validated token to one broker session.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct OAuthSessionFacts {
    pub expiry: OAuthExpiryPresence,
    pub token_expires_at_ms: i64,
    pub now_ms: i64,
    pub cap: OAuthSessionCap,
    pub cap_ms: i64,
    pub authentication: OAuthAuthenticationKind,
    pub principal: OAuthPrincipalMatch,
}

/// Session state selected after token validation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum OAuthSessionDecision {
    Reject,
    Admit {
        session_lifetime_ms: i64,
        effective_expires_at_ms: i64,
    },
}

/// Admit only a positive, exactly representable session lifetime.
#[ensures(match result {
    OAuthSessionDecision::Reject => true,
    OAuthSessionDecision::Admit {
        session_lifetime_ms,
        effective_expires_at_ms,
    } => {
        facts.expiry == OAuthExpiryPresence::Present
            && facts.token_expires_at_ms@ > facts.now_ms@
            && facts.token_expires_at_ms@ - facts.now_ms@ <= 9223372036854775807
            && (facts.cap == OAuthSessionCap::Disabled || facts.cap_ms@ > 0)
            && (facts.authentication == OAuthAuthenticationKind::Initial
                || facts.principal == OAuthPrincipalMatch::Matches)
            && session_lifetime_ms@ == if facts.cap == OAuthSessionCap::Enabled
                && facts.cap_ms@ < facts.token_expires_at_ms@ - facts.now_ms@
            {
                facts.cap_ms@
            } else {
                facts.token_expires_at_ms@ - facts.now_ms@
            }
            && effective_expires_at_ms@ == facts.now_ms@ + session_lifetime_ms@
    }
})]
#[must_use]
pub fn oauth_session_admission(facts: OAuthSessionFacts) -> OAuthSessionDecision {
    if let OAuthExpiryPresence::Missing = facts.expiry {
        return OAuthSessionDecision::Reject;
    }
    if facts.token_expires_at_ms <= facts.now_ms {
        return OAuthSessionDecision::Reject;
    }
    if let (OAuthAuthenticationKind::Reauthentication, OAuthPrincipalMatch::Differs) =
        (facts.authentication, facts.principal)
    {
        return OAuthSessionDecision::Reject;
    }
    if let OAuthSessionCap::Enabled = facts.cap
        && facts.cap_ms <= 0
    {
        return OAuthSessionDecision::Reject;
    }
    let Some(token_lifetime_ms) = facts.token_expires_at_ms.checked_sub(facts.now_ms) else {
        return OAuthSessionDecision::Reject;
    };
    let session_lifetime_ms = match facts.cap {
        OAuthSessionCap::Enabled => token_lifetime_ms.min(facts.cap_ms),
        OAuthSessionCap::Disabled => token_lifetime_ms,
    };
    let Some(effective_expires_at_ms) = facts.now_ms.checked_add(session_lifetime_ms) else {
        return OAuthSessionDecision::Reject;
    };
    OAuthSessionDecision::Admit {
        session_lifetime_ms,
        effective_expires_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OAuthAuthenticationKind, OAuthExpiryPresence, OAuthPrincipalMatch, OAuthSessionCap,
        OAuthSessionDecision, OAuthSessionFacts, oauth_session_admission,
    };

    fn facts() -> OAuthSessionFacts {
        OAuthSessionFacts {
            expiry: OAuthExpiryPresence::Present,
            token_expires_at_ms: 1_100,
            now_ms: 1_000,
            cap: OAuthSessionCap::Disabled,
            cap_ms: 0,
            authentication: OAuthAuthenticationKind::Initial,
            principal: OAuthPrincipalMatch::Matches,
        }
    }

    #[test]
    fn session_expiry_is_exact_and_capped() {
        assert2::check!(
            oauth_session_admission(facts())
                == OAuthSessionDecision::Admit {
                    session_lifetime_ms: 100,
                    effective_expires_at_ms: 1_100,
                }
        );
        assert2::check!(
            oauth_session_admission(OAuthSessionFacts {
                cap: OAuthSessionCap::Enabled,
                cap_ms: 40,
                ..facts()
            }) == OAuthSessionDecision::Admit {
                session_lifetime_ms: 40,
                effective_expires_at_ms: 1_040,
            }
        );
    }

    #[test]
    fn invalid_lifetime_or_principal_fails_closed() {
        for rejected in [
            OAuthSessionFacts {
                expiry: OAuthExpiryPresence::Missing,
                ..facts()
            },
            OAuthSessionFacts {
                token_expires_at_ms: 1_000,
                ..facts()
            },
            OAuthSessionFacts {
                token_expires_at_ms: i64::MAX,
                now_ms: -1,
                ..facts()
            },
            OAuthSessionFacts {
                cap: OAuthSessionCap::Enabled,
                cap_ms: 0,
                ..facts()
            },
            OAuthSessionFacts {
                authentication: OAuthAuthenticationKind::Reauthentication,
                principal: OAuthPrincipalMatch::Differs,
                ..facts()
            },
        ] {
            assert2::check!(oauth_session_admission(rejected) == OAuthSessionDecision::Reject);
        }
    }
}
