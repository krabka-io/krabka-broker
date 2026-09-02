//! Kafka quota-entity precedence.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Selected user/client quota candidate, ordered from most to least specific.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum UserClientQuotaPrecedence {
    ExactPair,
    ExactClientDefaultUser,
    DefaultClientExactUser,
    DefaultPair,
    ExactUser,
    ExactClient,
    DefaultUser,
    DefaultClient,
    None,
}

/// Whether one canonical quota candidate exists in the metadata image.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum QuotaCandidatePresence {
    Absent,
    Present,
}

/// Presence of each canonical user/client quota candidate.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct UserClientQuotaFacts {
    pub exact_pair: QuotaCandidatePresence,
    pub exact_client_default_user: QuotaCandidatePresence,
    pub default_client_exact_user: QuotaCandidatePresence,
    pub default_pair: QuotaCandidatePresence,
    pub exact_user: QuotaCandidatePresence,
    pub exact_client: QuotaCandidatePresence,
    pub default_user: QuotaCandidatePresence,
    pub default_client: QuotaCandidatePresence,
}

/// Select Kafka's first present user/client quota candidate.
#[ensures((result == UserClientQuotaPrecedence::ExactPair)
    == (facts.exact_pair == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactClientDefaultUser)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultClientExactUser)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultPair)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactUser)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Absent
        && facts.exact_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactClient)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Absent
        && facts.exact_user == QuotaCandidatePresence::Absent
        && facts.exact_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultUser)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Absent
        && facts.exact_user == QuotaCandidatePresence::Absent
        && facts.exact_client == QuotaCandidatePresence::Absent
        && facts.default_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultClient)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_client_default_user == QuotaCandidatePresence::Absent
        && facts.default_client_exact_user == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Absent
        && facts.exact_user == QuotaCandidatePresence::Absent
        && facts.exact_client == QuotaCandidatePresence::Absent
        && facts.default_user == QuotaCandidatePresence::Absent
        && facts.default_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::None) == !(
    facts.exact_pair == QuotaCandidatePresence::Present
        || facts.exact_client_default_user == QuotaCandidatePresence::Present
        || facts.default_client_exact_user == QuotaCandidatePresence::Present
        || facts.default_pair == QuotaCandidatePresence::Present
        || facts.exact_user == QuotaCandidatePresence::Present
        || facts.exact_client == QuotaCandidatePresence::Present
        || facts.default_user == QuotaCandidatePresence::Present
        || facts.default_client == QuotaCandidatePresence::Present))]
#[must_use]
pub fn user_client_quota_precedence(facts: UserClientQuotaFacts) -> UserClientQuotaPrecedence {
    if matches!(facts.exact_pair, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactPair
    } else if matches!(
        facts.exact_client_default_user,
        QuotaCandidatePresence::Present
    ) {
        UserClientQuotaPrecedence::ExactClientDefaultUser
    } else if matches!(
        facts.default_client_exact_user,
        QuotaCandidatePresence::Present
    ) {
        UserClientQuotaPrecedence::DefaultClientExactUser
    } else if matches!(facts.default_pair, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultPair
    } else if matches!(facts.exact_user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactUser
    } else if matches!(facts.exact_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactClient
    } else if matches!(facts.default_user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultUser
    } else if matches!(facts.default_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultClient
    } else {
        UserClientQuotaPrecedence::None
    }
}

/// Selected IP quota candidate.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IpQuotaPrecedence {
    Exact,
    Default,
    None,
}

/// Select an exact IP quota before the IP default.
#[ensures((result == IpQuotaPrecedence::Exact) == exact)]
#[ensures((result == IpQuotaPrecedence::Default) == (!exact && default))]
#[ensures((result == IpQuotaPrecedence::None) == (!exact && !default))]
#[must_use]
pub fn ip_quota_precedence(exact: bool, default: bool) -> IpQuotaPrecedence {
    if exact {
        IpQuotaPrecedence::Exact
    } else if default {
        IpQuotaPrecedence::Default
    } else {
        IpQuotaPrecedence::None
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts, UserClientQuotaPrecedence,
        ip_quota_precedence, user_client_quota_precedence,
    };

    #[test]
    fn selectors_choose_the_first_present_candidate() {
        for mask in 0_u16..256 {
            let present = |index| mask & (1_u16 << index) != 0_u16;
            let candidate = |index| {
                if present(index) {
                    QuotaCandidatePresence::Present
                } else {
                    QuotaCandidatePresence::Absent
                }
            };
            let got = user_client_quota_precedence(UserClientQuotaFacts {
                exact_pair: candidate(0),
                exact_client_default_user: candidate(1),
                default_client_exact_user: candidate(2),
                default_pair: candidate(3),
                exact_user: candidate(4),
                exact_client: candidate(5),
                default_user: candidate(6),
                default_client: candidate(7),
            });
            let expected = match (0..8).find(|index| present(*index)) {
                Some(0) => UserClientQuotaPrecedence::ExactPair,
                Some(1) => UserClientQuotaPrecedence::ExactClientDefaultUser,
                Some(2) => UserClientQuotaPrecedence::DefaultClientExactUser,
                Some(3) => UserClientQuotaPrecedence::DefaultPair,
                Some(4) => UserClientQuotaPrecedence::ExactUser,
                Some(5) => UserClientQuotaPrecedence::ExactClient,
                Some(6) => UserClientQuotaPrecedence::DefaultUser,
                Some(7) => UserClientQuotaPrecedence::DefaultClient,
                Some(_) | None => UserClientQuotaPrecedence::None,
            };
            assert2::check!(got == expected, "mask {mask:#010b}");
        }
        assert2::check!(ip_quota_precedence(true, true) == IpQuotaPrecedence::Exact);
        assert2::check!(ip_quota_precedence(false, true) == IpQuotaPrecedence::Default);
        assert2::check!(ip_quota_precedence(false, false) == IpQuotaPrecedence::None);
    }
}
