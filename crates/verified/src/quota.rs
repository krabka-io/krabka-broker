//! Kafka quota-entity precedence.
//!
//! Kafka's default quota callback, `ClientQuotaManager.DefaultQuotaCallback`
//! (`findUserClientQuota`, `findUserQuota`, `findClientQuota`, and the tag
//! selection in `quotaMetricTags`), consults the user and client-id quota
//! entities in one fixed order and uses the first one that is configured:
//!
//! | Rank | Entity path | Candidate |
//! | ---: | :--- | :--- |
//! | 0 | `/config/users/<user>/clients/<client-id>` | [`UserClient`] |
//! | 1 | `/config/users/<user>/clients/<default>` | [`UserDefaultClient`] |
//! | 2 | `/config/users/<user>` | [`User`] |
//! | 3 | `/config/users/<default>/clients/<client-id>` | [`DefaultUserClient`] |
//! | 4 | `/config/users/<default>/clients/<default>` | [`DefaultUserDefaultClient`] |
//! | 5 | `/config/users/<default>` | [`DefaultUser`] |
//! | 6 | `/config/clients/<client-id>` | [`Client`] |
//! | 7 | `/config/clients/<default>` | [`DefaultClient`] |
//!
//! The Creusot logic function `kafka_rank` states this table once, and
//! [`user_client_quota_precedence`] is proved to return the first present
//! candidate in it (`first_present`).
//!
//! Source: [`ClientQuotaManager.java`].
//!
//! [`UserClient`]: UserClientQuotaPrecedence::UserClient
//! [`UserDefaultClient`]: UserClientQuotaPrecedence::UserDefaultClient
//! [`User`]: UserClientQuotaPrecedence::User
//! [`DefaultUserClient`]: UserClientQuotaPrecedence::DefaultUserClient
//! [`DefaultUserDefaultClient`]: UserClientQuotaPrecedence::DefaultUserDefaultClient
//! [`DefaultUser`]: UserClientQuotaPrecedence::DefaultUser
//! [`Client`]: UserClientQuotaPrecedence::Client
//! [`DefaultClient`]: UserClientQuotaPrecedence::DefaultClient
//! [`ClientQuotaManager.java`]: https://github.com/apache/kafka/blob/trunk/server/src/main/java/org/apache/kafka/server/quota/ClientQuotaManager.java

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Selected user/client quota candidate, named after its Kafka entity path.
///
/// The variants are declared in Kafka's precedence order. `None` means that no
/// candidate is configured.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum UserClientQuotaPrecedence {
    /// `/config/users/<user>/clients/<client-id>`.
    UserClient,
    /// `/config/users/<user>/clients/<default>`.
    UserDefaultClient,
    /// `/config/users/<user>`.
    User,
    /// `/config/users/<default>/clients/<client-id>`.
    DefaultUserClient,
    /// `/config/users/<default>/clients/<default>`.
    DefaultUserDefaultClient,
    /// `/config/users/<default>`.
    DefaultUser,
    /// `/config/clients/<client-id>`.
    Client,
    /// `/config/clients/<default>`.
    DefaultClient,
    /// No candidate is configured.
    None,
}

/// Whether one canonical quota candidate exists in the metadata image.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum QuotaCandidatePresence {
    Absent,
    Present,
}

/// Presence of each canonical user/client quota candidate, one field per
/// [`UserClientQuotaPrecedence`] variant of the same name.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct UserClientQuotaFacts {
    pub user_client: QuotaCandidatePresence,
    pub user_default_client: QuotaCandidatePresence,
    pub user: QuotaCandidatePresence,
    pub default_user_client: QuotaCandidatePresence,
    pub default_user_default_client: QuotaCandidatePresence,
    pub default_user: QuotaCandidatePresence,
    pub client: QuotaCandidatePresence,
    pub default_client: QuotaCandidatePresence,
}

/// Kafka's precedence table: the rank at which `DefaultQuotaCallback`
/// consults each candidate, 0 first. `None` ranks after every candidate.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn kafka_rank(candidate: UserClientQuotaPrecedence) -> Int {
    pearlite! {
        match candidate {
            UserClientQuotaPrecedence::UserClient => 0,
            UserClientQuotaPrecedence::UserDefaultClient => 1,
            UserClientQuotaPrecedence::User => 2,
            UserClientQuotaPrecedence::DefaultUserClient => 3,
            UserClientQuotaPrecedence::DefaultUserDefaultClient => 4,
            UserClientQuotaPrecedence::DefaultUser => 5,
            UserClientQuotaPrecedence::Client => 6,
            UserClientQuotaPrecedence::DefaultClient => 7,
            UserClientQuotaPrecedence::None => 8,
        }
    }
}

/// Whether `facts` reports `candidate` as configured. `None` is never present.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn is_present(facts: UserClientQuotaFacts, candidate: UserClientQuotaPrecedence) -> bool {
    pearlite! {
        match candidate {
            UserClientQuotaPrecedence::UserClient =>
                facts.user_client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::UserDefaultClient =>
                facts.user_default_client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::User =>
                facts.user == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::DefaultUserClient =>
                facts.default_user_client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::DefaultUserDefaultClient =>
                facts.default_user_default_client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::DefaultUser =>
                facts.default_user == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::Client =>
                facts.client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::DefaultClient =>
                facts.default_client == QuotaCandidatePresence::Present,
            UserClientQuotaPrecedence::None => false,
        }
    }
}

/// `candidate` is the first present entry of `kafka_rank`'s table: it is
/// present (or it is `None`), and every candidate Kafka consults before it is
/// absent. For `None` this says that no candidate is present.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn first_present(facts: UserClientQuotaFacts, candidate: UserClientQuotaPrecedence) -> bool {
    pearlite! {
        (is_present(facts, candidate) || candidate == UserClientQuotaPrecedence::None)
            && forall<earlier: UserClientQuotaPrecedence>
                kafka_rank(earlier) < kafka_rank(candidate) ==> !is_present(facts, earlier)
    }
}

/// At most one candidate is first present, so `first_present` pins the
/// selector's result for every input and no variant is left unconstrained.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(first_present(facts, a) && first_present(facts, b))]
#[ensures(a == b)]
pub fn lemma_first_present_unique(
    facts: UserClientQuotaFacts,
    a: UserClientQuotaPrecedence,
    b: UserClientQuotaPrecedence,
) {
}

/// Select Kafka's first present user/client quota candidate.
#[ensures(first_present(facts, result))]
#[must_use]
pub fn user_client_quota_precedence(facts: UserClientQuotaFacts) -> UserClientQuotaPrecedence {
    if matches!(facts.user_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::UserClient
    } else if matches!(facts.user_default_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::UserDefaultClient
    } else if matches!(facts.user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::User
    } else if matches!(facts.default_user_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultUserClient
    } else if matches!(
        facts.default_user_default_client,
        QuotaCandidatePresence::Present
    ) {
        UserClientQuotaPrecedence::DefaultUserDefaultClient
    } else if matches!(facts.default_user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultUser
    } else if matches!(facts.client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::Client
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
    use assert2::check;

    use super::{
        IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts,
        UserClientQuotaPrecedence::{
            self, Client, DefaultClient, DefaultUser, DefaultUserClient, DefaultUserDefaultClient,
            User, UserClient, UserDefaultClient,
        },
        ip_quota_precedence, user_client_quota_precedence,
    };

    /// Facts in which exactly the listed candidates are configured.
    fn configured(candidates: &[UserClientQuotaPrecedence]) -> UserClientQuotaFacts {
        let presence = |candidate| {
            if candidates.contains(&candidate) {
                QuotaCandidatePresence::Present
            } else {
                QuotaCandidatePresence::Absent
            }
        };
        UserClientQuotaFacts {
            user_client: presence(UserClient),
            user_default_client: presence(UserDefaultClient),
            user: presence(User),
            default_user_client: presence(DefaultUserClient),
            default_user_default_client: presence(DefaultUserDefaultClient),
            default_user: presence(DefaultUser),
            client: presence(Client),
            default_client: presence(DefaultClient),
        }
    }

    /// Rows from the "Quotas" section of the Kafka documentation and
    /// `ClientQuotaManager.DefaultQuotaCallback`, which consult
    /// `/config/users/<user>/clients/<client-id>`,
    /// `/config/users/<user>/clients/<default>`, `/config/users/<user>`,
    /// `/config/users/<default>/clients/<client-id>`,
    /// `/config/users/<default>/clients/<default>`, `/config/users/<default>`,
    /// `/config/clients/<client-id>`, and `/config/clients/<default>`, in that
    /// order.
    #[test]
    fn user_client_selector_follows_kafka_precedence() {
        let rows: [(
            &str,
            &[UserClientQuotaPrecedence],
            UserClientQuotaPrecedence,
        ); 20] = [
            ("nothing configured", &[], UserClientQuotaPrecedence::None),
            ("only the user/client pair", &[UserClient], UserClient),
            (
                "only the user's default client",
                &[UserDefaultClient],
                UserDefaultClient,
            ),
            ("only the user", &[User], User),
            (
                "only the default user's client",
                &[DefaultUserClient],
                DefaultUserClient,
            ),
            (
                "only the default pair",
                &[DefaultUserDefaultClient],
                DefaultUserDefaultClient,
            ),
            ("only the default user", &[DefaultUser], DefaultUser),
            ("only the client", &[Client], Client),
            ("only the default client", &[DefaultClient], DefaultClient),
            (
                "an exact user beats a default user with an exact client",
                &[User, DefaultUserClient],
                User,
            ),
            (
                "an exact user beats the default pair",
                &[User, DefaultUserDefaultClient],
                User,
            ),
            (
                "the user's default client beats the user alone",
                &[UserDefaultClient, User],
                UserDefaultClient,
            ),
            (
                "the user's default client beats a default user with an exact client",
                &[UserDefaultClient, DefaultUserClient],
                UserDefaultClient,
            ),
            (
                "the default pair beats the default user alone",
                &[DefaultUserDefaultClient, DefaultUser],
                DefaultUserDefaultClient,
            ),
            (
                "a default user beats an exact client",
                &[DefaultUser, Client],
                DefaultUser,
            ),
            (
                "a default user with an exact client beats the exact client alone",
                &[DefaultUserClient, Client],
                DefaultUserClient,
            ),
            (
                "an exact client beats the default client",
                &[Client, DefaultClient],
                Client,
            ),
            ("an exact user beats an exact client", &[User, Client], User),
            (
                "everything configured selects the exact pair",
                &[
                    UserClient,
                    UserDefaultClient,
                    User,
                    DefaultUserClient,
                    DefaultUserDefaultClient,
                    DefaultUser,
                    Client,
                    DefaultClient,
                ],
                UserClient,
            ),
            (
                "everything but the exact pair selects the user's default client",
                &[
                    UserDefaultClient,
                    User,
                    DefaultUserClient,
                    DefaultUserDefaultClient,
                    DefaultUser,
                    Client,
                    DefaultClient,
                ],
                UserDefaultClient,
            ),
        ];
        for (scenario, candidates, expected) in rows {
            check!(
                user_client_quota_precedence(configured(candidates)) == expected,
                "{scenario}"
            );
        }
    }

    #[test]
    fn ip_selector_prefers_the_exact_ip() {
        let rows = [
            (true, true, IpQuotaPrecedence::Exact),
            (true, false, IpQuotaPrecedence::Exact),
            (false, true, IpQuotaPrecedence::Default),
            (false, false, IpQuotaPrecedence::None),
        ];
        for (exact, default, expected) in rows {
            check!(
                ip_quota_precedence(exact, default) == expected,
                "exact={exact} default={default}"
            );
        }
    }
}
