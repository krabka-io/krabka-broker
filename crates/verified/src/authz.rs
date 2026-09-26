//! Kafka ACL precedence: super-user bypass, deny-wins, and default deny.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Why ACL evaluation allowed or denied a request.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum AclDecision {
    AllowSuperuser,
    AllowAcl,
    /// `allow.everyone.if.no.acl.found` allowed the request because no ACL
    /// at all applies to the resource.
    AllowNoAcl,
    DenyExplicit,
    DenyDefault,
}

/// Resource-pattern class used by the verified ACL applicability adapter.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum AclPatternKind {
    Literal,
    Prefixed,
}

/// ACL operation class used by the verified implication table.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum AclOperationKind {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    TwoPhaseCommit,
}

/// Match a principal or host by exact equality or its axis-specific wildcard.
#[ensures(result == (wildcard || exact))]
#[must_use]
pub fn acl_identity_match(wildcard: bool, exact: bool) -> bool {
    wildcard || exact
}

/// Whether a stored ACL names the requested resource type.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum AclResourceTypeMatch {
    Same,
    Different,
}

/// How a stored ACL's resource name relates to the requested resource name.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct AclResourceFacts {
    pub resource_type: AclResourceTypeMatch,
    /// The stored name equals the requested name.
    pub exact_name: bool,
    /// The stored name is the literal wildcard `*`.
    pub wildcard_name: bool,
    /// The requested name starts with the stored name.
    pub name_has_prefix: bool,
}

/// Match an ACL resource type and literal or prefixed name pattern.
///
/// Kafka's `AclAuthorizer.matchingAcls`: a LITERAL ACL applies to its exact
/// name and, when that name is `*`, to every name; a PREFIXED ACL applies to
/// every name that starts with its name. Neither applies across resource
/// types.
#[ensures(result == (facts.resource_type == AclResourceTypeMatch::Same && match pattern {
    AclPatternKind::Literal => facts.exact_name || facts.wildcard_name,
    AclPatternKind::Prefixed => facts.name_has_prefix,
}))]
#[must_use]
pub fn acl_resource_match(pattern: AclPatternKind, facts: AclResourceFacts) -> bool {
    match (facts.resource_type, pattern) {
        (AclResourceTypeMatch::Different, _) => false,
        (AclResourceTypeMatch::Same, AclPatternKind::Literal) => {
            facts.exact_name || facts.wildcard_name
        }
        (AclResourceTypeMatch::Same, AclPatternKind::Prefixed) => facts.name_has_prefix,
    }
}

/// Match exact operations, `All`, and Kafka's one-way implication arrows.
///
/// The implication arrows (`Read`/`Write`/`Delete`/`Alter` → `Describe`, and
/// `AlterConfigs` → `DescribeConfigs`) apply only when the stored ACL is an
/// ALLOW entry: Kafka's operation-implication table never widens what a DENY
/// ACL blocks.
#[ensures(result == (stored == requested
    || stored == AclOperationKind::All
    || (is_allow && stored == AclOperationKind::Read && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Write && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Delete && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Alter && requested == AclOperationKind::Describe)
    || (is_allow
        && stored == AclOperationKind::AlterConfigs
        && requested == AclOperationKind::DescribeConfigs)))]
#[must_use]
pub fn acl_operation_match(
    stored: AclOperationKind,
    requested: AclOperationKind,
    is_allow: bool,
) -> bool {
    match stored {
        AclOperationKind::All => true,
        AclOperationKind::Read => {
            matches!(requested, AclOperationKind::Read)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Write => {
            matches!(requested, AclOperationKind::Write)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Delete => {
            matches!(requested, AclOperationKind::Delete)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Alter => {
            matches!(requested, AclOperationKind::Alter)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::AlterConfigs => {
            matches!(requested, AclOperationKind::AlterConfigs)
                || (is_allow && matches!(requested, AclOperationKind::DescribeConfigs))
        }
        AclOperationKind::Create => matches!(requested, AclOperationKind::Create),
        AclOperationKind::Describe => matches!(requested, AclOperationKind::Describe),
        AclOperationKind::ClusterAction => matches!(requested, AclOperationKind::ClusterAction),
        AclOperationKind::DescribeConfigs => {
            matches!(requested, AclOperationKind::DescribeConfigs)
        }
        AclOperationKind::IdempotentWrite => {
            matches!(requested, AclOperationKind::IdempotentWrite)
        }
        AclOperationKind::TwoPhaseCommit => {
            matches!(requested, AclOperationKind::TwoPhaseCommit)
        }
    }
}

/// Authentication phase used to admit a Kafka request.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RequestAuthState {
    PreAuth,
    Reauthenticating,
    Authenticated,
}

/// Decide whether an API key may run in the current authentication phase.
#[ensures(state == RequestAuthState::Authenticated ==> result)]
#[ensures(state == RequestAuthState::Reauthenticating ==>
    result == (api_key@ == 36))]
#[ensures(state == RequestAuthState::PreAuth ==>
    result == (api_key@ == 17 || api_key@ == 18 || api_key@ == 36))]
#[must_use]
pub fn request_auth_admission(state: RequestAuthState, api_key: i16) -> bool {
    match state {
        RequestAuthState::Authenticated => true,
        RequestAuthState::Reauthenticating => api_key == 36,
        RequestAuthState::PreAuth => matches!(api_key, 17 | 18 | 36),
    }
}

/// The outcome when no ACL matched the request.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum AclDefault {
    Deny,
    /// `allow.everyone.if.no.acl.found` is set and no ACL at all applies to
    /// the resource.
    Allow,
}

/// What the precedence loop observed for one request.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct AclFacts {
    pub super_user: bool,
    /// Some applicable ALLOW ACL matched the principal, host, and operation.
    pub saw_allow: bool,
    /// Some applicable DENY ACL matched the principal, host, and operation.
    pub saw_deny: bool,
    pub default_decision: AclDefault,
}

/// Decide a request from whether any matching ACL allowed or denied it.
///
/// Kafka's `AclAuthorizer.authorizeAction` order: a super user is always
/// allowed; otherwise a matching DENY wins over any ALLOW; otherwise a
/// matching ALLOW allows; otherwise `default_decision` decides. It reflects
/// `allow.everyone.if.no.acl.found` (default `false`), which allows a request
/// only when no ACL at all applies to the resource, so it never overrides an
/// explicit ALLOW or DENY.
#[ensures(match result {
    AclDecision::AllowSuperuser => facts.super_user,
    AclDecision::DenyExplicit => !facts.super_user && facts.saw_deny,
    AclDecision::AllowAcl => !facts.super_user && !facts.saw_deny && facts.saw_allow,
    AclDecision::AllowNoAcl => !facts.super_user && !facts.saw_deny && !facts.saw_allow
        && facts.default_decision == AclDefault::Allow,
    AclDecision::DenyDefault => !facts.super_user && !facts.saw_deny && !facts.saw_allow
        && facts.default_decision == AclDefault::Deny,
})]
#[must_use]
pub fn acl_decision(facts: AclFacts) -> AclDecision {
    if facts.super_user {
        AclDecision::AllowSuperuser
    } else if facts.saw_deny {
        AclDecision::DenyExplicit
    } else if facts.saw_allow {
        AclDecision::AllowAcl
    } else {
        match facts.default_decision {
            AclDefault::Allow => AclDecision::AllowNoAcl,
            AclDefault::Deny => AclDecision::DenyDefault,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn facts() -> AclFacts {
        AclFacts {
            super_user: false,
            saw_allow: false,
            saw_deny: false,
            default_decision: AclDefault::Deny,
        }
    }

    /// Kafka `AclAuthorizer` precedence scenarios.
    #[test]
    fn acl_precedence_matches_kafka_scenarios() {
        use AclDecision::{AllowAcl, AllowNoAcl, AllowSuperuser, DenyDefault, DenyExplicit};

        for (scenario, facts, expected) in [
            ("no ACL and the default config denies", facts(), DenyDefault),
            (
                "a matching ALLOW allows",
                AclFacts {
                    saw_allow: true,
                    ..facts()
                },
                AllowAcl,
            ),
            (
                "a matching DENY denies",
                AclFacts {
                    saw_deny: true,
                    ..facts()
                },
                DenyExplicit,
            ),
            (
                "DENY takes precedence over a matching ALLOW",
                AclFacts {
                    saw_allow: true,
                    saw_deny: true,
                    ..facts()
                },
                DenyExplicit,
            ),
            (
                "a super user bypasses a matching DENY",
                AclFacts {
                    super_user: true,
                    saw_deny: true,
                    ..facts()
                },
                AllowSuperuser,
            ),
            (
                "allow.everyone.if.no.acl.found allows a resource with no ACL",
                AclFacts {
                    default_decision: AclDefault::Allow,
                    ..facts()
                },
                AllowNoAcl,
            ),
            (
                "allow.everyone.if.no.acl.found does not override a DENY",
                AclFacts {
                    saw_deny: true,
                    default_decision: AclDefault::Allow,
                    ..facts()
                },
                DenyExplicit,
            ),
            (
                "an explicit ALLOW is reported as an ACL grant, not the default",
                AclFacts {
                    saw_allow: true,
                    default_decision: AclDefault::Allow,
                    ..facts()
                },
                AllowAcl,
            ),
        ] {
            check!(acl_decision(facts) == expected, "{scenario}");
        }
    }

    /// Kafka principal and host matching: `User:*` and host `*` are wildcards.
    #[test]
    fn identity_wildcards_match_kafka_scenarios() {
        // (scenario, stored is the wildcard, stored equals the request, expected)
        for (scenario, wildcard, exact, expected) in [
            ("User:alice ACL, request from User:alice", false, true, true),
            ("User:* ACL, request from User:alice", true, false, true),
            ("User:bob ACL, request from User:alice", false, false, false),
            ("host * ACL, request from any host", true, false, true),
        ] {
            check!(
                acl_identity_match(wildcard, exact) == expected,
                "{scenario}"
            );
        }
    }

    /// Kafka LITERAL, wildcard, and PREFIXED resource patterns against the
    /// requested topic `orders`.
    #[test]
    fn resource_patterns_match_kafka_scenarios() {
        use AclPatternKind::{Literal, Prefixed};
        use AclResourceTypeMatch::{Different, Same};

        let topic = |exact_name, wildcard_name, name_has_prefix| AclResourceFacts {
            resource_type: Same,
            exact_name,
            wildcard_name,
            name_has_prefix,
        };
        for (scenario, pattern, facts, expected) in [
            ("LITERAL orders", Literal, topic(true, false, true), true),
            ("LITERAL *", Literal, topic(false, true, false), true),
            (
                "LITERAL ord is not a prefix",
                Literal,
                topic(false, false, true),
                false,
            ),
            ("PREFIXED ord", Prefixed, topic(false, false, true), true),
            ("PREFIXED orders", Prefixed, topic(true, false, true), true),
            ("PREFIXED pay", Prefixed, topic(false, false, false), false),
            (
                "PREFIXED * is not a wildcard",
                Prefixed,
                topic(false, true, false),
                false,
            ),
            (
                "LITERAL orders on a GROUP",
                Literal,
                AclResourceFacts {
                    resource_type: Different,
                    ..topic(true, false, true)
                },
                false,
            ),
            (
                "PREFIXED ord on a GROUP",
                Prefixed,
                AclResourceFacts {
                    resource_type: Different,
                    ..topic(false, false, true)
                },
                false,
            ),
        ] {
            check!(acl_resource_match(pattern, facts) == expected, "{scenario}");
        }
    }

    /// Kafka's operation-implication table (`AclAuthorizer` /
    /// `AclEntry.supportedOperations`) for stored ALLOW and DENY ACLs.
    #[test]
    fn operation_implications_match_kafka_scenarios() {
        use AclOperationKind::{
            All, Alter, AlterConfigs, ClusterAction, Create, Delete, Describe, DescribeConfigs,
            Read, Write,
        };

        // (stored, requested, stored ACL is ALLOW, expected)
        for (stored, requested, is_allow, expected) in [
            (Read, Read, true, true),
            (Read, Read, false, true),
            (All, ClusterAction, true, true),
            (All, Describe, false, true),
            // ALLOW Read, Write, Delete, and Alter each imply Describe.
            (Read, Describe, true, true),
            (Write, Describe, true, true),
            (Delete, Describe, true, true),
            (Alter, Describe, true, true),
            (AlterConfigs, DescribeConfigs, true, true),
            // A DENY never gains the implied operations.
            (Read, Describe, false, false),
            (AlterConfigs, DescribeConfigs, false, false),
            // The implications are one-way and do not chain further.
            (Describe, Read, true, false),
            (DescribeConfigs, AlterConfigs, true, false),
            (Create, Describe, true, false),
            (Write, Read, true, false),
            (Alter, AlterConfigs, true, false),
        ] {
            check!(acl_operation_match(stored, requested, is_allow) == expected);
        }
    }

    #[test]
    fn request_auth_admission_truth_table() {
        use RequestAuthState::{Authenticated, PreAuth, Reauthenticating};

        for (state, api_key, allowed) in [
            (PreAuth, 17, true),
            (PreAuth, 18, true),
            (PreAuth, 36, true),
            (PreAuth, -1, false),
            (PreAuth, 0, false),
            (Reauthenticating, 36, true),
            (Reauthenticating, 17, false),
            (Reauthenticating, i16::MAX, false),
            (Authenticated, 0, true),
            (Authenticated, i16::MAX, true),
        ] {
            check!(request_auth_admission(state, api_key) == allowed);
        }
    }
}
