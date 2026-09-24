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

/// Match an ACL resource type and literal or prefixed name pattern.
#[ensures(result == (facts.0 && match pattern {
    AclPatternKind::Literal => facts.1 || facts.2,
    AclPatternKind::Prefixed => facts.3,
}))]
#[must_use]
pub fn acl_resource_match(pattern: AclPatternKind, facts: (bool, bool, bool, bool)) -> bool {
    let (same_type, exact, wildcard, prefix) = facts;
    same_type
        && match pattern {
            AclPatternKind::Literal => exact || wildcard,
            AclPatternKind::Prefixed => prefix,
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

/// Decide a request from whether any matching ACL allowed or denied it.
///
/// `default_allow` selects the outcome when nothing matched: Kafka's
/// `allow.everyone.if.no.acl.found` (default `false`) allows a request when
/// no ACL at all applies to the resource, rather than denying it. It only
/// changes the "nothing matched" case -- an explicit DENY still wins, and an
/// explicit ALLOW still allows, regardless of `default_allow`.
#[ensures(facts.0 ==> result == AclDecision::AllowSuperuser)]
#[ensures(!facts.0 && facts.2 ==> result == AclDecision::DenyExplicit)]
#[ensures(!facts.0 && !facts.2 && facts.1 ==> result == AclDecision::AllowAcl)]
#[ensures(!facts.0 && !facts.2 && !facts.1 && facts.3 ==>
    result == AclDecision::AllowNoAcl)]
#[ensures(!facts.0 && !facts.2 && !facts.1 && !facts.3 ==>
    result == AclDecision::DenyDefault)]
#[must_use]
pub fn acl_decision(facts: (bool, bool, bool, bool)) -> AclDecision {
    let (super_user, saw_allow, saw_deny, default_allow) = facts;
    if super_user {
        AclDecision::AllowSuperuser
    } else if saw_deny {
        AclDecision::DenyExplicit
    } else if saw_allow {
        AclDecision::AllowAcl
    } else if default_allow {
        AclDecision::AllowNoAcl
    } else {
        AclDecision::DenyDefault
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn acl_precedence_is_default_deny_and_order_independent() {
        use AclDecision::{AllowAcl, AllowSuperuser, DenyDefault, DenyExplicit};
        check!(acl_decision((false, false, false, false)) == DenyDefault);
        check!(acl_decision((false, true, false, false)) == AllowAcl);
        check!(acl_decision((false, false, true, false)) == DenyExplicit);
        check!(acl_decision((false, true, true, false)) == DenyExplicit);
        check!(acl_decision((true, false, true, false)) == AllowSuperuser);
    }

    #[test]
    fn acl_precedence_default_allow_only_applies_when_nothing_else_matched() {
        use AclDecision::{AllowAcl, AllowNoAcl, AllowSuperuser, DenyExplicit};
        // `default_allow` (allow.everyone.if.no.acl.found) only takes effect
        // when neither an ALLOW nor a DENY ACL matched.
        check!(acl_decision((false, false, false, true)) == AllowNoAcl);
        // An explicit DENY still wins over default_allow.
        check!(acl_decision((false, false, true, true)) == DenyExplicit);
        // An explicit ALLOW still wins over default_allow.
        check!(acl_decision((false, true, false, true)) == AllowAcl);
        // Super-user bypass still wins over everything.
        check!(acl_decision((true, false, false, true)) == AllowSuperuser);
    }

    #[test]
    fn acl_applicability_truth_tables_are_exact() {
        use AclOperationKind::{
            All, Alter, AlterConfigs, ClusterAction, Create, Delete, Describe, DescribeConfigs,
            IdempotentWrite, Read, TwoPhaseCommit, Write,
        };

        for (wildcard, exact, expected) in [
            (false, false, false),
            (false, true, true),
            (true, false, true),
            (true, true, true),
        ] {
            check!(acl_identity_match(wildcard, exact) == expected);
        }

        for same_type in [false, true] {
            for pattern in [AclPatternKind::Literal, AclPatternKind::Prefixed] {
                for exact in [false, true] {
                    for wildcard in [false, true] {
                        for prefix in [false, true] {
                            let expected = same_type
                                && match pattern {
                                    AclPatternKind::Literal => exact || wildcard,
                                    AclPatternKind::Prefixed => prefix,
                                };
                            check!(
                                acl_resource_match(pattern, (same_type, exact, wildcard, prefix))
                                    == expected
                            );
                        }
                    }
                }
            }
        }

        let operations = [
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
        ];
        let arrows = [
            (Read, Describe),
            (Write, Describe),
            (Delete, Describe),
            (Alter, Describe),
            (AlterConfigs, DescribeConfigs),
        ];
        for stored in operations {
            for requested in operations {
                for is_allow in [false, true] {
                    let expected = stored == requested
                        || stored == All
                        || (is_allow && arrows.contains(&(stored, requested)));
                    check!(acl_operation_match(stored, requested, is_allow) == expected);
                }
            }
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
