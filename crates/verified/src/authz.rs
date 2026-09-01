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
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies four independent host matching facts"
)]
#[ensures(result == (same_type && match pattern {
    AclPatternKind::Literal => exact || wildcard,
    AclPatternKind::Prefixed => prefix,
}))]
#[must_use]
pub fn acl_resource_match(
    same_type: bool,
    pattern: AclPatternKind,
    exact: bool,
    wildcard: bool,
    prefix: bool,
) -> bool {
    same_type
        && match pattern {
            AclPatternKind::Literal => exact || wildcard,
            AclPatternKind::Prefixed => prefix,
        }
}

/// Match exact operations, `All`, and Kafka's one-way implication arrows.
#[ensures(result == (stored == requested
    || stored == AclOperationKind::All
    || (stored == AclOperationKind::Read && requested == AclOperationKind::Describe)
    || (stored == AclOperationKind::Write && requested == AclOperationKind::Describe)
    || (stored == AclOperationKind::Delete && requested == AclOperationKind::Describe)
    || (stored == AclOperationKind::Alter && requested == AclOperationKind::Describe)
    || (stored == AclOperationKind::AlterConfigs
        && requested == AclOperationKind::DescribeConfigs)))]
#[must_use]
pub fn acl_operation_match(stored: AclOperationKind, requested: AclOperationKind) -> bool {
    match stored {
        AclOperationKind::All => true,
        AclOperationKind::Read => matches!(
            requested,
            AclOperationKind::Read | AclOperationKind::Describe
        ),
        AclOperationKind::Write => matches!(
            requested,
            AclOperationKind::Write | AclOperationKind::Describe
        ),
        AclOperationKind::Create => matches!(requested, AclOperationKind::Create),
        AclOperationKind::Delete => matches!(
            requested,
            AclOperationKind::Delete | AclOperationKind::Describe
        ),
        AclOperationKind::Alter => matches!(
            requested,
            AclOperationKind::Alter | AclOperationKind::Describe
        ),
        AclOperationKind::Describe => matches!(requested, AclOperationKind::Describe),
        AclOperationKind::ClusterAction => matches!(requested, AclOperationKind::ClusterAction),
        AclOperationKind::DescribeConfigs => {
            matches!(requested, AclOperationKind::DescribeConfigs)
        }
        AclOperationKind::AlterConfigs => matches!(
            requested,
            AclOperationKind::AlterConfigs | AclOperationKind::DescribeConfigs
        ),
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
#[ensures(super_user ==> result == AclDecision::AllowSuperuser)]
#[ensures(!super_user && saw_deny ==> result == AclDecision::DenyExplicit)]
#[ensures(!super_user && !saw_deny && saw_allow ==> result == AclDecision::AllowAcl)]
#[ensures(!super_user && !saw_deny && !saw_allow ==> result == AclDecision::DenyDefault)]
#[must_use]
pub fn acl_decision(super_user: bool, saw_allow: bool, saw_deny: bool) -> AclDecision {
    if super_user {
        AclDecision::AllowSuperuser
    } else if saw_deny {
        AclDecision::DenyExplicit
    } else if saw_allow {
        AclDecision::AllowAcl
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
        check!(acl_decision(false, false, false) == DenyDefault);
        check!(acl_decision(false, true, false) == AllowAcl);
        check!(acl_decision(false, false, true) == DenyExplicit);
        check!(acl_decision(false, true, true) == DenyExplicit);
        check!(acl_decision(true, false, true) == AllowSuperuser);
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
                                acl_resource_match(same_type, pattern, exact, wildcard, prefix)
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
                let expected =
                    stored == requested || stored == All || arrows.contains(&(stored, requested));
                check!(acl_operation_match(stored, requested) == expected);
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
