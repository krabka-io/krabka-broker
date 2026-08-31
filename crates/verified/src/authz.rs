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
