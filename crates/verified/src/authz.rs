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
}
