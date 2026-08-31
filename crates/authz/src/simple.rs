//! ACL-based authorizer behind the [`Authorizer`] trait.
//!
//! The authorizer applies the super-user bypass, deny-wins-over-allow, LITERAL
//! and PREFIXED matching, and principal, host, and operation wildcards.
//!
//! There is no "empty source + no super-users ⇒ Allow" compatibility shim. That
//! case lives in [`crate::AllowAllAuthorizer`]. [`SimpleAclAuthorizer`] with an
//! empty source and empty super-users denies everything. This default-deny
//! behavior matches Kafka's `StandardAuthorizer` once an operator explicitly
//! configures an authorizer.

use std::collections::HashSet;

use krabka_metadata::PermissionType;
use krabka_verified::{AclDecision, acl_decision};

mod matching;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::matching::{matches_host, matches_operation, matches_principal};
use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};

/// Authorizer that consults the cluster's persisted ACLs.
///
/// The caller supplies the [`AclSource`] per call: a `MetadataImage` for the
/// broker, an `AclCache` for the gateway.
///
/// This type holds the configured super-user set. Principals in this set bypass
/// ACL evaluation and always get `Allow`.
#[derive(Debug)]
pub struct SimpleAclAuthorizer {
    super_users: HashSet<String>,
}

impl SimpleAclAuthorizer {
    #[must_use]
    pub fn new(super_users: HashSet<String>) -> Self {
        Self { super_users }
    }
}

impl Authorizer for SimpleAclAuthorizer {
    // Per-request ACL decision: skip_all keeps the borrowed principal/host
    // structs (which may carry the raw name) out of span fields; only
    // non-sensitive routing context is recorded. No `err` — this returns a
    // plain Allow/Deny, not a Result. `decision` is filled in before each
    // return path.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            principal = %req.principal.name,
            resource_type = ?req.resource_type,
            resource = %req.resource_name,
            operation = ?req.operation,
            host = %req.host.ip(),
            decision = tracing::field::Empty,
        )
    )]
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let span = tracing::Span::current();
        let super_user = self.super_users.contains(&req.principal.name);
        let mut saw_allow = false;
        let mut saw_deny = false;
        if !super_user {
            let user_pattern = format!("User:{}", req.principal.name);
            let host_str = req.host.ip().to_string();
            for entry in source.matching_acls(req.resource_type, req.resource_name) {
                if !matches_principal(entry, &user_pattern)
                    || !matches_host(entry, &host_str)
                    || !matches_operation(entry.operation, req.operation)
                {
                    continue;
                }
                match entry.permission_type {
                    PermissionType::Allow => saw_allow = true,
                    PermissionType::Deny => {
                        saw_deny = true;
                        break;
                    }
                }
            }
        }
        let decision = acl_decision(super_user, saw_allow, saw_deny);

        let (label, result) = match decision {
            AclDecision::AllowSuperuser => ("allow-superuser", AuthorizationResult::Allow),
            AclDecision::AllowAcl => ("allow-acl", AuthorizationResult::Allow),
            AclDecision::DenyExplicit => ("deny-explicit", AuthorizationResult::Deny),
            AclDecision::DenyDefault => ("deny-default", AuthorizationResult::Deny),
        };
        span.record("decision", label);
        result
    }
}
