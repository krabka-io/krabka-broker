//! ACL-based authorizer behind the [`Authorizer`] trait.
//!
//! The authorizer applies the super-user bypass, deny-wins-over-allow, LITERAL
//! and PREFIXED matching, and principal, host, and operation wildcards.
//!
//! [`SimpleAclAuthorizer`] with an empty source and empty super-users denies
//! everything by default. This default-deny behavior matches Kafka's
//! `StandardAuthorizer` once an operator explicitly configures an authorizer.
//! Kafka's `allow.everyone.if.no.acl.found` (default `false`) flips that
//! default: when enabled and NO ACL at all applies to the resource --
//! regardless of principal, host, operation, or permission type -- the
//! request is allowed. If at least one ACL applies to the resource and none
//! of them matches, the request is still denied. Set it with
//! [`SimpleAclAuthorizer::with_allow_everyone_if_no_acl_found`].

use std::{collections::HashSet, net::SocketAddr};

use krabka_metadata::{AclOperation, PermissionType, ResourceType};
use krabka_security::Principal;
use krabka_verified::{AclDecision, acl_decision};

mod matching;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::matching::{matches_host, matches_operation, matches_principal, matches_resource};
use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer, jdk_host_address};

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
    allow_everyone_if_no_acl_found: bool,
}

impl SimpleAclAuthorizer {
    #[must_use]
    pub fn new(super_users: HashSet<String>) -> Self {
        Self {
            super_users,
            allow_everyone_if_no_acl_found: false,
        }
    }

    /// Sets Kafka's `allow.everyone.if.no.acl.found` (default `false`). See
    /// the module doc for the exact semantics.
    #[must_use]
    pub fn with_allow_everyone_if_no_acl_found(mut self, allow: bool) -> Self {
        self.allow_everyone_if_no_acl_found = allow;
        self
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
            host = %jdk_host_address(req.host.ip()),
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
        let mut has_resource_acls = false;
        if !super_user {
            let user_pattern = format!("User:{}", req.principal.name);
            let host_str = jdk_host_address(req.host.ip());
            for entry in source.matching_acls(req.resource_type, req.resource_name) {
                if !matches_resource(entry, req.resource_type, req.resource_name) {
                    continue;
                }
                has_resource_acls = true;
                if !matches_principal(entry, &user_pattern)
                    || !matches_host(entry, &host_str, req.host.ip())
                    || !matches_operation(entry.operation, req.operation, entry.permission_type)
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
        let default_allow = self.allow_everyone_if_no_acl_found && !has_resource_acls;
        let decision = acl_decision((super_user, saw_allow, saw_deny, default_allow));

        let (label, result) = match decision {
            AclDecision::AllowSuperuser => ("allow-superuser", AuthorizationResult::Allow),
            AclDecision::AllowAcl => ("allow-acl", AuthorizationResult::Allow),
            AclDecision::AllowNoAcl => ("allow-no-acl", AuthorizationResult::Allow),
            AclDecision::DenyExplicit => ("deny-explicit", AuthorizationResult::Deny),
            AclDecision::DenyDefault => ("deny-default", AuthorizationResult::Deny),
        };
        span.record("decision", label);
        result
    }

    /// Scans every stored entry of `resource_type` for an ALLOW ACL, matching
    /// `principal`, `host`, and `operation`, that no DENY covers.
    ///
    /// For each such ALLOW entry, the resource name its own pattern names is
    /// a resource that grant covers -- a literal names itself, and a prefixed
    /// pattern's own prefix is a name it covers by definition. Running the
    /// ordinary [`Self::authorize`] decision against that one candidate name
    /// reuses its ordering (super-user bypass, deny-wins) and its resource
    /// matching, so a DENY that reaches that candidate -- an exact-pattern
    /// DENY, a DENY on the `*` wildcard, or a broader PREFIXED DENY -- is
    /// exactly the DENY that would also apply if a real resource used that
    /// name. This is Kafka's `AclAuthorizer.authorizeByResourceType`: an ALLOW
    /// grants only when some concrete resource it covers survives that scan.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            principal = %principal.name,
            resource_type = ?resource_type,
            operation = ?operation,
            host = %jdk_host_address(host.ip()),
            decision = tracing::field::Empty,
        )
    )]
    fn authorize_by_resource_type(
        &self,
        source: &dyn AclSource,
        principal: &Principal,
        host: &SocketAddr,
        resource_type: ResourceType,
        operation: AclOperation,
    ) -> AuthorizationResult {
        let span = tracing::Span::current();
        if self.super_users.contains(&principal.name) {
            span.record("decision", "allow-superuser");
            return AuthorizationResult::Allow;
        }
        let user_pattern = format!("User:{}", principal.name);
        let host_str = jdk_host_address(host.ip());
        for entry in source.acls_of_type(resource_type) {
            if entry.permission_type != PermissionType::Allow
                || !matches_principal(entry, &user_pattern)
                || !matches_host(entry, &host_str, host.ip())
                || !matches_operation(entry.operation, operation, entry.permission_type)
            {
                continue;
            }
            // Testing only the ALLOW entry's own stored name misses a
            // PREFIXED grant when a LITERAL DENY happens to name that exact
            // prefix string: e.g. ALLOW prefixed "ord" plus DENY literal
            // "ord" would report the whole grant denied, even though
            // "orders" and every other name under the prefix is still
            // allowed. Test the prefix name itself (for a LITERAL entry,
            // the only resource it can ever grant) and, for a PREFIXED
            // entry, a synthetic name that is strictly under the prefix but
            // cannot equal any real LITERAL ACL name, so a LITERAL DENY on
            // the bare prefix string cannot shadow the grant it does not
            // actually cover.
            let mut candidate_names = vec![entry.resource_name.clone()];
            if entry.pattern_type == krabka_metadata::PatternType::Prefixed {
                candidate_names.push(format!("{}\u{10ffff}", entry.resource_name));
            }
            for resource_name in &candidate_names {
                let candidate = AuthorizationRequest {
                    principal,
                    host,
                    resource_type,
                    resource_name: resource_name.as_str(),
                    operation,
                };
                if self.authorize(source, &candidate) == AuthorizationResult::Allow {
                    span.record("decision", "allow-acl");
                    return AuthorizationResult::Allow;
                }
            }
        }
        span.record("decision", "deny-default");
        AuthorizationResult::Deny
    }
}
