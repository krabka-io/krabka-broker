use super::*;

// ----- independent oracle (separate source of truth) -----

/// Independent re-derivation of the ACL decision.
///
/// This function uses its OWN matching predicates and its OWN implication
/// table. It never calls the production `matches_*` or `implies`. Production
/// and oracle must agree on every input.
pub(super) fn oracle_decision(
    super_users: &HashSet<String>,
    entries: &[AclEntry],
    req: &AuthorizationRequest<'_>,
) -> AuthorizationResult {
    if super_users.contains(&req.principal.name) {
        return AuthorizationResult::Allow;
    }
    let mut saw_allow = false;
    let mut saw_deny = false;
    for e in entries {
        if oracle_resource_match(e, req.resource_type, req.resource_name)
            && oracle_principal_match(e, &req.principal.name)
            && oracle_host_match(e, req.host)
            && oracle_op_match(e.operation, req.operation, e.permission_type)
        {
            match e.permission_type {
                PermissionType::Deny => saw_deny = true,
                PermissionType::Allow => saw_allow = true,
            }
        }
    }
    if saw_deny {
        AuthorizationResult::Deny
    } else if saw_allow {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
}

fn oracle_resource_match(e: &AclEntry, rt: ResourceType, name: &str) -> bool {
    if e.resource_type != rt {
        return false;
    }
    match e.pattern_type {
        PatternType::Literal => e.resource_name == name || e.resource_name == "*",
        PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
    }
}

fn oracle_principal_match(e: &AclEntry, name: &str) -> bool {
    e.principal == "User:*" || e.principal == format!("User:{name}")
}

fn oracle_host_match(e: &AclEntry, host: &SocketAddr) -> bool {
    e.host == "*" || e.host == host.ip().to_string()
}

/// The one-way operation-implication table, declared independently of
/// production.
///
/// The table holds an exact match, `All` implies everything, and the explicit
/// arrows `{Read,Write,Delete,Alter}` -> `Describe` and `AlterConfigs` ->
/// `DescribeConfigs`. The arrows apply only when `permission` is ALLOW: a
/// DENY ACL never gains the implied operations (#649).
fn oracle_op_match(
    stored: AclOperation,
    requested: AclOperation,
    permission: PermissionType,
) -> bool {
    use AclOperation::{All, Alter, AlterConfigs, Delete, Describe, DescribeConfigs, Read, Write};
    // Implication arrows as an explicit data table — deliberately a different
    // structure from production's `matches!`-based `implies`, so the cross-check
    // catches a regression in either form.
    const ARROWS: &[(AclOperation, AclOperation)] = &[
        (Read, Describe),
        (Write, Describe),
        (Delete, Describe),
        (Alter, Describe),
        (AlterConfigs, DescribeConfigs),
    ];
    if stored == requested || stored == All {
        return true;
    }
    permission == PermissionType::Allow && ARROWS.contains(&(stored, requested))
}
