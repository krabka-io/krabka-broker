use super::*;

// ----- builders -----

pub(super) fn entry(
    rt: ResourceType,
    pattern: PatternType,
    name: &str,
    principal: &str,
    host: &str,
    op: AclOperation,
    perm: PermissionType,
) -> AclEntry {
    AclEntry {
        resource_type: rt,
        resource_name: name.into(),
        pattern_type: pattern,
        principal: principal.into(),
        host: host.into(),
        operation: op,
        permission_type: perm,
    }
}

pub(super) fn principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::SaslPlain,
        groups: vec![],
    }
}

fn image_of(entries: &[AclEntry]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for e in entries {
        img.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
    }
    img
}

/// Assert that the real authorizer agrees with the oracle.
///
/// This function drives the authorizer through BOTH the broker
/// `MetadataImage` and the gateway `AclCache`. It also asserts that the two
/// sources agree with each other, with no broker-against-gateway drift.
pub(super) fn check(
    super_users: &HashSet<String>,
    entries: &[AclEntry],
    req: &AuthorizationRequest<'_>,
) {
    let auth = SimpleAclAuthorizer::new(super_users.clone());
    let image = image_of(entries);
    let cache = AclCache::new(entries.to_vec());
    let want = oracle_decision(super_users, entries, req);
    let got_image = auth.authorize(&image, req);
    let got_cache = auth.authorize(&cache, req);
    assert2::assert!((got_image, got_cache) == (want, want));
}
