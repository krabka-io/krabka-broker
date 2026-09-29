//! Abstraction over "where ACL entries come from".
//!
//! One evaluator therefore serves both the broker and the gateway. The broker
//! uses a `MetadataImage` snapshot. The gateway uses a `Vec<AclEntry>` cache
//! that it fetched with `DescribeAcls`.

use krabka_metadata::{AclEntry, ResourceType};

/// A source of ACL entries the authorizer can match against.
///
/// `matching_acls` MUST return every entry whose resource pattern matches
/// `(rt, name)`: LITERAL entries equal to `name`, the LITERAL `*` wildcard, and
/// PREFIXED entries where `name.starts_with(entry.resource_name)`.
///
/// Mirror [`krabka_metadata::MetadataImage::matching_acls`] in
/// `crates/metadata/src/image.rs`.
pub trait AclSource {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a>;

    /// Every stored entry of resource type `rt`, regardless of resource name.
    ///
    /// [`crate::SimpleAclAuthorizer`]'s `authorize_by_resource_type` scans
    /// this set for an ALLOW grant that no DENY covers, so unlike
    /// `matching_acls` it is not scoped to one candidate resource name.
    fn acls_of_type<'a>(&'a self, rt: ResourceType) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a>;

    /// Whether a stored host containing `/` is a CIDR range (KIP-1276) rather
    /// than plain text. Kafka 4.3.1 compares every host as text, and trunk
    /// reads a range only once the cluster's `metadata.version` reaches
    /// 4.4-IV1, so a source that knows the version says so here. The default
    /// is `true`, for a source that holds only entries.
    fn cidr_hosts_supported(&self) -> bool {
        true
    }
}

// The broker's MetadataImage already implements the exact matching semantics;
// adapt its iterator. (Trait is local ⇒ orphan rule satisfied for the foreign
// MetadataImage type.)
impl AclSource for krabka_metadata::MetadataImage {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        Box::new(krabka_metadata::MetadataImage::matching_acls(
            self, rt, name,
        ))
    }

    fn acls_of_type<'a>(&'a self, rt: ResourceType) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        Box::new(
            krabka_metadata::MetadataImage::all_acls(self).filter(move |e| e.resource_type == rt),
        )
    }

    /// An unfinalized `metadata.version` is the bootstrap level, `4.3-IV0`,
    /// which is below the CIDR floor.
    fn cidr_hosts_supported(&self) -> bool {
        self.finalized_metadata_version()
            .is_some_and(|level| level >= krabka_metadata::metadata_version::CIDR_ACL_MIN_LEVEL)
    }
}
