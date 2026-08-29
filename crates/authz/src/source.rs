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
}
