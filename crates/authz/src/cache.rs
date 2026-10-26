//! Gateway-side ACL snapshot: a flat `Vec<AclEntry>` from `describe_acls`.
//!
//! The snapshot implements [`AclSource`] with EXACTLY the broker's matching
//! semantics. It indexes the entries once, when the snapshot is built, so a
//! lookup neither allocates nor scans entries of other resources: a LITERAL
//! name and the `*` wildcard are hash lookups, and the PREFIXED entries that
//! apply to a name are found by probing each distinct stored prefix length.

use {
    crate::{AclEntries, AclSource},
    krabka_metadata::{AclEntry, PatternType, ResourceType},
    std::{collections::HashMap, iter},
};

/// The resource name of Kafka's `WILDCARD_RESOURCE`, a LITERAL pattern that
/// matches every resource of its type.
const WILDCARD_RESOURCE: &str = "*";

/// Immutable ACL snapshot. Each refresh rebuilds it wholesale.
#[derive(Debug, Clone, Default)]
pub struct AclCache {
    entries: Vec<AclEntry>,
    by_type: HashMap<ResourceType, TypeIndex>,
    cidr_hosts_supported: bool,
}

/// Positions in [`AclCache::entries`] of the entries of one resource type.
#[derive(Debug, Clone, Default)]
struct TypeIndex {
    all: Vec<usize>,
    literal: HashMap<String, Vec<usize>>,
    prefixed: HashMap<String, Vec<usize>>,
    /// The distinct byte lengths of the keys of `prefixed`, ascending.
    prefix_lens: Vec<usize>,
}

impl TypeIndex {
    fn literal(&self, name: &str) -> impl Iterator<Item = usize> + '_ {
        self.literal.get(name).into_iter().flatten().copied()
    }

    /// Every PREFIXED entry whose resource name `name` starts with. A stored
    /// prefix is valid UTF-8, so it ends on a character boundary of every
    /// name it prefixes; `str::get` skips the lengths that are not one.
    fn prefixed<'a>(&'a self, name: &'a str) -> impl Iterator<Item = usize> + 'a {
        self.prefix_lens
            .iter()
            .take_while(move |&&len| len <= name.len())
            .filter_map(move |&len| name.get(..len))
            .filter_map(|prefix| self.prefixed.get(prefix))
            .flatten()
            .copied()
    }
}

impl AclCache {
    /// A snapshot of `entries` that compares every host as text, as Kafka
    /// 4.3.1 does. Say that the cluster reads a range from a host containing
    /// `/` with [`Self::with_cidr_hosts_supported`].
    #[must_use]
    pub fn new(entries: Vec<AclEntry>) -> Self {
        let mut by_type: HashMap<ResourceType, TypeIndex> = HashMap::new();
        for (position, entry) in entries.iter().enumerate() {
            let index = by_type.entry(entry.resource_type).or_default();
            index.all.push(position);
            let names = match entry.pattern_type {
                PatternType::Literal => &mut index.literal,
                PatternType::Prefixed => &mut index.prefixed,
            };
            names
                .entry(entry.resource_name.clone())
                .or_default()
                .push(position);
        }
        for index in by_type.values_mut() {
            index.prefix_lens = index.prefixed.keys().map(String::len).collect();
            index.prefix_lens.sort_unstable();
            index.prefix_lens.dedup();
        }
        Self {
            entries,
            by_type,
            cidr_hosts_supported: false,
        }
    }

    /// Sets whether a stored host containing `/` is a CIDR range (KIP-1276),
    /// which it is once the broker the entries came from has a
    /// `metadata.version` of 4.4-IV1 or higher. A gateway that reads a range
    /// where that broker compares text would authorize a peer the broker
    /// denies, and the other way round.
    #[must_use]
    pub fn with_cidr_hosts_supported(mut self, supported: bool) -> Self {
        self.cidr_hosts_supported = supported;
        self
    }

    #[must_use]
    pub fn len(&self) -> usize { self.entries.len() }

    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    fn positions<'a>(&'a self, positions: impl Iterator<Item = usize> + 'a) -> AclEntries<'a> {
        Box::new(positions.filter_map(|position| self.entries.get(position)))
    }
}

impl AclSource for AclCache {
    fn matching_acls<'a>(&'a self, rt: ResourceType, name: &'a str) -> AclEntries<'a> {
        // MUST mirror MetadataImage::matching_acls: same resource_type, and
        // (LITERAL == name) || (LITERAL == "*") || (PREFIXED && name.starts_with(resource_name)).
        // The wildcard bucket is skipped when `name` is itself "*", whose
        // LITERAL lookup already yields it.
        let Some(index) = self.by_type.get(&rt) else {
            return Box::new(iter::empty());
        };
        let wildcard = (name != WILDCARD_RESOURCE)
            .then(|| index.literal(WILDCARD_RESOURCE))
            .into_iter()
            .flatten();
        self.positions(
            index
                .literal(name)
                .chain(wildcard)
                .chain(index.prefixed(name)),
        )
    }

    fn acls_of_type(&self, rt: ResourceType) -> AclEntries<'_> {
        self.positions(
            self.by_type
                .get(&rt)
                .into_iter()
                .flat_map(|index| index.all.iter().copied()),
        )
    }

    fn cidr_hosts_supported(&self) -> bool { self.cidr_hosts_supported }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{AuthorizationResult, test_support::acl as entry},
        krabka_metadata::{
            AclOperation, MetadataImage, MetadataRecord, ResourceType,
            metadata_version::CIDR_ACL_MIN_LEVEL,
        },
        proptest::{collection, strategy::Strategy},
        std::collections::HashSet,
        uuid::Uuid,
    };

    /// A stable, comparable identity for an `AclEntry`.
    ///
    /// A test can then compare the `matching_acls` output of the two sources as
    /// sets, in any order. The ACL enums derive `Debug` but not `Ord`, so this
    /// function keys on the debug rendering of the identifying fields. That
    /// rendering is a `String`, which is `Ord`.
    fn key(e: &AclEntry) -> String {
        format!(
            "{:?}|{:?}|{}|{:?}",
            e.resource_type, e.pattern_type, e.resource_name, e.operation
        )
    }

    fn sorted_keys(it: AclEntries<'_>) -> Vec<String> {
        let mut v: Vec<_> = it.map(key).collect();
        v.sort();
        v
    }

    /// Cross-validation guard for the two matching implementations.
    ///
    /// The test builds the same `AclEntry` set into BOTH a `MetadataImage`,
    /// with `apply`, and an `AclCache`. The two must give the SAME matching set
    /// for every probe. This protects against drift between the broker's image
    /// matching and the gateway cache's reimplementation.
    #[test]
    fn cache_matches_image_for_every_probe() {
        // A representative ACL set: literal exact, the literal "*" wildcard,
        // a prefixed entry, an unrelated topic, and a different resource type.
        let entries = vec![
            entry().call(),
            entry().op(AclOperation::Write).name("*").call(),
            entry().pattern(PatternType::Prefixed).name("team-").call(),
            entry().name("bar").call(),
            entry().rt(ResourceType::Group).name("cg-1").call(),
            entry()
                .rt(ResourceType::Group)
                .pattern(PatternType::Prefixed)
                .name("app-")
                .call(),
        ];

        // Build the same set into a MetadataImage (broker side) ...
        let mut image = MetadataImage::new(Uuid::nil());
        for e in &entries {
            image.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
        }
        // ... and into an AclCache (gateway side).
        let cache = AclCache::new(entries);

        // Probes covering every matching code path.
        let probes = [
            (ResourceType::Topic, "foo"),      // literal exact hit (+ "*" wildcard)
            (ResourceType::Topic, "*"),        // querying the wildcard resource itself
            (ResourceType::Topic, "team-foo"), // prefixed hit (+ "*" wildcard)
            (ResourceType::Topic, "team-"),    // prefix boundary (starts_with itself)
            (ResourceType::Topic, "nomatch"),  // only the "*" wildcard matches
            (ResourceType::Group, "cg-1"),     // literal hit, no wildcard for Group
            (ResourceType::Group, "app-svc"),  // prefixed hit on Group
            (ResourceType::Group, "other"),    // prefixed miss, no wildcard → empty
            (ResourceType::Cluster, "kafka-cluster"), // wrong type → empty in both
        ];

        for (rt, name) in probes {
            let from_image = sorted_keys(AclSource::matching_acls(&image, rt, name));
            let from_cache = sorted_keys(AclSource::matching_acls(&cache, rt, name));
            assert2::assert!(from_image == from_cache);
        }
    }

    /// A host containing `/` is a CIDR range only where the cluster's
    /// `metadata.version` has reached 4.4-IV1 (KIP-1276), and a snapshot of its
    /// ACLs must decide as the cluster does. Each row builds the same range
    /// ALLOW into an image at a `metadata.version` and into a cache told what
    /// that image supports, and both must give the answer of the row: text
    /// comparison below the level, where the range applies to no peer, and a
    /// range at or above it. A cache that is not told compares as text.
    #[yare::parameterized(
        unfinalized_version = { None, Some(false), AuthorizationResult::Deny },
        below_range_level = {
            Some(CIDR_ACL_MIN_LEVEL - 1),
            Some(false),
            AuthorizationResult::Deny,
        },
        minimum_range_level = {
            Some(CIDR_ACL_MIN_LEVEL),
            Some(true),
            AuthorizationResult::Allow,
        },
        unconfigured_cache = { None, None, AuthorizationResult::Deny },
    )]
    fn a_cache_reads_a_slash_host_as_the_cluster_does(
        level: Option<i16>,
        cache_support: Option<bool>,
        expected: AuthorizationResult,
    ) {
        use {
            crate::{AuthorizationRequest, Authorizer, SimpleAclAuthorizer},
            krabka_metadata::{FeatureLevelRecord, metadata_version::METADATA_VERSION_FEATURE},
            krabka_security::{AuthMethod, Principal},
        };

        let range_allow = AclEntry {
            host: "10.0.0.0/8".into(),
            ..entry().call()
        };
        let alice = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        };
        let peer = "10.1.2.3:5000".parse().unwrap();
        let request = AuthorizationRequest {
            principal: &alice,
            host: &peer,
            resource_type: ResourceType::Topic,
            resource_name: "foo",
            operation: AclOperation::Read,
        };
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let mut image = MetadataImage::new(Uuid::nil());
        if let Some(level) = level {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: METADATA_VERSION_FEATURE.into(),
                level,
            }));
        }
        image.apply(&MetadataRecord::V1AccessControlEntry(range_allow.clone()));
        let cache = match cache_support {
            Some(supported) => {
                AclCache::new(vec![range_allow]).with_cidr_hosts_supported(supported)
            }
            None => AclCache::new(vec![range_allow]),
        };
        assert2::assert!(
            (
                auth.authorize(&image, &request),
                auth.authorize(&cache, &request)
            ) == (expected, expected)
        );
    }

    /// The scan the index replaces: every entry of the snapshot, tested
    /// one by one against the matching rule.
    fn scanned(entries: &[AclEntry], rt: ResourceType, name: &str) -> Vec<String> {
        let mut v: Vec<_> = entries
            .iter()
            .filter(|e| {
                e.resource_type == rt
                    && match e.pattern_type {
                        PatternType::Literal => e.resource_name == name || e.resource_name == "*",
                        PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
                    }
            })
            .map(key)
            .collect();
        v.sort();
        v
    }

    fn arb_entry() -> impl Strategy<Value = AclEntry> {
        use proptest::prelude::*;
        (
            prop_oneof![Just(ResourceType::Topic), Just(ResourceType::Group)],
            prop_oneof![Just(PatternType::Literal), Just(PatternType::Prefixed)],
            prop_oneof![Just(AclOperation::Read), Just(AclOperation::Write)],
            "(\\*|[aé-]{0,4})",
        )
            .prop_map(|(rt, pattern, op, name)| {
                entry().rt(rt).pattern(pattern).op(op).name(&name).call()
            })
    }

    proptest::proptest! {
        /// The indexed lookup yields exactly the entries, duplicates included,
        /// that a scan of the whole snapshot does, for names that share
        /// prefixes, multi-byte characters a prefix length can split, the
        /// empty name and the wildcard name.
        #[test]
        fn index_matches_a_full_scan(
            entries in collection::vec(arb_entry(), 0..24),
            probes in collection::vec("(\\*|[aé-]{0,6})", 1..8),
        ) {
            let cache = AclCache::new(entries.clone());
            for name in &probes {
                for rt in [ResourceType::Topic, ResourceType::Group, ResourceType::Cluster] {
                    let indexed = sorted_keys(AclSource::matching_acls(&cache, rt, name));
                    assert2::assert!(indexed == scanned(&entries, rt, name));
                    let mut of_type: Vec<_> = cache.acls_of_type(rt).map(key).collect();
                    of_type.sort();
                    let mut expected: Vec<_> =
                        entries.iter().filter(|e| e.resource_type == rt).map(key).collect();
                    expected.sort();
                    assert2::assert!(of_type == expected);
                }
            }
        }
    }

    /// Several prefixes of different lengths all apply to a name that extends
    /// each of them, and a prefix longer than the name never does.
    #[test]
    fn every_applicable_prefix_length_is_found() {
        let cache = AclCache::new(vec![
            entry().pattern(PatternType::Prefixed).name("t").call(),
            entry().pattern(PatternType::Prefixed).name("team-").call(),
            entry().pattern(PatternType::Prefixed).name("team-a").call(),
            entry()
                .pattern(PatternType::Prefixed)
                .name("team-a-long")
                .call(),
            entry().pattern(PatternType::Prefixed).name("other").call(),
        ]);
        let names: Vec<_> = AclSource::matching_acls(&cache, ResourceType::Topic, "team-a1")
            .map(|e| e.resource_name.as_str())
            .collect();
        assert2::assert!(names == ["t", "team-", "team-a"]);
    }

    #[test]
    fn len_and_is_empty_track_entries() {
        let empty = AclCache::default();
        assert2::assert!(empty.is_empty());
        assert2::assert!(empty.len() == 0);

        let cache = AclCache::new(vec![entry().call()]);
        assert2::assert!(!cache.is_empty());
        assert2::assert!(cache.len() == 1);
    }
}
