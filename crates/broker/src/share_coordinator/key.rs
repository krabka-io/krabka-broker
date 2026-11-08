//! The key of one share partition: Kafka's `SharePartitionKey`, the
//! `(group_id, topic_id, partition)` that the share coordinator stores state
//! for and the share-partition leader caches acquisitions for.
//!
//! [`ShareKey`] owns its group id, for a map key. [`ShareKeyRef`] borrows it,
//! for a lookup: a map keyed by [`ShareKey`] finds an entry from a
//! [`ShareKeyRef`] through [`ShareKeyRef::lookup`], and no lookup allocates.

use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
};

/// The owned key of one share partition.
///
/// It orders by group id, then topic id, then partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShareKey {
    pub(crate) group: String,
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: i32,
}

/// The borrowed key of one share partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShareKeyRef<'a> {
    pub(crate) group: &'a str,
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: i32,
}

impl ShareKey {
    /// The borrowed form of this key.
    pub(crate) fn as_key_ref(&self) -> ShareKeyRef<'_> {
        ShareKeyRef {
            group: &self.group,
            topic_id: self.topic_id,
            partition: self.partition,
        }
    }
}

impl<'a> ShareKeyRef<'a> {
    /// The key of `partition` of `topic_id` in `group`.
    #[must_use]
    pub(crate) const fn new(group: &'a str, topic_id: uuid::Uuid, partition: i32) -> Self {
        Self {
            group,
            topic_id,
            partition,
        }
    }

    /// The owned form of this key.
    pub(crate) fn to_key(self) -> ShareKey {
        ShareKey {
            group: self.group.to_owned(),
            topic_id: self.topic_id,
            partition: self.partition,
        }
    }

    /// This key as the lookup type of a map keyed by [`ShareKey`].
    pub(crate) fn lookup(&self) -> &(dyn ShareKeyView + '_) { self }
}

/// A key that a map keyed by [`ShareKey`] can look up: either form, compared
/// and hashed through its borrowed view.
pub trait ShareKeyView {
    fn view(&self) -> ShareKeyRef<'_>;
}

impl ShareKeyView for ShareKey {
    fn view(&self) -> ShareKeyRef<'_> { self.as_key_ref() }
}

impl ShareKeyView for ShareKeyRef<'_> {
    fn view(&self) -> ShareKeyRef<'_> { *self }
}

impl<'a> Borrow<dyn ShareKeyView + 'a> for ShareKey {
    fn borrow(&self) -> &(dyn ShareKeyView + 'a) { self }
}

/// Hashes as its borrowed view, so that `Borrow` keeps the hashes of both
/// forms equal.
impl Hash for ShareKey {
    fn hash<H: Hasher>(&self, state: &mut H) { self.as_key_ref().hash(state); }
}

impl Hash for dyn ShareKeyView + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) { self.view().hash(state); }
}

impl PartialEq for dyn ShareKeyView + '_ {
    fn eq(&self, other: &Self) -> bool { self.view() == other.view() }
}

impl Eq for dyn ShareKeyView + '_ {}

#[cfg(test)]
mod tests {
    use {
        super::{ShareKey, ShareKeyRef},
        assert2::check,
        dashmap::DashMap,
        std::collections::HashMap,
    };

    fn key(group: &str, topic: u8, partition: i32) -> ShareKey {
        ShareKey {
            group: group.to_owned(),
            topic_id: uuid::Uuid::from_bytes([topic; 16]),
            partition,
        }
    }

    /// Both maps that hold share keys find an entry from its borrowed key,
    /// and miss a key that differs in any one part.
    #[test]
    fn a_borrowed_key_finds_its_owned_entry() {
        let stored = key("g", 1, 0);
        let other_group = key("h", 1, 0);
        let hash_map = HashMap::from([(stored.clone(), "state")]);
        let dash_map = DashMap::new();
        dash_map.insert(stored.clone(), "state");
        let probes = [
            (stored.as_key_ref(), Some("state")),
            (other_group.as_key_ref(), None),
            (
                ShareKeyRef {
                    topic_id: uuid::Uuid::from_bytes([2; 16]),
                    ..stored.as_key_ref()
                },
                None,
            ),
            (
                ShareKeyRef {
                    partition: 1,
                    ..stored.as_key_ref()
                },
                None,
            ),
        ];
        for (probe, expected) in probes {
            check!(
                hash_map.get(probe.lookup()).copied() == expected,
                "{probe:?}"
            );
            check!(
                dash_map.get(probe.lookup()).map(|entry| *entry.value()) == expected,
                "{probe:?}"
            );
        }
    }

    /// Keys sort as the `(group, topic_id, partition)` tuple they replace.
    #[test]
    fn keys_sort_by_group_then_topic_then_partition() {
        let mut keys = vec![
            key("b", 1, 0),
            key("a", 2, 0),
            key("a", 1, 5),
            key("a", 1, 2),
        ];
        keys.sort_unstable();
        check!(
            keys == [
                key("a", 1, 2),
                key("a", 1, 5),
                key("a", 2, 0),
                key("b", 1, 0)
            ]
        );
    }

    #[test]
    fn the_owned_and_borrowed_forms_convert_both_ways() {
        let owned = key("g", 3, 7);
        check!(owned.as_key_ref().to_key() == owned);
    }
}
