//! Diskless WAL offset-to-object index records and in-memory projection.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One partition's byte range within a flushed WAL object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
}

/// Stable Kafka compaction key for one logical WAL range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WalIndexKey {
    pub(crate) topic_id: Uuid,
    pub(crate) partition: i32,
    pub(crate) first_offset: i64,
}

impl WalIndexKey {
    const LEN: usize = 16 + 4 + 8;

    #[must_use]
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut bytes = Vec::with_capacity(Self::LEN);
        bytes.extend_from_slice(self.topic_id.as_bytes());
        bytes.extend_from_slice(&self.partition.to_be_bytes());
        bytes.extend_from_slice(&self.first_offset.to_be_bytes());
        Bytes::from(bytes)
    }

    #[must_use]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::LEN] = bytes.try_into().ok()?;
        Some(Self {
            topic_id: Uuid::from_bytes(bytes[..16].try_into().ok()?),
            partition: i32::from_be_bytes(bytes[16..20].try_into().ok()?),
            first_offset: i64::from_be_bytes(bytes[20..].try_into().ok()?),
        })
    }
}

impl From<&WalIndexEntry> for WalIndexKey {
    fn from(entry: &WalIndexEntry) -> Self {
        Self {
            topic_id: entry.topic_id,
            partition: entry.partition,
            first_offset: entry.first_offset,
        }
    }
}

/// Durable index event for one flushed diskless WAL object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    /// Serialize this record with the workspace `serde-wincode` codec.
    ///
    /// # Errors
    ///
    /// Returns an error if wincode cannot encode the record.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map(Bytes::from)
            .map_err(|error| error.to_string())
    }

    /// Deserialize a record written by [`Self::to_bytes`].
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not a valid encoded `WalFlushRecord`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|error| error.to_string())
    }
}

/// In-memory projection of committed `WalFlushRecord`s.
#[derive(Default)]
pub struct WalIndexCache {
    by_topic_partition: HashMap<(Uuid, i32), BTreeMap<i64, (String, WalIndexEntry)>>,
}

impl WalIndexCache {
    /// Apply one committed flush record to the projection.
    pub fn apply(&mut self, record: &WalFlushRecord) {
        for entry in &record.entries {
            self.by_topic_partition
                .entry((entry.topic_id, entry.partition))
                .or_default()
                .insert(
                    entry.first_offset,
                    (record.object_key.clone(), entry.clone()),
                );
        }
    }

    /// Remove one compacted range after its Kafka tombstone is committed.
    pub(crate) fn remove(&mut self, key: WalIndexKey) {
        let partition = (key.topic_id, key.partition);
        let mut empty = false;
        if let Some(entries) = self.by_topic_partition.get_mut(&partition) {
            entries.remove(&key.first_offset);
            empty = entries.is_empty();
        }
        if empty {
            self.by_topic_partition.remove(&partition);
        }
    }

    /// Keys currently held for a topic, used to publish compaction tombstones
    /// after the topic disappears from the metadata image.
    #[must_use]
    pub(crate) fn keys_for_topic(&self, topic_id: Uuid) -> Vec<WalIndexKey> {
        self.by_topic_partition
            .iter()
            .filter(|((id, _), _)| *id == topic_id)
            .flat_map(|((_, partition), entries)| {
                entries.keys().map(|first_offset| WalIndexKey {
                    topic_id,
                    partition: *partition,
                    first_offset: *first_offset,
                })
            })
            .collect()
    }

    /// Topic ids represented in the projection.
    #[must_use]
    pub(crate) fn topic_ids(&self) -> HashSet<Uuid> {
        self.by_topic_partition
            .keys()
            .map(|(topic_id, _)| *topic_id)
            .collect()
    }

    /// Object keys referenced by at least one projected range.
    #[must_use]
    pub(crate) fn referenced_objects(&self) -> HashSet<String> {
        self.by_topic_partition
            .values()
            .flat_map(BTreeMap::values)
            .map(|(object_key, _)| object_key.clone())
            .collect()
    }

    /// Return whether every entry from this flush record is present in the
    /// committed projection under the same object key.
    #[must_use]
    pub fn contains_record(&self, record: &WalFlushRecord) -> bool {
        record.entries.iter().all(|entry| {
            self.by_topic_partition
                .get(&(entry.topic_id, entry.partition))
                .and_then(|entries| entries.get(&entry.first_offset))
                .is_some_and(|(object_key, indexed)| {
                    object_key == &record.object_key && indexed == entry
                })
        })
    }

    #[cfg(test)]
    pub(crate) fn lookup(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
    ) -> Option<(String, u64, u32)> {
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        let (_, (object_key, entry)) = entries.range(..=offset).next_back()?;
        (offset <= entry.last_offset)
            .then(|| (object_key.clone(), entry.byte_start, entry.byte_len))
    }

    /// Return the contiguous whole-batch range covering `offset`, capped by
    /// `max_bytes` after the first batch.
    #[must_use]
    pub fn lookup_fetch_range(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Option<(String, u64, u64)> {
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        let (&first_offset, (object_key, first)) = entries.range(..=offset).next_back()?;
        if offset > first.last_offset {
            return None;
        }

        let mut byte_len = u64::from(first.byte_len);
        let max_bytes = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        for (_, (next_key, next)) in entries.range((
            std::ops::Bound::Excluded(first_offset),
            std::ops::Bound::Unbounded,
        )) {
            if next_key != object_key
                || first.byte_start.checked_add(byte_len) != Some(next.byte_start)
                || byte_len.saturating_add(u64::from(next.byte_len)) > max_bytes
            {
                break;
            }
            byte_len += u64::from(next.byte_len);
        }
        Some((object_key.clone(), first.byte_start, byte_len))
    }

    /// Return the highest flushed offset plus one for the partition.
    #[must_use]
    pub fn flushed_frontier(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        entries
            .values()
            .next_back()
            .map(|(_, entry)| entry.last_offset + 1)
    }

    /// Return the smallest first offset covered by object storage for the partition.
    #[must_use]
    pub fn earliest_covered(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        self.by_topic_partition
            .get(&(topic_id, partition))?
            .values()
            .next()
            .map(|(_, entry)| entry.first_offset)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    fn entry(p: i32, f: i64, l: i64) -> WalIndexEntry {
        WalIndexEntry {
            topic_id: Uuid::from_u128(1),
            partition: p,
            first_offset: f,
            last_offset: l,
            byte_start: 0,
            byte_len: 1,
        }
    }

    #[test]
    fn floor_lookup_returns_covering_object() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o1".into(),
            format_version: 1,
            entries: vec![entry(0, 0, 4)],
        });
        c.apply(&WalFlushRecord {
            object_key: "o2".into(),
            format_version: 1,
            entries: vec![entry(0, 5, 9)],
        });
        let t = Uuid::from_u128(1);
        assert!(c.lookup(t, 0, 3).unwrap().0 == "o1");
        assert!(c.lookup(t, 0, 7).unwrap().0 == "o2");
        assert!(c.lookup(t, 0, 20).is_none());
        assert!(c.flushed_frontier(t, 0) == Some(10));
    }

    #[test]
    fn fetch_range_stops_on_a_batch_boundary_after_the_first_batch() {
        let mut c = WalIndexCache::default();
        let mut entries = vec![entry(0, 0, 0), entry(0, 1, 1), entry(0, 2, 2)];
        for (index, entry) in entries.iter_mut().enumerate() {
            entry.byte_start = u64::try_from(index * 10).unwrap();
            entry.byte_len = 10;
        }
        c.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries,
        });
        let t = Uuid::from_u128(1);

        assert!(c.lookup_fetch_range(t, 0, 0, 5) == Some(("o".into(), 0, 10)));
        assert!(c.lookup_fetch_range(t, 0, 0, 20) == Some(("o".into(), 0, 20)));
    }

    #[test]
    fn apply_is_idempotent() {
        let mut c = WalIndexCache::default();
        let rec = WalFlushRecord {
            object_key: "o1".into(),
            format_version: 1,
            entries: vec![entry(0, 0, 4)],
        };
        c.apply(&rec);
        c.apply(&rec);
        let t = Uuid::from_u128(1);
        assert!(c.flushed_frontier(t, 0) == Some(5));
    }

    #[test]
    fn contains_record_requires_the_exact_committed_object_and_entry() {
        let mut cache = WalIndexCache::default();
        let record = WalFlushRecord {
            object_key: "o1".into(),
            format_version: 1,
            entries: vec![entry(0, 0, 4)],
        };
        assert!(!cache.contains_record(&record));

        cache.apply(&record);
        assert!(cache.contains_record(&record));

        let mut wrong_object = record.clone();
        wrong_object.object_key = "o2".into();
        assert!(!cache.contains_record(&wrong_object));

        let mut wrong_entry = record.clone();
        wrong_entry.entries[0].byte_len += 1;
        assert!(!cache.contains_record(&wrong_entry));
    }

    #[test]
    fn wincode_round_trips() {
        let rec = WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![entry(3, 1, 2)],
        };
        let bytes = rec.to_bytes().unwrap();
        assert!(WalFlushRecord::from_bytes(&bytes).unwrap() == rec);
    }

    #[test]
    fn earliest_covered_is_smallest_first_offset() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o2".into(),
            format_version: 1,
            entries: vec![entry(0, 5, 9)],
        });
        c.apply(&WalFlushRecord {
            object_key: "o1".into(),
            format_version: 1,
            entries: vec![entry(0, 0, 4)],
        });

        assert!(c.earliest_covered(Uuid::from_u128(1), 0) == Some(0));
        assert!(c.earliest_covered(Uuid::from_u128(1), 1).is_none());
    }

    #[test]
    fn compaction_key_round_trips() {
        let key = WalIndexKey {
            topic_id: Uuid::from_u128(7),
            partition: 3,
            first_offset: 42,
        };
        assert!(WalIndexKey::from_bytes(&key.to_bytes()) == Some(key));
        assert!(WalIndexKey::from_bytes(&key.to_bytes()[..27]).is_none());
    }

    #[test]
    fn replacing_a_range_drops_only_the_unreferenced_object() {
        let mut cache = WalIndexCache::default();
        let mut shared = entry(0, 0, 4);
        cache.apply(&WalFlushRecord {
            object_key: "old".into(),
            format_version: 1,
            entries: vec![shared.clone(), entry(1, 0, 4)],
        });
        shared.byte_len = 2;
        cache.apply(&WalFlushRecord {
            object_key: "new".into(),
            format_version: 1,
            entries: vec![shared],
        });
        assert!(cache.referenced_objects() == ["new".into(), "old".into()].into());

        cache.remove(WalIndexKey {
            topic_id: Uuid::from_u128(1),
            partition: 1,
            first_offset: 0,
        });
        assert!(cache.referenced_objects() == ["new".into()].into());
    }
}
