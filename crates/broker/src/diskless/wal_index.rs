//! Diskless WAL offset-to-object index records and in-memory projection.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use krabka_verified::{
    DisklessWalReplayAction, diskless_logical_range, diskless_retention_prefix,
    diskless_span_extension, diskless_wal_replay_decision,
};
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
    /// Newest record timestamp in the range, taken from the batch header at
    /// flush time. `retention.ms` reads this and nothing else.
    pub max_timestamp_ms: i64,
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
    keyed_ranges: HashSet<WalIndexKey>,
    replay_tombstones: HashSet<WalIndexKey>,
    legacy_replay_finished: bool,
    /// `DeleteRecords` floors, by partition. Records below one of these are
    /// deleted as far as every client is concerned, so the object tier stops
    /// answering for them the moment the trim lands rather than one flush tick
    /// later, when the flusher has tombstoned the ranges that hold them.
    ///
    /// The tombstones are the durable half. This map is not: a broker that
    /// restarts inside that tick comes back with an empty one and serves the
    /// deleted offsets again until its first flush tick expires the ranges.
    delete_floors: HashMap<(Uuid, i32), i64>,
}

impl WalIndexCache {
    /// Apply one legacy, unkeyed flush record to the projection.
    pub fn apply(&mut self, record: &WalFlushRecord) {
        for entry in &record.entries {
            let key = WalIndexKey::from(entry);
            let decision = diskless_wal_replay_decision(
                0,
                self.keyed_ranges.contains(&key),
                self.replay_tombstones.contains(&key),
                self.legacy_replay_finished,
            );
            if decision.action == DisklessWalReplayAction::Store {
                self.insert(record, entry);
            }
        }
    }

    /// Apply a keyed record, which remains authoritative over legacy replay
    /// regardless of cross-partition delivery order during upgrades.
    pub(crate) fn apply_keyed(&mut self, key: WalIndexKey, record: &WalFlushRecord) {
        let decision = diskless_wal_replay_decision(
            1,
            self.keyed_ranges.contains(&key),
            self.replay_tombstones.contains(&key),
            self.legacy_replay_finished,
        );
        self.set_replay_markers(key, decision.keyed_range, decision.replay_tombstone);
        if decision.action == DisklessWalReplayAction::Store {
            // A decoded keyed event is authoritative even when its payload is
            // malformed and does not contain the key. Remove any legacy value
            // first so malformed input fails closed instead of exposing it.
            self.remove_projected(key);
            for entry in &record.entries {
                if WalIndexKey::from(entry) == key {
                    self.insert(record, entry);
                }
            }
        }
    }

    fn set_replay_markers(&mut self, key: WalIndexKey, keyed: bool, tombstone: bool) {
        if keyed {
            self.keyed_ranges.insert(key);
        } else {
            self.keyed_ranges.remove(&key);
        }
        if tombstone {
            self.replay_tombstones.insert(key);
        } else {
            self.replay_tombstones.remove(&key);
        }
    }

    fn insert(&mut self, record: &WalFlushRecord, entry: &WalIndexEntry) {
        self.by_topic_partition
            .entry((entry.topic_id, entry.partition))
            .or_default()
            .insert(
                entry.first_offset,
                (record.object_key.clone(), entry.clone()),
            );
    }

    /// Remove one compacted range after its Kafka tombstone is committed.
    pub(crate) fn remove(&mut self, key: WalIndexKey) {
        let decision = diskless_wal_replay_decision(
            2,
            self.keyed_ranges.contains(&key),
            self.replay_tombstones.contains(&key),
            self.legacy_replay_finished,
        );
        self.set_replay_markers(key, decision.keyed_range, decision.replay_tombstone);
        if decision.action == DisklessWalReplayAction::Remove {
            self.remove_projected(key);
        }
    }

    fn remove_projected(&mut self, key: WalIndexKey) {
        let partition = (key.topic_id, key.partition);
        let empty = self
            .by_topic_partition
            .get_mut(&partition)
            .is_some_and(|entries| {
                entries.remove(&key.first_offset);
                entries.is_empty()
            });
        if empty {
            self.by_topic_partition.remove(&partition);
        }
    }

    /// Cross-partition legacy records cannot arrive before the replay fences
    /// anymore, so tombstone migration guards no longer need heap space.
    pub(crate) fn finish_legacy_replay(&mut self) {
        self.replay_tombstones.clear();
        self.legacy_replay_finished = true;
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

    /// Raise one partition's `DeleteRecords` floor. The floor only moves
    /// forward, so a stale retry cannot expose records an earlier trim removed.
    pub(crate) fn raise_delete_floor(&mut self, topic_id: Uuid, partition: i32, floor: i64) {
        let entry = self.delete_floors.entry((topic_id, partition)).or_insert(0);
        *entry = (*entry).max(floor);
    }

    /// The partition's `DeleteRecords` floor, or zero when none was set.
    #[must_use]
    pub(crate) fn delete_floor(&self, topic_id: Uuid, partition: i32) -> i64 {
        self.delete_floors
            .get(&(topic_id, partition))
            .copied()
            .unwrap_or(0)
    }

    /// Drop the floors a deleted topic left behind, so the map does not grow
    /// with every topic the cluster has ever trimmed.
    pub(crate) fn forget_topic(&mut self, topic_id: Uuid) {
        self.delete_floors.retain(|(id, _), _| *id != topic_id);
    }

    /// Keys of the oldest ranges this partition's retention allows to expire,
    /// oldest first.
    ///
    /// `retention_ms` and `retention_bytes` are `None` for Kafka's unlimited
    /// sentinel; `log_start_offset` is the `DeleteRecords` floor. The newest
    /// range is never returned, so the partition keeps a `flushed_frontier`.
    #[must_use]
    pub(crate) fn retention_expired_keys(
        &self,
        topic_id: Uuid,
        partition: i32,
        retention_ms: Option<i64>,
        retention_bytes: Option<u64>,
        log_start_offset: i64,
        now_ms: i64,
    ) -> Vec<WalIndexKey> {
        let Some(entries) = self.by_topic_partition.get(&(topic_id, partition)) else {
            return Vec::new();
        };
        // `BTreeMap` iterates by `first_offset`, which is the oldest-first
        // order the kernel's prefix walk expects.
        let ranges: Vec<&WalIndexEntry> = entries.values().map(|(_, entry)| entry).collect();
        let max_timestamps: Vec<i64> = ranges.iter().map(|entry| entry.max_timestamp_ms).collect();
        let byte_lens: Vec<u64> = ranges
            .iter()
            .map(|entry| u64::from(entry.byte_len))
            .collect();
        let last_offsets: Vec<i64> = ranges.iter().map(|entry| entry.last_offset).collect();
        let expired = diskless_retention_prefix(
            &max_timestamps,
            &byte_lens,
            &last_offsets,
            retention_ms,
            retention_bytes,
            log_start_offset,
            now_ms,
        );
        ranges
            .into_iter()
            .take(expired)
            .map(WalIndexKey::from)
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

    /// Whether at least one projected range still names this object.
    #[must_use]
    pub(crate) fn references_object(&self, object_key: &str) -> bool {
        self.by_topic_partition
            .values()
            .flat_map(BTreeMap::values)
            .any(|(candidate, _)| candidate == object_key)
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
        if offset < self.delete_floor(topic_id, partition) {
            return None;
        }
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        let indexed: Vec<_> = entries.values().collect();
        let logical: Vec<_> = indexed
            .iter()
            .map(|(_, entry)| (entry.first_offset, entry.last_offset))
            .collect();
        // Replay supplies this invariant. Fail closed if a truncated or
        // otherwise malformed index violates the proof kernel's preconditions.
        if logical.iter().any(|(first, last)| first > last)
            || logical.windows(2).any(|pair| pair[0].1 >= pair[1].0)
        {
            return None;
        }
        let first_index = diskless_logical_range(&logical, offset)?;
        let (object_key, first) = indexed[first_index];

        let mut byte_len = u64::from(first.byte_len);
        let max_bytes = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        for (next_key, next) in indexed.into_iter().skip(first_index).skip(1) {
            let Some(total) = diskless_span_extension(
                first.byte_start,
                byte_len,
                next.byte_start,
                u64::from(next.byte_len),
                next_key == object_key,
                max_bytes,
            ) else {
                break;
            };
            byte_len = total;
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
            .and_then(|(_, entry)| entry.last_offset.checked_add(1))
    }

    /// Return the smallest offset object storage still answers for, which is
    /// the smallest indexed first offset raised to the `DeleteRecords` floor.
    #[must_use]
    pub fn earliest_covered(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        let floor = self.delete_floor(topic_id, partition);
        self.by_topic_partition
            .get(&(topic_id, partition))?
            .values()
            .next()
            .map(|(_, entry)| entry.first_offset.max(floor))
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
            max_timestamp_ms: 0,
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
    fn fetch_range_falls_forward_across_an_offset_gap() {
        let mut c = WalIndexCache::default();
        let mut first = entry(0, 0, 0);
        first.byte_len = 10;
        let mut next = entry(0, 2, 2);
        next.byte_start = 10;
        next.byte_len = 10;
        c.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![first, next],
        });

        assert!(c.lookup_fetch_range(Uuid::from_u128(1), 0, 1, 10) == Some(("o".into(), 10, 10)));
    }

    #[test]
    fn fetch_range_keeps_offsets_below_the_object_floor_out_of_range() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![entry(0, 5, 5)],
        });

        assert!(c.lookup_fetch_range(Uuid::from_u128(1), 0, 4, 10).is_none());
    }

    #[test]
    fn fetch_range_stops_at_object_boundaries() {
        let mut c = WalIndexCache::default();
        let mut first = entry(0, 0, 0);
        first.byte_len = 10;
        c.apply(&WalFlushRecord {
            object_key: "first".into(),
            format_version: 1,
            entries: vec![first],
        });
        let mut next = entry(0, 1, 1);
        next.byte_start = 10;
        next.byte_len = 10;
        c.apply(&WalFlushRecord {
            object_key: "next".into(),
            format_version: 1,
            entries: vec![next],
        });

        assert!(
            c.lookup_fetch_range(Uuid::from_u128(1), 0, 0, 20) == Some(("first".into(), 0, 10))
        );
    }

    #[test]
    fn fetch_range_rejects_malformed_logical_indexes() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![entry(0, 4, 3)],
        });
        assert!(c.lookup_fetch_range(Uuid::from_u128(1), 0, 4, 10).is_none());

        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![entry(0, 0, 5), entry(0, 4, 6)],
        });
        assert!(c.lookup_fetch_range(Uuid::from_u128(1), 0, 4, 10).is_none());
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

    /// Three ranges, one batch each, with ascending timestamps and offsets.
    fn retention_cache() -> WalIndexCache {
        let mut cache = WalIndexCache::default();
        for (index, (first, last, timestamp)) in [(0i64, 4i64, 100i64), (5, 9, 200), (10, 14, 900)]
            .into_iter()
            .enumerate()
        {
            let mut entry = entry(0, first, last);
            entry.byte_len = 100;
            entry.max_timestamp_ms = timestamp;
            cache.apply(&WalFlushRecord {
                object_key: format!("o{index}"),
                format_version: 1,
                entries: vec![entry],
            });
        }
        cache
    }

    fn first_offsets(keys: &[WalIndexKey]) -> Vec<i64> {
        keys.iter().map(|key| key.first_offset).collect()
    }

    #[test]
    fn retention_expires_the_oldest_ranges_and_keeps_the_newest() {
        let cache = retention_cache();
        let topic = Uuid::from_u128(1);

        // Nothing configured expires nothing.
        assert!(
            cache
                .retention_expired_keys(topic, 0, None, None, 0, 1_000)
                .is_empty()
        );
        // `retention.ms` leaves everything newer than now - 500.
        assert!(
            first_offsets(&cache.retention_expired_keys(topic, 0, Some(500), None, 0, 1_000))
                == [0, 5]
        );
        // `retention.bytes` pays a 150-byte debt down with the oldest range.
        assert!(
            first_offsets(&cache.retention_expired_keys(topic, 0, None, Some(150), 0, 1_000))
                == [0]
        );
        // The `DeleteRecords` floor clears every range that ends below it.
        assert!(
            first_offsets(&cache.retention_expired_keys(topic, 0, None, None, 10, 1_000)) == [0, 5]
        );
        // A partition the projection has never seen has nothing to expire.
        assert!(
            cache
                .retention_expired_keys(topic, 1, Some(1), Some(0), 99, 1_000)
                .is_empty()
        );
    }

    #[test]
    fn a_delete_floor_hides_the_offsets_below_it_from_the_object_tier() {
        let mut cache = retention_cache();
        let topic = Uuid::from_u128(1);
        assert!(cache.earliest_covered(topic, 0) == Some(0));
        assert!(cache.lookup_fetch_range(topic, 0, 0, 100).is_some());

        cache.raise_delete_floor(topic, 0, 5);

        assert!(cache.earliest_covered(topic, 0) == Some(5));
        assert!(cache.lookup_fetch_range(topic, 0, 4, 100).is_none());
        assert!(cache.lookup_fetch_range(topic, 0, 5, 100).is_some());
        // The floor never moves back, so a stale retry cannot expose records
        // an earlier trim removed.
        cache.raise_delete_floor(topic, 0, 1);
        assert!(cache.delete_floor(topic, 0) == 5);
        // And it leaves with its topic.
        cache.forget_topic(topic);
        assert!(cache.delete_floor(topic, 0) == 0);
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

    #[test]
    fn keyed_value_wins_over_late_legacy_replay() {
        let mut cache = WalIndexCache::default();
        let entry = entry(0, 0, 4);
        let key = WalIndexKey::from(&entry);
        cache.apply_keyed(
            key,
            &WalFlushRecord {
                object_key: "new".into(),
                format_version: 1,
                entries: vec![entry.clone()],
            },
        );
        cache.apply(&WalFlushRecord {
            object_key: "legacy".into(),
            format_version: 1,
            entries: vec![entry],
        });

        assert!(cache.lookup(Uuid::from_u128(1), 0, 0).unwrap().0 == "new");
    }

    #[test]
    fn keyed_tombstone_prevents_legacy_resurrection() {
        let mut cache = WalIndexCache::default();
        let entry = entry(0, 0, 4);
        let key = WalIndexKey::from(&entry);
        cache.remove(key);
        cache.apply(&WalFlushRecord {
            object_key: "legacy".into(),
            format_version: 1,
            entries: vec![entry],
        });

        assert!(cache.lookup(Uuid::from_u128(1), 0, 0).is_none());
    }

    #[test]
    fn keyed_tombstone_dominates_legacy_in_both_replay_orders_and_on_retry() {
        let entry = entry(0, 0, 4);
        let key = WalIndexKey::from(&entry);
        let legacy = WalFlushRecord {
            object_key: "legacy".into(),
            format_version: 1,
            entries: vec![entry],
        };

        let mut legacy_first = WalIndexCache::default();
        legacy_first.apply(&legacy);
        legacy_first.remove(key);
        legacy_first.remove(key);
        assert!(legacy_first.lookup(Uuid::from_u128(1), 0, 0).is_none());

        let mut tombstone_first = WalIndexCache::default();
        tombstone_first.remove(key);
        tombstone_first.apply(&legacy);
        tombstone_first.remove(key);
        assert!(tombstone_first.lookup(Uuid::from_u128(1), 0, 0).is_none());
    }

    #[test]
    fn malformed_keyed_value_fails_closed_against_legacy_replay() {
        let expected = entry(0, 0, 4);
        let key = WalIndexKey::from(&expected);
        let wrong = entry(0, 1, 4);
        let mut cache = WalIndexCache::default();

        cache.apply(&WalFlushRecord {
            object_key: "legacy".into(),
            format_version: 1,
            entries: vec![expected.clone()],
        });

        cache.apply_keyed(
            key,
            &WalFlushRecord {
                object_key: "wrong".into(),
                format_version: 1,
                entries: vec![wrong],
            },
        );
        cache.apply(&WalFlushRecord {
            object_key: "late-legacy".into(),
            format_version: 1,
            entries: vec![expected],
        });

        assert!(cache.lookup(Uuid::from_u128(1), 0, 0).is_none());
    }

    #[test]
    fn frontier_fails_closed_when_the_successor_overflows() {
        let mut cache = WalIndexCache::default();
        cache.apply(&WalFlushRecord {
            object_key: "overflow".into(),
            format_version: 1,
            entries: vec![entry(0, i64::MAX, i64::MAX)],
        });

        assert!(cache.flushed_frontier(Uuid::from_u128(1), 0).is_none());
        assert!(cache.references_object("overflow"));
        assert!(!cache.references_object("missing"));
    }
}
