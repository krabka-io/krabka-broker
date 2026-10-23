//! Reduction of committed index events into a deterministic capture boundary.

use {
    super::{
        super::{
            WalIndexEntry,
            index::{
                WalDeleteFloorKey, WalDeleteFloorRecord, WalFlushRecord, WalIndexKey,
                wal_index_key_version,
            },
        },
        CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture,
    },
    std::collections::{BTreeMap, HashMap},
    uuid::Uuid,
};

/// A partition in a capture, ordered by topic name, then topic id, then partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    topic: String,
    topic_id: Uuid,
    partition: i32,
}

/// Reducer for committed keyed index events.
#[derive(Default)]
pub struct WalCaptureProjection {
    ranges: HashMap<WalIndexKey, CapturedWalRange>,
    floors: HashMap<WalDeleteFloorKey, i64>,
}

impl WalCaptureProjection {
    /// Apply one committed keyed value or tombstone.
    ///
    /// # Errors
    /// Returns an error for malformed keys, values, or inconsistent records.
    pub fn apply(&mut self, key: Option<&[u8]>, value: Option<&[u8]>) -> Result<(), String> {
        // Every index record is keyed: the flusher publishes one keyed record
        // per range, and compaction keeps only the latest per key. An
        // unkeyed record has no range it could be the latest for.
        let key = key.ok_or_else(|| "diskless WAL index record has no key".to_owned())?;
        if let Some(floor_key) = WalDeleteFloorKey::from_bytes(key) {
            if let Some(value) = value {
                let record = WalDeleteFloorRecord::from_bytes(value)?;
                if record.topic_id != floor_key.topic_id
                    || record.partition != floor_key.partition
                    || record.floor < 0
                {
                    return Err("diskless WAL delete-floor key/value mismatch".to_owned());
                }
                let floor = self.floors.entry(floor_key).or_default();
                *floor = (*floor).max(record.floor);
            } else {
                self.floors.remove(&floor_key);
            }
            return Ok(());
        }
        let range_key =
            WalIndexKey::from_bytes(key).ok_or_else(|| match wal_index_key_version(key) {
                Some(version)
                    if version != WalIndexKey::KEY_VERSION
                        && version != WalDeleteFloorKey::KEY_VERSION =>
                {
                    format!("unknown diskless WAL index key version {version}")
                }
                _ => "invalid diskless WAL index key".to_owned(),
            })?;
        let Some(value) = value else {
            self.ranges.remove(&range_key);
            return Ok(());
        };
        let record = WalFlushRecord::from_bytes(value)?;
        let entry = record
            .entries
            .into_iter()
            .find(|entry| WalIndexKey::from(entry) == range_key)
            .ok_or_else(|| "diskless WAL index key/value mismatch".to_owned())?;
        Self::validate_range(&entry)?;
        self.store_range(range_key, record.object_key, entry);
        Ok(())
    }

    fn validate_range(entry: &WalIndexEntry) -> Result<(), String> {
        if entry.first_offset < 0 || entry.last_offset < entry.first_offset || entry.byte_len == 0 {
            return Err("invalid diskless WAL index range".to_owned());
        }
        Ok(())
    }

    fn store_range(&mut self, key: WalIndexKey, object_key: String, entry: WalIndexEntry) {
        self.ranges
            .insert(key, CapturedWalRange { object_key, entry });
    }

    /// Freeze the projection into a deterministic portable capture.
    ///
    /// # Errors
    /// Returns an error for unknown topic ids or inconsistent live ranges.
    pub fn capture<S: std::hash::BuildHasher>(
        &self,
        topics: &HashMap<Uuid, (String, i32), S>,
        source_cutoffs: Vec<i64>,
        captured_at_ms: u64,
    ) -> Result<DisklessWalCapture, String> {
        let mut grouped: BTreeMap<GroupKey, Vec<CapturedWalRange>> = BTreeMap::new();
        for (topic_id, (topic, partition_count)) in topics {
            if *partition_count < 0 {
                return Err(format!(
                    "negative partition count for diskless topic {topic}"
                ));
            }
            for partition in 0..*partition_count {
                grouped
                    .entry(GroupKey {
                        topic: topic.clone(),
                        topic_id: *topic_id,
                        partition,
                    })
                    .or_default();
            }
        }
        for range in self.ranges.values() {
            let Some((topic, _)) = topics.get(&range.entry.topic_id) else {
                continue;
            };
            grouped
                .entry(GroupKey {
                    topic: topic.clone(),
                    topic_id: range.entry.topic_id,
                    partition: range.entry.partition,
                })
                .or_default()
                .push(range.clone());
        }
        for WalDeleteFloorKey {
            topic_id,
            partition,
        } in self.floors.keys()
        {
            let Some((topic, _)) = topics.get(topic_id) else {
                continue;
            };
            grouped
                .entry(GroupKey {
                    topic: topic.clone(),
                    topic_id: *topic_id,
                    partition: *partition,
                })
                .or_default();
        }
        let mut partitions = Vec::with_capacity(grouped.len());
        for (
            GroupKey {
                topic,
                topic_id,
                partition,
            },
            mut ranges,
        ) in grouped
        {
            ranges.sort_by_key(|range| range.entry.first_offset);
            if ranges
                .windows(2)
                .any(|pair| pair[0].entry.last_offset >= pair[1].entry.first_offset)
            {
                return Err(format!(
                    "overlapping diskless WAL ranges for {topic}-{partition}"
                ));
            }
            let mut runs: Vec<CapturedWalRange> = Vec::with_capacity(ranges.len());
            for range in ranges {
                let joins_previous = runs.last().is_some_and(|previous| {
                    previous.object_key == range.object_key
                        && previous.entry.last_offset.checked_add(1)
                            == Some(range.entry.first_offset)
                        && previous
                            .entry
                            .byte_start
                            .checked_add(u64::from(previous.entry.byte_len))
                            == Some(range.entry.byte_start)
                });
                if joins_previous {
                    if let Some(previous) = runs.last_mut() {
                        previous.entry.last_offset = range.entry.last_offset;
                        previous.entry.byte_len = previous
                            .entry
                            .byte_len
                            .checked_add(range.entry.byte_len)
                            .ok_or_else(|| {
                                format!("diskless WAL run exceeds 4 GiB for {topic}-{partition}")
                            })?;
                        previous.entry.max_timestamp_ms = previous
                            .entry
                            .max_timestamp_ms
                            .max(range.entry.max_timestamp_ms);
                    }
                } else {
                    runs.push(range);
                }
            }
            let ranges = runs;
            let delete_floor = self
                .floors
                .get(&WalDeleteFloorKey {
                    topic_id,
                    partition,
                })
                .copied()
                .unwrap_or(0);
            let recovery_cutoff = ranges
                .last()
                .map_or(delete_floor, |range| {
                    range.entry.last_offset.saturating_add(1)
                })
                .max(delete_floor);
            partitions.push(DisklessPartitionCapture {
                topic,
                topic_id,
                partition,
                delete_floor,
                recovery_cutoff,
                ranges,
            });
        }
        Ok(DisklessWalCapture {
            format_version: DisklessWalCapture::FORMAT_VERSION,
            captured_at_ms,
            source_cutoffs,
            partitions,
            metadata_snapshot_sha256: None,
            rlmm_snapshot_sha256: None,
            group_offsets_sha256: None,
            authentication: None,
        })
    }
}
