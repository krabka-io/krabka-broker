//! In-memory cache for quorum-committed diskless WAL tail batches.

use {
    crate::{config::DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE, wal::quorum::registry::ShardId},
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_protocol::records::RecordBatch,
    krabka_units::convert::ByteSizeExt as _,
    std::{
        collections::{BTreeMap, HashMap, VecDeque},
        sync::Mutex,
    },
    uuid::Uuid,
};

/// Advisory cache of recently quorum-committed diskless WAL batches.
#[derive(Debug)]
pub(crate) struct HotTailCache {
    max_bytes: usize,
    state: Mutex<HotTailState>,
}

impl Default for HotTailCache {
    fn default() -> Self {
        Self::new(
            DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE
                .bytes_u64()
                .try_into()
                .unwrap_or(usize::MAX),
        )
    }
}

impl HotTailCache {
    #[must_use]
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(HotTailState::default()),
        }
    }

    pub(crate) fn insert_run(&self, topic_id: Uuid, partition: PartitionIndex, bytes: &Bytes) {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let mut cur = bytes.slice(offset..);
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                return;
            };
            // What the decode consumed is the batch's length on the wire.
            // `encoded_len` is the decoded batch's uncompressed length, which a
            // compressed batch does not occupy.
            let len = bytes.len() - offset - cur.len();
            if len == 0 || offset + len > bytes.len() {
                return;
            }
            self.insert_batch(
                topic_id,
                partition,
                bytes.slice(offset..offset + len),
                &batch,
            );
            offset += len;
        }
    }

    /// The cached batch that covers `fetch_offset`, or `None`.
    ///
    /// `limit_offset` is the exclusive upper bound of the fetch's visibility
    /// window, and the cache honours it in whole batches: a batch that reaches
    /// at or beyond it is a miss, and the fetch falls through to the log read
    /// path that can clamp inside the run. Nothing here may be looser than that
    /// window. A `read_uncommitted` fetch is capped at the lower of the high
    /// watermark and KFC-1's delivery watermark, and the delivery watermark can
    /// sit below a batch that is durable and quorum-committed but not yet due.
    pub(crate) fn get(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        fetch_offset: i64,
        limit_offset: i64,
        max_bytes: usize,
    ) -> Option<Bytes> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let map = state.entries.get(&ShardId {
            topic_id,
            partition,
        })?;
        let (_base, entry) = map.range(..=fetch_offset).next_back()?;
        if fetch_offset <= entry.last_offset
            && entry.last_offset < limit_offset
            && entry.bytes.len() <= max_bytes
        {
            Some(entry.bytes.clone())
        } else {
            None
        }
    }

    fn insert_batch(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        bytes: Bytes,
        batch: &RecordBatch,
    ) {
        let base_offset = batch.base_offset;
        let last_offset = base_offset + i64::from(batch.last_offset_delta);
        let key = ShardId {
            topic_id,
            partition,
        };
        let len = bytes.len();
        let bytes = if len <= self.max_bytes {
            Bytes::copy_from_slice(&bytes)
        } else {
            bytes
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(old) = state
            .entries
            .get_mut(&key)
            .and_then(|batches| batches.remove(&base_offset))
        {
            state.total_bytes -= old.bytes.len();
            state.order.retain(|cached| {
                *cached
                    != CachedBatch {
                        shard: key,
                        base_offset,
                    }
            });
        }
        if len > self.max_bytes {
            if state.entries.get(&key).is_some_and(BTreeMap::is_empty) {
                state.entries.remove(&key);
            }
            return;
        }

        state
            .entries
            .entry(key)
            .or_default()
            .insert(base_offset, HotTailEntry { last_offset, bytes });
        state.order.push_back(CachedBatch {
            shard: key,
            base_offset,
        });
        state.total_bytes += len;
        while state.total_bytes > self.max_bytes {
            let Some(CachedBatch {
                shard: old_key,
                base_offset: old_offset,
            }) = state.order.pop_front()
            else {
                break;
            };
            let removed = state
                .entries
                .get_mut(&old_key)
                .and_then(|batches| batches.remove(&old_offset));
            if let Some(removed) = removed {
                state.total_bytes -= removed.bytes.len();
            }
            if state.entries.get(&old_key).is_some_and(BTreeMap::is_empty) {
                state.entries.remove(&old_key);
            }
        }
    }

    pub(crate) fn remove_partition(&self, topic_id: Uuid, partition: PartitionIndex) {
        let key = ShardId {
            topic_id,
            partition,
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entries) = state.entries.remove(&key) {
            state.total_bytes -= entries
                .into_values()
                .map(|entry| entry.bytes.len())
                .sum::<usize>();
            state.order.retain(|cached| cached.shard != key);
        }
    }
}

#[derive(Debug, Default)]
struct HotTailState {
    total_bytes: usize,
    entries: HashMap<ShardId, BTreeMap<i64, HotTailEntry>>,
    order: VecDeque<CachedBatch>,
}

/// A cached batch, named by the partition it belongs to and its base offset.
#[derive(Debug, PartialEq, Eq)]
struct CachedBatch {
    shard: ShardId,
    base_offset: i64,
}

#[derive(Debug, Clone)]
struct HotTailEntry {
    last_offset: i64,
    bytes: Bytes,
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::test_support::default_records_batch, assert2::check, bytes::BytesMut,
        krabka_compression::CompressionType,
    };

    /// An offset above every cached batch, for a lookup the window does not
    /// bound.
    const UNBOUNDED: i64 = i64::MAX;

    fn assert_run_floor_and_hit(
        cache: &HotTailCache,
        topic_id: Uuid,
        first_partition: PartitionIndex,
        second_partition: PartitionIndex,
        first: &Bytes,
        second: Bytes,
    ) {
        cache.insert_run(topic_id, first_partition, first);
        cache.insert_run(topic_id, second_partition, &second);

        check!(
            cache
                .get(topic_id, first_partition, 0, UNBOUNDED, usize::MAX)
                .is_none()
        );
        check!(cache.get(topic_id, second_partition, 2, UNBOUNDED, usize::MAX) == Some(second));
    }

    #[test]
    fn hot_tail_cache_floor_lookup_and_bound() {
        let topic_id = Uuid::from_u128(7);
        let first = batch_bytes(0, 1);
        let second = batch_bytes(2, 2);
        let cache = HotTailCache::new(second.len());

        assert_run_floor_and_hit(
            &cache,
            topic_id,
            PartitionIndex(0),
            PartitionIndex(0),
            &first,
            second,
        );
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 3, UNBOUNDED, usize::MAX)
                .is_some()
        );
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 3, UNBOUNDED, 1)
                .is_none()
        );
    }

    #[test]
    fn hot_tail_cache_serves_only_a_batch_that_ends_below_the_limit() {
        let cache = HotTailCache::new(usize::MAX);
        let topic_id = Uuid::from_u128(11);
        // One batch per run: [0, 1] and [2, 3].
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(0, 2));
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(2, 2));

        // The window ends inside the second batch, which is what a delivery
        // watermark below a batch that is not due yet looks like.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 2, 3, usize::MAX)
                .is_none()
        );
        // The window ends exactly at the batch's last offset, so the batch is
        // still partly outside it.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 1, usize::MAX)
                .is_none()
        );
        // The window covers the whole batch.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 2, usize::MAX)
                .is_some()
        );
        // Nothing at all is deliverable yet.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 0, usize::MAX)
                .is_none()
        );
    }

    #[test]
    fn hot_tail_cache_is_byte_bounded_across_partitions() {
        let cache = HotTailCache::new(batch_bytes(2, 1).len());
        let topic_id = Uuid::from_u128(12);
        let first = batch_bytes(0, 1);
        let second = batch_bytes(2, 1);

        assert_run_floor_and_hit(
            &cache,
            topic_id,
            PartitionIndex(0),
            PartitionIndex(1),
            &first,
            second,
        );
    }

    #[test]
    fn oversized_replacement_removes_the_cached_batch() {
        let topic_id = Uuid::from_u128(13);
        let first = batch_bytes(0, 1);
        let cache = HotTailCache::new(first.len());

        cache.insert_run(topic_id, PartitionIndex(0), &first);
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(0, 2));

        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, UNBOUNDED, usize::MAX)
                .is_none()
        );
    }

    #[test]
    fn cached_batch_does_not_retain_the_run_allocation() {
        let topic_id = Uuid::from_u128(14);
        let large = batch_bytes(0, 100);
        let small = batch_bytes(100, 1);
        let mut run = BytesMut::with_capacity(large.len() + small.len());
        run.extend_from_slice(&large);
        run.extend_from_slice(&small);
        let run = run.freeze();
        let small_in_run = run[large.len()..].as_ptr();
        let cache = HotTailCache::new(small.len());

        cache.insert_run(topic_id, PartitionIndex(0), &run);

        let cached = cache
            .get(topic_id, PartitionIndex(0), 100, UNBOUNDED, usize::MAX)
            .expect("small batch is cached");
        check!(cached.as_ptr() != small_in_run);
        check!(cached == small);
    }

    /// A batch of three records whose values are `value`, at `base_offset`,
    /// encoded with `codec`, as a producer would send it.
    fn coded_batch_bytes(base_offset: i64, codec: CompressionType, value: &[u8]) -> Bytes {
        let mut batch = default_records_batch().count(3).call();
        batch.base_offset = base_offset;
        batch.attributes = batch.attributes.with_compression(codec);
        for record in &mut batch.records {
            record.value = Some(Bytes::copy_from_slice(value));
        }
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    }

    /// 512 bytes no codec can shrink: a run of distinct byte pairs.
    fn incompressible_value() -> Vec<u8> {
        (0..256_u16)
            .flat_map(|index| {
                let [high, low] = (index.wrapping_mul(40_503)).to_be_bytes();
                [high, low]
            })
            .collect()
    }

    /// A run cached on its way to durability is split into the batches it
    /// holds, each one exactly the bytes it occupies in the run. A compressed
    /// batch occupies its compressed length, which the decoded batch's
    /// `encoded_len` does not report, so a split by that length cut the first
    /// batch short or long and lost every batch after it (#1525).
    #[yare::parameterized(
        uncompressed = { CompressionType::None, b"value".to_vec() },
        lz4_compressible = { CompressionType::Lz4, vec![b'a'; 512] },
        lz4_incompressible = { CompressionType::Lz4, incompressible_value() },
        gzip_compressible = { CompressionType::Gzip, vec![b'a'; 512] },
    )]
    fn a_run_is_cached_as_the_exact_bytes_of_each_batch(codec: CompressionType, value: Vec<u8>) {
        let topic_id = Uuid::from_u128(11);
        let partition = PartitionIndex(0);
        let batches = [0, 3, 6].map(|base| coded_batch_bytes(base, codec, &value));
        let cache = HotTailCache::new(usize::MAX);

        cache.insert_run(topic_id, partition, &Bytes::from(batches.concat()));

        let cached =
            [0, 3, 6].map(|offset| cache.get(topic_id, partition, offset, UNBOUNDED, usize::MAX));
        check!(cached == batches.map(Some));
    }

    fn batch_bytes(base_offset: i64, records: i32) -> Bytes {
        let mut batch = default_records_batch().count(records).call();
        batch.base_offset = base_offset;
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    }
}
