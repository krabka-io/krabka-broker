//! The first compaction pass. It builds the key-to-newest-offset dedup map
//! over the sealed segments, then derives from it which transactional
//! producers still have surviving data. Both walk the same segment list before
//! any rewrite starts.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use krabka_ids::{Offset, ProducerId};
use tracing::instrument;

use super::{TxnDataState, batch_reader::read_all_batches, should_index_key};
use crate::{
    error::LogError,
    segment::Segment,
    txn_index::{AbortedTxn, TxnIndex},
};

/// Build a map of `key → latest absolute offset` across the given sealed
/// segments in input order.
///
/// The map excludes records with `key.is_none()`, because
/// [`rewrite_segments`] drops them. The map's value is the absolute offset of
/// the **newest** record seen for each key. Later writes overwrite earlier
/// ones.
#[instrument(
    level = "debug",
    skip_all,
    fields(segments = segments.len(), keys = tracing::field::Empty),
    err,
)]
pub fn build_offset_map(segments: &[&Segment]) -> Result<HashMap<Bytes, Offset>, LogError> {
    // Keyed by `Bytes` (cheap refcounted clone of the record key) rather
    // than `Vec<u8>` to avoid a heap copy of every key. Zero-length keys
    // are legal in Kafka and dedup as a distinct "empty key" like any other.
    let mut map: HashMap<Bytes, Offset> = HashMap::new();
    for seg in segments {
        for batch in read_all_batches(seg)? {
            // Control batches (txn commit/abort markers) carry a control-type
            // key that must NEVER enter the dedup map. Skip them entirely —
            // indexing their key silently dropped all-but-newest markers and
            // broke read_committed (the control-batch data-loss bug).
            if batch.attributes.is_control_batch() {
                continue;
            }
            for record in &batch.records {
                if !should_index_key(record.key.as_deref(), false) {
                    continue;
                }
                let key_bytes = record.key.as_ref().expect("should_index_key checked Some");
                let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
                map.insert(key_bytes.clone(), absolute);
            }
        }
    }
    tracing::Span::current().record("keys", map.len());
    Ok(map)
}

/// Per-producer transactional-data survival, computed in a first pass over the
/// sealed segments.
///
/// KIP-534 keeps a transaction's commit or abort marker as long as any of that
/// transaction's data records survive compaction. Once compaction removes all
/// of the data, the marker ages out through the delete horizon.
///
/// This type is seeded with the aborted-txn entries from each sealed segment's
/// `.txnindex`, so the rewrite can rebuild the survivor `.txnindex` for
/// transactions whose data still partly survives.
pub struct CleanedTransactionMetadata {
    /// Producers (`producer_id`) with at least one surviving data record.
    survivors: HashSet<ProducerId>,
    /// Aborted-txn entries gathered from the consumed segments' `.txnindex`
    /// files, in input order.
    aborted: Vec<AbortedTxn>,
}

impl CleanedTransactionMetadata {
    /// Build the metadata. For each producer, this records whether any of its
    /// transactional DATA records will survive, that is, a data record that is
    /// newest-for-key in `offset_map`. The aborted-txn entries come from every
    /// sealed segment's `.txnindex`.
    #[instrument(
        level = "debug",
        skip_all,
        fields(segments = segments.len(), survivors = tracing::field::Empty),
        err,
    )]
    pub fn build(
        segments: &[&Segment],
        offset_map: &HashMap<Bytes, Offset>,
    ) -> Result<Self, LogError> {
        let mut survivors: HashSet<ProducerId> = HashSet::new();
        let mut aborted: Vec<AbortedTxn> = Vec::new();
        for seg in segments {
            // Seed aborted-txn entries from this segment's transaction index.
            let idx = TxnIndex::open(seg.txn_index_path())?;
            aborted.extend(idx.entries().iter().copied());

            for batch in read_all_batches(seg)? {
                // Only data batches contribute survivors. Control batches
                // carry no data records.
                if batch.attributes.is_control_batch() {
                    continue;
                }
                // Only transactional producers (producer_id >= 0) matter for
                // marker retention.
                if batch.producer_id < 0 {
                    continue;
                }
                for record in &batch.records {
                    // A surviving data record is one that is newest-for-key.
                    let Some(key_bytes) = record.key.as_ref() else {
                        continue;
                    };
                    let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
                    if offset_map.get(key_bytes.as_ref()).copied() == Some(absolute) {
                        survivors.insert(ProducerId(batch.producer_id));
                        break;
                    }
                }
            }
        }
        tracing::Span::current().record("survivors", survivors.len());
        Ok(Self { survivors, aborted })
    }

    /// The transactional-data state for a given producer.
    #[must_use]
    pub fn txn_state(&self, producer_id: ProducerId) -> TxnDataState {
        if producer_id.get() < 0 {
            return TxnDataState::NotTransactional;
        }
        if self.survivors.contains(&producer_id) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }

    /// Aborted-txn entries to carry forward into the rewritten survivor
    /// `.txnindex`. These are the entries whose aborted data still partly
    /// survives, that is, the producer is in the survivor set. The rewrite
    /// drops the entries of producers whose data is fully gone, together with
    /// the marker, which is then removable.
    pub(super) fn retained_aborted(&self) -> impl Iterator<Item = &AbortedTxn> {
        self.aborted
            .iter()
            .filter(move |e| self.survivors.contains(&e.producer_id))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_ids::Offset;
    use krabka_protocol::records::{Attributes, RecordBatch};
    use tempfile::tempdir;

    use super::*;
    use crate::compact::test_support::{
        control_batch, make_record, write_sealed_batches, write_sealed_segment,
    };

    #[test]
    fn control_batch_key_is_not_indexed() {
        let dir = tempdir().unwrap();
        // A control batch (commit marker) at offset 0, then keyed data at
        // offset 1. Only the data key should appear in the map; the control
        // marker's key must be absent.
        let mut data = RecordBatch {
            base_offset: 1,
            last_offset_delta: 0,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            attributes: Attributes::default(),
            ..RecordBatch::default()
        };
        data.records[0].offset_delta = 0;
        let seg = write_sealed_batches(dir.path(), &[control_batch(0, 1000, 1 /* COMMIT */), data]);
        let segment_refs: Vec<&Segment> = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(1)});
    }

    #[test]
    fn build_offset_map_keeps_newest_offset_per_key() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v2")),
                make_record(2, Some(b"k1"), Some(b"v3")), // k1 overwritten
            ],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(
            map == maplit::hashmap! {
            Bytes::from_static(b"k1") => Offset(2),
            Bytes::from_static(b"k2") => Offset(1)}
        );
    }

    #[test]
    fn build_offset_map_drops_null_key_records() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, None, Some(b"no-key-1")),
                make_record(1, Some(b"k1"), Some(b"v1")),
                make_record(2, None, Some(b"no-key-2")),
            ],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(1)});
    }

    #[test]
    fn build_offset_map_across_segments_uses_newest() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        );
        let second_segment = write_sealed_segment(
            dir.path(),
            10,
            vec![make_record(0, Some(b"k1"), Some(b"v2"))],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment, &second_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(10)});
    }

    // Survivor detection compares each record's absolute offset
    // (`base_offset + offset_delta`) against the newest-for-key offset in the
    // offset map (`== Some(absolute)`). Two transactional producers write the
    // SAME key k1:
    //   - producer 1000 at offset 0 (superseded), and
    //   - producer 2000 at base 10, delta 5 → offset 15 (newest for k1).
    // The map therefore holds k1 → 15, so producer 2000 survives and producer
    // 1000 does not. This pins:
    //   - `absolute = base_offset + offset_delta` (line 429): mutating `+`→`-`
    //     makes 2000's record resolve to 5, not 15 → 2000 misclassified
    //     `DataFullyGone`.
    //   - the `== Some(absolute)` equality (line 430): mutating `==`→`!=`
    //     inverts the match — 2000 (the match) becomes `DataFullyGone` and
    //     1000 (the non-match) becomes `DataSurvives`.
    #[test]
    fn build_detects_surviving_txn_producer() {
        let dir = tempdir().unwrap();
        // Producer 1000: k1 at offset 0 — superseded by producer 2000.
        let old = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: 1000,
            attributes: Attributes::default().with_transactional(true),
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            ..RecordBatch::default()
        };
        // Producer 2000: k1 at base 10, offset_delta 5 → absolute offset 15,
        // the newest-for-key record.
        let newest = RecordBatch {
            base_offset: 10,
            last_offset_delta: 5,
            producer_id: 2000,
            attributes: Attributes::default().with_transactional(true),
            records: vec![make_record(5, Some(b"k1"), Some(b"v2"))],
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[old, newest]);
        let segment_refs: Vec<&Segment> = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        // Sanity: the newest-for-key absolute offset is 15 (10 + 5).
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(15)});

        let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
        // Producer 2000's newest data survives; producer 1000's is superseded.
        assert2::assert!(txn.txn_state(ProducerId(2000)) == TxnDataState::DataSurvives);
        assert2::assert!(txn.txn_state(ProducerId(1000)) == TxnDataState::DataFullyGone);
        assert2::assert!(txn.txn_state(ProducerId(0)) == TxnDataState::DataFullyGone);
        assert2::assert!(txn.txn_state(ProducerId(-2)) == TxnDataState::NotTransactional);
    }
}
