//! One compaction pass over the sealed segment list, and the broker-side
//! inputs that pass depends on.
//!
//! The pass rewrites every sealed segment into a single new one and never
//! touches the active segment, so the log-end offset a producer sees does
//! not move.

use krabka_ids::{Offset, ProducerId};
use tracing::instrument;

use super::Log;
use crate::{error::LogError, retention, segment::Segment};

/// Inputs to one [`Log::compact`] pass that depend on broker-side state.
///
/// These inputs are the wall clock that computes the KIP-534 delete horizons,
/// and the set of producers that count as active.
///
/// `active_producers` maps `producer_id` to the `base_offset` of that
/// producer's last batch. When compaction removes every record of that batch,
/// the cleaner writes a bare batch header (`RETAIN_EMPTY`) again, so the
/// producer's sequence and epoch state and the log-end offset survive.
#[derive(Debug, Clone)]
pub struct CompactionContext {
    /// Wall clock for this pass. It drives delete-horizon stamps and expiry.
    pub now: std::time::SystemTime,
    /// `producer_id` → last batch `base_offset` for currently-active
    /// producers.
    pub active_producers: std::collections::HashMap<ProducerId, Offset>,
}

impl Log {
    /// Run one compaction pass over the sealed segment list.
    ///
    /// This method does nothing when fewer than 2 sealed segments exist,
    /// because there is nothing to dedup yet. It never touches the active
    /// segment. The output is a single new sealed segment at the lowest input
    /// base offset, and it replaces all consumed sealed segments.
    ///
    /// `ctx` carries the wall clock, which drives the KIP-534 delete-horizon
    /// computation, and the set of currently-active producers. The cleaner
    /// keeps the last batch of each active producer with `RETAIN_EMPTY`, even
    /// when compaction removes all of its records.
    #[instrument(
        level = "info",
        skip_all,
        fields(sealed_segments = self.segments.len()),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn compact(&mut self, ctx: &CompactionContext) -> Result<(), LogError> {
        if self.segments.is_empty() {
            return Ok(());
        }

        let (index_interval, delete_retention) = {
            let cfg_guard = self.config.read().unwrap();
            if cfg_guard.cleanup_policy != crate::CleanupPolicy::Compact {
                return Ok(());
            }
            (cfg_guard.index_interval, cfg_guard.delete_retention)
        };

        let now_ms = retention::now_ms(ctx.now);
        // Compaction rewrites sealed segments, so any record the last
        // activation walk read may not survive it.
        let compacted_from = self.log_start_offset();
        self.invalidate_delivery_schedule(compacted_from);
        let consumed_bases: Vec<Offset> = self.segments.iter().map(Segment::base_offset).collect();

        // Borrow sealed segments to run map + rewrite (which open
        // additional file handles internally for reading). Then drop the
        // borrows and clear self.segments so the original segments'
        // file handles close before atomic_swap deletes/renames
        // (Windows requires no open handle on a file before remove/rename).
        let rewrite = {
            let sealed_refs: Vec<&Segment> = self.segments.iter().collect();
            let offset_map = crate::compact::build_offset_map(&sealed_refs)?;
            let txn_meta =
                crate::compact::CleanedTransactionMetadata::build(&sealed_refs, &offset_map)?;
            crate::compact::rewrite_segments(
                &self.dir,
                &sealed_refs,
                &offset_map,
                &txn_meta,
                crate::compact::RewriteRetention {
                    now_ms,
                    delete_retention,
                },
                &ctx.active_producers,
                index_interval,
            )?
        };

        self.segments.clear();
        crate::compact::atomic_swap(&self.dir, &consumed_bases, &rewrite)?;

        // open_active(validate=true) tail-scans the new .log to populate
        // last_offset + max_timestamp; then seal() flips the flag. The rewrite
        // leaves the index sidecars empty, so that scan starts at position 0
        // and sees every batch: the segment's maximum is exact.
        let mut new_seg = Segment::open_active(&self.dir, rewrite.new_base_offset, true)?;
        new_seg.set_io(self.io.clone());
        new_seg.seal();
        self.segments.push(new_seg);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::{bytes, mebibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{compaction_ctx, keyed_batch},
    };

    #[test]
    fn compact_no_op_when_only_one_segment() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        let mut b = keyed_batch(0, &[(0, b"k1", b"v1")]);
        log.append(&mut b).unwrap();
        // Only the active segment exists; sealed list is empty.
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == 1);
    }

    #[test]
    fn compact_dedupes_sealed_segments_keeps_active_intact() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256), // force rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Write 3 sealed segments, each with one record under "k1".
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
            // Roll the active segment by forcing a tick or a large pad batch.
            // Easiest: call set_segment_size or rely on the small segment_size.
        }
        // Add one more append to ensure the last write is in a fresh active
        // segment (not part of what compaction touches).
        let mut b = keyed_batch(0, &[(0, b"active-key", b"active-value")]);
        log.append(&mut b).unwrap();

        let active_leo_before = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == active_leo_before);

        // After compaction: read everything, assert only the newest k1 plus
        // the active "active-key" survive.
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let all_records: Vec<_> = out.batches.iter().flat_map(|b| b.records.iter()).collect();
        let keys: Vec<&[u8]> = all_records
            .iter()
            .map(|r| r.key.as_ref().unwrap().as_ref())
            .collect();
        assert2::assert!(keys.contains(&b"k1".as_ref()));
        assert2::assert!(keys.contains(&b"active-key".as_ref()));
    }

    /// Compaction must run and must not do nothing. Three sealed segments
    /// each carry a record under the SAME key "k1". After `compact`, exactly
    /// ONE k1 record must remain, the newest one, "v2". The sealed segment
    /// list must collapse to a single rewritten segment. A compaction that
    /// returned `Ok(())` at once would leave all three k1 records and three
    /// sealed segments.
    #[test]
    fn compact_actually_dedupes_reducing_record_count() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1), // one batch per segment: every append exceeds this and rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Three sealed segments, each one record under "k1" (v0, v1, v2).
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        // A final append lands in a fresh active segment (untouched by compact).
        let mut tail = keyed_batch(0, &[(0, b"tail", b"t")]);
        log.append(&mut tail).unwrap();

        // Sanity: before compaction there are >= 2 sealed segments holding the
        // three k1 versions.
        assert2::assert!(log.segments.len() >= 2);

        log.compact(&compaction_ctx()).unwrap();

        // Sealed segments collapse to exactly one rewritten segment.
        assert2::assert!(log.segments.len() == 1);

        // Exactly one surviving k1 record, and it is the newest value "v2".
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let k1_values: Vec<&[u8]> = out
            .batches
            .iter()
            .flat_map(|b| b.records.iter())
            .filter(|r| r.key.as_deref() == Some(b"k1".as_ref()))
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert2::assert!(k1_values == vec![b"v2".as_ref()]);
    }

    #[test]
    fn compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        let mut b = keyed_batch(0, &[(0, b"active", b"x")]);
        log.append(&mut b).unwrap();
        log.compact(&compaction_ctx()).unwrap();
        let leo1 = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        let leo2 = log.log_end_offset();
        assert2::assert!(leo1 == leo2);
    }
}
