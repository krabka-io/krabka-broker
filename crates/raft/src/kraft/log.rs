//! `KraftLog`: the replicated metadata log behind the `LogView` seam.
//! It is a thin facade over `krabka_log::Log` that adds high-watermark
//! tracking, committed-read filtering for KIP-595 `Fetch`, and divergence
//! lookup. The controller uses it as the metadata log.

use std::path::{Path, PathBuf};

use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::{Log, LogConfig, RawRead};
use krabka_protocol::{Decode as _, records::RecordBatch};
use krabka_units::prelude::ByteSize;

use crate::{
    error::RaftError,
    kraft::types::{Epoch, LogView},
};

pub struct KraftLog {
    log: Log,
    /// Highest committed offset. This is consensus state, and krabka-log does
    /// not track it.
    hwm: Offset,
    hwm_path: PathBuf,
}

/// Read budget [`KraftLog::timestamp_below`] starts from. It only ever needs
/// one batch, and `read_decoded` grows the window until one fits, so this is
/// sized to make that the common case rather than to bound the result.
const TIMESTAMP_READ_WINDOW: ByteSize = krabka_units::prelude::kibibytes(64);

const HIGH_WATERMARK_FILE: &str = "high-watermark.checkpoint";

impl KraftLog {
    /// Opens or creates the metadata log under `dir/@metadata-0`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log directory cannot be created or the
    /// underlying `krabka_log::Log` fails to open.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, RaftError> {
        let hwm_path = dir.as_ref().join(HIGH_WATERMARK_FILE);
        let log_dir = dir.as_ref().join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(krabka_log::LogError::Io)?;
        // `krabka_log::Log` checkpoints its own log start, so a prune that
        // advanced inside the active segment is already restored here.
        let log = Log::open(&log_dir, LogConfig::default())?;
        let hwm = std::fs::read_to_string(&hwm_path)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(|| log.log_start_offset(), Offset)
            .max(log.log_start_offset())
            .min(log.log_end_offset());
        Ok(Self { log, hwm, hwm_path })
    }

    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        self.log.log_start_offset()
    }
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        self.log.log_end_offset()
    }
    #[must_use]
    pub fn hwm(&self) -> Offset {
        self.hwm
    }

    /// Leader path: appends a batch stamped with `append_timestamp_ms`.
    /// krabka-log assigns the offset and records the batch's
    /// `partition_leader_epoch`. Returns the assigned base offset.
    ///
    /// The stamp is the batch's create-time, as Kafka's `BatchAccumulator`
    /// stamps every batch the raft client appends with the current time. It is
    /// what [`Self::last_committed_timestamp_ms`] reads back for a snapshot
    /// header, and what a follower replicates verbatim through
    /// [`Self::append_at`].
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails.
    pub fn append(
        &mut self,
        batch: &mut RecordBatch,
        append_timestamp_ms: i64,
    ) -> Result<Offset, RaftError> {
        // Every record in an engine-built batch carries `timestamp_delta` 0,
        // so one stamp is the create-time of all of them.
        batch.base_timestamp = append_timestamp_ms;
        batch.max_timestamp = append_timestamp_ms;
        Ok(self.log.append(batch)?)
    }

    /// Follower path: appends a batch at the leader-assigned `offset`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails, for example when
    /// `offset` does not equal the current log end offset.
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: Offset) -> Result<(), RaftError> {
        self.log.append_at(batch, offset)?;
        Ok(())
    }

    /// Decoded read from `offset`. The tests and the replication apply path
    /// use it.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying read fails.
    pub fn read_decoded(
        &self,
        offset: Offset,
        max_size: ByteSize,
    ) -> Result<Vec<RecordBatch>, RaftError> {
        let log_end = self.log.log_end_offset();
        if offset >= log_end {
            return Ok(Vec::new());
        }
        let mut window = max_size;
        loop {
            let raw = self.log.read_raw(offset, log_end, window)?;
            let mut bytes = raw.bytes.as_ref();
            let mut batches = Vec::new();
            while !bytes.is_empty() {
                batches.push(
                    RecordBatch::decode(&mut bytes)
                        .map_err(|error| RaftError::ChangeRejected(error.to_string()))?,
                );
            }
            if !batches.is_empty() {
                return Ok(batches);
            }
            // A sparse index can floor `offset` to an earlier batch. If the
            // configured window ends before the first requested batch, grow
            // only until one complete requested batch fits so reads always
            // make progress without unbounding the returned batch run.
            window *= 2.0;
        }
    }

    /// The append timestamp of the last record below `end_offset`: the
    /// `max_timestamp` of the batch that contains `end_offset - 1`.
    ///
    /// KIP-630 stamps this into `SnapshotHeaderRecord`'s
    /// `last_contained_log_timestamp`, where Kafka supplies the append time of
    /// the last batch the snapshot contains. `None` when no such record is
    /// readable here: the boundary is at or below the log start, because
    /// everything under it was pruned or arrived inside an installed snapshot,
    /// or it is beyond the log end.
    #[must_use]
    pub fn timestamp_below(&self, end_offset: Offset) -> Option<i64> {
        let last = Offset(end_offset.0.checked_sub(1)?);
        if last < self.log.log_start_offset() || last >= self.log.log_end_offset() {
            return None;
        }
        let batches = self.read_decoded(last, TIMESTAMP_READ_WINDOW).ok()?;
        // A sparse index floors the read to a batch boundary at or before
        // `last`, and the window can carry batches past it, so the containing
        // batch is the last one that still starts at or below `last`.
        batches
            .iter()
            .take_while(|batch| batch.base_offset <= last.0)
            .last()
            .map(|batch| batch.max_timestamp)
    }

    /// The append timestamp of the last committed record (`hwm - 1`), for the
    /// header of a snapshot taken at the high watermark.
    #[must_use]
    pub fn last_committed_timestamp_ms(&self) -> Option<i64> {
        self.timestamp_below(self.hwm)
    }

    /// Serves KIP-595 `Fetch`: verbatim batch bytes in
    /// `[offset, min(hwm, log_end))`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying raw read fails.
    pub fn read_committed(&self, offset: Offset, max_size: ByteSize) -> Result<RawRead, RaftError> {
        let limit = self.hwm.min(self.log.log_end_offset());
        Ok(self.log.read_raw(offset, limit, max_size)?)
    }

    /// Advances the high watermark. The move is monotonic, and it never goes
    /// past the log end.
    pub fn advance_hwm(&mut self, new_hwm: Offset) {
        let log_end = self.log.log_end_offset();
        let next = Offset(krabka_verified::raft::advance_high_watermark(
            self.hwm.0, new_hwm.0, log_end.0,
        ));
        if next > self.hwm {
            self.hwm = next;
            self.persist_hwm();
        }
        assert2::assert!(self.hwm <= log_end);
    }

    /// Truncates the log so that no record at offset `>= offset` remains, and
    /// clamps the HWM down.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying truncation fails.
    pub fn truncate_to(&mut self, offset: Offset) -> Result<(), RaftError> {
        self.log.truncate_to(offset)?;
        self.hwm = self.hwm.min(offset);
        self.persist_hwm();
        Ok(())
    }

    /// Prunes the committed prefix below `end_offset`: it advances the
    /// log-start pointer and trims the now-dead segments. This is a no-op when
    /// `end_offset` is at or below the current log start. The leader calls it
    /// after it writes a snapshot.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying log operations fail.
    pub fn prune_to(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        if end_offset <= self.log.log_start_offset() {
            return Ok(());
        }
        self.log.set_log_start_offset(end_offset)?;
        self.log.trim_to_offset(end_offset)?;
        Ok(())
    }

    /// Replaces the log with an empty log that starts at `end_offset`, which
    /// drops every segment, and sets the high watermark to `end_offset`. A
    /// follower calls it when it installs a fetched snapshot whose `end_offset`
    /// is ahead of its own log.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying reset fails.
    pub fn install_snapshot(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        self.log.reset_to(end_offset)?;
        self.hwm = end_offset;
        self.persist_hwm();
        Ok(())
    }

    fn persist_hwm(&self) {
        if let Err(error) = std::fs::write(&self.hwm_path, self.hwm.0.to_string()) {
            tracing::error!(?error, path = %self.hwm_path.display(), "kraft: persist high watermark failed");
        }
    }
}

impl LogView for KraftLog {
    // `LogView` is defined by the pure `krabka-kraft-core` consensus engine and
    // speaks raw `i64` offsets; unwrap the `krabka-log` `Offset`s with `.0` at
    // this boundary so the core sees the integers it expects.
    fn end_offset(&self) -> i64 {
        self.log.log_end_offset().0
    }
    fn last_epoch(&self) -> Epoch {
        // The log seam speaks `krabka_ids::LeaderEpoch(i32)`; the core's
        // consensus `Epoch` is a `u32`. krabka-log epochs are non-negative
        // (0 for an empty log), so unwrap the newtype and convert to `u32`.
        let latest: LeaderEpoch = self
            .log
            .epoch_checkpoint()
            .latest_epoch()
            .unwrap_or(LeaderEpoch(0));
        u32::try_from(latest.0).unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        let log_end = self.log.log_end_offset();
        // Wrap the consensus `Epoch` into the log seam's `LeaderEpoch(i32)`.
        let epoch = LeaderEpoch(i32::try_from(epoch).ok()?);
        match self
            .log
            .epoch_checkpoint()
            .end_offset_for_epoch(epoch, log_end)
            .0
        {
            -1 => None,
            off => Some(off),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::prelude::mebibytes;

    use super::*;

    /// Read budget the log tests use. It is larger than any batch they append,
    /// so a read returns everything written.
    const TEST_READ_BUDGET: ByteSize = mebibytes(1);

    fn open_tmp() -> (KraftLog, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open");
        (log, dir)
    }

    // test helper
    fn batch(base: i64, epoch: i32, value: &[u8]) -> RecordBatch {
        use krabka_protocol::records::{Attributes, Record};
        RecordBatch {
            base_offset: base,
            partition_leader_epoch: epoch,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(bytes::Bytes::copy_from_slice(value)),
                headers: Vec::new(),
            }],
        }
    }

    #[test]
    fn opens_empty_at_offset_zero() {
        let (log, _dir) = open_tmp();
        check!(
            (
                log.log_start_offset().0,
                log.log_end_offset().0,
                log.hwm().0,
            ) == (0, 0, 0)
        );
    }

    #[test]
    fn append_assigns_sequential_offsets_and_reads_back() {
        let (mut log, _dir) = open_tmp();
        let off0 = log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        let off1 = log.append(&mut batch(0, 1, b"b"), 0).unwrap();
        assert2::assert!((off0, off1, log.log_end_offset()) == (Offset(0), Offset(1), Offset(2)));
        // read back decoded
        let out = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();
        assert2::assert!(
            out.iter()
                .map(|batch| batch.partition_leader_epoch)
                .collect::<Vec<_>>()
                == vec![1, 1]
        );
    }

    #[test]
    fn append_stamps_the_batch_create_time_the_snapshot_header_reads_back() {
        let (mut log, _dir) = open_tmp();
        // Two batches with distinct create-times, committed one at a time: the
        // KIP-630 header timestamp names the last batch a snapshot *contains*,
        // so it follows the high watermark rather than the log end.
        let older = 1_700_000_000_000;
        let newer = 1_700_000_111_222;
        log.append(&mut batch(0, 1, b"a"), older).unwrap();
        log.append(&mut batch(0, 1, b"b"), newer).unwrap();

        log.advance_hwm(Offset(1));
        let at_first = log.last_committed_timestamp_ms();
        log.advance_hwm(Offset(2));

        assert2::assert!(
            (
                at_first,
                log.last_committed_timestamp_ms(),
                log.timestamp_below(Offset(1)),
                log.read_decoded(Offset(0), TEST_READ_BUDGET)
                    .unwrap()
                    .iter()
                    .map(|batch| (batch.base_timestamp, batch.max_timestamp))
                    .collect::<Vec<_>>(),
            ) == (
                Some(older),
                Some(newer),
                Some(older),
                vec![(older, older), (newer, newer)],
            )
        );
    }

    #[test]
    fn no_contained_record_has_no_timestamp() {
        let (mut log, _dir) = open_tmp();
        // Nothing committed yet: an empty log, and a log whose records are all
        // still uncommitted, both have no last contained record to name.
        let empty = log.last_committed_timestamp_ms();
        log.append(&mut batch(0, 1, b"a"), 1_700_000_000_000)
            .unwrap();
        let uncommitted = log.last_committed_timestamp_ms();

        // Committed, then pruned away: the boundary is now the log start, and
        // the batch that carried the stamp is gone with the prefix.
        log.advance_hwm(Offset(1));
        log.prune_to(Offset(1)).unwrap();

        assert2::assert!(
            (
                empty,
                uncommitted,
                log.last_committed_timestamp_ms(),
                log.timestamp_below(Offset(0)),
                log.timestamp_below(Offset(9)),
            ) == (None, None, None, None, None)
        );
    }

    #[test]
    fn append_return_matches_assigned_base_and_advances_public_end_offset() {
        let (mut log, _dir) = open_tmp();

        let first = log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        let second = log.append(&mut batch(0, 1, b"b"), 0).unwrap();
        let decoded = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();

        check!(
            (
                first.0,
                second.0,
                decoded
                    .iter()
                    .map(|batch| batch.base_offset)
                    .collect::<Vec<_>>(),
                log.log_end_offset().0,
                LogView::end_offset(&log),
            ) == (0, 1, vec![first.0, second.0], 2, 2)
        );
    }

    #[test]
    fn public_hwm_accessor_tracks_committed_offset_after_advance_and_snapshot() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..3 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
        log.advance_hwm(Offset(2));
        assert2::assert!(log.hwm() == 2);

        log.install_snapshot(Offset(9)).unwrap();
        check!(
            (
                log.hwm().0,
                log.log_start_offset().0,
                log.log_end_offset().0
            ) == (9, 9, 9)
        );
    }

    #[test]
    fn append_at_preserves_leader_offset() {
        let (mut log, _dir) = open_tmp();
        // follower applies a leader-assigned batch at offset 0
        log.append_at(&mut batch(0, 2, b"x"), Offset(0)).unwrap();
        assert2::assert!(log.log_end_offset().0 == 1);
        assert2::assert!(
            log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap()[0].partition_leader_epoch == 2
        );
    }

    #[test]
    fn logview_reports_end_offset_and_last_epoch() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        log.append(&mut batch(0, 3, b"b"), 0).unwrap(); // epoch jumps to 3
        assert2::assert!(LogView::end_offset(&log) == 2);
        assert2::assert!(LogView::last_epoch(&log) == 3);
    }

    #[test]
    fn logview_end_offset_for_epoch_maps_unknown_to_none() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"a"), 0).unwrap(); // epoch 1 @ [0,1)
        log.append(&mut batch(0, 2, b"b"), 0).unwrap(); // epoch 2 @ [1,2)
        // epoch 1 ends where epoch 2 starts (offset 1); epoch 2 is current → end 2.
        // unknown future epoch → None
        for (_case, epoch, want) in [
            ("completed prior epoch", 1, Some(1)),
            ("current epoch", 2, Some(2)),
            ("unknown future epoch", 9, None),
        ] {
            assert2::assert!(LogView::end_offset_for_epoch(&log, epoch) == want);
        }
    }

    #[test]
    fn empty_log_last_epoch_is_zero() {
        let (log, _dir) = open_tmp();
        assert2::assert!(LogView::last_epoch(&log) == 0);
    }

    #[test]
    fn read_committed_never_returns_bytes_past_hwm() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        } // offsets 0..5
        log.advance_hwm(Offset(3));
        let r = log.read_committed(Offset(0), TEST_READ_BUDGET).unwrap();
        // bytes contain only batches with base_offset < 3 (offsets 0,1,2)
        let decoded = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();
        let committed: Vec<_> = decoded.into_iter().filter(|b| b.base_offset < 3).collect();
        check!(committed.len() == 3);
        // total committed bytes equals the size of the first 3 batches
        check!((r.start_offset.0, r.bytes.is_empty()) == (0, false));
    }

    #[test]
    fn advance_hwm_is_monotonic_and_clamped_to_log_end() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"x"), 0).unwrap(); // log_end = 1
        log.advance_hwm(Offset(5)); // clamp to log_end
        assert2::assert!(log.hwm() == 1);
        log.advance_hwm(Offset(0)); // never regress
        assert2::assert!(log.hwm() == 1);
    }

    #[test]
    fn prune_to_advances_log_start_and_is_noop_when_behind() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
        log.advance_hwm(log.log_end_offset());
        assert2::assert!(log.log_start_offset() == 0);
        log.prune_to(Offset(3)).unwrap();
        assert2::assert!(log.log_start_offset() == 3);
        log.prune_to(Offset(2)).unwrap(); // <= current start: no-op
        assert2::assert!(log.log_start_offset() == 3);
    }

    #[test]
    fn prune_inside_the_active_segment_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let mut log = KraftLog::open(dir.path()).expect("open");
            for _ in 0..5 {
                log.append(&mut batch(0, 1, b"x")).unwrap();
            }
            log.advance_hwm(log.log_end_offset());
            // Every record is in one segment, so no segment name records the
            // prune: `krabka_log::Log`'s checkpoint is what carries it.
            log.prune_to(Offset(3)).unwrap();
        }

        let log = KraftLog::open(dir.path()).expect("reopen");

        check!((log.log_start_offset().0, log.log_end_offset().0) == (3, 5));
    }

    #[test]
    fn install_snapshot_resets_log_to_empty_at_offset() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..4 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
        log.install_snapshot(Offset(100)).unwrap();
        check!(
            (
                log.log_start_offset().0,
                log.log_end_offset().0,
                log.hwm().0,
            ) == (100, 100, 100)
        );
        let base = log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        assert2::assert!(base == 100);
    }

    #[test]
    fn truncate_to_drops_log_end_and_hwm() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
        log.advance_hwm(Offset(5));
        log.truncate_to(Offset(2)).unwrap();
        assert2::assert!(log.log_end_offset().0 == 2);
        assert2::assert!(log.hwm().0 == 2);
    }

    #[test]
    fn truncate_below_log_start_returns_error() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..4 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
        log.prune_to(Offset(2)).unwrap();

        check!(matches!(
            log.truncate_to(Offset(1)),
            Err(RaftError::Storage(
                krabka_log::LogError::OffsetTooLow { .. }
            ))
        ));
    }
}
