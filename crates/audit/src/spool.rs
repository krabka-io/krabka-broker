//! Durable-across-process-crash local spool for the AU-5 degraded path.
//!
//! The spool holds exactly the chained audit records that are not yet written
//! to the topic, in order. By default, `append` calls `fsync` before it returns
//! success. Callers that explicitly choose a wider sync cadence can batch that
//! cost and use [`Spool::sync`] when they need a durable acknowledgement.
//! `open` heals and durably truncates a torn tail frame from a crash during an
//! append.
//!
//! Frame: `[u32 len][record]`. Record: `[u8 class_tag][u32 value_len]
//! [value][u32 header_count]([u32 klen][k][u32 vlen][v])*`. This module uses
//! synchronous `std::fs`, because the path is degraded and low-frequency. It
//! treats a truncated tail frame as end-of-data.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use krabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, ByteSizeExt as _},
};

use self::codec::{decode_record, encode_frame};
use crate::{
    ids::{MaxSpoolBytes, RecordCount, SpoolBytes},
    sink::{AuditError, AuditRecord},
};

mod codec;
mod resume;

#[cfg(test)]
mod test_support;

const SPOOL_FILE: &str = "audit.spool";
const LOSS_STATE_FILE: &str = "audit.losses";
const LOSS_STATE_TMP: &str = "audit.losses.tmp";
const LOSS_STATE_LEN: usize = 16;
const REPLAY_OFFSET_FILE: &str = "audit.replay-offset";
const REPLAY_OFFSET_TMP: &str = "audit.replay-offset.tmp";
const REPLAY_POISON_FILE: &str = "audit.replay-poison";
const REPLAY_POISON_TMP: &str = "audit.replay-poison.tmp";

#[derive(Debug, Clone, Copy)]
pub(crate) struct LossBatch {
    pub(crate) generation: u64,
    pub(crate) count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LossState {
    generation: u64,
    count: u64,
}

/// Writer-persisted count of fail-open records awaiting a chain marker.
#[derive(Debug)]
pub(crate) struct PendingLosses {
    state: Mutex<LossState>,
    path: Option<PathBuf>,
}

impl PendingLosses {
    pub(crate) fn memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LossState::default()),
            path: None,
        })
    }

    fn open(dir: &Path) -> Result<Arc<Self>, AuditError> {
        let path = dir.join(LOSS_STATE_FILE);
        let state = if path.exists() {
            let bytes = std::fs::read(&path).map_err(io)?;
            if bytes.len() != LOSS_STATE_LEN {
                return Err(AuditError::Io(format!(
                    "invalid audit loss state length {}",
                    bytes.len()
                )));
            }
            LossState {
                generation: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                count: u64::from_be_bytes(bytes[8..].try_into().unwrap()),
            }
        } else {
            let state = LossState::default();
            persist_loss_state(&path, state)?;
            state
        };
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            path: Some(path),
        }))
    }

    pub(crate) fn add(&self, count: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.count == 0 {
            state.generation = state.generation.saturating_add(1);
        }
        state.count = state.count.saturating_add(count);
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .count
    }

    pub(crate) fn snapshot(&self) -> Option<LossBatch> {
        let state = *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.count > 0).then_some(LossBatch {
            generation: state.generation,
            count: state.count,
        })
    }

    pub(crate) fn commit(&self, batch: LossBatch) {
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != batch.generation {
                return;
            }
            state.count = state.count.saturating_sub(batch.count);
            *state
        };
        self.persist_or_warn(state);
    }

    pub(crate) fn persist(&self) -> Result<(), AuditError> {
        let state = *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &self.path {
            Some(path) => persist_loss_state(path, state),
            None => Ok(()),
        }
    }

    pub(crate) fn persist_with<T>(
        &self,
        write_marker: impl FnOnce(LossBatch) -> Result<T, AuditError>,
    ) -> Result<Option<T>, AuditError> {
        let Some(batch) = self.snapshot() else {
            return Ok(None);
        };
        self.persist()?;
        let result = write_marker(batch)?;
        self.commit(batch);
        Ok(Some(result))
    }

    fn reconcile(&self, records: &[AuditRecord]) {
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.count == 0
                || !records.iter().any(|record| {
                    record.class == crate::event::AuditEventClass::RecordsLost
                        && serde_json::from_slice::<serde_json::Value>(&record.value)
                            .ok()
                            .and_then(|value| value.get("loss_generation")?.as_u64())
                            == Some(state.generation)
                })
            {
                return;
            }
            state.count = 0;
            *state
        };
        self.persist_or_warn(state);
    }

    fn persist_or_warn(&self, state: LossState) {
        if let Some(path) = &self.path
            && let Err(error) = persist_loss_state(path, state)
        {
            tracing::error!(%error, "failed to persist pending audit loss count");
        }
    }
}

fn io<E: std::fmt::Display>(e: E) -> AuditError {
    AuditError::Io(e.to_string())
}

fn persist_loss_state(path: &Path, state: LossState) -> Result<(), AuditError> {
    let mut bytes = [0_u8; LOSS_STATE_LEN];
    bytes[..8].copy_from_slice(&state.generation.to_be_bytes());
    bytes[8..].copy_from_slice(&state.count.to_be_bytes());
    persist_bytes(path, LOSS_STATE_TMP, &bytes)
}

fn read_or_create_u64(path: &Path, tmp_name: &str) -> Result<u64, AuditError> {
    match std::fs::read(path) {
        Ok(bytes) => <[u8; 8]>::try_from(bytes.as_slice())
            .map(u64::from_be_bytes)
            .map_err(|_| AuditError::Io(format!("invalid state length for {}", path.display()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            persist_u64(path, tmp_name, 0)?;
            Ok(0)
        }
        Err(error) => Err(io(error)),
    }
}

fn persist_u64(path: &Path, tmp_name: &str, value: u64) -> Result<(), AuditError> {
    persist_bytes(path, tmp_name, &value.to_be_bytes())
}

fn persist_bytes(path: &Path, tmp_name: &str, bytes: &[u8]) -> Result<(), AuditError> {
    let tmp = path.with_file_name(tmp_name);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(io)?;
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    std::fs::rename(tmp, path).map_err(io)?;
    sync_parent(path)
}

/// Append-only durable spool file.
///
/// The byte cap and the running total are raw [`MaxSpoolBytes`] and
/// [`SpoolBytes`] newtypes. The spool adds and compares both of them exactly,
/// which an `f64`-backed [`ByteSize`] cannot promise. The quantity stays the
/// boundary type: [`Spool::open`] takes one and [`Spool::size`] returns one.
#[derive(Debug)]
pub struct Spool {
    path: PathBuf,
    file: File,
    max_bytes: MaxSpoolBytes,
    bytes: SpoolBytes,
    count: RecordCount,
    recovered_torn_tail: bool,
    sync_every: NonZeroU64,
    unsynced: u64,
    replay_offset: u64,
    pending_losses: Arc<PendingLosses>,
}

impl Spool {
    /// Open the spool and recover its existing contents.
    ///
    /// This function creates the directory and the file if they do not exist.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), max_size = %max_size.human(), count = tracing::field::Empty, bytes = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error if the spool directory cannot be created. Returns an
    /// error if the spool file cannot be opened, read, or truncated.
    pub fn open(dir: &Path, max_size: ByteSize) -> Result<Self, AuditError> {
        Self::open_with_sync_every(dir, max_size, NonZeroU64::MIN)
    }

    /// Open the spool with an explicit append-to-`fsync` cadence.
    ///
    /// A cadence of `N` syncs after every `N` successful appends. Appends before
    /// that boundary are visible after a process crash but are not promised to
    /// survive power loss until [`Self::sync`] succeeds.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), max_size = %max_size.human(), sync_every = sync_every.get(), count = tracing::field::Empty, bytes = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error if the spool directory cannot be created. Returns an
    /// error if the spool file cannot be opened, read, synced, or truncated.
    pub fn open_with_sync_every(
        dir: &Path,
        max_size: ByteSize,
        sync_every: NonZeroU64,
    ) -> Result<Self, AuditError> {
        std::fs::create_dir_all(dir).map_err(io)?;
        let pending_losses = PendingLosses::open(dir)?;
        let path = dir.join(SPOOL_FILE);
        let replay_offset_path = dir.join(REPLAY_OFFSET_FILE);
        let replay_offset = read_or_create_u64(&replay_offset_path, REPLAY_OFFSET_TMP)?;
        let created = !path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io)?;
        let mut s = Self {
            path,
            file,
            max_bytes: MaxSpoolBytes(max_size.bytes_u64()),
            bytes: SpoolBytes(0),
            count: RecordCount(0),
            recovered_torn_tail: false,
            sync_every,
            unsynced: 0,
            replay_offset,
            pending_losses,
        };
        if created {
            s.file.sync_all().map_err(io)?;
            sync_parent(&s.path)?;
        }
        let (records, valid_bytes) = s.scan()?;
        let physical = s.file.metadata().map_err(io)?.len();
        s.recovered_torn_tail = valid_bytes.0 < physical;
        if s.recovered_torn_tail {
            s.file.set_len(valid_bytes.0).map_err(io)?;
            s.file.sync_all().map_err(io)?;
            tracing::warn!(
                physical,
                valid_bytes = valid_bytes.0,
                "audit spool: truncated torn tail frame on open"
            );
        }
        s.bytes = valid_bytes;
        let unread = s.unread_records(&records, valid_bytes)?;
        s.count = RecordCount(u64::try_from(unread.len()).unwrap_or(u64::MAX));
        s.reconcile_replay_poison()?;
        s.pending_losses.reconcile(&records);
        if s.count.0 == 0 && s.replay_offset > 0 {
            s.truncate()?;
        }
        let span = tracing::Span::current();
        span.record("count", s.count.0);
        span.record("bytes", s.bytes.0);
        Ok(s)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count.0 == 0
    }

    #[must_use]
    pub fn count(&self) -> RecordCount {
        self.count
    }

    pub(crate) fn pending_losses(&self) -> Arc<PendingLosses> {
        Arc::clone(&self.pending_losses)
    }

    /// How many bytes the spool currently holds.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        ByteSize::from_bytes(self.bytes.0)
    }

    /// Whether [`Spool::open`] found a torn tail frame and cut it off.
    ///
    /// A crash partway through an append leaves a frame whose length prefix
    /// promises more bytes than follow it. `open` heals that, and this reports
    /// whether it had to -- healing a spool that was intact would mean
    /// discarding good bytes, so the two cases are worth telling apart.
    #[must_use]
    pub fn recovered_torn_tail(&self) -> bool {
        self.recovered_torn_tail
    }

    /// Append a record to the spool.
    ///
    /// Returns `Ok(false)` if the record would make the spool exceed the
    /// configured cap.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, value_bytes = record.value.len(), count = self.count.0),
        err
    )]
    /// # Errors
    /// Returns an error if the seek, write, rollback, or configured sync on the
    /// spool file fails.
    pub fn append(&mut self, record: &AuditRecord) -> Result<bool, AuditError> {
        self.append_inner(record)
    }

    /// Persist a loss marker within the configured cap.
    pub(crate) fn append_loss_marker(&mut self, record: &AuditRecord) -> Result<(), AuditError> {
        if !self.append(record)? {
            return Err(AuditError::Unavailable("audit spool is full".into()));
        }
        self.sync()
    }

    pub(crate) fn begin_replay(&self, record: &AuditRecord) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        let mut poison = self.replay_offset.to_be_bytes().to_vec();
        poison.extend_from_slice(&encode_frame(record));
        persist_bytes(&path, REPLAY_POISON_TMP, &poison)
    }

    pub(crate) fn commit_replay(&mut self, record: &AuditRecord) -> Result<(), AuditError> {
        let frame_len = u64::try_from(encode_frame(record).len()).unwrap_or(u64::MAX);
        let replay_offset = self
            .replay_offset
            .checked_add(frame_len)
            .ok_or_else(|| AuditError::Io("audit replay offset overflow".into()))?;
        persist_u64(
            &self.path.with_file_name(REPLAY_OFFSET_FILE),
            REPLAY_OFFSET_TMP,
            replay_offset,
        )?;
        self.replay_offset = replay_offset;
        self.count.0 = self.count.0.saturating_sub(1);
        self.clear_replay_poison()
    }

    pub(crate) fn abort_replay(&self) -> Result<(), AuditError> {
        self.clear_replay_poison()
    }

    fn clear_replay_poison(&self) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        match std::fs::remove_file(&path) {
            Ok(()) => sync_parent(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    fn reconcile_replay_poison(&self) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io(error)),
        };
        let poison_offset = bytes
            .get(..8)
            .and_then(|prefix| <[u8; 8]>::try_from(prefix).ok())
            .map(u64::from_be_bytes);
        let valid_frame = bytes
            .get(8..12)
            .and_then(|prefix| <[u8; 4]>::try_from(prefix).ok())
            .map(u32::from_be_bytes)
            .and_then(|len| usize::try_from(len).ok())
            .is_some_and(|len| len.checked_add(12) == Some(bytes.len()));
        if !valid_frame || poison_offset.is_none_or(|offset| offset >= self.replay_offset) {
            return Err(AuditError::Poisoned(path.display().to_string()));
        }
        self.clear_replay_poison()
    }

    fn append_inner(&mut self, record: &AuditRecord) -> Result<bool, AuditError> {
        let frame = encode_frame(record);
        let frame_len = SpoolBytes(u64::try_from(frame.len()).unwrap_or(u64::MAX));
        if (self.bytes + frame_len).0 > self.max_bytes.0 {
            return Ok(false);
        }
        let old_len = self.file.metadata().map_err(io)?.len();
        self.file.seek(SeekFrom::End(0)).map_err(io)?;
        if let Err(error) = self.file.write_all(&frame) {
            return Err(self.rollback_append(old_len, error));
        }
        let unsynced = self.unsynced.saturating_add(1);
        if unsynced >= self.sync_every.get()
            && let Err(error) = self.file.sync_all()
        {
            return Err(self.rollback_append(old_len, error));
        }
        self.unsynced = if unsynced >= self.sync_every.get() {
            0
        } else {
            unsynced
        };
        self.bytes += frame_len;
        self.count.0 += 1;
        Ok(true)
    }

    /// Force all appended records to stable storage.
    ///
    /// Callers using a sync cadence greater than one use this before reporting
    /// a durable acknowledgement.
    /// # Errors
    /// Returns an error if the spool file cannot be synced.
    pub fn sync(&mut self) -> Result<(), AuditError> {
        self.file.sync_all().map_err(io)?;
        self.unsynced = 0;
        Ok(())
    }

    fn rollback_append<E: std::fmt::Display>(&mut self, old_len: u64, error: E) -> AuditError {
        let original = error.to_string();
        let rollback = self
            .file
            .set_len(old_len)
            .and_then(|()| self.file.seek(SeekFrom::Start(old_len)).map(drop))
            .and_then(|()| self.file.sync_all());
        match rollback {
            Ok(()) => AuditError::Io(original),
            Err(rollback) => AuditError::Io(format!(
                "{original}; failed to roll back partial append: {rollback}"
            )),
        }
    }

    /// Scan the spool file for the decoded records and the logical length.
    ///
    /// The logical length is the byte offset immediately after the last
    /// complete frame that decoded successfully. This method treats a truncated
    /// or corrupt tail frame as end-of-data. `valid_bytes` then points to the
    /// position immediately before that torn frame.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(records = tracing::field::Empty, valid_bytes = tracing::field::Empty),
        err
    )]
    fn scan(&self) -> Result<(Vec<AuditRecord>, SpoolBytes), AuditError> {
        let mut buf = Vec::new();
        {
            let mut f = File::open(&self.path).map_err(io)?;
            f.read_to_end(&mut buf).map_err(io)?;
        }
        let mut out = Vec::new();
        let mut cur: &[u8] = &buf;
        let mut valid_bytes = SpoolBytes(0);
        while cur.len() >= 4 {
            let len =
                usize::try_from(u32::from_be_bytes([cur[0], cur[1], cur[2], cur[3]])).unwrap_or(0);
            if cur.len() < 4 + len {
                break; // truncated tail frame
            }
            match decode_record(&cur[4..4 + len]) {
                Some(rec) => {
                    out.push(rec);
                    valid_bytes += SpoolBytes(u64::try_from(4 + len).unwrap_or(0));
                }
                None => break, // corrupt frame: stop (visible)
            }
            cur = &cur[4 + len..];
        }
        let span = tracing::Span::current();
        span.record("records", out.len());
        span.record("valid_bytes", valid_bytes.0);
        Ok((out, valid_bytes))
    }

    fn unread_records(
        &self,
        records: &[AuditRecord],
        valid_bytes: SpoolBytes,
    ) -> Result<Vec<AuditRecord>, AuditError> {
        if self.replay_offset > valid_bytes.0 {
            if valid_bytes.0 == 0 {
                return Ok(Vec::new());
            }
            return Err(AuditError::Io(format!(
                "audit replay offset {} exceeds spool length {}",
                self.replay_offset, valid_bytes.0
            )));
        }
        let mut offset = 0_u64;
        let mut unread = Vec::new();
        let mut at_boundary = self.replay_offset == 0;
        for record in records {
            if offset == self.replay_offset {
                at_boundary = true;
            }
            if at_boundary {
                unread.push(record.clone());
            }
            offset = offset
                .saturating_add(u64::try_from(encode_frame(record).len()).unwrap_or(u64::MAX));
        }
        if offset == self.replay_offset {
            at_boundary = true;
        }
        if !at_boundary {
            return Err(AuditError::Io(format!(
                "audit replay offset {} is not a frame boundary",
                self.replay_offset
            )));
        }
        Ok(unread)
    }

    /// Read every record from the start of the spool, in order.
    /// # Errors
    /// Returns an error if the spool file cannot be opened or read.
    pub fn read_all(&self) -> Result<Vec<AuditRecord>, AuditError> {
        let (records, valid_bytes) = self.scan()?;
        self.unread_records(&records, valid_bytes)
    }

    /// Clear the spool.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0, bytes = self.bytes.0), err)]
    /// # Errors
    /// Returns an error if the spool file cannot be truncated, or if the seek
    /// or sync that follows fails.
    pub fn truncate(&mut self) -> Result<(), AuditError> {
        self.file.set_len(0).map_err(io)?;
        self.bytes = SpoolBytes(0);
        self.count = RecordCount(0);
        self.unsynced = 0;
        self.file.seek(SeekFrom::Start(0)).map_err(io)?;
        self.file.sync_all().map_err(io)?;
        persist_u64(
            &self.path.with_file_name(REPLAY_OFFSET_FILE),
            REPLAY_OFFSET_TMP,
            0,
        )?;
        self.replay_offset = 0;
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), AuditError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(io)?
        .sync_all()
        .map_err(io)
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), AuditError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

    use super::*;
    use crate::{
        chain::{GENESIS_HEAD, chain_hash},
        event::{
            AuditEndpoint, AuditEvent, AuditEventClass, AuditOutcome, AuditPrincipal,
            PrivilegedPhase,
        },
        ocsf::ProductInfo,
        spool::test_support::{ROOMY_CAP, chained_record},
    };

    #[test]
    fn append_then_read_round_trips_records_with_headers() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.is_empty());
        let r0 = chained_record(0, &GENESIS_HEAD, b"{\"i\":0}");
        let r1 = chained_record(1, &chain_hash(&GENESIS_HEAD, 0, b"{\"i\":0}"), b"{\"i\":1}");
        check!((s.append(&r0).unwrap(), s.append(&r1).unwrap()) == (true, true));
        let read = s.read_all().unwrap();
        check!((s.count().0, s.is_empty(), read) == (2, false, vec![r0.clone(), r1.clone()]));
    }

    /// The privileged-action event shares the `ApiActivity` class tag, so the
    /// frame format carries it with no change. This proves the round trip on a
    /// record the broker actually builds, headers and OCSF value included.
    #[test]
    fn privileged_action_record_round_trips_through_the_frame_format() {
        let product = ProductInfo {
            vendor_name: "Krabka".into(),
            name: "krabka-broker".into(),
            version: "0".into(),
        };
        let event = AuditEvent::PrivilegedAction {
            outcome: AuditOutcome::Success,
            phase: PrivilegedPhase::Approved,
            action: "unclean_elect_leaders".into(),
            target: "orders-3".into(),
            proposal_id: "bg-7".into(),
            principal: AuditPrincipal {
                name: "User:bob".into(),
                auth_method: "MTls".into(),
            },
            counterparties: vec![AuditPrincipal {
                name: "User:alice".into(),
                auth_method: "MTls".into(),
            }],
            approver_set_fingerprint: "f00dcafe".into(),
            key_id: "op-1".into(),
            signature: vec![0xde, 0xad, 0xbe, 0xef],
            signature_verified: true,
            signed_at_ms: 0,
            source: AuditEndpoint {
                ip: "10.0.0.4".into(),
                port: 9092,
            },
            reason: "incident 42".into(),
            time_ms: 10,
        };
        let mut rec = AuditRecord::from_event(&event, &product);
        rec.push_chain_headers(0, &GENESIS_HEAD);

        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.append(&rec).unwrap());
        // Reopen, so the read goes through the on-disk frames and not through
        // any state `append` kept in memory.
        let s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.read_all().unwrap() == vec![rec.clone()]);
        check!(rec.class == AuditEventClass::ApiActivity);
    }

    #[test]
    fn overflow_is_rejected_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        // tiny cap: first record fits, second does not
        let r = chained_record(0, &GENESIS_HEAD, b"0123456789");
        let one = {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        let dir2 = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir2.path(), one).unwrap(); // exactly one record fits
        check!(s.append(&r).unwrap()); // accepted
        check!(!s.append(&r).unwrap()); // rejected (would exceed the cap)
        check!((s.count().0, s.read_all().unwrap().len()) == (1, 1)); // not corrupted
    }

    #[test]
    fn replay_cursor_keeps_only_unacknowledged_records_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"a");
        let r1 = chained_record(1, &GENESIS_HEAD, b"b");
        let r2 = chained_record(2, &GENESIS_HEAD, b"c");
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&r2).unwrap();
        let physical_size = s.size();
        s.begin_replay(&r0).unwrap();
        s.commit_replay(&r0).unwrap();
        drop(s);

        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.size() > ByteSize::ZERO); // `*=` mutant would leave bytes at 0
        check!(s.size() == physical_size);
        check!((s.count().0, s.read_all().unwrap()) == (2, vec![r1, r2]));
        s.truncate().unwrap();
        check!((s.is_empty(), s.read_all().unwrap().is_empty()) == (true, true));
    }

    #[test]
    fn append_accepts_record_that_exactly_fills_to_max() {
        let probe = tempfile::tempdir().unwrap();
        let r = chained_record(0, &GENESIS_HEAD, b"payload");
        let one = {
            let mut s = Spool::open(probe.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        // Cap at exactly two records: the 2nd append fills to max and MUST be
        // accepted (bytes + frame == max, not >). The `+ -> *` mutant computes
        // bytes * frame, which exceeds max and would wrongly reject.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), one * 2.0).unwrap();
        check!(s.append(&r).unwrap());
        check!(s.append(&r).unwrap());
        check!(s.count() == 2);
    }

    #[test]
    fn size_reports_the_bytes_actually_on_disk() {
        // Anchors the `SpoolBytes` -> `ByteSize` accessor to the real file
        // length, so a scale slip at that seam cannot hide behind a matching
        // slip at the `open` seam.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        s.append(&chained_record(0, &GENESIS_HEAD, b"payload"))
            .unwrap();
        let on_disk = std::fs::metadata(dir.path().join(SPOOL_FILE))
            .unwrap()
            .len();
        check!(s.size() == ByteSize::from_bytes(on_disk));
    }

    #[test]
    fn open_heals_torn_tail_frame() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"good");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }
        // Simulate a crash mid-append: a length prefix claiming 100 bytes, only 3 follow.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join("audit.spool"))
                .unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
        }
        // Reopen heals the torn tail; the good record survives and appends continue contiguously.
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        assert2::check!((s.count().0, s.read_all().unwrap()) == (1, vec![r0.clone()]));
        let r1 = chained_record(1, &GENESIS_HEAD, b"more");
        assert2::check!(s.append(&r1).unwrap());
        assert2::check!(s.read_all().unwrap() == vec![r0, r1]);
    }

    /// `open` heals a torn tail and reports that it did. Opening an intact
    /// spool must report the opposite: healing one that was whole would mean
    /// cutting good bytes off the end.
    #[test]
    fn open_reports_whether_it_healed_a_torn_tail() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let spool_file = dir.path().join(SPOOL_FILE);
        let r0 = chained_record(0, &GENESIS_HEAD, b"good");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }

        let intact_len = std::fs::metadata(&spool_file).unwrap().len();
        {
            let intact = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            check!(!intact.recovered_torn_tail());
        }
        check!(std::fs::metadata(&spool_file).unwrap().len() == intact_len);

        // A crash mid-append: a length prefix promising 100 bytes, 3 behind it.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&spool_file)
                .unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
        }
        let healed = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(healed.recovered_torn_tail());
        check!(std::fs::metadata(&spool_file).unwrap().len() == intact_len);
    }

    #[test]
    fn reopen_recovers_existing_contents() {
        let dir = tempfile::tempdir().unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"x");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }
        let s2 = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!((s2.count().0, s2.read_all().unwrap()) == (1, vec![r0]));
    }

    #[test]
    fn explicit_sync_finishes_a_batched_cadence() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool =
            Spool::open_with_sync_every(dir.path(), ROOMY_CAP, NonZeroU64::new(2).unwrap())
                .unwrap();
        check!(
            spool
                .append(&chained_record(0, &GENESIS_HEAD, b"a"))
                .unwrap()
        );
        check!(spool.unsynced == 1);
        spool.sync().unwrap();
        check!(spool.unsynced == 0);
    }

    #[test]
    fn acknowledged_record_survives_child_crash_and_torn_tail() {
        const CHILD_DIR: &str = "KRABKA_AUDIT_SPOOL_CRASH_DIR";
        if let Some(dir) = std::env::var_os(CHILD_DIR) {
            let dir = PathBuf::from(dir);
            let mut spool = Spool::open(&dir, ROOMY_CAP).unwrap();
            let record = chained_record(0, &GENESIS_HEAD, b"acknowledged");
            check!(spool.append(&record).unwrap());
            drop(spool);

            // Model a process dying after the next frame's prefix and only a
            // fragment of its body reached the file.
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.join(SPOOL_FILE))
                .unwrap();
            file.write_all(&100u32.to_be_bytes()).unwrap();
            file.write_all(b"torn").unwrap();
            std::process::abort();
        }

        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("acknowledged_record_survives_child_crash_and_torn_tail")
            .env(CHILD_DIR, dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        check!(!status.success());

        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let record = chained_record(0, &GENESIS_HEAD, b"acknowledged");
        check!(spool.recovered_torn_tail());
        check!(spool.read_all().unwrap() == vec![record]);
    }

    #[test]
    fn pending_loss_survives_reopen_and_reconciles_a_durable_marker() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ByteSize::from_bytes(0)).unwrap();
        let losses = spool.pending_losses();
        losses.add(3);
        losses.persist().unwrap();
        let batch = losses.snapshot().unwrap();
        drop(losses);
        drop(spool);

        let mut reopened = Spool::open(dir.path(), ByteSize::from_bytes(0)).unwrap();
        check!(reopened.pending_losses().count() == 3);
        let mut marker = AuditRecord::records_lost_with_generation(batch.count, batch.generation);
        marker.push_chain_headers(0, &GENESIS_HEAD);
        check!(reopened.append_loss_marker(&marker).is_err());
        check!(reopened.size() == ByteSize::ZERO);
        drop(reopened);

        let mut reopened = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        reopened.append_loss_marker(&marker).unwrap();
        // Model a crash after the marker fsync but before clearing the sidecar.
        drop(reopened);

        let reopened = Spool::open(dir.path(), ByteSize::from_bytes(0)).unwrap();
        check!(reopened.pending_losses().count() == 0);
        check!(reopened.read_all().unwrap() == vec![marker]);
    }

    #[test]
    fn uncertain_replay_poison_survives_reopen_until_record_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let record = chained_record(0, &GENESIS_HEAD, b"uncertain");
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(spool.append(&record).unwrap());
        spool.begin_replay(&record).unwrap();

        check!(matches!(
            Spool::open(dir.path(), ROOMY_CAP),
            Err(AuditError::Poisoned(_))
        ));

        spool.commit_replay(&record).unwrap();
        drop(spool);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(spool.is_empty());
        check!(!dir.path().join(REPLAY_POISON_FILE).exists());
    }
}
