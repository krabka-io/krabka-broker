//! Kafka-compatible producer-state snapshot encoding and recovery.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use bytes::BufMut as _;
use krabka_ids::{Offset, ProducerId};
use krabka_verified::producer_snapshot::{self as kernel, ProducerReloadRange};

use crate::{
    LogError,
    io::{IoTarget, LogIo},
    name,
};

const VERSION: i16 = 1;
const HEADER_LEN: usize = 6;
const ENTRY_LEN: usize = 46;

type SnapshotState = HashMap<ProducerId, ProducerSnapshotEntry>;
type LoadedSnapshot = (Offset, SnapshotState);

/// Durable producer metadata stored in Kafka's `.snapshot` sidecar format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSnapshotEntry {
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    pub last_sequence: i32,
    pub last_offset: Offset,
    pub offset_delta: i32,
    pub timestamp: i64,
    pub coordinator_epoch: i32,
    pub current_txn_first_offset: Option<Offset>,
}

/// Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`: the number of a
/// producer's most recent batches whose retry answers as a duplicate.
pub const NUM_BATCHES_TO_RETAIN: usize = 5;

/// One data batch a producer appended: Kafka's `BatchMetadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchMetadata {
    pub last_sequence: i32,
    pub last_offset: Offset,
    pub offset_delta: i32,
    pub timestamp: i64,
}

/// One producer's state as the log holds it: its snapshot entry, whose last
/// batch is the producer's last one, and the batches the producer appended
/// before that one at the same epoch, oldest first.
///
/// Kafka's `ProducerStateEntry` retains up to [`NUM_BATCHES_TO_RETAIN`]
/// batches. Its `.snapshot` file stores only the last, so a reopen rebuilds
/// the earlier ones only from the tail it replays past the snapshot, as
/// `UnifiedLog.rebuildProducerState` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredProducer {
    pub entry: ProducerSnapshotEntry,
    pub earlier: Vec<ProducerBatchMetadata>,
}

impl ProducerSnapshotEntry {
    /// The entry's last data batch, or `None` when it holds none.
    #[must_use]
    pub fn last_batch(&self) -> Option<ProducerBatchMetadata> {
        (self.last_offset.0 >= 0).then_some(ProducerBatchMetadata {
            last_sequence: self.last_sequence,
            last_offset: self.last_offset,
            offset_delta: self.offset_delta,
            timestamp: self.timestamp,
        })
    }

    pub(crate) fn empty(producer_id: ProducerId, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence: -1,
            last_offset: Offset(-1),
            offset_delta: 0,
            timestamp: -1,
            coordinator_epoch: -1,
            current_txn_first_offset: None,
        }
    }
}

pub(crate) fn path(dir: &Path, offset: Offset) -> PathBuf {
    name::producer_snapshot_path(dir, offset.0)
}

pub(crate) fn list(dir: &Path) -> Result<Vec<(Offset, PathBuf)>, LogError> {
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".snapshot") else {
            continue;
        };
        if stem.len() != name::FILENAME_DIGITS {
            continue;
        }
        let Ok(offset) = stem.parse::<i64>() else {
            continue;
        };
        snapshots.push((Offset(offset), entry.path()));
    }
    snapshots.sort_unstable_by_key(|(offset, _)| *offset);
    Ok(snapshots)
}

/// Reload producer state the way Kafka's
/// `ProducerStateManager.truncateAndReload` does.
///
/// Every snapshot outside `(range.log_start, range.log_end]` is deleted
/// first. The newest one left is then loaded; a corrupt one is deleted and
/// the next older one tried, as Kafka's `loadFromSnapshot` does.
pub(crate) fn reload(
    dir: &Path,
    range: ProducerReloadRange,
) -> Result<Option<LoadedSnapshot>, LogError> {
    let mut eligible = retain_reload_range(dir, range)?;
    while !eligible.is_empty() {
        let offsets: Vec<i64> = eligible.iter().map(|(offset, _)| offset.0).collect();
        let Some(selected) = kernel::producer_snapshot_latest_index(&offsets, range) else {
            return Err(LogError::Corrupt(
                "retained producer snapshots have no reloadable offset".into(),
            ));
        };
        let (offset, path) = eligible.swap_remove(selected);
        match read(&path, offset) {
            Ok(entries) => return Ok(Some((offset, entries))),
            Err(LogError::Corrupt(_)) => {
                // Match Kafka recovery: discard a corrupt newest snapshot and
                // retry the preceding one instead of failing the partition.
                let _ = fs::remove_file(path);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// Delete every snapshot Kafka's `truncateAndReload` deletes: those at or
/// below `range.log_start` and those above `range.log_end`. Returns the
/// snapshots left, oldest first.
pub(crate) fn retain_reload_range(
    dir: &Path,
    range: ProducerReloadRange,
) -> Result<Vec<(Offset, PathBuf)>, LogError> {
    let mut retained = Vec::new();
    for (offset, path) in list(dir)? {
        if kernel::producer_snapshot_reload_keeps(offset.0, range) {
            retained.push((offset, path));
        } else {
            fs::remove_file(path)?;
        }
    }
    Ok(retained)
}

/// Delete every snapshot Kafka's `removeStraySnapshots` deletes, given the
/// base offset of every local segment: each one no segment starts at, except
/// the newest snapshot when it lies above every segment.
pub(crate) fn remove_strays(dir: &Path, segment_bases: &[i64]) -> Result<(), LogError> {
    let snapshots = list(dir)?;
    let offsets: Vec<i64> = snapshots.iter().map(|(offset, _)| offset.0).collect();
    for (index, (_, path)) in snapshots.into_iter().enumerate() {
        if kernel::producer_snapshot_stray(&offsets, index, segment_bases) {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// Delete the snapshot at a deleted segment's base offset, as Kafka's
/// `UnifiedLog.deleteProducerSnapshots` does for every segment it deletes.
pub(crate) fn remove_at(dir: &Path, offset: Offset) -> Result<(), LogError> {
    match fs::remove_file(path(dir, offset)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
        _ => Ok(()),
    }
}

pub(crate) fn remove_all(dir: &Path) -> Result<(), LogError> {
    for (_, path) in list(dir)? {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub(crate) fn write(
    io: &dyn LogIo,
    dir: &Path,
    offset: Offset,
    entries: &HashMap<ProducerId, ProducerSnapshotEntry>,
) -> Result<PathBuf, LogError> {
    write_if_missing(io, dir, offset, || prepare(dir, offset, entries))
}

fn write_if_missing(
    io: &dyn LogIo,
    dir: &Path,
    offset: Offset,
    prepare: impl FnOnce() -> Result<PreparedSnapshot, LogError>,
) -> Result<PathBuf, LogError> {
    let destination = path(dir, offset);
    if destination.exists() {
        // A prior rename may have succeeded while its directory sync failed.
        io.sync_dir(dir)?;
        return Ok(destination);
    }
    Ok(prepare()?.write(io)?)
}

/// Captured at the roll boundary, before subsequent appends change the state.
#[derive(Debug)]
pub(crate) struct PreparedSnapshot {
    dir: PathBuf,
    offset: Offset,
    bytes: Vec<u8>,
}

#[cfg(all(test, not(target_os = "wasi")))]
std::thread_local! {
    static PREPARING: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, not(target_os = "wasi")))]
pub(crate) fn test_observe_prepare(observer: std::sync::mpsc::Sender<()>) {
    PREPARING.set(Some(observer));
}

pub(crate) fn prepare(
    dir: &Path,
    offset: Offset,
    entries: &HashMap<ProducerId, ProducerSnapshotEntry>,
) -> Result<PreparedSnapshot, LogError> {
    #[cfg(all(test, not(target_os = "wasi")))]
    PREPARING.with_borrow(|observer| {
        if let Some(observer) = observer {
            let _ = observer.send(());
        }
    });
    Ok(PreparedSnapshot {
        dir: dir.to_path_buf(),
        offset,
        bytes: encode(entries)?,
    })
}

impl PreparedSnapshot {
    pub(crate) fn write(&self, io: &dyn LogIo) -> std::io::Result<PathBuf> {
        let destination = path(&self.dir, self.offset);
        if destination.exists() {
            io.sync_dir(&self.dir)?;
            return Ok(destination);
        }

        let temporary = destination.with_extension("snapshot.tmp");
        crate::io::write_atomic(
            io,
            IoTarget::ProducerSnapshot,
            &temporary,
            &destination,
            &self.bytes,
            true,
        )?;
        Ok(destination)
    }
}

fn encoded_size(entries: usize) -> Result<usize, LogError> {
    HEADER_LEN
        .checked_add(4)
        .and_then(|size| size.checked_add(entries.checked_mul(ENTRY_LEN)?))
        .ok_or_else(|| LogError::InvalidArgument("producer snapshot size overflow".into()))
}

#[cfg(not(target_os = "wasi"))]
pub(crate) fn allocation_size(entries: usize) -> Result<usize, LogError> {
    encoded_size(entries)?
        .checked_add(
            entries
                .checked_mul(std::mem::size_of::<ProducerSnapshotEntry>())
                .ok_or_else(|| {
                    LogError::InvalidArgument("producer snapshot size overflow".into())
                })?,
        )
        .ok_or_else(|| LogError::InvalidArgument("producer snapshot size overflow".into()))
}

fn encode(entries: &HashMap<ProducerId, ProducerSnapshotEntry>) -> Result<Vec<u8>, LogError> {
    let count = i32::try_from(entries.len())
        .map_err(|_| LogError::InvalidArgument("too many producer snapshot entries".into()))?;
    let mut buffer = Vec::with_capacity(encoded_size(entries.len())?);
    buffer.put_i16(VERSION);
    buffer.put_u32(0);
    buffer.put_i32(count);

    let mut ordered: Vec<_> = entries.values().copied().collect();
    ordered.sort_unstable_by_key(|entry| entry.producer_id);
    for entry in ordered {
        buffer.put_i64(entry.producer_id.get());
        buffer.put_i16(entry.producer_epoch);
        buffer.put_i32(entry.last_sequence);
        buffer.put_i64(entry.last_offset.0);
        buffer.put_i32(entry.offset_delta);
        buffer.put_i64(entry.timestamp);
        buffer.put_i32(entry.coordinator_epoch);
        buffer.put_i64(entry.current_txn_first_offset.map_or(-1, |offset| offset.0));
    }

    let crc = crc32c::crc32c(&buffer[HEADER_LEN..]);
    buffer[2..6].copy_from_slice(&crc.to_be_bytes());
    Ok(buffer)
}

fn read(
    path: &Path,
    snapshot_offset: Offset,
) -> Result<HashMap<ProducerId, ProducerSnapshotEntry>, LogError> {
    let bytes = fs::read(path)?;
    if bytes.len() < HEADER_LEN + 4 {
        return Err(corrupt(path, "file is shorter than the snapshot header"));
    }
    let version = i16::from_be_bytes([bytes[0], bytes[1]]);
    if version != VERSION {
        return Err(corrupt(path, &format!("unsupported version {version}")));
    }
    let stored_crc = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
    let computed_crc = crc32c::crc32c(&bytes[HEADER_LEN..]);
    if stored_crc != computed_crc {
        return Err(corrupt(path, "CRC32C mismatch"));
    }

    let count = i32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    let count = usize::try_from(count).map_err(|_| corrupt(path, "negative entry count"))?;
    let expected = HEADER_LEN
        .checked_add(4)
        .and_then(|size| size.checked_add(count.checked_mul(ENTRY_LEN)?))
        .ok_or_else(|| corrupt(path, "entry count overflows file size"))?;
    if bytes.len() != expected {
        return Err(corrupt(path, "entry count does not match file length"));
    }

    let mut entries = HashMap::with_capacity(count);
    let mut cursor = HEADER_LEN + 4;
    for _ in 0..count {
        let producer_id = ProducerId(take_i64(&bytes, &mut cursor));
        let producer_epoch = take_i16(&bytes, &mut cursor);
        let last_sequence = take_i32(&bytes, &mut cursor);
        let last_offset = Offset(take_i64(&bytes, &mut cursor));
        let offset_delta = take_i32(&bytes, &mut cursor);
        let timestamp = take_i64(&bytes, &mut cursor);
        let coordinator_epoch = take_i32(&bytes, &mut cursor);
        let txn_offset = take_i64(&bytes, &mut cursor);
        if !kernel::producer_snapshot_entry_valid(
            snapshot_offset.0,
            kernel::ProducerSnapshotEntryFacts {
                producer_id: producer_id.get(),
                producer_epoch,
                last_sequence,
                last_offset: last_offset.0,
                offset_delta,
                coordinator_epoch,
                current_txn_first_offset: txn_offset,
            },
        ) {
            return Err(corrupt(path, "entry contains an invalid producer state"));
        }
        let current_txn_first_offset = (txn_offset >= 0).then_some(Offset(txn_offset));
        let entry = ProducerSnapshotEntry {
            producer_id,
            producer_epoch,
            last_sequence,
            last_offset,
            offset_delta,
            timestamp,
            coordinator_epoch,
            current_txn_first_offset,
        };
        if entries.insert(producer_id, entry).is_some() {
            return Err(corrupt(path, "duplicate producer id"));
        }
    }
    Ok(entries)
}

fn corrupt(path: &Path, reason: &str) -> LogError {
    LogError::Corrupt(format!("producer snapshot {}: {reason}", path.display()))
}

fn take_i16(bytes: &[u8], cursor: &mut usize) -> i16 {
    let value = i16::from_be_bytes([bytes[*cursor], bytes[*cursor + 1]]);
    *cursor += 2;
    value
}

fn take_i32(bytes: &[u8], cursor: &mut usize) -> i32 {
    let value = i32::from_be_bytes(bytes[*cursor..*cursor + 4].try_into().expect("four bytes"));
    *cursor += 4;
    value
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> i64 {
    let value = i64::from_be_bytes(bytes[*cursor..*cursor + 8].try_into().expect("eight bytes"));
    *cursor += 8;
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIo;

    fn sample_entry() -> ProducerSnapshotEntry {
        ProducerSnapshotEntry {
            producer_id: ProducerId(42),
            producer_epoch: 3,
            last_sequence: 9,
            last_offset: Offset(101),
            offset_delta: 4,
            timestamp: 1_234_567,
            coordinator_epoch: 8,
            current_txn_first_offset: Some(Offset(99)),
        }
    }

    fn sample() -> HashMap<ProducerId, ProducerSnapshotEntry> {
        let entry = sample_entry();
        maplit::hashmap! {entry.producer_id => entry}
    }

    fn assert_rejected(entry: ProducerSnapshotEntry) {
        let dir = tempfile::tempdir().unwrap();
        let entries = maplit::hashmap! {entry.producer_id => entry};
        let path = write(&FileIo, dir.path(), Offset(102), &entries).unwrap();
        assert2::assert!(matches!(
            read(&path, Offset(102)),
            Err(LogError::Corrupt(_))
        ));
    }

    #[test]
    fn kafka_v1_snapshot_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        assert2::assert!(read(&path, Offset(102)).unwrap() == sample());
    }

    #[test]
    fn zero_producer_id_snapshot_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = sample_entry();
        entry.producer_id = ProducerId(0);
        let entries = maplit::hashmap! {entry.producer_id => entry};
        let path = write(&FileIo, dir.path(), Offset(102), &entries).unwrap();
        assert2::assert!(read(&path, Offset(102)).unwrap() == entries);
    }

    #[test]
    fn empty_entry_uses_kafka_marker_sentinels() {
        let entry = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        assert2::assert!(
            entry
                == ProducerSnapshotEntry {
                    producer_id: ProducerId(7),
                    producer_epoch: 0,
                    last_sequence: -1,
                    last_offset: Offset(-1),
                    offset_delta: 0,
                    timestamp: -1,
                    coordinator_epoch: -1,
                    current_txn_first_offset: None,
                }
        );
    }

    #[test]
    fn encoding_is_ordered_and_uses_minus_one_for_no_transaction() {
        let mut entries = sample();
        let marker = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        entries.insert(marker.producer_id, marker);

        let bytes = encode(&entries).unwrap();
        assert2::assert!(bytes.len() == HEADER_LEN + 4 + 2 * ENTRY_LEN);
        assert2::assert!(i32::from_be_bytes(bytes[6..10].try_into().unwrap()) == 2);
        assert2::assert!(i64::from_be_bytes(bytes[10..18].try_into().unwrap()) == 7);
        assert2::assert!(i64::from_be_bytes(bytes[48..56].try_into().unwrap()) == -1);
        assert2::assert!(i64::from_be_bytes(bytes[56..64].try_into().unwrap()) == 42);
        assert2::assert!(i64::from_be_bytes(bytes[94..102].try_into().unwrap()) == 99);
    }

    #[test]
    fn empty_snapshot_has_exact_header_length_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&FileIo, dir.path(), Offset(0), &HashMap::new()).unwrap();
        assert2::assert!(fs::metadata(&path).unwrap().len() == 10);
        assert2::assert!(read(&path, Offset(0)).unwrap().is_empty());
    }

    #[test]
    fn every_short_header_length_is_rejected_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.snapshot");
        for length in 0..10 {
            let mut bytes = vec![0; length];
            if length >= 2 {
                bytes[..2].copy_from_slice(&VERSION.to_be_bytes());
            }
            fs::write(&path, bytes).unwrap();
            assert2::assert!(matches!(read(&path, Offset(0)), Err(LogError::Corrupt(_))));
        }
    }

    #[test]
    fn crc_covers_entry_count_and_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[9] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert2::assert!(matches!(
            read(&path, Offset(102)),
            Err(LogError::Corrupt(_))
        ));
    }

    #[test]
    fn rejects_unknown_version_and_truncated_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let mut unknown = fs::read(&path).unwrap();
        unknown[1] = 2;
        fs::write(&path, unknown).unwrap();
        assert2::assert!(matches!(
            read(&path, Offset(102)),
            Err(LogError::Corrupt(_))
        ));

        fs::remove_file(&path).unwrap();
        let path = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let mut truncated = fs::read(&path).unwrap();
        truncated.pop();
        fs::write(&path, truncated).unwrap();
        assert2::assert!(matches!(
            read(&path, Offset(102)),
            Err(LogError::Corrupt(_))
        ));
    }

    fn range(log_start: i64, log_end: i64) -> ProducerReloadRange {
        ProducerReloadRange {
            log_start,
            local_start: log_start,
            log_end,
        }
    }

    fn offsets(dir: &Path) -> Vec<i64> {
        list(dir)
            .unwrap()
            .into_iter()
            .map(|(offset, _)| offset.0)
            .collect()
    }

    /// Kafka's `truncateAndReload` deletes every snapshot outside
    /// `(logStartOffset, logEndOffset]` -- the one at the log start included
    /// -- and `loadFromSnapshot` loads the newest one left.
    #[test]
    fn reload_deletes_snapshots_outside_the_range_and_loads_the_newest_left() {
        let on_disk = [100, 101, 102, 103, 104];
        for (log_start, log_end, loaded, left) in [
            // Log start below every snapshot, log end inside: 104 is future.
            (0, 103, Some(103), vec![100, 101, 102, 103]),
            // A snapshot below the log start is deleted, never loaded.
            (100, 103, Some(103), vec![101, 102, 103]),
            // A snapshot at the log start is deleted too.
            (102, 104, Some(104), vec![103, 104]),
            // One snapshot between the log start and the log end.
            (102, 103, Some(103), vec![103]),
            // Only the snapshot at the log start is in reach: nothing loads.
            (103, 103, None, vec![]),
            // Every snapshot is at or below the log start.
            (104, 110, None, vec![]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            for offset in on_disk {
                write(&FileIo, dir.path(), Offset(offset), &sample()).unwrap();
            }

            let reloaded = reload(dir.path(), range(log_start, log_end)).unwrap();

            assert2::check!(
                reloaded == loaded.map(|offset| (Offset(offset), sample())),
                "({log_start}, {log_end}]"
            );
            assert2::check!(offsets(dir.path()) == left, "({log_start}, {log_end}]");
        }
    }

    #[test]
    fn corrupt_latest_snapshot_is_removed_and_previous_snapshot_is_loaded() {
        for corrupt_count in 0..=4 {
            let dir = tempfile::tempdir().unwrap();
            for offset in 102..=105 {
                write(&FileIo, dir.path(), Offset(offset), &sample()).unwrap();
            }
            for offset in (106 - corrupt_count)..=105 {
                fs::write(path(dir.path(), Offset(offset)), b"broken").unwrap();
            }
            let loaded = reload(dir.path(), range(0, 105)).unwrap();
            if corrupt_count == 4 {
                assert2::assert!(loaded == None);
            } else {
                let (offset, entries) = loaded.unwrap();
                assert2::assert!(offset == Offset(105 - corrupt_count));
                assert2::assert!(entries == sample());
                assert2::assert!(path(dir.path(), offset).exists());
            }
            for offset in (106 - corrupt_count)..=105 {
                assert2::assert!(!path(dir.path(), Offset(offset)).exists());
            }
        }
    }

    #[test]
    fn future_entry_state_is_removed_and_previous_snapshot_is_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let previous = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let future_state = write(&FileIo, dir.path(), Offset(103), &sample()).unwrap();
        let mut bytes = fs::read(&future_state).unwrap();
        bytes[24..32].copy_from_slice(&103_i64.to_be_bytes());
        let crc = crc32c::crc32c(&bytes[HEADER_LEN..]);
        bytes[2..6].copy_from_slice(&crc.to_be_bytes());
        fs::write(&future_state, bytes).unwrap();

        let (offset, entries) = reload(dir.path(), range(0, 103)).unwrap().unwrap();
        assert2::assert!(offset == Offset(102));
        assert2::assert!(entries == sample());
        assert2::assert!(previous.exists());
        assert2::assert!(!future_state.exists());
    }

    #[test]
    fn duplicate_producer_ids_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let duplicate = bytes[HEADER_LEN + 4..].to_vec();
        bytes.extend_from_slice(&duplicate);
        bytes[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&2_i32.to_be_bytes());
        let crc = crc32c::crc32c(&bytes[HEADER_LEN..]);
        bytes[2..6].copy_from_slice(&crc.to_be_bytes());
        fs::write(&path, bytes).unwrap();

        assert2::assert!(matches!(
            read(&path, Offset(102)),
            Err(LogError::Corrupt(message)) if message.contains("duplicate producer id")
        ));
    }

    #[test]
    fn snapshot_write_retry_preserves_the_first_durable_state() {
        let dir = tempfile::tempdir().unwrap();
        let first = sample();
        let path = write(&FileIo, dir.path(), Offset(102), &first).unwrap();

        assert2::assert!(write(&FileIo, dir.path(), Offset(102), &HashMap::new()).unwrap() == path);
        assert2::assert!(read(&path, Offset(102)).unwrap() == first);
    }

    #[derive(Debug, Default)]
    struct DirectoryDebt {
        fail: std::sync::atomic::AtomicBool,
        syncs: std::sync::atomic::AtomicUsize,
    }

    impl LogIo for DirectoryDebt {
        fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
            use std::sync::atomic::Ordering::SeqCst;
            self.syncs.fetch_add(1, SeqCst);
            if self.fail.load(SeqCst) {
                Err(std::io::ErrorKind::StorageFull.into())
            } else {
                FileIo.sync_dir(dir)
            }
        }
    }

    #[test]
    fn snapshot_retry_pays_directory_sync_debt_without_overwriting_state() {
        use std::sync::atomic::Ordering::SeqCst;
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let io = DirectoryDebt::default();
            io.fail.store(true, SeqCst);
            let snapshot = prepare(dir.path(), Offset(102), &sample()).unwrap();
            let attempt = || {
                if prepared {
                    snapshot.write(&io).map_err(LogError::from)
                } else {
                    write(&io, dir.path(), Offset(102), &sample())
                }
            };
            assert2::assert!(
                matches!(attempt(), Err(LogError::Io(e)) if e.kind() == std::io::ErrorKind::StorageFull)
            );
            // The rename succeeded, but this name still owes directory durability.
            assert2::assert!(path(dir.path(), Offset(102)).exists());
            assert2::assert!(
                matches!(attempt(), Err(LogError::Io(e)) if e.kind() == std::io::ErrorKind::StorageFull)
            );
            io.fail.store(false, SeqCst);
            let destination = attempt().unwrap();
            assert2::assert!(io.syncs.load(SeqCst) == 3);
            assert2::assert!(read(&destination, Offset(102)).unwrap() == sample());
        }
    }

    #[test]
    fn existing_snapshot_does_not_prepare_state_but_still_syncs_the_directory() {
        use std::sync::atomic::Ordering::SeqCst;
        let dir = tempfile::tempdir().unwrap();
        let destination = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let io = DirectoryDebt::default();
        let result = write_if_missing(&io, dir.path(), Offset(102), || {
            panic!("an existing snapshot must not encode or sort producer state")
        })
        .unwrap();
        assert2::assert!(result == destination);
        assert2::assert!(io.syncs.load(SeqCst) == 1);
        assert2::assert!(read(&destination, Offset(102)).unwrap() == sample());
    }

    #[test]
    fn snapshot_read_io_failure_is_not_treated_as_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let previous = write(&FileIo, dir.path(), Offset(102), &sample()).unwrap();
        let unreadable = path(dir.path(), Offset(103));
        fs::create_dir(&unreadable).unwrap();
        let corrupt = write(&FileIo, dir.path(), Offset(104), &sample()).unwrap();
        fs::write(&corrupt, b"broken").unwrap();

        assert2::assert!(matches!(
            reload(dir.path(), range(0, 104)),
            Err(LogError::Io(_))
        ));
        assert2::assert!(previous.exists() && unreadable.exists());
        assert2::assert!(!corrupt.exists());
    }

    #[test]
    fn retain_reload_range_keeps_the_log_end_and_drops_the_log_start() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2, 3] {
            write(&FileIo, dir.path(), Offset(offset), &sample()).unwrap();
        }

        let retained = retain_reload_range(dir.path(), range(1, 2)).unwrap();

        assert2::check!(retained == vec![(Offset(2), path(dir.path(), Offset(2)))]);
        assert2::check!(offsets(dir.path()) == vec![2]);
    }

    /// Kafka's `removeStraySnapshots`, run at log load with every local
    /// segment's base offset.
    #[test]
    fn remove_strays_keeps_segment_bases_and_the_newest_snapshot_above_them() {
        for (on_disk, bases, left) in [
            (vec![0, 4, 8], vec![0, 4, 8], vec![0, 4, 8]),
            (vec![2, 4, 6], vec![0, 4, 8], vec![4]),
            (vec![4, 9, 11], vec![0, 4, 8], vec![4, 11]),
            (vec![3, 7], vec![], vec![7]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            for offset in &on_disk {
                write(&FileIo, dir.path(), Offset(*offset), &sample()).unwrap();
            }

            remove_strays(dir.path(), &bases).unwrap();

            assert2::check!(offsets(dir.path()) == left, "{on_disk:?} {bases:?}");
        }
    }

    #[test]
    fn remove_at_deletes_one_snapshot_and_tolerates_a_missing_one() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2] {
            write(&FileIo, dir.path(), Offset(offset), &sample()).unwrap();
        }

        remove_at(dir.path(), Offset(1)).unwrap();
        remove_at(dir.path(), Offset(5)).unwrap();

        assert2::check!(offsets(dir.path()) == vec![2]);
    }

    #[test]
    fn remove_all_removes_every_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2] {
            write(&FileIo, dir.path(), Offset(offset), &sample()).unwrap();
        }

        remove_all(dir.path()).unwrap();
        assert2::assert!(list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn rejects_each_invalid_entry_field_independently() {
        let mut entry = sample_entry();
        entry.producer_id = ProducerId(-2);
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.producer_id = ProducerId(-1);
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.producer_epoch = -1;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.offset_delta = -1;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.current_txn_first_offset = Some(Offset(-2));
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(-1);
        entry.last_sequence = 0;
        entry.offset_delta = 0;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(0);
        entry.last_sequence = -1;
        entry.offset_delta = 0;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(0);
        entry.last_sequence = 0;
        entry.offset_delta = 1;
        assert_rejected(entry);
    }

    #[test]
    fn accepts_every_valid_entry_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let marker = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        let data = ProducerSnapshotEntry {
            producer_id: ProducerId(8),
            producer_epoch: 0,
            last_sequence: 0,
            last_offset: Offset(0),
            offset_delta: 0,
            timestamp: 0,
            coordinator_epoch: 0,
            current_txn_first_offset: None,
        };
        let entries = maplit::hashmap! {marker.producer_id => marker, data.producer_id => data};
        let path = write(&FileIo, dir.path(), Offset(1), &entries).unwrap();
        assert2::assert!(read(&path, Offset(1)).unwrap() == entries);
    }
}
