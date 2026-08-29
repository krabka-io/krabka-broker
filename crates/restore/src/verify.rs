//! Integrity of one archived segment, and the facts it yields.
//!
//! This module owns the decision that a segment is safe to rehydrate. It
//! fetches the segment's artifacts, walks the `.log` batch by batch with
//! `krabka_protocol::records::validate_one_v2_batch`, and checks framing and
//! CRC without decoding records. A batch whose declared length overruns the
//! object is a truncated segment; a batch whose CRC disagrees with its body is
//! a checksum mismatch, reported with the object key and the byte position. It
//! also checks that the copy is not torn: a segment that carries a log but no
//! time index was archived only in part. From the batch headers it derives the
//! two facts the rest of the pipeline needs and the archive metadata cannot be
//! trusted for, the segment's true `end_offset` and `max_timestamp_ms`, and it
//! returns the verified log bytes so the segment is fetched exactly once.

use bytes::Bytes;
use crc32c::crc32c;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_object_store::{ObjectOps, ObjectStoreError};
use krabka_protocol::records::{RecordsError, validate_one_v2_batch};
use krabka_remote_storage::{
    TopicIdPartition,
    index::{
        AbortedTxnIndexEntry, OffsetIndexEntry, TimeIndexEntry, parse_offset_index,
        parse_time_index, parse_txn_index,
    },
};
use object_store::path::Path;
use uuid::Uuid;

use crate::{
    backend::ArchiveStore,
    discover::{ArchiveObject, SegmentInventory},
    error::RestoreError,
};

/// What verification established about one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFacts {
    /// The per-copy segment id.
    pub segment_id: Uuid,
    /// First offset the segment holds.
    pub base_offset: Offset,
    /// Last offset the segment holds, derived from the batch headers.
    pub end_offset: Offset,
    /// Highest record timestamp in the segment, derived from the batch
    /// headers. It is `-1` for a segment with no timestamped record.
    pub max_timestamp_ms: i64,
    /// Batches the segment holds.
    pub batches: u64,
    /// Records the batch headers account for.
    pub records: u64,
    /// Size of the verified `.log`, in bytes.
    pub log_bytes: u64,
    /// The offset each leader epoch starts at, from the leader-epoch
    /// checkpoint. The target partition needs it to answer `OffsetForLeaderEpoch`.
    pub leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

/// A segment that passed verification, with the bytes that passed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSegment {
    /// What the verification established.
    pub facts: SegmentFacts,
    /// The verified `.log` bytes.
    pub log: Bytes,
}

/// Guard against an unbounded read of a corrupt or hostile `.log` object.
/// Kafka's default `segment.bytes` is 1 GiB, and an operator can raise it, so
/// this cap is a generous multiple of that default rather than the exact
/// configured value, which this offline tool never sees.
const MAX_LOG_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Guard for the sparse `.index` and `.timeindex` sidecars. An entry lands
/// only every `index.interval.bytes` (4 KiB by default), so even a segment at
/// the [`MAX_LOG_BYTES`] cap produces a sidecar many orders of magnitude
/// smaller than this.
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;

/// Guard for the `.txnindex` sidecar: one entry per aborted transaction.
const MAX_TXN_INDEX_BYTES: u64 = MAX_INDEX_BYTES;

/// Guard for the `.snapshot` producer-state sidecar: 46 bytes per producer.
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// Guard for the `.leader_epoch_checkpoint` text sidecar: one short line per
/// leader change over the segment's lifetime.
const MAX_LEADER_EPOCH_BYTES: u64 = 16 * 1024 * 1024;

/// Byte length of one serialized offset-index entry. `krabka_remote_storage`
/// keeps its own copy of this constant private, so it is recomputed here from
/// the zerocopy struct it mirrors.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

/// Byte length of one serialized time-index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

/// Byte length of one serialized aborted-transaction index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

/// Header length of a Kafka producer-state `.snapshot`: version (2 bytes),
/// CRC32C (4 bytes), entry count (4 bytes). This is where the first entry
/// starts, not where the CRC-covered region starts -- see
/// [`SNAPSHOT_CRC_COVERAGE_START`].
const SNAPSHOT_HEADER_LEN: usize = 10;

/// Byte offset where the `.snapshot`'s CRC32C coverage begins: right after
/// the CRC field itself (bytes `2..6`), so the covered region includes the
/// entry count (bytes `6..10`) as well as every entry. This must match
/// `krabka_log::producer_snapshot`'s writer exactly -- it computes the CRC
/// over `&buffer[HEADER_LEN..]` with its own `HEADER_LEN = 6` -- or every
/// genuinely archived snapshot fails verification with a false
/// `ChecksumMismatch`, since [`SNAPSHOT_HEADER_LEN`] (10) is 4 bytes past
/// where the real writer's coverage actually starts.
const SNAPSHOT_CRC_COVERAGE_START: usize = 6;

/// Byte length of one producer-state snapshot entry.
const SNAPSHOT_ENTRY_LEN: usize = 46;

/// The only producer-state snapshot version this restore understands, and the
/// only one the broker's copy path ever archives.
const SNAPSHOT_VERSION: i16 = 1;

/// Fetch and verify one segment.
///
/// # Errors
///
/// Returns [`RestoreError::TornCopy`] when a required artifact is absent,
/// [`RestoreError::TruncatedSegment`] when the log ends inside a batch,
/// [`RestoreError::ChecksumMismatch`] when a batch CRC disagrees with its
/// body, and [`RestoreError::ObjectStore`] when a fetch fails.
pub async fn verify_segment(
    store: &ArchiveStore,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<VerifiedSegment, RestoreError> {
    // Checked in the order the broker's copy path writes the artifacts, so a
    // torn copy is reported by the first one actually missing.
    let log = require_artifact(segment.log.as_ref(), ".log", partition, segment)?;
    let offset_index =
        require_artifact(segment.offset_index.as_ref(), ".index", partition, segment)?;
    let time_index = require_artifact(
        segment.time_index.as_ref(),
        ".timeindex",
        partition,
        segment,
    )?;
    let producer_snapshot = require_artifact(
        segment.producer_snapshot.as_ref(),
        ".snapshot",
        partition,
        segment,
    )?;
    let leader_epoch = require_artifact(
        segment.leader_epoch.as_ref(),
        ".leader_epoch_checkpoint",
        partition,
        segment,
    )?;

    let ops = store.ops();
    // The `.log` is fetched exactly once, here; every later stage reads it
    // from the `Bytes` this function returns, not from the archive again.
    let log_bytes = fetch_capped(ops, &log.key, MAX_LOG_BYTES).await?;
    let offset_index_bytes = fetch_capped(ops, &offset_index.key, MAX_INDEX_BYTES).await?;
    let time_index_bytes = fetch_capped(ops, &time_index.key, MAX_INDEX_BYTES).await?;
    let producer_snapshot_bytes =
        fetch_capped(ops, &producer_snapshot.key, MAX_SNAPSHOT_BYTES).await?;
    let leader_epoch_bytes = fetch_capped(ops, &leader_epoch.key, MAX_LEADER_EPOCH_BYTES).await?;
    let transaction_index = match &segment.transaction_index {
        Some(artifact) => Some((
            artifact.key.clone(),
            fetch_capped(ops, &artifact.key, MAX_TXN_INDEX_BYTES).await?,
        )),
        None => None,
    };

    let walked = walk_log(&log.key, segment, &log_bytes)?;
    let log_bytes_len = u64::try_from(log_bytes.len()).unwrap_or(u64::MAX);

    validate_offset_index(
        &offset_index.key,
        &offset_index_bytes,
        segment.base_offset,
        walked.end_offset,
        log_bytes_len,
    )?;
    validate_time_index(
        &time_index.key,
        &time_index_bytes,
        segment.base_offset,
        walked.end_offset,
    )?;
    if let Some((key, bytes)) = &transaction_index {
        validate_txn_index(key, bytes, segment.base_offset, walked.end_offset)?;
    }
    validate_producer_snapshot(&producer_snapshot.key, &producer_snapshot_bytes)?;
    let leader_epochs = parse_leader_epoch_checkpoint(&leader_epoch.key, &leader_epoch_bytes)?;

    Ok(VerifiedSegment {
        facts: SegmentFacts {
            segment_id: segment.segment_id,
            base_offset: segment.base_offset,
            end_offset: walked.end_offset,
            max_timestamp_ms: walked.max_timestamp_ms,
            batches: walked.batches,
            records: walked.records,
            log_bytes: log_bytes_len,
            leader_epochs,
        },
        log: log_bytes,
    })
}

/// Look up one mandatory artifact, or report the torn copy it reveals.
fn require_artifact<'a>(
    artifact: Option<&'a ArchiveObject>,
    name: &str,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<&'a ArchiveObject, RestoreError> {
    artifact.ok_or_else(|| RestoreError::TornCopy {
        topic: partition.topic.clone(),
        partition: partition.partition,
        segment_id: segment.segment_id,
        artifact: name.to_owned(),
    })
}

/// Fetch a whole object, refusing it before buffering any bytes if it exceeds
/// `max_bytes`. This mirrors `krabka_object_store::read_capped`'s head-then-get
/// guard against OOM on a corrupt or oversized archive object; it is
/// reimplemented here because that helper takes the concrete
/// `Arc<dyn object_store::ObjectStore>` rather than the [`ObjectOps`] surface
/// [`ArchiveStore`] exposes.
async fn fetch_capped(
    ops: &dyn ObjectOps,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, RestoreError> {
    let meta = ops.head(key).await?;
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        }
        .into());
    }
    Ok(ops.get(key).await?)
}

/// What one pass over a `.log`'s batches established.
struct LogWalk {
    end_offset: Offset,
    max_timestamp_ms: i64,
    batches: u64,
    records: u64,
}

/// Walk every v2 record batch in `log_bytes`, checking framing and CRC, and
/// derive the segment's true `end_offset`, `max_timestamp_ms`, batch count,
/// and record count.
///
/// This checks CRC and framing only, through [`validate_one_v2_batch`]. It
/// does not also call `ValidatedBatch::validate_records` to re-verify the
/// declared `records_count` against the decoded records: the CRC already
/// guarantees every byte of the batch is exactly what the producer wrote, a
/// restore only ever forwards `records_count` as a diagnostic count rather
/// than a safety invariant a later stage relies on, and `validate_records`
/// would decompress every compressed batch a second time for no benefit this
/// pass needs.
fn walk_log(
    key: &Path,
    segment: &SegmentInventory,
    log_bytes: &[u8],
) -> Result<LogWalk, RestoreError> {
    let mut cursor = log_bytes;
    let mut position: u64 = 0;
    let mut batches: u64 = 0;
    let mut records: u64 = 0;
    let mut max_timestamp_ms: i64 = -1;
    let mut previous_end_offset: Option<Offset> = None;

    while !cursor.is_empty() {
        let validated = match validate_one_v2_batch(cursor) {
            Ok(validated) => validated,
            Err(RecordsError::CrcMismatch { expected, computed }) => {
                return Err(RestoreError::ChecksumMismatch {
                    key: key.to_string(),
                    position,
                    expected,
                    computed,
                });
            }
            Err(
                RecordsError::HeaderTooShort { needed } | RecordsError::BodyTooShort { needed },
            ) => {
                return Err(truncated_at(key, position, cursor.len(), needed));
            }
            Err(other) => return Err(RestoreError::Records(other)),
        };

        let header = validated.header;
        let base_offset = header.base_offset.get();

        // The first batch must open exactly where the archive key says the
        // segment starts, and every later batch must start strictly after the
        // previous one ends. Either violation means the `.log` bytes and the
        // segment's own key name disagree about the segment's extent, which
        // is corruption or a mis-keyed archive; `TruncatedSegment` is reused
        // for it because it already carries the right shape ("the archive's
        // account of this segment's extent does not hold up"), even though
        // its rendered message text ("ends inside a batch ... N declared, M
        // available") was written for the framing case below and reads
        // awkwardly here. A dedicated variant would fit better; see the
        // implementer's report.
        if let Some(previous) = previous_end_offset {
            if base_offset <= previous.0 {
                return Err(RestoreError::TruncatedSegment {
                    key: key.to_string(),
                    position,
                    declared: offset_as_u64(previous.0 + 1),
                    available: offset_as_u64(base_offset),
                });
            }
        } else if base_offset != segment.base_offset.0 {
            return Err(RestoreError::TruncatedSegment {
                key: key.to_string(),
                position,
                declared: offset_as_u64(segment.base_offset.0),
                available: offset_as_u64(base_offset),
            });
        }

        previous_end_offset = Some(Offset(
            base_offset + i64::from(header.last_offset_delta.get()),
        ));
        batches += 1;
        records =
            records.saturating_add(u64::try_from(header.records_count.get().max(0)).unwrap_or(0));
        let timestamp = header.max_timestamp.get();
        if timestamp != -1 {
            max_timestamp_ms = max_timestamp_ms.max(timestamp);
        }

        let total_len = validated.total_len;
        cursor = &cursor[total_len..];
        position = position.saturating_add(u64::try_from(total_len).unwrap_or(u64::MAX));
    }

    Ok(LogWalk {
        // An empty `.log` (no batches at all) has nothing to derive an end
        // offset from; it holds exactly what its base offset already states.
        end_offset: previous_end_offset.unwrap_or(segment.base_offset),
        max_timestamp_ms,
        batches,
        records,
    })
}

/// Build the [`RestoreError::TruncatedSegment`] a short header or a short
/// body reports. Both `RecordsError::HeaderTooShort` and `BodyTooShort` carry
/// `needed`, the count of bytes still missing from `cursor`; `available` is
/// what `cursor` actually holds at `position`, and `declared` is what the
/// batch's framing says `cursor` needed to hold: `available + needed`.
fn truncated_at(key: &Path, position: u64, cursor_len: usize, needed: usize) -> RestoreError {
    let available = u64::try_from(cursor_len).unwrap_or(u64::MAX);
    let declared = available.saturating_add(u64::try_from(needed).unwrap_or(u64::MAX));
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position,
        declared,
        available,
    }
}

/// Best-effort `i64` → `u64` for a diagnostic [`RestoreError`] field. A Kafka
/// offset or timestamp is never negative in a well-formed segment; this only
/// has to render something sensible for corrupt input, never panic, and never
/// allocate.
fn offset_as_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// The highest relative offset an index entry may legally point at: the
/// segment's last offset, expressed relative to its base offset.
fn max_relative_offset(base_offset: Offset, end_offset: Offset) -> u32 {
    u32::try_from((end_offset.0 - base_offset.0).max(0)).unwrap_or(u32::MAX)
}

/// Byte position of one fixed-width index entry, for a
/// [`RestoreError::TruncatedSegment`].
fn index_position(entry_index: usize, entry_len: usize) -> u64 {
    u64::try_from(entry_index.saturating_mul(entry_len)).unwrap_or(u64::MAX)
}

/// Build the [`RestoreError::TruncatedSegment`] one bad index entry reports.
fn index_error(
    key: &Path,
    entry_index: usize,
    entry_len: usize,
    declared: u64,
    available: u64,
) -> RestoreError {
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position: index_position(entry_index, entry_len),
        declared,
        available,
    }
}

/// Check that `bytes`, parsed as a Kafka `.index`, is internally consistent
/// with the segment the `.log` walk established: `relative_offset` and
/// `position` both non-decreasing across entries, every `position` inside the
/// verified `.log`, and every `relative_offset` at or before the segment's
/// last offset.
fn validate_offset_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
    log_bytes: u64,
) -> Result<(), RestoreError> {
    let entries = parse_offset_index(bytes)?;
    let max_relative = max_relative_offset(base_offset, end_offset);
    let mut previous: Option<(u32, u32)> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let relative_offset = entry.relative_offset.get();
        let position = entry.position.get();

        if let Some((previous_relative, previous_position)) = previous {
            if relative_offset < previous_relative {
                return Err(index_error(
                    key,
                    entry_index,
                    OFFSET_INDEX_ENTRY_LEN,
                    u64::from(previous_relative),
                    u64::from(relative_offset),
                ));
            }
            if position < previous_position {
                return Err(index_error(
                    key,
                    entry_index,
                    OFFSET_INDEX_ENTRY_LEN,
                    u64::from(previous_position),
                    u64::from(position),
                ));
            }
        }
        if u64::from(position) >= log_bytes {
            return Err(index_error(
                key,
                entry_index,
                OFFSET_INDEX_ENTRY_LEN,
                log_bytes,
                u64::from(position),
            ));
        }
        if relative_offset > max_relative {
            return Err(index_error(
                key,
                entry_index,
                OFFSET_INDEX_ENTRY_LEN,
                u64::from(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some((relative_offset, position));
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.timeindex`, is internally
/// consistent: `relative_offset` and `timestamp` both non-decreasing across
/// entries, and every `relative_offset` at or before the segment's last
/// offset.
///
/// A time-index entry has no on-disk byte position to check against
/// `log_bytes`, unlike an offset-index entry, so that check does not apply
/// here; `timestamp` is checked non-decreasing in its place, the analogous
/// invariant the broker's append-only writer also holds for this index.
fn validate_time_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    let entries = parse_time_index(bytes)?;
    let max_relative = max_relative_offset(base_offset, end_offset);
    let mut previous: Option<(i64, u32)> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let timestamp = entry.timestamp.get();
        let relative_offset = entry.relative_offset.get();

        if let Some((previous_timestamp, previous_relative)) = previous {
            if relative_offset < previous_relative {
                return Err(index_error(
                    key,
                    entry_index,
                    TIME_INDEX_ENTRY_LEN,
                    u64::from(previous_relative),
                    u64::from(relative_offset),
                ));
            }
            if timestamp < previous_timestamp {
                return Err(index_error(
                    key,
                    entry_index,
                    TIME_INDEX_ENTRY_LEN,
                    offset_as_u64(previous_timestamp),
                    offset_as_u64(timestamp),
                ));
            }
        }
        if relative_offset > max_relative {
            return Err(index_error(
                key,
                entry_index,
                TIME_INDEX_ENTRY_LEN,
                u64::from(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some((timestamp, relative_offset));
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.txnindex`, is internally
/// consistent: `start_offset` non-decreasing across entries, `start_offset <=
/// last_offset` within each entry, and both ends of every entry inside
/// `[base_offset, end_offset]`.
fn validate_txn_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    let entries = parse_txn_index(bytes)?;
    let mut previous_start: Option<i64> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let start_offset = entry.start_offset.get();
        let last_offset = entry.last_offset.get();

        if let Some(previous) = previous_start
            && start_offset < previous
        {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(previous),
                offset_as_u64(start_offset),
            ));
        }
        if start_offset > last_offset {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(last_offset),
                offset_as_u64(start_offset),
            ));
        }
        if start_offset < base_offset.0 || last_offset > end_offset.0 {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(end_offset.0),
                offset_as_u64(last_offset.max(start_offset)),
            ));
        }

        previous_start = Some(start_offset);
    }
    Ok(())
}

/// Check a Kafka producer-state `.snapshot`: `version` (bytes `0..2`) must be
/// [`SNAPSHOT_VERSION`], the object length must equal
/// `SNAPSHOT_HEADER_LEN + count * SNAPSHOT_ENTRY_LEN` for the `count` (bytes
/// `6..10`) it declares, and the CRC32C (bytes `2..6`) over
/// `bytes[SNAPSHOT_CRC_COVERAGE_START..]` must match. Entry fields are not
/// decoded; only the framing and the checksum are verified.
fn validate_producer_snapshot(key: &Path, bytes: &[u8]) -> Result<(), RestoreError> {
    if bytes.len() < SNAPSHOT_HEADER_LEN {
        return Err(RestoreError::TruncatedSegment {
            key: key.to_string(),
            position: 0,
            declared: u64::try_from(SNAPSHOT_HEADER_LEN).unwrap_or(u64::MAX),
            available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }

    let version = i16::from_be_bytes([bytes[0], bytes[1]]);
    if version != SNAPSHOT_VERSION {
        // A version mismatch is a framing problem, like a length mismatch, not
        // a checksum problem, so it is reported as `TruncatedSegment` too:
        // `declared` is the version this restore understands, `available` is
        // the version the snapshot actually declares.
        return Err(RestoreError::TruncatedSegment {
            key: key.to_string(),
            position: 0,
            declared: offset_as_u64(i64::from(SNAPSHOT_VERSION)),
            available: offset_as_u64(i64::from(version)),
        });
    }

    let declared_count = i32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    let expected_len = usize::try_from(declared_count)
        .ok()
        .and_then(|count| count.checked_mul(SNAPSHOT_ENTRY_LEN))
        .and_then(|body| body.checked_add(SNAPSHOT_HEADER_LEN));
    if expected_len != Some(bytes.len()) {
        return Err(RestoreError::TruncatedSegment {
            key: key.to_string(),
            position: 0,
            declared: expected_len
                .and_then(|len| u64::try_from(len).ok())
                .unwrap_or(u64::MAX),
            available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }

    let stored_crc = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
    let computed_crc = crc32c(&bytes[SNAPSHOT_CRC_COVERAGE_START..]);
    if stored_crc != computed_crc {
        return Err(RestoreError::ChecksumMismatch {
            key: key.to_string(),
            position: 0,
            expected: stored_crc,
            computed: computed_crc,
        });
    }
    Ok(())
}

/// Build the [`RestoreError::TruncatedSegment`] a malformed
/// `.leader_epoch_checkpoint` row reports. `declared` is the row count the
/// checkpoint's own header line states; `available` is how many rows had
/// already parsed successfully when the malformed row was reached. The
/// checkpoint format carries no byte-length field of its own to compare
/// against, unlike the `.log` and index cases, so this is the closest
/// reading of `TruncatedSegment`'s "declared vs. available" shape that the
/// format supports.
fn checkpoint_error(key: &Path, position: u64, declared: u64, available: usize) -> RestoreError {
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position,
        declared,
        available: u64::try_from(available).unwrap_or(u64::MAX),
    }
}

/// Parse a `.leader_epoch_checkpoint`: `"0\n{n}\n"` followed by exactly `n`
/// `"{epoch} {offset}"` rows, matching the format
/// `krabka_log::LeaderEpochCheckpoint` and the broker's remote-log-manager
/// copy path both write.
///
/// Unlike `LeaderEpochCheckpoint::parse`, which is written for a local file a
/// process itself created and is lenient about a bad version line or a short
/// count, this rejects any deviation: the bytes here came out of an archive
/// that can be corrupt, so the version line, the count, and every row are all
/// checked strictly. The declared row count is never used to size an
/// allocation: the loop below grows a `Vec` by `push`, and it can iterate at
/// most as many times as the text actually has lines, regardless of what a
/// corrupt or hostile count line claims.
fn parse_leader_epoch_checkpoint(
    key: &Path,
    bytes: &[u8],
) -> Result<Vec<(LeaderEpoch, Offset)>, RestoreError> {
    let text = std::str::from_utf8(bytes).map_err(|_| checkpoint_error(key, 0, 0, 0))?;
    let mut lines = text.split('\n');
    let mut position: u64 = 0;

    let version_line = lines.next().unwrap_or_default();
    if version_line != "0" {
        return Err(checkpoint_error(key, position, 0, 0));
    }
    position += u64::try_from(version_line.len().saturating_add(1)).unwrap_or(u64::MAX);

    let count_line = lines
        .next()
        .ok_or_else(|| checkpoint_error(key, position, 0, 0))?;
    let count: usize = count_line
        .trim()
        .parse()
        .map_err(|_| checkpoint_error(key, position, 0, 0))?;
    let declared = u64::try_from(count).unwrap_or(u64::MAX);
    position += u64::try_from(count_line.len().saturating_add(1)).unwrap_or(u64::MAX);

    let mut entries = Vec::new();
    for _ in 0..count {
        let line = lines
            .next()
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        let mut parts = line.split_whitespace();
        let epoch: i32 = parts
            .next()
            .and_then(|token| token.parse().ok())
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        let start_offset: i64 = parts
            .next()
            .and_then(|token| token.parse().ok())
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        if parts.next().is_some() {
            return Err(checkpoint_error(key, position, declared, entries.len()));
        }
        entries.push((LeaderEpoch(epoch), Offset(start_offset)));
        position += u64::try_from(line.len().saturating_add(1)).unwrap_or(u64::MAX);
    }

    // The broker's writer always emits exactly `count` rows after the header,
    // so anything left beyond a single trailing empty split (the newline
    // after the last row) is itself a count mismatch.
    if lines.any(|remaining| !remaining.is_empty()) {
        return Err(checkpoint_error(key, position, declared, entries.len()));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::{BufMut, BytesMut};
    use clap::Parser as _;
    use krabka_protocol::records::{Record, RecordBatch};
    use tempfile::TempDir;

    use super::*;

    fn test_partition() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(0xA5CD), "orders", 0)
    }

    fn archive_at(dir: &std::path::Path) -> ArchiveStore {
        let cli = crate::Cli::parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-local",
            &dir.display().to_string(),
        ]);
        crate::open_archive(&cli.args).expect("archive store")
    }

    fn write_object(root: &std::path::Path, relative: &str, bytes: &[u8]) -> ArchiveObject {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(&path, bytes).expect("write object");
        ArchiveObject {
            key: Path::from(relative),
            size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        }
    }

    fn record(offset_delta: i32, timestamp_delta: i64) -> Record {
        Record {
            offset_delta,
            timestamp_delta,
            value: Some(Bytes::from_static(b"value")),
            ..Default::default()
        }
    }

    fn batch(
        base_offset: i64,
        base_timestamp: i64,
        max_timestamp: i64,
        record_count: i32,
    ) -> RecordBatch {
        RecordBatch {
            base_offset,
            last_offset_delta: record_count - 1,
            base_timestamp,
            max_timestamp,
            records: (0..record_count)
                .map(|i| record(i, i64::from(i) * 10))
                .collect(),
            ..RecordBatch::default()
        }
    }

    fn encode_all(batches: &[RecordBatch]) -> Bytes {
        let mut buf = BytesMut::new();
        for b in batches {
            b.encode(&mut buf).expect("encode batch");
        }
        buf.freeze()
    }

    fn offset_index_bytes(entries: &[(u32, u32)]) -> Bytes {
        let mut buf = BytesMut::new();
        for &(rel, pos) in entries {
            buf.put_u32(rel);
            buf.put_u32(pos);
        }
        buf.freeze()
    }

    fn time_index_bytes(entries: &[(i64, u32)]) -> Bytes {
        let mut buf = BytesMut::new();
        for &(ts, rel) in entries {
            buf.put_i64(ts);
            buf.put_u32(rel);
        }
        buf.freeze()
    }

    fn build_snapshot(entry_count: i32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(SNAPSHOT_VERSION);
        buf.put_u32(0); // CRC placeholder, patched below.
        buf.put_i32(entry_count);
        for _ in 0..entry_count {
            buf.extend_from_slice(&[0u8; SNAPSHOT_ENTRY_LEN]);
        }
        let crc = crc32c(&buf[SNAPSHOT_CRC_COVERAGE_START..]);
        buf[2..6].copy_from_slice(&crc.to_be_bytes());
        buf.freeze()
    }

    fn build_checkpoint(entries: &[(i32, i64)]) -> Bytes {
        use std::fmt::Write as _;

        let mut s = format!("0\n{}\n", entries.len());
        for (epoch, offset) in entries {
            let _ = writeln!(s, "{epoch} {offset}");
        }
        Bytes::from(s.into_bytes())
    }

    /// The byte blobs of one internally-consistent two-batch segment: offsets
    /// 100..=102 in the first batch, 103..=104 in the second, plus the facts
    /// `verify_segment` should derive from them.
    struct SegmentBytes {
        log: Bytes,
        offset_index: Bytes,
        time_index: Bytes,
        snapshot: Bytes,
        checkpoint: Bytes,
        base_offset: Offset,
        end_offset: Offset,
        max_timestamp_ms: i64,
        batches: u64,
        records: u64,
        leader_epochs: Vec<(LeaderEpoch, Offset)>,
    }

    fn segment_bytes(batch1_max_ts: i64, batch2_max_ts: i64) -> SegmentBytes {
        let batch1 = batch(100, 1000, batch1_max_ts, 3);
        let batch2 = batch(103, 1030, batch2_max_ts, 2);
        let batch1_len = batch1.encoded_len();
        let log = encode_all(&[batch1, batch2]);
        let max_timestamp_ms = [batch1_max_ts, batch2_max_ts]
            .into_iter()
            .filter(|&ts| ts != -1)
            .max()
            .unwrap_or(-1);

        SegmentBytes {
            log,
            offset_index: offset_index_bytes(&[(0, 0), (3, u32::try_from(batch1_len).unwrap())]),
            time_index: time_index_bytes(&[(0, 0)]),
            snapshot: build_snapshot(0),
            checkpoint: build_checkpoint(&[(0, 100), (1, 103)]),
            base_offset: Offset(100),
            end_offset: Offset(104),
            max_timestamp_ms,
            batches: 2,
            records: 5,
            leader_epochs: vec![(LeaderEpoch(0), Offset(100)), (LeaderEpoch(1), Offset(103))],
        }
    }

    fn valid_segment_bytes() -> SegmentBytes {
        segment_bytes(1020, 1040)
    }

    fn write_segment(
        dir: &std::path::Path,
        bytes: &SegmentBytes,
        omit: &[&str],
    ) -> SegmentInventory {
        let present = |name: &str| !omit.contains(&name);
        SegmentInventory {
            segment_id: Uuid::from_u128(0xBEEF),
            base_offset: bytes.base_offset,
            log: present(".log").then(|| write_object(dir, "orders-0/seg.log", &bytes.log)),
            offset_index: present(".index")
                .then(|| write_object(dir, "orders-0/seg.index", &bytes.offset_index)),
            time_index: present(".timeindex")
                .then(|| write_object(dir, "orders-0/seg.timeindex", &bytes.time_index)),
            producer_snapshot: present(".snapshot")
                .then(|| write_object(dir, "orders-0/seg.snapshot", &bytes.snapshot)),
            leader_epoch: present(".leader_epoch_checkpoint").then(|| {
                write_object(
                    dir,
                    "orders-0/seg.leader_epoch_checkpoint",
                    &bytes.checkpoint,
                )
            }),
            transaction_index: None,
        }
    }

    #[tokio::test]
    async fn a_clean_segment_verifies_and_reports_its_facts() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let fixture = valid_segment_bytes();
        let segment = write_segment(dir.path(), &fixture, &[]);

        let verified = verify_segment(&store, &partition, &segment)
            .await
            .expect("verify");

        check!(
            verified.facts
                == SegmentFacts {
                    segment_id: segment.segment_id,
                    base_offset: fixture.base_offset,
                    end_offset: fixture.end_offset,
                    max_timestamp_ms: fixture.max_timestamp_ms,
                    batches: fixture.batches,
                    records: fixture.records,
                    log_bytes: u64::try_from(fixture.log.len()).unwrap(),
                    leader_epochs: fixture.leader_epochs.clone(),
                }
        );
        check!(verified.log == fixture.log);
    }

    #[tokio::test]
    async fn missing_mandatory_artifacts_are_torn_copies_in_written_order() {
        let cases: [(&[&str], &str); 7] = [
            (&[".log"], ".log"),
            (&[".index"], ".index"),
            (&[".timeindex"], ".timeindex"),
            (&[".snapshot"], ".snapshot"),
            (&[".leader_epoch_checkpoint"], ".leader_epoch_checkpoint"),
            // When more than one artifact is absent, the first in the
            // documented order is the one reported.
            (&[".log", ".timeindex"], ".log"),
            (
                &[".index", ".snapshot", ".leader_epoch_checkpoint"],
                ".index",
            ),
        ];

        for (omit, expected) in cases {
            let dir = TempDir::new().expect("tempdir");
            let store = archive_at(dir.path());
            let partition = test_partition();
            let fixture = valid_segment_bytes();
            let segment = write_segment(dir.path(), &fixture, omit);

            let error = verify_segment(&store, &partition, &segment)
                .await
                .expect_err("torn copy");
            let RestoreError::TornCopy { artifact, .. } = error else {
                panic!("omit {omit:?}: expected TornCopy, got {error:?}");
            };
            check!(artifact.as_str() == expected, "omit {omit:?}");
        }
    }

    #[tokio::test]
    async fn a_flipped_crc_covered_byte_is_a_checksum_mismatch() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let mut fixture = valid_segment_bytes();

        // Byte 62 sits inside the first batch's body, well past its 61-byte
        // header and the CRC field it carries.
        let mut corrupted = fixture.log.to_vec();
        corrupted[62] ^= 0xFF;
        fixture.log = Bytes::from(corrupted);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("checksum mismatch");
        let RestoreError::ChecksumMismatch { key, position, .. } = error else {
            panic!("expected ChecksumMismatch, got {error:?}");
        };
        check!(key == "orders-0/seg.log");
        check!(position == 0);
    }

    #[tokio::test]
    async fn a_log_truncated_mid_batch_is_a_truncated_segment() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let mut fixture = valid_segment_bytes();

        let full_len = fixture.log.len();
        fixture.log = fixture.log.slice(0..full_len - 10);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("truncated segment");
        check!(matches!(error, RestoreError::TruncatedSegment { .. }));
    }

    #[tokio::test]
    async fn an_offset_index_entry_past_the_log_end_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let mut fixture = valid_segment_bytes();

        let log_len = u32::try_from(fixture.log.len()).unwrap();
        fixture.offset_index = offset_index_bytes(&[(0, 0), (3, log_len + 1_000)]);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("index entry past the log end");
        check!(matches!(error, RestoreError::TruncatedSegment { .. }));
    }

    #[tokio::test]
    async fn a_non_monotonic_time_index_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let mut fixture = valid_segment_bytes();

        fixture.time_index = time_index_bytes(&[(1_030, 3), (1_000, 0)]);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("non-monotonic time index");
        check!(matches!(error, RestoreError::TruncatedSegment { .. }));
    }

    #[tokio::test]
    async fn a_corrupt_snapshot_crc_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let mut fixture = valid_segment_bytes();

        let mut corrupted = fixture.snapshot.to_vec();
        corrupted[2] ^= 0xFF;
        fixture.snapshot = Bytes::from(corrupted);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("corrupt snapshot CRC");
        check!(matches!(error, RestoreError::ChecksumMismatch { .. }));
    }

    #[tokio::test]
    async fn malformed_leader_epoch_checkpoints_are_rejected() {
        let cases = [
            "1\n0\n",               // wrong version header
            "0\nnot-a-number\n",    // non-numeric row count
            "0\n2\n0 100\n",        // declares 2 rows, only 1 present
            "0\n1\nzero hundred\n", // non-numeric row
        ];

        for bad in cases {
            let dir = TempDir::new().expect("tempdir");
            let store = archive_at(dir.path());
            let partition = test_partition();
            let mut fixture = valid_segment_bytes();
            fixture.checkpoint = Bytes::from_static(bad.as_bytes());

            let segment = write_segment(dir.path(), &fixture, &[]);
            let error = verify_segment(&store, &partition, &segment)
                .await
                .expect_err("malformed checkpoint");
            check!(
                matches!(error, RestoreError::TruncatedSegment { .. }),
                "case {bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn every_batch_reporting_unknown_timestamp_keeps_the_sentinel() {
        let dir = TempDir::new().expect("tempdir");
        let store = archive_at(dir.path());
        let partition = test_partition();
        let fixture = segment_bytes(-1, -1);

        let segment = write_segment(dir.path(), &fixture, &[]);
        let verified = verify_segment(&store, &partition, &segment)
            .await
            .expect("verify");
        check!(verified.facts.max_timestamp_ms == -1);
    }
}
