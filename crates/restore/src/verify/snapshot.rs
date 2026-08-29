//! The framing and checksum check for a segment's producer-state `.snapshot`.
//! It is the one sidecar with a header and a CRC of its own rather than a run
//! of fixed-width entries, and its layout constants have to agree byte for byte
//! with the writer in `krabka_log::producer_snapshot`, so it keeps its own file
//! where that contract is stated once.

use crc32c::crc32c;
use object_store::path::Path;

use super::offset_as_u64;
use crate::error::RestoreError;

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
pub(super) const SNAPSHOT_CRC_COVERAGE_START: usize = 6;

/// Byte length of one producer-state snapshot entry.
pub(super) const SNAPSHOT_ENTRY_LEN: usize = 46;

/// The only producer-state snapshot version this restore understands, and the
/// only one the broker's copy path ever archives.
pub(super) const SNAPSHOT_VERSION: i16 = 1;

/// Check a Kafka producer-state `.snapshot`: `version` (bytes `0..2`) must be
/// [`SNAPSHOT_VERSION`], the object length must equal
/// `SNAPSHOT_HEADER_LEN + count * SNAPSHOT_ENTRY_LEN` for the `count` (bytes
/// `6..10`) it declares, and the CRC32C (bytes `2..6`) over
/// `bytes[SNAPSHOT_CRC_COVERAGE_START..]` must match. Entry fields are not
/// decoded; only the framing and the checksum are verified.
pub(super) fn validate_producer_snapshot(key: &Path, bytes: &[u8]) -> Result<(), RestoreError> {
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
