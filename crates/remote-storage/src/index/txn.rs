//! The Kafka transaction-index on-disk layout and the range-overlap test the
//! fetch path applies to it.
//!
//! An entry records one aborted transaction's offset span and the producer
//! that wrote it, so a fetch can report the aborted transactions that
//! intersect the offsets it returns.

use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned, byteorder::I64};

use super::{LogOffset, corrupt_index};
use crate::error::RemoteStorageError;

/// 24 bytes per entry: `start_offset` i64 BE, `last_offset` i64 BE, then
/// `producer_id` i64 BE. It mirrors `krabka_log::txn_index::AbortedTxnRaw`, so
/// the remote-tier copy of a `.txnindex` file decodes through the same byte
/// layout that wrote the local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct AbortedTxnIndexEntry {
    /// First offset the aborted transaction wrote.
    pub start_offset: I64<BigEndian>,
    /// Last offset the aborted transaction wrote.
    pub last_offset: I64<BigEndian>,
    /// Producer that wrote, and then aborted, the transaction.
    pub producer_id: I64<BigEndian>,
}

/// Byte length of one serialized aborted-transaction index entry.
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

const _: [(); 24] = [(); TXN_INDEX_ENTRY_LEN];

/// Borrows Kafka's transaction-index format as a zero-copy
/// `&[AbortedTxnIndexEntry]`, at 24 bytes per entry: `start_offset` i64 BE,
/// `last_offset` i64 BE, then `producer_id` i64 BE. It ignores trailing bytes
/// that do not complete a 24-byte entry. The result borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_txn_index(bytes: &[u8]) -> Result<&[AbortedTxnIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TXN_INDEX_ENTRY_LEN) * TXN_INDEX_ENTRY_LEN;
    <[AbortedTxnIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("transaction"))
}

/// Reports whether an aborted-transaction entry overlaps the inclusive offset
/// range `[from_offset, to_offset]`. It mirrors the overlap test in
/// `TxnIndex::aborted_in_range` against an inclusive range: the entry's
/// `[start, last]` intersects `[from, to]` if and only if
/// `start <= to && last >= from`.
#[must_use]
pub fn txn_overlaps(
    entry: &AbortedTxnIndexEntry,
    from_offset: LogOffset,
    to_offset: LogOffset,
) -> bool {
    entry.start_offset.get() <= to_offset && entry.last_offset.get() >= from_offset
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_txn_index_round_trips_known_entries() {
        // Mirror TxnIndex::append: 8B start_offset BE, 8B last_offset BE,
        // 8B producer_id BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend_from_slice(&start.to_be_bytes());
            buf.extend_from_slice(&last.to_be_bytes());
            buf.extend_from_slice(&pid.to_be_bytes());
        }
        let entries = parse_txn_index(&buf).expect("valid txn index");
        let decoded: Vec<(i64, i64, i64)> = entries
            .iter()
            .map(|e| {
                (
                    e.start_offset.get(),
                    e.last_offset.get(),
                    e.producer_id.get(),
                )
            })
            .collect();
        assert!(decoded == vec![(0, 4, 1000), (10, 14, 2000)]);
    }

    #[test]
    fn parse_txn_index_truncates_trailing_partial_bytes() {
        let mut buf = Vec::new();
        for v in [0_i64, 4, 1000] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        // 5 trailing bytes that don't complete a 24-byte entry.
        buf.extend_from_slice(&[0xAA; 5]);
        let entries = parse_txn_index(&buf).expect("valid txn index");
        assert!(entries.len() == 1, "partial trailing entry ignored");
        assert!(entries[0].producer_id.get() == 1000);
    }

    #[test]
    fn parse_txn_index_empty_is_empty() {
        assert!(parse_txn_index(&[]).expect("empty is valid").is_empty());
    }

    #[test]
    fn txn_overlaps_boundaries() {
        let e = AbortedTxnIndexEntry {
            start_offset: I64::new(10),
            last_offset: I64::new(14),
            producer_id: I64::new(1),
        };
        let cases = [
            // Range fully before the entry → excluded.
            (0, 9, false),
            // Range touching the entry's first offset → included.
            (0, 10, true),
            // Range fully inside the entry → included.
            (11, 13, true),
            // Range touching the entry's last offset → included.
            (14, 100, true),
            // Range fully after the entry → excluded.
            (15, 100, false),
            // Range fully covering the entry → included.
            (0, 100, true),
        ];
        for (start, end, want) in cases {
            assert!(
                txn_overlaps(&e, start, end) == want,
                "range [{start},{end}]"
            );
        }
    }
}
