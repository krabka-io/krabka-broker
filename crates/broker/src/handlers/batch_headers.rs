//! The v2 record batch headers of a read, walked one batch at a time.
//!
//! Fetch's remote band and `ShareFetch` both need each batch's span and header
//! without decoding its records. Produce frames batches by Kafka's
//! `ByteBufferLogInputStream.nextBatch` order instead, so it shares only
//! [`LOG_OVERHEAD`].

use {
    krabka_protocol::records::{HEADER_LEN, RecordBatchHeader},
    std::{iter, ops::Range},
    zerocopy::FromBytes as _,
};

/// The bytes of a v2 batch in front of what its `batch_length` field counts:
/// the base offset (8) and the length itself (4). Kafka calls the pair
/// `Records.LOG_OVERHEAD`.
pub(super) const LOG_OVERHEAD: usize = 12;

/// Why the bytes of a read are not a run of whole v2 batches. Each caller maps
/// [`CorruptBatch::what`] into its own error type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CorruptBatch {
    /// Fewer bytes are left than a batch header needs.
    TruncatedHeader,
    /// `batch_length` is shorter than a header or runs past the read.
    LengthOutsideRead,
}

impl CorruptBatch {
    /// The phrase the corrupt-records error names the fault with.
    pub(super) fn what(self) -> &'static str {
        match self {
            Self::TruncatedHeader => "a truncated record batch header",
            Self::LengthOutsideRead => "a record batch length outside the read",
        }
    }
}

/// Every v2 batch in `bytes`, front to back: the byte range the whole batch
/// covers, and its header. It reads only the headers.
///
/// The walk stops after the first [`CorruptBatch`] it yields.
pub(super) fn v2_batch_headers(
    bytes: &[u8],
) -> impl Iterator<Item = Result<(Range<usize>, &RecordBatchHeader), CorruptBatch>> {
    let mut at = 0_usize;
    let mut failed = false;
    iter::from_fn(move || {
        if failed || at >= bytes.len() {
            return None;
        }
        let next = next_batch(bytes, at);
        match &next {
            Ok((span, _)) => at = span.end,
            Err(_) => failed = true,
        }
        Some(next)
    })
}

/// The batch that starts at `at`.
fn next_batch(bytes: &[u8], at: usize) -> Result<(Range<usize>, &RecordBatchHeader), CorruptBatch> {
    let header = bytes
        .get(at..at + HEADER_LEN)
        .and_then(|raw| RecordBatchHeader::ref_from_bytes(raw).ok())
        .ok_or(CorruptBatch::TruncatedHeader)?;
    let length = usize::try_from(header.batch_length.get())
        .ok()
        .and_then(|length| length.checked_add(LOG_OVERHEAD))
        .filter(|length| *length >= HEADER_LEN && at + length <= bytes.len())
        .ok_or(CorruptBatch::LengthOutsideRead)?;
    Ok((at..at + length, header))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A v2 header whose `batch_length` is `batch_length`, padded with
    /// `body` zero bytes after the header.
    fn batch(batch_length: i32, body: usize) -> Vec<u8> {
        let mut bytes = vec![0_u8; HEADER_LEN + body];
        bytes[8..12].copy_from_slice(&batch_length.to_be_bytes());
        bytes
    }

    fn spans(bytes: &[u8]) -> Vec<Result<Range<usize>, CorruptBatch>> {
        v2_batch_headers(bytes)
            .map(|item| item.map(|(span, _)| span))
            .collect()
    }

    /// The length a batch of exactly one header declares.
    fn header_only() -> i32 { i32::try_from(HEADER_LEN - LOG_OVERHEAD).unwrap_or(i32::MAX) }

    #[test]
    fn walks_whole_batches_in_order() {
        let mut bytes = batch(header_only(), 0);
        bytes.extend(batch(header_only() + 3, 3));
        assert!(spans(&bytes) == vec![Ok(0..HEADER_LEN), Ok(HEADER_LEN..2 * HEADER_LEN + 3)]);
    }

    #[test]
    fn empty_read_has_no_batches() {
        assert!(spans(&[]).is_empty());
    }

    #[test]
    fn faults_and_stops_on_a_truncated_header() {
        let mut bytes = batch(header_only(), 0);
        bytes.extend([0_u8; 5]);
        assert!(spans(&bytes) == vec![Ok(0..HEADER_LEN), Err(CorruptBatch::TruncatedHeader)]);
    }

    #[test]
    fn faults_on_lengths_outside_the_read() {
        for length in [-1, 0, header_only() - 1, header_only() + 1, i32::MAX] {
            assert!(
                spans(&batch(length, 0)) == vec![Err(CorruptBatch::LengthOutsideRead)],
                "batch_length {length}"
            );
        }
    }

    #[test]
    fn names_each_fault_as_before() {
        assert!(CorruptBatch::TruncatedHeader.what() == "a truncated record batch header");
        assert!(CorruptBatch::LengthOutsideRead.what() == "a record batch length outside the read");
    }
}
