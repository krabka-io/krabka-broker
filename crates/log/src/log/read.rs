//! The read paths: decoded batches, verbatim wire bytes, and file-region
//! descriptors for the zero-copy `sendfile` fetch.
//!
//! All three walk the sealed segments and then the active one, so a read
//! can span a segment boundary, and all three ask each segment for at
//! least one whole batch header to satisfy Kafka's anti-stall rule.

use bytes::{Bytes, BytesMut};
use krabka_ids::Offset;
use krabka_protocol::records::{HEADER_LEN, RecordBatch};
use krabka_units::prelude::{ByteSize, ByteSizeExt, bytes};
use tracing::instrument;

use super::Log;
use crate::{error::LogError, segment::RawSegmentRead};

/// A `usize` byte length as a quantity.
///
/// The read paths track their budget as a [`ByteSize`], but the lengths they
/// subtract come from `Bytes::len` and `FileRegion::len`, which are `usize`.
fn size_from_len(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}

/// The fixed v2 record-batch header, as a quantity.
///
/// The log asks every per-segment read for at least this much, so the header
/// walk that finds batch boundaries always has a header to read. This is
/// Kafka's anti-stall rule. The value comes from [`HEADER_LEN`] rather than a
/// restated constant, so the floor cannot drift from the protocol.
fn batch_header() -> ByteSize {
    size_from_len(HEADER_LEN)
}

/// Result of [`Log::read`]: the absolute offset of the first batch
/// returned and the batches themselves.
///
/// `start_offset` falls back to the requested offset when the read returns
/// no batches, for example a read at the log end. Callers can then resume
/// from the value and need no special case for an empty result.
#[derive(Debug)]
pub struct ReadOutput {
    /// Absolute offset of the first record in [`Self::batches`], or the
    /// requested offset when no batches were returned.
    pub start_offset: Offset,
    /// Decoded batches in offset order. May be empty if the log has no
    /// data at or after the requested offset.
    pub batches: Vec<RecordBatch>,
}

/// Verbatim, decode-free output of [`Log::read_raw`].
#[derive(Debug, Clone)]
pub struct RawRead {
    /// Absolute offset of the first batch in [`Self::bytes`], or the
    /// requested offset when no bytes were returned.
    pub start_offset: Offset,
    /// Verbatim `.log` bytes: zero or more complete v2 batches. The bytes
    /// can span segment boundaries.
    pub bytes: Bytes,
    /// Length of [`Self::bytes`] in bytes.
    pub total: usize,
    /// Last offset included in [`Self::bytes`], or `None` when empty.
    pub last_offset: Option<Offset>,
}

impl RawRead {
    fn empty(off: Offset) -> Self {
        Self {
            start_offset: off,
            bytes: Bytes::new(),
            total: 0,
            last_offset: None,
        }
    }
}

crate::sendfile_cfg! {
    /// Descriptor form of [`Log::read_raw`] for the zero-copy `sendfile` fetch
    /// path, Increments D + E.
    ///
    /// One [`krabka_protocol::records::FileRegion`] describes the records run
    /// for each contributing segment. A multi-segment fetch therefore goes
    /// out as several `sendfile` regions with **no** coalescing copy.
    /// `read_raw` instead concatenates cross-segment chunks into a fresh
    /// `BytesMut`. This type is compiled on the SENDFILE alias: Linux, Apple,
    /// and FreeBSD/DragonFly.
    #[derive(Debug, Clone)]
    pub struct RawReadDesc {
        /// Absolute offset of the first batch in the regions, or the requested
        /// offset when no bytes were returned.
        pub start_offset: Offset,
        /// One file-backed region per contributing segment, in wire order.
        pub regions: Vec<krabka_protocol::records::FileRegion>,
        /// Total byte length across all regions.
        pub total: usize,
    }

    impl RawReadDesc {
        fn empty(off: Offset) -> Self {
            Self {
                start_offset: off,
                regions: Vec::new(),
                total: 0,
            }
        }
    }
}

impl Log {
    /// Reject an offset no local segment can answer.
    ///
    /// Below the global log start there is nothing anywhere, and the error
    /// says so. Between the global floor and the local one (KIP-405) the
    /// records are in the remote tier: the read fails all the same, so the
    /// broker's fetch path falls through to the remote reader rather than
    /// serving the first batch a surviving segment happens to start with.
    fn check_locally_readable(&self, offset: Offset) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }
        let local_log_start = self.local_log_start_offset();
        if offset < local_log_start {
            return Err(LogError::OffsetBelowLocalStart {
                requested: offset,
                local_log_start,
            });
        }
        Ok(())
    }

    /// Read batches from `offset` and return up to about `max_size` of
    /// `.log` data.
    ///
    /// The read walks sealed segments first, then the active segment, so a
    /// read can span segment boundaries.
    #[instrument(
        level = "debug",
        skip(self),
        fields(batches = tracing::field::Empty),
        err,
    )]
    // cargo-mutants: the `current_offset = base + last_offset_delta + 1` cursor advance only
    // ever moves the cursor too LOW under these mutations; each segment's
    // `read` self-filters via `batch_last >= offset` and clamps sub-base
    // offsets, so a too-low cursor yields the same batches and `start_offset`
    // (taken from `batches.first()`). No distinguishing input exists.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read(&self, offset: Offset, max_size: ByteSize) -> Result<ReadOutput, LogError> {
        let read_buffer_cap = self.config.read().unwrap().read_buffer_cap;
        let log_end = self.log_end_offset();
        self.check_locally_readable(offset)?;
        if offset >= log_end {
            return Ok(ReadOutput {
                start_offset: log_end,
                batches: Vec::new(),
            });
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut current_offset = offset;
        let mut remaining = max_size;

        for seg in &self.segments {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs = seg.read_with_buffer_cap(current_offset, remaining, read_buffer_cap)?;
            if !bs.is_empty() {
                let consumed: usize = bs.iter().map(RecordBatch::encoded_len).sum();
                remaining = (remaining - size_from_len(consumed)).max(ByteSize::ZERO);
                let last = bs.last().expect("non-empty by branch");
                current_offset = Offset(last.base_offset + i64::from(last.last_offset_delta) + 1);
                batches.extend(bs);
                if remaining == ByteSize::ZERO {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || batches.is_empty())
            && let Some(active) = &self.active
            && current_offset <= active.last_offset()
        {
            let bs = active.read_with_buffer_cap(
                current_offset,
                remaining.max(bytes(1)),
                read_buffer_cap,
            )?;
            batches.extend(bs);
        }

        let start_offset = batches.first().map_or(offset, |b| Offset(b.base_offset));
        tracing::Span::current().record("batches", batches.len());
        Ok(ReadOutput {
            start_offset,
            batches,
        })
    }

    /// Like [`Log::read`], but returns verbatim wire bytes with no decode.
    ///
    /// The read walks sealed segments and then the active segment. It
    /// includes only batches with `base_offset < limit_offset`, up to about
    /// `max_size`, and always at least one batch.
    #[instrument(
        level = "debug",
        skip(self),
        fields(total = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawRead, LogError> {
        let read_buffer_cap = self.config.read().unwrap().read_buffer_cap;
        self.check_locally_readable(fetch_offset)?;
        if fetch_offset >= limit_offset {
            return Ok(RawRead::empty(fetch_offset));
        }

        let mut chunks: Vec<Bytes> = Vec::new();
        let mut start_offset = fetch_offset;
        let mut current = fetch_offset;
        let mut remaining = max_size;
        let mut got_first = false;
        let mut last_offset = None;

        for seg in &self.segments {
            if seg.last_offset() < current {
                continue;
            }
            let r: RawSegmentRead = seg.read_raw_with_buffer_cap(
                current,
                limit_offset,
                remaining.max(batch_header()),
                read_buffer_cap,
            )?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                    got_first = true;
                }
                remaining = (remaining - size_from_len(r.bytes.len())).max(ByteSize::ZERO);
                current = r.last_offset + 1;
                last_offset = Some(r.last_offset);
                chunks.push(r.bytes);
                if remaining == ByteSize::ZERO || current >= limit_offset {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || !got_first)
            && current < limit_offset
            && let Some(active) = &self.active
            && current <= active.last_offset()
        {
            let r = active.read_raw_with_buffer_cap(
                current,
                limit_offset,
                remaining.max(batch_header()),
                read_buffer_cap,
            )?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                }
                chunks.push(r.bytes);
                last_offset = Some(r.last_offset);
            }
        }

        let bytes = match chunks.len() {
            0 => Bytes::new(),
            1 => chunks.pop().expect("len==1"),
            _ => {
                let total: usize = chunks.iter().map(Bytes::len).sum();
                let mut b = BytesMut::with_capacity(total);
                for c in &chunks {
                    b.extend_from_slice(c);
                }
                b.freeze()
            }
        };
        let total = bytes.len();
        tracing::Span::current().record("total", total);
        Ok(RawRead {
            start_offset,
            bytes,
            total,
            last_offset,
        })
    }

    crate::sendfile_cfg! {
    /// Descriptor variant of [`Log::read_raw`] for the zero-copy `sendfile`
    /// fetch path.
    ///
    /// This method walks sealed segments and then the active segment exactly
    /// as `read_raw` does. It collects one [`krabka_protocol::records::FileRegion`] for each contributing segment
    /// through `Segment::read_raw_desc`, instead of owned `Bytes`.
    /// Multi-segment fetches are **not** coalesced. Each region goes out in
    /// its own `sendfile` call, so the cross-segment copy disappears.
    ///
    /// The selected byte ranges are byte-identical to what `read_raw` would
    /// have returned for the same `(fetch_offset, limit_offset, max_size)`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(regions = tracing::field::Empty, total = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawReadDesc, LogError> {
        self.check_locally_readable(fetch_offset)?;
        if fetch_offset >= limit_offset {
            return Ok(RawReadDesc::empty(fetch_offset));
        }

        let mut regions: Vec<krabka_protocol::records::FileRegion> = Vec::new();
        let mut start_offset = fetch_offset;
        let mut current = fetch_offset;
        let mut remaining = max_size;
        let mut got_first = false;

        for seg in &self.segments {
            if seg.last_offset() < current {
                continue;
            }
            let r = seg.read_raw_desc(current, limit_offset, remaining.max(batch_header()))?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                    got_first = true;
                }
                remaining = (remaining - size_from_len(r.len())).max(ByteSize::ZERO);
                current = r.last_offset + 1;
                if let Some(region) = r.region {
                    regions.push(region);
                }
                if remaining == ByteSize::ZERO || current >= limit_offset {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || !got_first)
            && current < limit_offset
            && let Some(active) = &self.active
            && current <= active.last_offset()
        {
            let r = active.read_raw_desc(current, limit_offset, remaining.max(batch_header()))?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                }
                if let Some(region) = r.region {
                    regions.push(region);
                }
            }
        }

        let total: usize = regions.iter().map(|r| r.len).sum();
        let span = tracing::Span::current();
        span.record("regions", regions.len());
        span.record("total", total);
        Ok(RawReadDesc {
            start_offset,
            regions,
            total,
        })
    }
    }
}

#[cfg(test)]
mod tests;
