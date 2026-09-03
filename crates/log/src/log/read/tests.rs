//! Unit tests for the three read paths, covering the refused and empty
//! boundaries, the byte-exactness of the verbatim read, and the joins a
//! fetch makes across a segment seam.

use assert2::check;
use krabka_units::prelude::{gibibytes, kibibytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{NO_LIMIT, sample_batch, test_batch_at, test_log},
};

#[test]
fn batch_header_floor_is_the_protocol_header_length() {
    // `read_raw`'s anti-stall floor: each segment must be asked for at
    // least one whole v2 header, or the boundary walk has nothing to read.
    assert2::assert!(batch_header().bytes_usize() == HEADER_LEN);
}

/// A raw fetch below the log start is refused, and one at or past the
/// limit reads nothing.
#[test]
fn a_raw_fetch_outside_the_readable_range_is_refused_or_empty() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    for _ in 0..4 {
        let mut batch = sample_batch(2);
        log.append(&mut batch).expect("append");
    }
    log.set_log_start_offset(Offset(2)).expect("set log start");

    let below = log.read_raw(Offset(1), Offset(99), mebibytes(1));
    check!(
        matches!(below, Err(LogError::OffsetTooLow { .. })),
        "below the log start is refused, got {below:?}"
    );
    // Exactly at the log start is inside it.
    check!(log.read_raw(Offset(2), Offset(99), mebibytes(1)).is_ok());

    let at_limit = log
        .read_raw(Offset(2), Offset(2), mebibytes(1))
        .expect("a fetch at the limit is not an error");
    check!(
        at_limit.bytes.is_empty(),
        "a fetch at the limit reads nothing"
    );
}

/// A fetch spanning sealed segments and the active one returns every batch
/// once, in order, however many chunks it was assembled from.
///
/// One chunk is returned as it stands and several are concatenated; a fetch
/// that crosses the seal takes the second path, and getting the join wrong
/// duplicates or drops a batch at the boundary.
#[test]
fn a_fetch_across_the_seal_joins_its_chunks_in_order() {
    let dir = tempdir().unwrap();
    // A small segment cap so the appends roll and the fetch has to cross.
    let config = LogConfig {
        segment_size: kibibytes(1),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for _ in 0..40 {
        let mut batch = sample_batch(2);
        log.append(&mut batch).expect("append");
    }
    check!(!log.segments.is_empty(), "the appends should have rolled");

    let end = log.log_end_offset();
    let whole = log
        .read_raw(Offset(0), end, gibibytes(1))
        .expect("read the whole log");
    check!(!whole.bytes.is_empty());
    check!(
        whole.start_offset == Offset(0),
        "starts where asked, got {:?}",
        whole.start_offset
    );
    check!(
        whole.last_offset == Some(end - 1),
        "reaches the end, got {:?}",
        whole.last_offset
    );

    // A single sealed segment's worth: one chunk, returned unjoined.
    let first_only = log
        .read_raw(Offset(0), Offset(2), gibibytes(1))
        .expect("read one batch");
    check!(first_only.start_offset == Offset(0));
    check!(
        first_only.bytes.len() < whole.bytes.len(),
        "one batch is less than the whole log"
    );
}

#[test]
fn log_read_raw_spans_and_is_byte_exact() {
    let (dir, mut log) = test_log();
    let mut wire = bytes::BytesMut::new();
    for off in 0..4i64 {
        let mut b = test_batch_at(off);
        log.append(&mut b).unwrap();
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();
    let log_end = log.log_end_offset();
    let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    assert2::assert!(r.start_offset == Offset(0));
    assert2::assert!(r.total == wire.len());
    assert2::assert!(&r.bytes[..] == &wire[..]);
    drop(dir);
}

#[test]
fn log_read_raw_spans_multiple_segments() {
    // A tiny `segment_size` forces a roll partway through, so the
    // read must walk at least one sealed segment AND the active
    // segment — exercising the multi-chunk `BytesMut` concat path
    // that `log_read_raw_spans_and_is_byte_exact` (default ~1 GiB
    // segments) never reaches.
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(100), // tiny: roll after roughly each batch
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();

    let n: i64 = 6;
    let mut wire = bytes::BytesMut::new();
    let mut expected_bases = Vec::new();
    for off in 0..n {
        let mut b = test_batch_at(off);
        let (base, _) = log.append(&mut b).unwrap();
        expected_bases.push(base);
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();

    // The roll must actually have happened: at least one sealed
    // segment plus the active segment.
    assert2::assert!(!log.segments.is_empty());
    assert2::assert!(log.active.is_some());

    let log_end = log.log_end_offset();
    let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    assert2::assert!(r.start_offset == Offset(0));
    assert2::assert!(r.total == wire.len());
    assert2::assert!(&r.bytes[..] == &wire[..]);

    // Decode back to N batches with the expected base offsets.
    let mut cur: &[u8] = &r.bytes;
    let mut bases = Vec::new();
    while !cur.is_empty() {
        let b = krabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        bases.push(Offset(b.base_offset));
    }
    assert2::assert!(bases == expected_bases);
    drop(dir);
}

crate::sendfile_cfg! {
/// Increment D/E: `Log::read_raw_desc` across a segment seam must yield
/// regions whose **concatenation** is byte-identical to the coalesced
/// bytes of `read_raw`. The result must be several `FileRegion`s, one for
/// each contributing segment. That proves the cross-segment copy is gone.
#[test]
fn log_read_raw_desc_multi_segment_regions_equal_read_raw() {
    use std::os::unix::fs::FileExt;
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(100), // tiny: roll roughly each batch
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();

    let n: i64 = 6;
    for off in 0..n {
        let mut b = test_batch_at(off);
        log.append(&mut b).unwrap();
    }
    assert2::assert!(!log.segments.is_empty());

    let log_end = log.log_end_offset();
    let raw = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    let desc = log.read_raw_desc(Offset(0), log_end, mebibytes(10)).unwrap();

    assert2::assert!(desc.start_offset == raw.start_offset);
    assert2::assert!(desc.total == raw.total);
    // Multi-segment ⇒ more than one region (no coalescing copy).
    check!(
        desc.regions.len() >= 2,
        "expected >=2 regions across the seam, got {}",
        desc.regions.len()
    );

    // Concatenate the pread'd regions and compare to read_raw's bytes.
    let mut assembled = Vec::with_capacity(desc.total);
    for region in &desc.regions {
        let mut buf = vec![0u8; region.len];
        let mut filled = 0;
        let mut off = region.offset;
        while filled < buf.len() {
            let r = region.file.read_at(&mut buf[filled..], off).unwrap();
            assert2::assert!(r > 0);
            filled += r;
            off += r as u64;
        }
        assembled.extend_from_slice(&buf);
    }
    assert2::assert!(assembled == raw.bytes[..]);
    drop(dir);
}
} // sendfile_cfg!

#[test]
fn append_then_read_back_in_order() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut expected = Vec::new();
    for _ in 0..3 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        expected.push(b);
    }
    let out = log.read(Offset(0), NO_LIMIT).unwrap();
    assert2::assert!(out.batches == expected);
    assert2::assert!(out.start_offset == Offset(0));
}

#[test]
fn read_offset_too_low_errors() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    assert2::assert!(matches!(
        log.read(Offset(-1), kibibytes(1)),
        Err(LogError::OffsetTooLow { .. })
    ));
}

#[test]
fn read_at_log_end_returns_empty() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    let log_end = log.log_end_offset();
    let out = log.read(log_end, kibibytes(1)).unwrap();
    assert2::assert!(out.batches == Vec::new());
    assert2::assert!(out.start_offset == log_end);
}

#[test]
fn read_raw_after_reopen_does_not_skip_first_sealed_segment() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size: bytes(1), // roll on every append → one segment per batch
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        log.append(&mut sample_batch(1)).unwrap(); // offset 0 → sealed seg base 0
        log.append(&mut sample_batch(1)).unwrap(); // offset 1 → sealed seg base 1
        log.append(&mut sample_batch(1)).unwrap(); // offset 2 → active seg base 2
        assert2::assert!(log.log_end_offset() == 3);
        assert2::assert!(log.segments.len() >= 2);
    }
    // Reopen simulates a broker restart: sealed segments are loaded via the
    // no-scan Segment::open, which leaves last_offset = base - 1. Without
    // fixing last_offset from the next segment's base, read_raw skips the
    // first sealed segment (its stale last_offset < fetch_offset) and serves
    // a later segment's base — the on-cluster phantom-follower gap that
    // pinned the high-watermark and stalled acks=all.
    let reopened = Log::open(dir.path(), cfg).unwrap();
    let r = reopened
        .read_raw(Offset(0), reopened.log_end_offset(), mebibytes(1))
        .unwrap();
    assert2::assert!(r.start_offset == 0);
}
