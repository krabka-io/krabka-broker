//! Fixtures shared by the unit tests of the fetch response writer and of its
//! child modules: the sample responses, the raw record batches behind them,
//! and the temp-file records payload the sendfile tests need.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    records::{Record, RecordBatch, RecordsPayload},
};

use super::WriteOp;

crate::sendfile_cfg! {
    use std::{io::Write as _, sync::Arc};

    use krabka_protocol::records::FileRegion;
}

pub(super) const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

pub(super) fn raw_batch(base: i64) -> Bytes {
    let rb = RecordBatch {
        base_offset: base,
        records: vec![Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"value-payload")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let mut buf = BytesMut::new();
    rb.encode(&mut buf).unwrap();
    buf.freeze()
}

/// Extract the bytes of an `Inline` op. It panics on a `File` op. This
/// avoids a `match`/`let-else` that is infallible on Windows, with one
/// variant, but refutable on SENDFILE-alias platforms, with two variants.
/// Clippy stays happy on both.
pub(super) fn inline_bytes(op: &WriteOp) -> &Bytes {
    match op {
        WriteOp::Inline(b) => b,
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        WriteOp::File(_) => panic!("expected an inline op"),
    }
}

pub(super) fn sample_response(version: i16) -> FetchResponse {
    let p0 = PartitionData {
        partition_index: 0,
        high_watermark: 1,
        last_stable_offset: 1,
        log_start_offset: 0,
        records: Some(RecordsPayload::Raw(raw_batch(0))),
        ..PartitionData::default()
    };
    let p1 = PartitionData {
        partition_index: 1,
        high_watermark: 2,
        last_stable_offset: 2,
        log_start_offset: 0,
        records: Some(RecordsPayload::Raw(raw_batch(1))),
        ..PartitionData::default()
    };
    FetchResponse {
        throttle_time_ms: 0,
        session_id: 7,
        responses: vec![FetchableTopicResponse {
            topic: if version <= 12 {
                "t".to_string()
            } else {
                String::new()
            },
            topic_id: if version >= 13 {
                krabka_protocol::primitives::uuid::Uuid([5u8; 16])
            } else {
                krabka_protocol::primitives::uuid::Uuid([0u8; 16])
            },
            partitions: vec![p0, p1],
            ..FetchableTopicResponse::default()
        }],
        ..FetchResponse::default()
    }
}

crate::sendfile_cfg! {
    /// Write `bytes` to a temp file and return a single-region
    /// `RecordsPayload::FileRegions` that covers the whole file from
    /// offset 0.
    pub(super) fn file_payload(bytes: &[u8]) -> (tempfile::NamedTempFile, RecordsPayload) {
        let mut tf = tempfile::NamedTempFile::new().unwrap();
        tf.write_all(bytes).unwrap();
        tf.flush().unwrap();
        let file = Arc::new(tf.reopen().unwrap());
        let payload = RecordsPayload::FileRegions(vec![FileRegion {
            file,
            offset: 0,
            len: bytes.len(),
        }]);
        (tf, payload)
    }
}
