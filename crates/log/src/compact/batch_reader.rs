//! Streaming decode of every `RecordBatch` in a sealed segment's `.log` file.
//! The offset-map pass and the rewrite pass both need it, so it sits beside
//! them instead of inside either one.

use krabka_protocol::records::RecordBatch;

use crate::{error::LogError, name, segment::Segment};

/// Read every `RecordBatch` from a sealed segment. This function streams the
/// whole `.log` file directly.
///
/// It avoids `Segment::read`, because that path returns early when the
/// segment's in-memory `last_offset` is stale. A sealed segment loaded from
/// disk through `Segment::open` has `last_offset = base_offset - 1` until a
/// tail scan fills it in, so `Segment::read(base_offset, ..)` would
/// short-circuit to an empty result.
pub(super) fn read_all_batches(seg: &Segment) -> Result<Vec<RecordBatch>, LogError> {
    let path = name::log_path(seg.dir(), seg.base_offset().0);
    let bytes = std::fs::read(&path)?;
    let mut cursor: &[u8] = &bytes;
    let mut out: Vec<RecordBatch> = Vec::new();
    while !cursor.is_empty() {
        let Ok(batch) = RecordBatch::decode(&mut cursor) else {
            break;
        };
        out.push(batch);
    }
    Ok(out)
}
