//! Serializes a segment's leader-epoch map into the Kafka
//! `leader-epoch-checkpoint` text format that the copy path uploads.

use std::collections::BTreeMap;

use bytes::Bytes;

/// Serialize a segment's leader-epoch map into Kafka's
/// `leader-epoch-checkpoint` text format (the bytes carried as
/// `LogSegmentData.leader_epoch_index`).
pub(super) fn leader_epoch_index_bytes(epochs: &BTreeMap<krabka_ids::LeaderEpoch, i64>) -> Bytes {
    use std::fmt::Write as _;
    let mut s = String::from("0\n");
    let _ = writeln!(s, "{}", epochs.len());
    for (epoch, start) in epochs {
        // On-disk `leader-epoch-checkpoint` text format: unwrap to the raw
        // `i32` so the serialized bytes stay byte-identical.
        let _ = writeln!(s, "{} {start}", epoch.0);
    }
    Bytes::from(s.into_bytes())
}
