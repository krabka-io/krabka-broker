//! Serialization of a [`MetadataImage`] into the canonical KIP-630
//! `.checkpoint` byte layout.
//!
//! The batch order, the base offsets of each batch, and the rule that
//! `V1Voters` and `V1KRaftVersion` travel as KIP-853 control records rather
//! than as KIP-631 metadata values are all decided here.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_metadata::{MetadataImage, MetadataRecord, to_kraft_values};
use krabka_protocol::{
    owned::{
        k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
        snapshot_footer_record::SnapshotFooterRecord, snapshot_header_record::SnapshotHeaderRecord,
    },
    records::{
        Record, RecordBatch,
        metadata::control::{ControlRecord, encode_typed_control_batch},
    },
};

use super::{
    SNAPSHOT_DATA_BASE_OFFSET, SNAPSHOT_HEADER_BASE_OFFSET, SNAPSHOT_KRAFT_VERSION_BASE_OFFSET,
    SNAPSHOT_VOTERS_BASE_OFFSET, SnapshotControlState, voters::voter_set_to_wire,
};
use crate::error::RaftError;

/// Identifies a snapshot by the log position it covers: `end_offset` is
/// the offset of the last record contained in the snapshot, and `epoch`
/// is the leader epoch at that offset. The engine names the on-disk artifact
/// `<end_offset>-<epoch>.checkpoint` (both fields zero-padded so lexical sort
/// matches numeric sort) and parses it back directly.
///
/// Serializes a [`MetadataImage`] into the canonical KIP-630
/// `.checkpoint` byte layout: header, `KRaftVersion`, and `Voters` control
/// batches, one data batch of `MetadataRecord` values, then a footer control
/// batch — concatenated encoded Kafka `RecordBatch`es.
pub(crate) struct SnapshotWriter;

impl SnapshotWriter {
    /// Produce the full `.checkpoint` bytes for `image`.
    /// `last_contained_log_timestamp` is the create-time of the last log
    /// record folded into this snapshot (recorded in the header).
    pub(crate) fn serialize(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, RaftError> {
        Self::serialize_with_control_state(
            image,
            last_contained_log_timestamp,
            &SnapshotControlState::from_image(image),
        )
    }

    /// Produce a checkpoint with an explicitly supplied committed KIP-853
    /// control state.
    pub(crate) fn serialize_with_control_state(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
        control_state: &SnapshotControlState,
    ) -> Result<Bytes, RaftError> {
        let records = image.to_records();
        let mut out = BytesMut::new();

        // (1) SnapshotHeader control batch at base_offset 0 — the real KIP-630
        // `SnapshotHeaderRecord` (flexible message), encoded via the protocol
        // control-batch builder so the JVM `kafka-dump-log` decoder parses it.
        let header = SnapshotHeaderRecord {
            last_contained_log_timestamp,
            ..Default::default()
        };
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_HEADER_BASE_OFFSET,
            &ControlRecord::SnapshotHeader(header),
        )?);

        // (2) KIP-853 control state. Kafka snapshots place the finalized
        // kraft.version before the voter set that it governs.
        let kraft_version = i16::try_from(control_state.kraft_version).map_err(|_| {
            RaftError::ChangeRejected("snapshot kraft.version exceeds int16".into())
        })?;
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_KRAFT_VERSION_BASE_OFFSET,
            &ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                version: 0,
                k_raft_version: kraft_version,
                ..Default::default()
            }),
        )?);
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_VOTERS_BASE_OFFSET,
            &ControlRecord::Voters(voter_set_to_wire(&control_state.voters)?),
        )?);

        // (3) Data batch at base_offset 3: one record per KIP-631 value blob.
        // Each `MetadataRecord` is translated against the very image being
        // snapshotted (a whole-map V1TopicConfig diffs against its own image and
        // so emits all-sets-no-tombstones — correct for a from-scratch snapshot).
        let mut value_blobs: Vec<Bytes> = Vec::new();
        for rec in &records {
            // `V1Voters` / `V1KRaftVersion` are encoded above as KIP-853 control
            // records, never as KIP-631 metadata values.
            if matches!(
                rec,
                MetadataRecord::V1Voters(_) | MetadataRecord::V1KRaftVersion(_)
            ) {
                continue;
            }
            let mut blobs = to_kraft_values(rec, image)
                .map_err(|e| RaftError::ChangeRejected(format!("snapshot encode: {e}")))?;
            value_blobs.append(&mut blobs);
        }
        let total_blobs = value_blobs.len();
        if !value_blobs.is_empty() {
            let last_offset_delta = total_blobs
                .checked_sub(1)
                .and_then(|delta| i32::try_from(delta).ok())
                .unwrap_or(i32::MAX);
            let data_records = value_blobs
                .into_iter()
                .enumerate()
                .map(|(i, blob)| {
                    let mut record = Record {
                        value: Some(blob),
                        ..Default::default()
                    };
                    record.offset_delta = i32::try_from(i).unwrap_or(i32::MAX);
                    record
                })
                .collect();
            let mut data_batch = RecordBatch {
                records: data_records,
                ..Default::default()
            };
            data_batch.base_offset = SNAPSHOT_DATA_BASE_OFFSET;
            data_batch.last_offset_delta = last_offset_delta;
            data_batch.encode(&mut out)?;
        }

        // (4) SnapshotFooter control batch (real KIP-630 `SnapshotFooterRecord`).
        let footer_base_offset = SNAPSHOT_DATA_BASE_OFFSET
            .saturating_add(i64::try_from(total_blobs).unwrap_or(i64::MAX));
        let footer = SnapshotFooterRecord::default();
        out.put_slice(&encode_typed_control_batch(
            footer_base_offset,
            &ControlRecord::SnapshotFooter(footer),
        )?);

        Ok(out.freeze())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{LeaderEpoch, NodeId, PartitionRecord, TopicRecord};
    use krabka_protocol::owned::voters_record::VotersRecord as WireVotersRecord;
    use uuid::Uuid;

    use super::*;

    fn decode_single_control(batch: &RecordBatch) -> ControlRecord {
        let record = batch.records.first().expect("one control record");
        ControlRecord::decode(
            record.key.as_ref().expect("control key"),
            record.value.as_ref().expect("control value"),
        )
        .expect("decode control")
    }

    #[test]
    fn writer_emits_canonical_header_data_offsets_and_footer() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 2,
            replication_factor: 1,
        }));
        for p in 0..2 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition: p,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        let timestamp = 1_700_000_000_123;
        let bytes = SnapshotWriter::serialize(&image, timestamp).unwrap();
        let mut cur: &[u8] = &bytes;

        let header = RecordBatch::decode(&mut cur).expect("header batch");
        check!(
            (
                header.base_offset,
                header.attributes.is_control_batch(),
                header.records.len()
            ) == (0, true, 1)
        );
        assert2::assert!(
            decode_single_control(&header)
                == ControlRecord::SnapshotHeader(SnapshotHeaderRecord {
                    version: 0,
                    last_contained_log_timestamp: timestamp,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                })
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch(),
                kraft_version.records.len(),
            ) == (1, true, 1)
        );
        assert2::assert!(
            decode_single_control(&kraft_version)
                == ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                    version: 0,
                    k_raft_version: 0,
                    ..Default::default()
                })
        );

        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!(
            (
                voters.base_offset,
                voters.attributes.is_control_batch(),
                voters.records.len(),
            ) == (2, true, 1)
        );
        assert2::assert!(
            decode_single_control(&voters) == ControlRecord::Voters(WireVotersRecord::default())
        );

        let data = RecordBatch::decode(&mut cur).expect("data batch");
        check!(
            (
                data.base_offset,
                data.attributes.is_control_batch(),
                data.records.len() >= 2
            ) == (3, false, true)
        );
        check!(
            data.last_offset_delta
                == i32::try_from(data.records.len() - 1).expect("record count fits")
        );
        for (i, record) in data.records.iter().enumerate() {
            assert2::assert!(record.offset_delta == i32::try_from(i).expect("index fits"));
            assert2::assert!(record.value.is_some());
        }

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            footer.base_offset == 3 + i64::try_from(data.records.len()).expect("record count fits")
        );
        check!((footer.attributes.is_control_batch(), footer.records.len()) == (true, 1));
        assert2::assert!(
            decode_single_control(&footer)
                == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default())
        );
        check!(cur.is_empty());
    }

    #[test]
    fn writer_emits_empty_snapshot_header_and_footer_offsets() {
        let image = MetadataImage::new(Uuid::new_v4());

        let bytes = SnapshotWriter::serialize(&image, 99).unwrap();
        let mut cur: &[u8] = &bytes;

        let header = RecordBatch::decode(&mut cur).expect("header batch");
        check!(
            (
                header.base_offset,
                header.attributes.is_control_batch(),
                header.records.len()
            ) == (0, true, 1)
        );
        assert2::assert!(
            decode_single_control(&header)
                == ControlRecord::SnapshotHeader(SnapshotHeaderRecord {
                    version: 0,
                    last_contained_log_timestamp: 99,
                    ..Default::default()
                })
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch()
            ) == (1, true)
        );
        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!((voters.base_offset, voters.attributes.is_control_batch()) == (2, true));

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            (
                footer.base_offset,
                footer.attributes.is_control_batch(),
                footer.records.len()
            ) == (3, true, 1)
        );
        assert2::assert!(
            decode_single_control(&footer)
                == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default())
        );
        check!(cur.is_empty());
    }
}
