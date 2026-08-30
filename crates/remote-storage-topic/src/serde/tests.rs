//! Round-trip tests for the [`MetadataEvent`] codec: every event variant
//! survives an encode/decode cycle unchanged, and malformed or out-of-scope
//! input is rejected rather than accepted or panicked on.
//!
//! [`MetadataEvent`]: super::MetadataEvent

use assert2::assert;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentState, RemotePartitionDeleteState,
    TopicIdPartition,
};
use uuid::Uuid;

use super::*;

fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(0xCAFE_BABE), "orders-📦", 7)
}

fn seg_id(id: u128) -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(tp(), Uuid::from_u128(id))
}

fn add(id: u128, start: i64, end: i64, custom: Option<Vec<u8>>) -> RemoteLogSegmentMetadata {
    let mut md = RemoteLogSegmentMetadata::new(
        seg_id(id),
        start,
        end,
        end + 1,
        42,
        123,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            4096,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {
            LeaderEpoch(0) => start,
            LeaderEpoch(1) => start + 10,
            LeaderEpoch(2) => start + 20},
        ),
    )
    .unwrap();
    if let Some(c) = custom {
        md = md.with_custom_metadata(CustomMetadata(c));
    }
    md
}

#[test]
fn round_trip_add_with_custom_metadata() {
    let event = MetadataEvent::AddSegment(add(1, 0, 99, Some(vec![1, 2, 3, 4])));
    let bytes = event.encode();
    let back = MetadataEvent::decode(&bytes).expect("decodes");
    assert!(back == event);
}

#[test]
fn round_trip_add_without_custom_metadata() {
    let event = MetadataEvent::AddSegment(add(2, 100, 199, None));
    let bytes = event.encode();
    assert!(MetadataEvent::decode(&bytes).unwrap() == event);
}

#[test]
fn round_trip_update_finish() {
    let event = MetadataEvent::UpdateSegment(RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: seg_id(3),
        event_timestamp_ms: 999,
        custom_metadata: Some(CustomMetadata(vec![9, 8, 7])),
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 13,
    });
    let bytes = event.encode();
    assert!(MetadataEvent::decode(&bytes).unwrap() == event);
}

#[test]
fn round_trip_update_no_custom_metadata() {
    let event = MetadataEvent::UpdateSegment(RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: seg_id(4),
        event_timestamp_ms: 1,
        custom_metadata: None,
        state: RemoteLogSegmentState::DeleteSegmentStarted,
        broker_id: 0,
    });
    let bytes = event.encode();
    assert!(MetadataEvent::decode(&bytes).unwrap() == event);
}

#[test]
fn round_trip_partition_delete_each_state() {
    for state in [
        RemotePartitionDeleteState::DeletePartitionMarked,
        RemotePartitionDeleteState::DeletePartitionStarted,
        RemotePartitionDeleteState::DeletePartitionFinished,
    ] {
        let event = MetadataEvent::PartitionDelete(RemotePartitionDeleteMetadata {
            topic_id_partition: tp(),
            state,
            event_timestamp_ms: 500,
            broker_id: 1,
        });
        let bytes = event.encode();
        assert!(MetadataEvent::decode(&bytes).unwrap() == event);
    }
}

#[test]
fn add_round_trips_txn_index_empty_true() {
    let md = add(5, 0, 49, None).with_txn_index_empty(true);
    let event = MetadataEvent::AddSegment(md);
    let bytes = event.encode();
    let back = MetadataEvent::decode(&bytes).expect("decodes");
    assert!(back == event);
    if let MetadataEvent::AddSegment(ref md) = back {
        assert!(md.txn_index_empty());
    }
}

#[test]
fn truncated_buffer_is_rejected() {
    let bytes = MetadataEvent::AddSegment(add(1, 0, 1, None))
        .encode()
        .to_vec();
    let err = MetadataEvent::decode(&bytes[..bytes.len() - 5]).unwrap_err();
    assert!(matches!(err, CodecError::Protocol(_)));
}

#[test]
fn unknown_segment_state_is_rejected() {
    // Build a SegmentMetadata record with an out-of-range state byte (7),
    // encode it through the protocol envelope, then assert decode → Err.
    use krabka_protocol::owned::remote_log_segment_metadata_record::{
        RemoteLogSegmentMetadataRecord, SegmentLeaderEpochEntry,
    };
    // Provide a minimal valid epoch list so domain construction doesn't fail first.
    let rec = RemoteLogSegmentMetadataRecord {
        segment_leader_epochs: vec![SegmentLeaderEpochEntry {
            leader_epoch: 0,
            offset: 0,
            ..Default::default()
        }],
        remote_log_segment_state: 7,
        ..Default::default()
    };
    let bytes = krabka_protocol::RemoteLogMetadataRecord::SegmentMetadata(rec)
        .encode_value()
        .unwrap();
    let err = MetadataEvent::decode(&bytes).unwrap_err();
    assert!(matches!(err, CodecError::UnknownState(7, _)));
}

#[test]
fn snapshot_apikey_is_rejected_on_topic() {
    use krabka_protocol::owned::remote_log_segment_metadata_snapshot_record::RemoteLogSegmentMetadataSnapshotRecord;
    let bytes = krabka_protocol::RemoteLogMetadataRecord::SegmentMetadataSnapshot(
        RemoteLogSegmentMetadataSnapshotRecord::default(),
    )
    .encode_value()
    .unwrap();
    let err = MetadataEvent::decode(&bytes).unwrap_err();
    assert!(matches!(err, CodecError::Protocol(_)));
}
