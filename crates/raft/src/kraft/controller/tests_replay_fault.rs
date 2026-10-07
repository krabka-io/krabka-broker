//! Tests for how a controller meets a committed record it cannot replay: live
//! apply and restart recovery stop on bytes that do not decode, as Kafka's
//! fatal fault handlers stop a controller, and skip a record that decodes but
//! names state the image does not hold. Control-record replay stops on an
//! invalid KIP-853 record.

use assert2::check;
use bytes::Bytes;
use krabka_metadata::{TopicConfigRecord, TopicRecord, TranslateError};
use krabka_protocol::{
    owned::{
        k_raft_version_record::KRaftVersionRecord, no_op_record::NoOpRecord,
        remove_topic_record::RemoveTopicRecord, voters_record::VotersRecord as WireVotersRecord,
    },
    primitives::uuid::Uuid as KUuid,
    records::metadata::KraftMetadataRecord,
    tagged_fields::{UnknownTaggedField, UnknownTaggedFields},
};

use super::*;
use crate::{
    error::MetadataReplayError,
    kraft::controller::{
        control_state::voter_set_from_wire,
        records::{decode_control_record, typed_control_batch},
        recovery::{replay_committed, replay_control_records},
        test_support::{build_engine_only, elect_single_voter_engine, one_offset_batch, voter_set},
    },
};

/// The value bytes of a valid `TopicRecord`: frame version, apiKey and
/// apiVersion are its first three bytes, each a one-byte varint.
fn topic_value() -> Vec<u8> {
    let record = MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 0,
        replication_factor: 1,
    });
    to_kraft_values(&record, &MetadataImage::new(uuid::Uuid::nil()))
        .expect("encode a topic")
        .remove(0)
        .to_vec()
}

/// `topic_value` with byte `index` replaced by `byte`.
fn patched_topic_value(index: usize, byte: u8) -> Vec<u8> {
    let mut value = topic_value();
    value[index] = byte;
    value
}

/// A `NoOpRecord` carrying private tag 1003 whose body is not a record.
fn undecodable_private_value() -> Vec<u8> {
    KraftMetadataRecord::NoOp(NoOpRecord {
        unknown_tagged_fields: UnknownTaggedFields(vec![UnknownTaggedField {
            tag: 1003,
            bytes: Bytes::from_static(&[0xff, 0xff, 0xff]),
        }]),
    })
    .encode_value(0)
    .expect("encode a private carrier")
    .to_vec()
}

/// A `RemoveTopicRecord` for a topic id no image holds.
fn unknown_topic_removal_value() -> Vec<u8> {
    KraftMetadataRecord::RemoveTopic(RemoveTopicRecord {
        topic_id: KUuid([7; 16]),
        ..Default::default()
    })
    .encode_value(0)
    .expect("encode a topic removal")
    .to_vec()
}

/// A config for a topic no image holds: it decodes, and fails `validate`.
fn unknown_topic_config_value() -> Vec<u8> {
    let record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "ghost".into(),
        overrides: [("retention.ms".to_string(), "1".to_string())].into(),
    });
    to_kraft_values(&record, &MetadataImage::new(uuid::Uuid::nil()))
        .expect("encode a topic config")
        .remove(0)
        .to_vec()
}

/// What replay must do with one committed value.
enum Want {
    /// Stop the controller: the value does not decode.
    Stop,
    /// Skip the value and keep going.
    Skip,
    /// Whatever the pinned `krabka-metadata` decoder says: stop where it
    /// refuses the bytes, apply where it reads them.
    AsTheDecoderSays,
}

/// The fault replay reports for `value` at `offset`, or `None` when it reads
/// the value.
fn fault_for(value: &[u8], offset: i64) -> Option<MetadataReplayError> {
    from_kraft_value(value, &MetadataImage::new(uuid::Uuid::nil()))
        .err()
        .map(|error| MetadataReplayError::UndecodableRecord { offset, error })
}

/// One row per failure kind. A controller stops on bytes that do not decode,
/// in live apply and in restart recovery, and names the record's offset and
/// the decoder's error. It skips a record that decodes but that the image
/// cannot take.
#[test]
fn a_controller_stops_on_a_committed_record_it_cannot_decode() {
    let cases: [(&str, Vec<u8>, Want); 6] = [
        ("unknown apiKey", patched_topic_value(1, 99), Want::Stop),
        (
            "value version above the highest supported",
            patched_topic_value(2, 99),
            Want::Stop,
        ),
        // `krabka-metadata` checks the KIP-631 frame version from the
        // revision that versions the private records on. Before it, the
        // decoder reads the bytes and replay applies them.
        (
            "frame version other than 1",
            patched_topic_value(0, 2),
            Want::AsTheDecoderSays,
        ),
        (
            "undecodable krabka-private record",
            undecodable_private_value(),
            Want::Stop,
        ),
        (
            "unknown topic id",
            unknown_topic_removal_value(),
            Want::Skip,
        ),
        ("validate failure", unknown_topic_config_value(), Want::Skip),
    ];

    for (case, value, want) in cases {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut engine);
        let offset = engine.log.log_end_offset().0;
        let epoch = i32::try_from(engine.core.quorum_state().leader_epoch).expect("epoch");
        engine
            .log
            .append(&mut one_offset_batch(offset, epoch, &value), 0)
            .expect("append the record");
        let image_before = engine.image.clone();

        engine.advance_and_apply(engine.log.log_end_offset());

        let want_fault = match want {
            Want::Stop => {
                let fault = fault_for(&value, offset);
                check!(fault.is_some(), "{case}: the decoder must refuse the bytes");
                fault
            }
            Want::Skip => None,
            Want::AsTheDecoderSays => fault_for(&value, offset),
        };
        check!(engine.replay_fault == want_fault, "{case}: live apply");
        let published = engine
            .publish_fault()
            .then(|| engine.fault_tx.borrow().clone());
        check!(
            published == want_fault.as_ref().map(|fault| Some(fault.to_string())),
            "{case}: the published fault"
        );
        if matches!(want, Want::Skip) {
            check!(engine.image == image_before, "{case}: nothing applied");
        }

        let mut recovered = MetadataImage::new(uuid::Uuid::nil());
        let replayed = replay_committed(
            &engine.log,
            &mut recovered,
            Offset(0),
            MetadataRaftFetchMax::default(),
        );
        let recovery_fault = match replayed {
            Ok(_) => None,
            Err(RaftError::MetadataReplay(fault)) => Some(fault),
            Err(other) => panic!("{case}: recovery failed otherwise: {other}"),
        };
        check!(recovery_fault == want_fault, "{case}: restart recovery");
    }
}

/// A controller that has stopped on a record applies nothing after it.
#[test]
fn a_stopped_controller_applies_no_later_record() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let epoch = i32::try_from(engine.core.quorum_state().leader_epoch).expect("epoch");
    let bad_offset = engine.log.log_end_offset().0;
    engine
        .log
        .append(
            &mut one_offset_batch(bad_offset, epoch, &patched_topic_value(1, 99)),
            epoch.into(),
        )
        .expect("append the undecodable record");
    engine.advance_and_apply(engine.log.log_end_offset());
    let after_fault = engine.image.clone();

    let next = engine.log.log_end_offset().0;
    engine
        .log
        .append(&mut one_offset_batch(next, epoch, &topic_value()), 0)
        .expect("append a valid record");
    engine.advance_and_apply(engine.log.log_end_offset());

    check!(engine.replay_fault == fault_for(&patched_topic_value(1, 99), bad_offset));
    check!(engine.image == after_fault);
    check!(engine.image.topic("t").is_none());
}

/// One row per invalid KIP-853 control record: restart replay of the control
/// records stops on each, naming its offset, as Kafka's
/// `KRaftControlRecordStateMachine` throws and the raft driver's fatal fault
/// handler halts the process.
#[test]
fn control_replay_stops_on_an_invalid_control_record() {
    let negative_version = typed_control_batch(
        1,
        &[ControlRecord::KRaftVersion(KRaftVersionRecord {
            version: 0,
            k_raft_version: -1,
            ..Default::default()
        })],
    )
    .expect("negative kraft.version batch");

    let mut undecodable = negative_version.clone();
    undecodable.records[0].value = Some(Bytes::from_static(&[0xff]));
    let undecodable_reason = decode_control_record(&undecodable.records[0])
        .expect_err("a control value that does not decode")
        .to_string();

    let empty_voters = WireVotersRecord {
        version: 0,
        voters: Vec::new(),
        ..Default::default()
    };
    let empty_voters_reason = voter_set_from_wire(&empty_voters)
        .expect_err("an empty voter set")
        .to_string();
    let empty_voters =
        typed_control_batch(1, &[ControlRecord::Voters(empty_voters)]).expect("voters batch");

    let cases = [
        (
            "negative kraft.version",
            negative_version,
            "negative kraft.version -1".to_owned(),
        ),
        (
            "control record that does not decode",
            undecodable,
            undecodable_reason,
        ),
        (
            "voter set that does not convert",
            empty_voters,
            empty_voters_reason,
        ),
    ];

    for (case, mut batch, reason) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut log =
            KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("open log");
        log.append(&mut batch, 0).expect("append the control batch");
        log.advance_hwm(log.log_end_offset());
        let mut state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(&[NodeId(1)]));

        let replayed = replay_control_records(&log, &mut state, MetadataRaftFetchMax::default());

        let Err(RaftError::MetadataReplay(fault)) = replayed else {
            panic!("{case}: expected a replay fault, got {replayed:?}");
        };
        check!(
            fault == MetadataReplayError::InvalidControlRecord { offset: 0, reason },
            "{case}"
        );
    }
}

/// The krabka-private and decoder errors that are not image lookups stop
/// replay; the image lookups are skips.
#[test]
fn only_image_lookups_are_skipped() {
    let image = MetadataImage::new(uuid::Uuid::nil());
    let cases: [(&str, Vec<u8>, bool); 3] = [
        ("unknown apiKey", patched_topic_value(1, 99), true),
        ("unknown topic id", unknown_topic_removal_value(), false),
        ("empty KIP-835 no-op", noop_value(), false),
    ];
    for (case, value, stops) in cases {
        let decoded = records::decode_committed_value(&value, &image, 4);
        check!(
            matches!(
                decoded,
                Err(MetadataReplayError::UndecodableRecord {
                    offset: 4,
                    error: TranslateError::NoCounterpart(_)
                })
            ) == stops,
            "{case}"
        );
        check!(
            decoded.is_ok_and(|record| record.is_none()) == !stops,
            "{case}"
        );
    }
}

/// The value bytes of the empty KIP-835 no-op.
fn noop_value() -> Vec<u8> {
    records::noop_record_value().expect("no-op").to_vec()
}
