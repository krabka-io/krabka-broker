//! Tests for the apply of newly committed records into the published
//! [`MetadataImage`] and for the views that apply refreshes: the leader and
//! quorum watches, and the committed slice an observer fetches.

use {
    super::*,
    crate::kraft::controller::{
        records::{decode_batches, metadata_record_batch},
        recovery::replay_committed_with_timestamp,
        test_support::{
            self, EngineSetup, await_leader, build_engine_only, one_offset_batch,
            submit_change_with_timeout, topic_record, topic_record_named,
        },
    },
    assert2::{assert, check},
    krabka_metadata::metadata_version::METADATA_VERSION_FEATURE,
};

#[test]
fn tiny_fetch_budget_does_not_skip_apply_or_replay_records() {
    let tiny = MetadataRaftFetchMax::try_from(krabka_units::bytes(1))
        .expect("one byte still makes progress");
    let (mut engine, _dir) = build_engine_only()
        .ids(&[NodeId(1)])
        .metadata_raft_fetch_max(tiny)
        .call();

    let mut scratch = engine.image.clone();
    for (name, id) in [("first", 1), ("second", 2)] {
        let records = topic_record_named(name, id);
        let mut blobs = Vec::new();
        for record in &records {
            blobs.extend(to_kraft_values(record, &scratch).expect("encode metadata"));
            scratch.apply(record);
        }
        let mut batch = metadata_record_batch(1, &blobs).expect("metadata batch");
        engine
            .log
            .append(&mut batch, 1_700_000_000_123)
            .expect("append metadata");
    }
    check!(
        engine
            .log
            .read_decoded(Offset(0), tiny.size())
            .expect("read first bounded batch")[0]
            .base_offset
            == 0
    );
    check!(
        engine
            .log
            .read_decoded(Offset(2), tiny.size())
            .expect("read second bounded batch")[0]
            .base_offset
            == 2
    );
    engine.advance_and_apply(engine.log.log_end_offset());

    assert2::assert!(engine.image.topic("first").is_some());
    assert2::assert!(engine.image.topic("second").is_some());

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    let recovered_timestamp =
        replay_committed_with_timestamp(&engine.log, &mut recovered, Offset(0), tiny)
            .expect("replay")
            .last_applied_timestamp_ms;
    assert2::assert!(recovered.topic("first").is_some());
    assert2::assert!(recovered.topic("second").is_some());
    assert2::assert!(recovered_timestamp == Some(1_700_000_000_123));
}

#[test]
fn publish_leader_updates_leader_and_quorum_watchers() {
    let (mut engine, _dir) = build_engine_only().ids(&[NodeId(1)]).call();
    let mut leader_rx = engine.leader_tx.subscribe();
    let quorum_rx = engine.quorum_tx.subscribe();

    engine.on_event(Event::ElectionTimeout);

    check!(
        (
            *leader_rx.borrow_and_update(),
            quorum_rx.borrow().leader_id,
            quorum_rx.borrow().log_end_offset,
        ) == (
            Some(NodeId(1)),
            Some(NodeId(1)),
            engine.log.log_end_offset().0,
        )
    );
}

#[test]
fn quorum_snapshot_timestamp_tracks_applied_not_newer_committed_records() {
    let (mut engine, _dir) = build_engine_only().ids(&[NodeId(1)]).call();
    let noop = super::records::noop_record_value().expect("no-op record");
    let mut batch = metadata_record_batch(1, &[noop]).expect("metadata batch");
    engine
        .log
        .append(&mut batch, 1_700_000_000_123)
        .expect("append");
    engine.advance_and_apply(Offset(1));
    let noop = super::records::noop_record_value().expect("no-op record");
    let mut later_batch = metadata_record_batch(1, &[noop]).expect("metadata batch");
    engine
        .log
        .append(&mut later_batch, 1_700_000_111_222)
        .expect("append later batch");
    // The quorum commits the later batch, but the metadata image has not
    // applied it yet.
    engine.log.advance_hwm(Offset(2));
    engine.leader_reported_hwm = 4;

    let snapshot = engine.quorum_state_snapshot();
    assert2::assert!(snapshot.last_applied_record_timestamp_ms == 1_700_000_000_123);
    assert2::assert!(engine.pending_record_processing_time_samples == 1);
}

#[test]
fn metadata_fetch_slice_excludes_negative_hwm_and_uncommitted_batches() {
    let (mut engine, _dir) = build_engine_only().ids(&[NodeId(1)]).call();
    let mut multi = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 1,
        last_offset_delta: 1,
        records: vec![
            krabka_protocol::records::Record {
                value: Some(bytes::Bytes::from_static(b"a")),
                offset_delta: 0,
                ..Default::default()
            },
            krabka_protocol::records::Record {
                value: Some(bytes::Bytes::from_static(b"b")),
                offset_delta: 1,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    engine.log.append(&mut multi, 0).expect("append multi");
    engine.log.advance_hwm(Offset(1));

    assert2::assert!(
        engine
            .metadata_fetch_slice(-1, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .records
            .is_empty()
    );
    assert2::assert!(
        engine
            .metadata_fetch_slice(1, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .records
            .is_empty()
    );

    let slice = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    let decoded = decode_batches(&slice.records).expect("decode fetch slice");
    check!(
        (
            decoded
                .iter()
                .map(|batch| batch.base_offset)
                .collect::<Vec<_>>(),
            slice.high_watermark,
        ) == (vec![0], 1)
    );
}

#[tokio::test]
async fn committed_batch_applies_to_image() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    assert2::assert!(ctrl.current_image().topic("t").is_none());

    let off = ctrl
        .test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(off >= 0);

    let mut img_rx = ctrl.watch_image();
    assert2::assert!(img_rx.borrow_and_update().topic("t").is_some());
    assert2::assert!(ctrl.current_image().topic("t").is_some());
    ctrl.shutdown().await;
}

#[tokio::test]
async fn duplicate_committed_record_rejected_on_apply() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    ctrl.test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(ctrl.current_image().topic("t").is_some());

    ctrl.test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(ctrl.current_image().topic("t").is_some());
    ctrl.shutdown().await;
}

/// One record of a batch in a finalized-features epoch scenario.
#[derive(Clone, Copy)]
enum EpochRecord {
    /// A `FeatureLevelRecord` for `name` at `level`.
    Feature(&'static str, i16),
    /// A topic and its one partition: two records.
    Topic(&'static str, u128),
}

impl EpochRecord {
    fn records(self) -> Vec<MetadataRecord> {
        match self {
            Self::Feature(name, level) => {
                vec![MetadataRecord::V1FeatureLevel(
                    krabka_metadata::FeatureLevelRecord {
                        name: name.into(),
                        level,
                    },
                )]
            }
            Self::Topic(name, id) => topic_record_named(name, id),
        }
    }
}

/// Kafka's `FeaturesPublisher` takes the finalized-features epoch from
/// `newImage.provenance().lastContainedOffset()` only when the delta has a
/// features delta. Each row appends and commits one batch per entry of
/// `batches` and expects, after each, the epoch the engine holds and the one
/// it publishes. A restart replay of the same log must reach the last one.
#[test]
fn the_finalized_features_epoch_moves_only_on_a_features_delta() {
    struct Row {
        name: &'static str,
        batches: &'static [&'static [EpochRecord]],
        want: &'static [i64],
    }
    use EpochRecord::{Feature, Topic};
    const MV: &str = METADATA_VERSION_FEATURE;
    let rows = [
        Row {
            name: "topic records before any feature record",
            batches: &[&[Topic("a", 1)]],
            // Offsets 0-1.
            want: &[-1],
        },
        Row {
            name: "topic records after a feature record",
            batches: &[&[Feature(MV, 25)], &[Topic("a", 1)], &[Topic("b", 2)]],
            // Offsets 0, 1-2, 3-4.
            want: &[0, 0, 0],
        },
        Row {
            name: "a later feature record at the same level",
            batches: &[&[Feature(MV, 25)], &[Topic("a", 1)], &[Feature(MV, 25)]],
            // Offsets 0, 1-2, 3.
            want: &[0, 0, 3],
        },
        Row {
            name: "a later feature record for another feature",
            batches: &[
                &[Feature(MV, 25)],
                &[Topic("a", 1)],
                &[Feature("group.version", 1)],
                &[Topic("b", 2)],
            ],
            // Offsets 0, 1-2, 3, 4-5.
            want: &[0, 0, 3, 3],
        },
        Row {
            name: "a batch whose feature record precedes topic records",
            batches: &[&[Feature(MV, 25)], &[Feature(MV, 25), Topic("a", 1)]],
            // Offsets 0, 1-3: the epoch is the batch's last offset.
            want: &[0, 3],
        },
    ];
    for Row {
        name,
        batches,
        want,
    } in rows
    {
        let (mut engine, _dir) = build_engine_only().ids(&[NodeId(1)]).call();
        let published = engine.features_epoch_tx.subscribe();
        let mut scratch = engine.image.clone();
        let mut held = Vec::new();
        let mut seen = Vec::new();
        for entries in batches {
            let mut blobs = Vec::new();
            for record in entries.iter().flat_map(|entry| entry.records()) {
                blobs.extend(to_kraft_values(&record, &scratch).expect("encode metadata"));
                scratch.apply(&record);
            }
            let mut batch = metadata_record_batch(1, &blobs).expect("metadata batch");
            engine
                .log
                .append(&mut batch, 1_700_000_000_000)
                .expect("append metadata");
            engine.advance_and_apply(engine.log.log_end_offset());
            held.push(engine.features_epoch);
            seen.push(*published.borrow());
        }
        let mut recovered = MetadataImage::new(uuid::Uuid::nil());
        let replayed = replay_committed_with_timestamp(
            &engine.log,
            &mut recovered,
            Offset(0),
            MetadataRaftFetchMax::default(),
        )
        .expect("replay")
        .features_epoch
        .unwrap_or(-1);

        check!(
            (held, seen, replayed) == (want.to_vec(), want.to_vec(), want[want.len() - 1]),
            "{name}"
        );
    }
}

/// The handle the controller listener reads: -1 before anything applies, the
/// committed offset of a feature record after it, unchanged by topic records,
/// and moved again by a feature record at the same level.
#[tokio::test]
async fn the_controller_handle_publishes_the_finalized_features_epoch() {
    let (controller, _dir) = test_support::build().ids(&[NodeId(1)]).call();
    let fresh = controller.finalized_features_epoch();
    controller
        .inject_event(Event::ElectionTimeout)
        .await
        .unwrap();
    await_leader(&controller, Some(NodeId(1))).await;
    let feature = || EpochRecord::Feature(METADATA_VERSION_FEATURE, 25).records();
    // Each answer is the epoch and the offset of the last committed record.
    let answer = async |records, context| {
        submit_change_with_timeout(&controller, records, context)
            .await
            .unwrap();
        (
            controller.finalized_features_epoch(),
            controller.quorum_snapshot().high_watermark - 1,
        )
    };
    let (after_feature, feature_offset) = answer(feature(), "feature").await;
    let (after_topic, topic_offset) = answer(topic_record("a"), "topic").await;
    let (after_refinalize, refinalize_offset) = answer(feature(), "same level").await;

    check!(
        (fresh, after_feature, after_topic, after_refinalize)
            == (-1, feature_offset, feature_offset, refinalize_offset)
    );
    check!(topic_offset > feature_offset);
    controller.shutdown().await;
}
