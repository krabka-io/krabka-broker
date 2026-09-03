//! Tests for the apply of newly committed records into the published
//! [`MetadataImage`] and for the views that apply refreshes: the leader and
//! quorum watches, and the committed slice an observer fetches.

use assert2::{assert, check};

use super::*;
use crate::kraft::controller::{
    records::{decode_batches, metadata_record_batch},
    recovery::replay_committed,
    test_support::{
        await_leader, build, build_engine_only, build_engine_only_with_policy, one_offset_batch,
        topic_record, topic_record_named,
    },
};

#[test]
fn tiny_fetch_budget_does_not_skip_apply_or_replay_records() {
    let tiny = MetadataRaftFetchMax::try_from(krabka_units::bytes(1))
        .expect("one byte still makes progress");
    let (mut engine, _dir) = build_engine_only_with_policy(
        NodeId(1),
        &[NodeId(1)],
        ControllerFetchMissLimit::default(),
        tiny,
    );

    let mut scratch = engine.image.clone();
    for (name, id) in [("first", 1), ("second", 2)] {
        let records = topic_record_named(name, id);
        let mut blobs = Vec::new();
        for record in &records {
            blobs.extend(to_kraft_values(record, &scratch).expect("encode metadata"));
            scratch.apply(record);
        }
        let mut batch = metadata_record_batch(1, &blobs).expect("metadata batch");
        engine.log.append(&mut batch, 0).expect("append metadata");
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
    replay_committed(&engine.log, &mut recovered, Offset(0), tiny).expect("replay");
    assert2::assert!(recovered.topic("first").is_some());
    assert2::assert!(recovered.topic("second").is_some());
}

#[test]
fn publish_leader_updates_leader_and_quorum_watchers() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
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
fn metadata_fetch_slice_excludes_negative_hwm_and_uncommitted_batches() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    let mut first = one_offset_batch(0, 1, b"a");
    let mut second = one_offset_batch(1, 1, b"b");
    engine.log.append(&mut first, 0).expect("append first");
    engine.log.append(&mut second, 0).expect("append second");
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
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

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
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

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
