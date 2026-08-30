//! Tests for `submit_change`: the offsets a submission is assigned, the
//! waiters it parks, and how those waiters resolve, fail on leadership loss,
//! or take a per-record rejection scoped to their own appended range.

use std::time::Duration as StdDuration;

use assert2::{assert, check};

use super::*;
use crate::kraft::{
    controller::test_support::{
        await_leader, build, build_engine_only, elect_leader_with_helper,
        elect_single_voter_engine, one_offset_batch, submit_change_with_timeout, topic_record,
        topic_record_named,
    },
    transport::NullPeerSender,
};

#[test]
fn direct_single_voter_submit_applies_image_and_resolves_waiter() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    assert2::assert!(engine.image.topic("direct").is_none());

    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&topic_record("direct"), reply);

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    check!(engine.image.topic("direct").is_some());
    check!(engine.log.hwm() == engine.log.log_end_offset());
    check!(engine.commit_waiters.is_empty());
}

#[test]
fn offset_advance_submit_returns_actor_ordered_base() {
    use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&topic_record("topic"), reply);
    assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let advance = |count| {
        vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count,
            },
        )]
    };

    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&advance(3), reply);
    let first = rx.try_recv().expect("first reply").expect("first ok");
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&advance(5), reply);
    let second = rx.try_recv().expect("second reply").expect("second ok");

    assert!(first.offset_reservations[0].base_offset == 0);
    assert!(first.offset_reservations[0].count == 3);
    assert!(second.offset_reservations[0].base_offset == 3);
    assert!(second.offset_reservations[0].count == 5);
    assert!(engine.image.partition_next_offset("topic", 0) == Some(8));
}

#[test]
fn offset_advance_submit_rejects_counts_outside_verified_domain() {
    use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);

    let advance = |count| {
        vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count,
            },
        )]
    };

    for records in [topic_record("topic"), advance(1)] {
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&records, reply);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    let log_end = engine.log.log_end_offset();

    for count in [-1, i64::MAX] {
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&advance(count), reply);

        assert!(matches!(
            rx.try_recv(),
            Ok(Err(RaftError::ChangeRejected(_)))
        ));
        assert!(engine.image.partition_next_offset("topic", 0) == Some(1));
        assert!(engine.log.log_end_offset() == log_end);
    }
}

#[test]
fn try_resolve_waiters_resolves_at_exact_hwm_and_keeps_future_waiter() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    for offset in 0..5 {
        let mut batch = one_offset_batch(offset, 1, b"x");
        engine.log.append(&mut batch).expect("append");
    }
    engine.log.advance_hwm(Offset(5));

    let (ready_tx, mut ready_rx) = oneshot::channel();
    let (future_tx, mut future_rx) = oneshot::channel();
    engine.commit_waiters.push(CommitWaiter {
        base_offset: Offset(4),
        need_offset: Offset(5),
        rejection: None,
        result: SubmitChangeResult::default(),
        reply: ready_tx,
    });
    engine.commit_waiters.push(CommitWaiter {
        base_offset: Offset(5),
        need_offset: Offset(6),
        rejection: None,
        result: SubmitChangeResult::default(),
        reply: future_tx,
    });

    engine.try_resolve_waiters();

    assert!(matches!(ready_rx.try_recv(), Ok(Ok(_))));
    assert!(matches!(
        future_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert2::assert!(
        engine
            .commit_waiters
            .iter()
            .map(|waiter| waiter.need_offset)
            .collect::<Vec<_>>()
            == vec![Offset(6)]
    );
}

#[test]
fn fail_waiters_reached_by_fails_only_waiters_at_or_below_target_hwm() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let (future_tx, mut future_rx) = oneshot::channel();
    engine.commit_waiters.push(CommitWaiter {
        base_offset: Offset(4),
        need_offset: Offset(5),
        rejection: None,
        result: SubmitChangeResult::default(),
        reply: ready_tx,
    });
    engine.commit_waiters.push(CommitWaiter {
        base_offset: Offset(5),
        need_offset: Offset(6),
        rejection: None,
        result: SubmitChangeResult::default(),
        reply: future_tx,
    });

    engine.fail_waiters_reached_by(Offset(5), "test hwm stall");

    assert2::assert!(matches!(
        ready_rx.try_recv(),
        Ok(Err(RaftError::ChangeRejected(_)))
    ));
    assert2::assert!(matches!(
        future_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert2::assert!(
        engine
            .commit_waiters
            .iter()
            .map(|waiter| waiter.need_offset)
            .collect::<Vec<_>>()
            == vec![Offset(6)]
    );
}

#[tokio::test]
async fn submit_change_commits_on_single_voter_leader() {
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    tokio::time::timeout(
        StdDuration::from_secs(5),
        ctrl.submit_change(topic_record("orders")),
    )
    .await
    .expect("submit did not hang")
    .expect("submit ok");
    assert2::assert!(ctrl.current_image().topic("orders").is_some());

    let qs = ctrl.quorum_state().await.unwrap();
    assert2::assert!(qs.leader_id == Some(NodeId(1)));
    assert2::assert!(qs.high_watermark > 0);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn submit_change_duplicate_rejected() {
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    submit_change_with_timeout(&ctrl, topic_record("t"), "first duplicate-test submit")
        .await
        .unwrap();
    let dup = submit_change_with_timeout(&ctrl, topic_record("t"), "duplicate-test submit").await;
    assert2::assert!(matches!(dup, Err(RaftError::Metadata(_))));
    ctrl.shutdown().await;
}

/// FIX 1: a leader that parks a `submit_change` waiter and then steps down
/// (higher-epoch `BeginQuorumEpoch` forces Leader → Follower) must fail the
/// parked waiter promptly with `NotLeader` rather than leaving it hung until
/// engine shutdown. In a 3-voter cluster with a `NullPeerSender`, no follower
/// ever fetches, so the appended record never commits — the only way the
/// waiter resolves is the leadership-loss drain.
#[tokio::test]
async fn submit_waiter_fails_on_leadership_loss() {
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

    // Park a submit on a separate task: it appends but cannot commit (no
    // peer fetches under NullPeerSender), so it stays parked.
    let ctrl2 = ctrl.clone();
    let submit = tokio::spawn(async move { ctrl2.submit_change(topic_record("orders")).await });

    // Give the submit a moment to reach the engine and park its waiter.
    tokio::time::sleep(StdDuration::from_millis(50)).await;

    // A strictly-higher-epoch BeginQuorumEpoch from node 2 forces node 1 to
    // step down from Leader to Follower.
    ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 9,
    })
    .await
    .unwrap();

    // The parked submit must resolve promptly (bounded) with NotLeader.
    let result = tokio::time::timeout(StdDuration::from_secs(5), submit)
        .await
        .expect("submit did not hang on leadership loss")
        .expect("join");
    assert2::assert!(matches!(
        result,
        Err(RaftError::NotLeader {
            current_leader: Some(NodeId(2))
        })
    ));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn submit_change_on_non_leader_rejects() {
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    // Never elected; node 1 is Unattached → not leader.
    let r = ctrl.submit_change(topic_record("t")).await;
    assert2::assert!(matches!(r, Err(RaftError::NotLeader { .. })));
    ctrl.shutdown().await;
}

/// FIX 2: a committed record that fails apply-`validate` must only fail the
/// waiter whose appended range actually contains it, not every later waiter.
/// Park three submits in a 3-voter leader (no peer fetches → nothing commits
/// on its own): A creates "first" (valid), B re-creates "first" (duplicate →
/// rejected at apply), C creates "third" (valid). Then drive a single HWM
/// advance past all three via a follower fetch. B must get `Err`; C must get
/// `Ok` (not bled the rejection from B's earlier offset).
#[tokio::test]
async fn rejection_scoped_to_owning_waiter_range() {
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

    let ca = ctrl.clone();
    let cb = ctrl.clone();
    let cc = ctrl.clone();
    // A and B both create topic "first"; B is the duplicate that fails apply.
    // C creates a distinct "third" and must commit cleanly.
    let a = tokio::spawn(async move { ca.submit_change(topic_record_named("first", 1)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let b = tokio::spawn(async move { cb.submit_change(topic_record_named("first", 1)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let c = tokio::spawn(async move { cc.submit_change(topic_record_named("third", 3)).await });
    tokio::time::sleep(StdDuration::from_millis(40)).await;

    // Drive the HWM past all appended batches by simulating a follower (node
    // 2) that has fetched the whole log. With a 3-voter majority of 2, the
    // leader's own log end plus node 2's fetch offset commits everything.
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();

    let ra = tokio::time::timeout(StdDuration::from_secs(5), a)
        .await
        .expect("A did not hang")
        .expect("join");
    let rb = tokio::time::timeout(StdDuration::from_secs(5), b)
        .await
        .expect("B did not hang")
        .expect("join");
    let rc = tokio::time::timeout(StdDuration::from_secs(5), c)
        .await
        .expect("C did not hang")
        .expect("join");

    check!(ra.is_ok(), "A (first valid) should commit: {ra:?}");
    assert2::assert!(matches!(rb, Err(RaftError::Metadata(_))));
    check!(
        rc.is_ok(),
        "C (distinct valid) must NOT bleed B's rejection: {rc:?}"
    );
    ctrl.shutdown().await;
}
