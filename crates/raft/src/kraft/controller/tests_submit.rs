//! Tests for `submit_change`: the offsets a submission is assigned, the
//! waiters it parks, and how those waiters resolve, fail on leadership loss,
//! or take a per-record rejection scoped to their own appended range.

use std::time::Duration as StdDuration;

use assert2::{assert, check};
use krabka_ids::Offset;
use tokio::sync::oneshot;

use super::CommitWaiter;
use crate::{
    SubmitChangeResult,
    error::RaftError,
    kraft::{
        controller::test_support::{
            await_leader, build, build_engine_only, elect_leader_with_helper,
            elect_single_voter_engine, one_offset_batch, submit_change_with_timeout, topic_record,
            topic_record_named,
        },
        event::Event,
        types::NodeId,
    },
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

    for count in [-1, 0, i64::MAX] {
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

#[tokio::test]
async fn pending_offset_reservations_are_contiguous_before_commit() {
    use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

    let create_ctrl = ctrl.clone();
    let create =
        tokio::spawn(async move { create_ctrl.submit_change(topic_record("topic")).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    create.await.unwrap().unwrap();

    let advance = |count| {
        vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count,
            },
        )]
    };
    let first_ctrl = ctrl.clone();
    let first = tokio::spawn(async move { first_ctrl.submit_change(advance(3)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let second_ctrl = ctrl.clone();
    let second = tokio::spawn(async move { second_ctrl.submit_change(advance(5)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(first.offset_reservations[0].base_offset == 0);
    assert!(first.offset_reservations[0].count == 3);
    assert!(first.offset_reservations[0].leader_epoch == u64::from(qs.leader_epoch));
    assert!(second.offset_reservations[0].base_offset == 3);
    assert!(second.offset_reservations[0].count == 5);
    assert!(second.offset_reservations[0].leader_epoch == u64::from(qs.leader_epoch));
    assert!(ctrl.current_image().partition_next_offset("topic", 0) == Some(8));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn break_glass_consume_is_exact_and_single_flight_until_commit() {
    use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataRecord};
    use uuid::Uuid;

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    let proposal = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(0x271),
        action: BreakGlassAction::DeleteRecords,
        target: "orders-3".to_owned(),
        proposer: "User:alice".to_owned(),
        reason: "incident".to_owned(),
        created_at_ms: 1,
        expires_at_ms: 1_000,
        approvals: Vec::new(),
        consumed_at_ms: 0,
        withdrawn: false,
    };

    let create_ctrl = ctrl.clone();
    let proposed = proposal.clone();
    let create = tokio::spawn(async move {
        create_ctrl
            .submit_change(vec![MetadataRecord::V1BreakGlassProposal(proposed)])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    create.await.unwrap().unwrap();

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    for malformed in [
        BreakGlassProposalRecord {
            consumed_at_ms: -1,
            ..proposal.clone()
        },
        BreakGlassProposalRecord {
            target: "orders-4".to_owned(),
            consumed_at_ms: 10,
            ..proposal.clone()
        },
    ] {
        let result = ctrl
            .submit_change(vec![MetadataRecord::V1BreakGlassProposal(malformed)])
            .await;
        assert2::assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
        assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);
    }

    let consumed = BreakGlassProposalRecord {
        consumed_at_ms: i64::MAX,
        ..proposal.clone()
    };
    let first_ctrl = ctrl.clone();
    let first_record = consumed.clone();
    let first = tokio::spawn(async move {
        first_ctrl
            .submit_change(vec![MetadataRecord::V1BreakGlassProposal(first_record)])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let concurrent = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_change(vec![MetadataRecord::V1BreakGlassProposal(consumed.clone())]),
    )
    .await
    .expect("a concurrent consume must be rejected before append");
    assert2::assert!(matches!(concurrent, Err(RaftError::ChangeRejected(_))));

    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    first.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .break_glass_proposal(proposal.proposal_id)
            .is_some_and(|stored| stored.consumed_at_ms == i64::MAX)
    );

    let retry = ctrl
        .submit_change(vec![MetadataRecord::V1BreakGlassProposal(consumed)])
        .await;
    assert2::assert!(matches!(retry, Err(RaftError::ChangeRejected(_))));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn topic_freeze_replacement_is_newer_only_and_single_flight_until_commit() {
    use krabka_metadata::{MetadataRecord, PatternType, TopicFreezeRecord};
    use uuid::Uuid;

    fn freeze(scope: &str, set_at_ms: i64, frozen: bool) -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type: PatternType::Literal,
            frozen,
            reason: "incident".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }
    }

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    let epoch = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: epoch.leader_epoch,
        fetch_offset: epoch.log_end_offset,
    })
    .await
    .unwrap();

    let create_ctrl = ctrl.clone();
    let create = tokio::spawn(async move {
        create_ctrl
            .submit_change(vec![MetadataRecord::V1TopicFreeze(freeze(
                "orders", 10, true,
            ))])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    create.await.unwrap().unwrap();

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    for rejected in [
        freeze("orders", 10, true),
        freeze("orders", 9, true),
        freeze("missing", 11, false),
    ] {
        let result = ctrl
            .submit_change(vec![MetadataRecord::V1TopicFreeze(rejected)])
            .await;
        assert2::assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
        assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);
    }
    let batch = ctrl
        .submit_change(vec![
            MetadataRecord::V1TopicFreeze(freeze("a", 11, true)),
            MetadataRecord::V1TopicFreeze(freeze("b", 11, true)),
        ])
        .await;
    assert2::assert!(matches!(batch, Err(RaftError::ChangeRejected(_))));
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let replacement = freeze("orders", i64::MAX, true);
    let replace_ctrl = ctrl.clone();
    let first = replacement.clone();
    let replace = tokio::spawn(async move {
        replace_ctrl
            .submit_change(vec![MetadataRecord::V1TopicFreeze(first)])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let concurrent = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_change(vec![MetadataRecord::V1TopicFreeze(replacement.clone())]),
    )
    .await
    .expect("a concurrent replacement must be rejected before append");
    assert2::assert!(matches!(concurrent, Err(RaftError::ChangeRejected(_))));

    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    replace.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .topic_freeze("orders")
            .is_some_and(|stored| stored.set_at_ms == i64::MAX)
    );

    let retry = ctrl
        .submit_change(vec![MetadataRecord::V1TopicFreeze(replacement)])
        .await;
    assert2::assert!(matches!(retry, Err(RaftError::ChangeRejected(_))));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn delegation_token_mutation_is_generation_bound_and_retry_idempotent() {
    use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
    use krabka_security::KafkaPrincipal;

    fn principal(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: name.to_string(),
        }
    }

    async fn commit_pending(ctrl: &crate::kraft::KraftController) {
        let quorum = ctrl.quorum_state().await.unwrap();
        ctrl.inject_event(Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: quorum.leader_epoch,
            fetch_offset: quorum.log_end_offset,
        })
        .await
        .unwrap();
    }

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let original = DelegationTokenRecord {
        token_id: "token-273".to_string(),
        owner: principal("alice"),
        hmac: vec![0x27; 32],
        issue_timestamp_ms: now - 1_000,
        expiry_timestamp_ms: now + 60_000,
        max_timestamp_ms: now + 600_000,
        renewers: vec![principal("bob")],
    };
    let renewed = DelegationTokenRecord {
        expiry_timestamp_ms: now + 120_000,
        ..original.clone()
    };

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    commit_pending(&ctrl).await;

    let create_ctrl = ctrl.clone();
    let create_record = original.clone();
    let create = tokio::spawn(async move {
        create_ctrl
            .submit_change(vec![MetadataRecord::V1DelegationToken(create_record)])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    commit_pending(&ctrl).await;
    create.await.unwrap().unwrap();

    let renew_ctrl = ctrl.clone();
    let renew = {
        let expected = original.clone();
        let replacement = renewed.clone();
        tokio::spawn(async move {
            renew_ctrl
                .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
                    expected,
                    replacement,
                }])
                .await
        })
    };
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let concurrent_delete = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: original.clone(),
        }]),
    )
    .await
    .expect("concurrent delete must return before the first mutation commits");
    assert2::assert!(matches!(
        concurrent_delete,
        Err(RaftError::ChangeRejected(_))
    ));

    commit_pending(&ctrl).await;
    renew.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .delegation_token_by_id(&original.token_id)
            .is_some_and(|token| token.expiry_timestamp_ms == renewed.expiry_timestamp_ms)
    );

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let retry = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
            expected: original.clone(),
            replacement: renewed.clone(),
        }])
        .await;
    assert2::assert!(retry.is_ok());
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let stale = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: original,
        }])
        .await;
    assert2::assert!(matches!(stale, Err(RaftError::ChangeRejected(_))));

    let malformed = DelegationTokenRecord {
        owner: principal("mallory"),
        expiry_timestamp_ms: i64::MAX,
        ..renewed.clone()
    };
    let rejected = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
            expected: renewed.clone(),
            replacement: malformed,
        }])
        .await;
    assert2::assert!(matches!(rejected, Err(RaftError::ChangeRejected(_))));

    let delete_ctrl = ctrl.clone();
    let delete_expected = renewed.clone();
    let delete = tokio::spawn(async move {
        delete_ctrl
            .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
                expected: delete_expected,
            }])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    commit_pending(&ctrl).await;
    delete.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .delegation_token_by_id(&renewed.token_id)
            .is_none()
    );

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let delete_retry = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: renewed,
        }])
        .await;
    assert2::assert!(delete_retry.is_ok());
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn offset_reservation_waits_for_current_epoch_commit_then_retries() {
    use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let result = ctrl
        .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count: 1,
            },
        )])
        .await;

    assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
    assert!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let create_ctrl = ctrl.clone();
    let create =
        tokio::spawn(async move { create_ctrl.submit_change(topic_record("topic")).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    create.await.unwrap().unwrap();

    let retry_ctrl = ctrl.clone();
    let retry = tokio::spawn(async move {
        retry_ctrl
            .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: "topic".to_string(),
                    partition: 0,
                    count: 1,
                },
            )])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let qs = ctrl.quorum_state().await.unwrap();
    ctrl.inject_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: qs.leader_epoch,
        fetch_offset: qs.log_end_offset,
    })
    .await
    .unwrap();
    let retry = retry.await.unwrap().unwrap();
    assert!(retry.offset_reservations[0].base_offset == 0);
    assert!(ctrl.current_image().partition_next_offset("topic", 0) == Some(1));
    ctrl.shutdown().await;
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
