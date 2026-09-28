//! Tests for KIP-903 broker registration: the broker epoch is the offset the
//! registration commits at, and a re-registration of an unchanged incarnation
//! keeps the epoch it was already assigned.

use assert2::assert;

use super::*;
use crate::kraft::controller::test_support::{
    await_leader, build, build_engine_only, elect_single_voter_engine, submit_change_with_timeout,
    topic_record,
};

#[test]
fn broker_registration_epoch_is_assigned_from_appended_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);

    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&topic_record("anchor"), reply);
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let base = engine.log.log_end_offset();
    let reg = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(7),
        broker_epoch: -1,
        incarnation_id: uuid::Uuid::from_u128(7),
        host: "broker-7".into(),
        port: 9092,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![],
        features: std::collections::BTreeMap::new(),
    });
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&[reg], reply);

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert!(engine.image.broker_epoch(NodeId(7)) == Some(base.0));
}

#[test]
fn broker_registration_projection_preserves_existing_epoch() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let registration = BrokerRegistrationRecord {
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(7),
        broker_epoch: -1,
        incarnation_id: uuid::Uuid::from_u128(7),
        host: "broker-7".into(),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs: vec![uuid::Uuid::from_u128(0xD1)],
        features: std::collections::BTreeMap::new(),
    };
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&[MetadataRecord::V1BrokerRegistration(registration)], reply);
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let mut projection = engine.image.broker(NodeId(7)).unwrap().clone();
    let assigned_epoch = projection.broker_epoch;
    projection.log_dirs.clear();

    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&[MetadataRecord::V1BrokerRegistration(projection)], reply);

    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let stored = engine.image.broker(NodeId(7)).unwrap();
    assert2::assert!(stored.broker_epoch == assigned_epoch);
    assert2::assert!(stored.log_dirs.is_empty());
}

#[tokio::test]
async fn broker_registration_epoch_equals_commit_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    let reg = |id: u64| {
        vec![MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                fenced: false,
                in_controlled_shutdown: false,
                cordoned_log_dirs: None,
                node_id: NodeId(id),
                broker_epoch: -1, // stamped by the leader at append
                incarnation_id: uuid::Uuid::from_u128(u128::from(id)),
                host: "h".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        )]
    };

    let base1 = ctrl.quorum_state().await.unwrap().log_end_offset;
    submit_change_with_timeout(&ctrl, reg(7), "first broker registration")
        .await
        .expect("first registration");
    let e1 = ctrl.current_image().broker_epoch(NodeId(7));
    assert2::assert!(e1 == Some(base1));

    let base2 = ctrl.quorum_state().await.unwrap().log_end_offset;
    submit_change_with_timeout(&ctrl, reg(7), "broker re-registration")
        .await
        .expect("re-registration");
    let e2 = ctrl.current_image().broker_epoch(NodeId(7));
    assert2::assert!(e2 == Some(base2));
    assert2::assert!(base2 > base1 && e2 > e1);

    ctrl.shutdown().await;
}

/// A new registration of broker 7 at no epoch yet, for the leader to stamp.
fn new_registration() -> krabka_metadata::BrokerRegistrationRecord {
    krabka_metadata::BrokerRegistrationRecord {
        fenced: true,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(7),
        broker_epoch: -1,
        incarnation_id: uuid::Uuid::from_u128(7),
        host: "broker-7".into(),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs: vec![uuid::Uuid::from_u128(0xD1)],
        features: std::collections::BTreeMap::new(),
    }
}

/// Codex review of krabka-io/krabka-broker#1166: a registration change names
/// the epoch of the registration it was built from and applies only while
/// the broker is still registered at that epoch, as Kafka's
/// `ClusterControlManager.replayRegistrationChange` requires of a
/// `BrokerRegistrationChangeRecord`. A change built at epoch E that reaches
/// the leader after the broker registered again at E2 is dropped: it neither
/// overwrites the new registration nor registers the broker at a third epoch.
#[test]
fn a_registration_change_applies_only_at_the_epoch_it_names() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let submit = |engine: &mut Engine, record: BrokerRegistrationRecord| {
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&[MetadataRecord::V1BrokerRegistration(record)], reply);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        engine.image.broker(NodeId(7)).cloned()
    };

    let registered = submit(&mut engine, new_registration()).expect("broker 7 registered");
    let unfenced = BrokerRegistrationRecord {
        fenced: false,
        ..registered.clone()
    };
    let after_unfence = submit(&mut engine, unfenced.clone());
    // Built from the image at the first epoch, before the broker restarts.
    let stale_shutdown = BrokerRegistrationRecord {
        in_controlled_shutdown: true,
        ..unfenced.clone()
    };
    let reregistered = submit(&mut engine, new_registration()).expect("broker 7 registered again");
    let after_stale_change = submit(&mut engine, stale_shutdown);

    assert!(after_unfence == Some(unfenced));
    assert!(reregistered.broker_epoch > registered.broker_epoch);
    assert!(
        reregistered
            == BrokerRegistrationRecord {
                broker_epoch: reregistered.broker_epoch,
                ..new_registration()
            }
    );
    assert!(after_stale_change == Some(reregistered));
}

/// Codex review of krabka-io/krabka-broker#1166: the leader decides a
/// registration change against its committed image, so while a new
/// registration of the same broker is appended but uncommitted, a change
/// waits with `UncommittedTail` rather than being appended behind it and
/// overwriting it on replay. Once the registration commits, the stale change
/// is dropped.
#[tokio::test]
async fn a_registration_change_waits_for_an_uncommitted_registration() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    crate::kraft::controller::test_support::elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2))
        .await;
    let commit = || async {
        let qs = ctrl.quorum_state().await.unwrap();
        ctrl.inject_event(Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: qs.leader_epoch,
            fetch_offset: qs.log_end_offset,
        })
        .await
        .unwrap();
    };
    let register = || {
        let ctrl = ctrl.clone();
        tokio::spawn(async move {
            ctrl.submit_change(vec![MetadataRecord::V1BrokerRegistration(
                new_registration(),
            )])
            .await
        })
    };

    let first = register();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    commit().await;
    first.await.unwrap().unwrap();
    let registered = ctrl.current_image().broker(NodeId(7)).cloned().unwrap();
    let stale_unfence = BrokerRegistrationRecord {
        fenced: false,
        ..registered
    };

    let second = register();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let while_uncommitted = ctrl
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(
            stale_unfence.clone(),
        )])
        .await;
    commit().await;
    second.await.unwrap().unwrap();
    let reregistered = ctrl.current_image().broker(NodeId(7)).cloned().unwrap();
    let once_committed = ctrl
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(stale_unfence)])
        .await;

    assert!(matches!(while_uncommitted, Err(RaftError::UncommittedTail)));
    assert!(once_committed.is_ok());
    assert!(ctrl.current_image().broker(NodeId(7)) == Some(&reregistered));
    assert!(
        reregistered
            == BrokerRegistrationRecord {
                broker_epoch: reregistered.broker_epoch,
                ..new_registration()
            }
    );
    ctrl.shutdown().await;
}
