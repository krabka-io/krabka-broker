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
        node_id: NodeId(7),
        broker_epoch: 0,
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
        node_id: NodeId(7),
        broker_epoch: 0,
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
                node_id: NodeId(id),
                broker_epoch: 0, // overwritten by the leader at append
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
