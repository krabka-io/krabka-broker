//! What the leader adds to a batch that registers a broker, against one
//! three-replica partition of topic `t` in which broker 3 is in the ISR.

use {
    super::*,
    crate::{
        handlers::forward_to_controller::is_active_controller,
        leader_election::test_support::{
            expected_partition, img_with_partition, liveness_with_alive, register_brokers,
        },
        test_support::{broker_registration, start_broker_with},
    },
    assert2::check,
    krabka_metadata::{LeaderEpoch, PartitionRecord, TopicRecord},
};

/// The incarnation `register_brokers` registers broker 3 under.
const REGISTERED: u128 = 3;

/// The incarnation a restarted broker 3 registers under.
const RESTARTED: u128 = 99;

/// Broker 3's self-registration under `incarnation`, as it submits it.
fn registration(incarnation: u128) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: true,
        broker_epoch: -1,
        incarnation_id: Uuid::from_u128(incarnation),
        ..broker_registration(NodeId(3))
    })
}

/// Partition `t`-0 with broker 3 out of the ISR, led by `leader` at
/// `leader_epoch`.
fn without_broker_3(leader: u64, leader_epoch: i32) -> MetadataRecord {
    MetadataRecord::V1Partition(
        expected_partition()
            .leader(leader)
            .isr(&[NodeId(1), NodeId(2)])
            .leader_epoch(leader_epoch)
            .call(),
    )
}

/// The batch the leader appends, registering broker 3's restarted
/// incarnation.
fn retiring(records: Vec<MetadataRecord>) -> Admitted {
    Admitted {
        records,
        new_incarnations: vec![(NodeId(3), Uuid::from_u128(RESTARTED))],
        recoveries: vec![],
    }
}

/// The batch the leader appends when it registers no new incarnation.
fn unchanged(records: Vec<MetadataRecord>) -> Admitted {
    Admitted {
        records,
        ..Admitted::default()
    }
}

/// One submitted batch and what the leader appends for it.
struct Case {
    what: &'static str,
    /// Which broker leads `t`-0 before the batch.
    leader: u64,
    /// The brokers the image holds a registration for.
    registered: &'static [u64],
    submitted: Vec<MetadataRecord>,
    expected: Admitted,
}

/// krabka-io/krabka-broker#1450: a new incarnation of broker 3 takes it out of
/// the ISR and out of its leadership in the batch that registers it, as
/// `ClusterControlManager.registerBroker` runs `handleBrokerShutdown` ahead of
/// the `RegisterBrokerRecord`. The same process registering again adds
/// nothing, and neither does a first registration or a batch whose changes
/// already retired the broker.
#[tokio::test]
async fn a_new_incarnation_leaves_its_isrs_and_leaderships_in_its_registration_batch() {
    let cases = [
        Case {
            what: "a restarted leader",
            leader: 3,
            registered: &[1, 2, 3],
            submitted: vec![registration(RESTARTED)],
            expected: retiring(vec![without_broker_3(1, 6), registration(RESTARTED)]),
        },
        Case {
            what: "a restarted follower",
            leader: 1,
            registered: &[1, 2, 3],
            submitted: vec![registration(RESTARTED)],
            expected: retiring(vec![without_broker_3(1, 5), registration(RESTARTED)]),
        },
        Case {
            what: "a first registration",
            leader: 3,
            registered: &[1, 2],
            submitted: vec![registration(RESTARTED)],
            expected: unchanged(vec![registration(RESTARTED)]),
        },
        Case {
            what: "the same process registering again",
            leader: 3,
            registered: &[1, 2, 3],
            submitted: vec![registration(REGISTERED)],
            expected: unchanged(vec![registration(REGISTERED)]),
        },
        Case {
            what: "a batch whose earlier records already retired the broker",
            leader: 3,
            registered: &[1, 2, 3],
            submitted: vec![without_broker_3(1, 6), registration(RESTARTED)],
            expected: retiring(vec![without_broker_3(1, 6), registration(RESTARTED)]),
        },
    ];
    for case in cases {
        let mut image = img_with_partition().leader(NodeId(case.leader)).call();
        register_brokers(&mut image, case.registered);
        let liveness = liveness_with_alive(&[1, 2]).await;

        let admitted = admit(&image, case.submitted, &liveness, &BrokerMetrics::new()).await;

        check!(admitted == case.expected, "{}", case.what);
    }
}

/// Before this node's broker layer exists, a batch that would retire another
/// registered broker's incarnation is refused, so its broker retries it once
/// the admission can retire it. This node's own registration, and a broker
/// that registers for the first time, go through as they were submitted.
#[yare::parameterized(
    another_brokers_restart = { 1, &[1, 2, 3], false },
    first_registration = { 1, &[1, 2], true },
    this_nodes_restart = { 3, &[1, 2, 3], true },
)]
#[test_macro(tokio::test)]
async fn an_unbound_admission_refuses_only_another_brokers_restart(
    this_node: u64,
    registered: &[u64],
    admitted: bool,
) {
    let mut image = img_with_partition().call();
    register_brokers(&mut image, registered);
    let (_images, receiver) = tokio::sync::watch::channel(Arc::new(image));
    let admission = BrokerRegistrationAdmission::new(
        NodeId(this_node),
        receiver,
        Arc::new(BrokerControllerAdminRouter::new()),
    );

    let answer = RegistrationAdmission::admit(&admission, vec![registration(RESTARTED)]).await;

    check!(answer.ok() == admitted.then(|| vec![registration(RESTARTED)]));
}

/// The bound admission on a running controller: broker 2, which leads `t`-0
/// and holds a heartbeat session, registers a new incarnation. The batch that
/// registers it hands the leadership to broker 3 at a new leader epoch, takes
/// broker 2 out of the ISR, and leaves broker 2 without a session.
#[tokio::test]
async fn a_controller_retires_a_restarted_leader_in_its_registration_batch() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker);
        boot [start_broker_with(|config| config.heartbeat_timeout = krabka_units::minutes(10))];
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !is_active_controller(&broker) {
        check!(
            std::time::Instant::now() <= deadline,
            "the broker leads the controller"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let registered = |node: u64, incarnation: u128| {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            fenced: false,
            broker_epoch: -1,
            incarnation_id: Uuid::from_u128(incarnation),
            ..broker_registration(NodeId(node))
        })
    };
    broker
        .controller
        .submit_change(vec![
            registered(2, 2),
            registered(3, 3),
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::from_u128(9),
                partitions: 1,
                replication_factor: 2,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: 0,
                leader: NodeId(2),
                replicas: vec![NodeId(2), NodeId(3)],
                isr: vec![NodeId(2), NodeId(3)],
                leader_epoch: LeaderEpoch(4),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![Uuid::nil(); 2],
                partition_epoch: 0,
            }),
        ])
        .await
        .expect("seed");
    broker.liveness.record_heartbeat(2).await;
    broker.liveness.record_heartbeat(3).await;

    broker
        .controller
        .submit_change(vec![registered(2, 99)])
        .await
        .expect("re-register broker 2");

    let image = broker.controller.current_image();
    let partition = image.partition("t", 0).expect("seeded partition");
    check!(
        (
            partition.leader,
            partition.isr.clone(),
            partition.leader_epoch
        ) == (NodeId(3), vec![NodeId(3)], LeaderEpoch(5))
    );
    check!(image.broker(NodeId(2)).map(|b| b.incarnation_id) == Some(Uuid::from_u128(99)));
    check!(!broker.liveness.has_valid_session(2).await);
    drop(image);
    drop(broker);
    broker_handle.shutdown().await;
}

/// The offset-aware recoveries a re-registration decides reach the Unclean
/// Recovery Manager only once the image holds the new incarnation, which is
/// when the batch that decided them has committed.
#[tokio::test]
async fn recoveries_are_handed_off_once_the_new_incarnation_is_registered() {
    let mut image = img_with_partition().call();
    register_brokers(&mut image, &[1, 2, 3]);
    let (images, receiver) = tokio::sync::watch::channel(Arc::new(image.clone()));
    let (sender, mut jobs) = tokio::sync::mpsc::channel(4);

    let task = hand_off_recoveries(
        receiver,
        UncleanRecoveryHandle::for_tests(sender),
        (NodeId(3), Uuid::from_u128(RESTARTED)),
        vec![("t".to_string(), 0, RecoveryStrategy::Balanced)],
    )
    .expect("a recovery to hand off");
    tokio::task::yield_now().await;
    check!(
        jobs.try_recv().is_err(),
        "nothing before the registration commits"
    );

    image.apply(&registration(RESTARTED));
    images.send_replace(Arc::new(image));
    task.await.expect("hand-off task");

    let job = jobs.try_recv().expect("the handed-off recovery");
    check!(
        (
            job.topic.as_str(),
            job.partition,
            job.strategy,
            job.proposal.is_none()
        ) == ("t", 0, RecoveryStrategy::Balanced, true)
    );
}

/// No recovery to hand off starts no task.
#[test]
fn no_recoveries_start_no_hand_off() {
    let (_images, receiver) = tokio::sync::watch::channel(Arc::new(img_with_partition().call()));
    let (sender, _jobs) = tokio::sync::mpsc::channel(1);

    check!(
        hand_off_recoveries(
            receiver,
            UncleanRecoveryHandle::for_tests(sender),
            (NodeId(3), Uuid::from_u128(RESTARTED)),
            vec![],
        )
        .is_none()
    );
}
