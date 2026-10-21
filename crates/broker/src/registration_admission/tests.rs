//! What the leader adds to a batch that registers a broker, against one
//! three-replica partition of topic `t` in which broker 3 is in the ISR.

use {
    super::*,
    crate::{
        handlers::forward_to_controller::is_active_controller,
        heartbeat::controller_state::{ControllerLivenessState, replicated_registrations},
        leader_election::test_support::{expected_partition, img_with_partition, register_brokers},
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
#[test]
fn a_new_incarnation_leaves_its_isrs_and_leaderships_in_its_registration_batch() {
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
        let alive = HashSet::from([NodeId(1), NodeId(2)]);

        let admitted = admit(&image, case.submitted, &alive, &BrokerMetrics::new());

        check!(admitted == case.expected, "{}", case.what);
    }
}

/// Broker `node`'s registration in `image`, fenced.
fn fence(image: &mut MetadataImage, node: u64) {
    image.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            fenced: true,
            ..image.broker(NodeId(node)).expect("registered").clone()
        },
    ));
}

/// krabka-io/krabka-broker#1442: before this node's broker layer exists, a
/// batch that would retire another registered broker's incarnation is refused,
/// so its broker retries it once the admission can retire it. This node's own
/// restart, which won the controller election before its broker bound, is
/// never refused, since its startup waits on it: it leaves `t`-0's ISR and
/// leadership in its registration batch, and the leader moves to the first
/// replica the image holds unfenced, at a new leader epoch. A first
/// registration goes through as it was submitted.
#[yare::parameterized(
    this_nodes_restart = { 3, &[1, 2, 3], &[], Some(vec![without_broker_3(1, 6), registration(RESTARTED)]) },
    this_nodes_restart_past_a_fenced_replica = { 3, &[1, 2, 3], &[1], Some(vec![without_broker_3(2, 6), registration(RESTARTED)]) },
    this_nodes_first_registration = { 3, &[1, 2], &[], Some(vec![registration(RESTARTED)]) },
    another_brokers_first_registration = { 1, &[1, 2], &[], Some(vec![registration(RESTARTED)]) },
    another_brokers_restart = { 1, &[1, 2, 3], &[], None },
)]
#[test_macro(tokio::test)]
async fn an_unbound_admission_retires_this_nodes_restart_and_refuses_another_brokers(
    this_node: u64,
    registered: &[u64],
    fenced: &[u64],
    expected: Option<Vec<MetadataRecord>>,
) {
    let mut image = img_with_partition().leader(NodeId(3)).call();
    register_brokers(&mut image, registered);
    for &node in fenced {
        fence(&mut image, node);
    }
    let (_images, receiver) = tokio::sync::watch::channel(Arc::new(image));
    let admission = BrokerRegistrationAdmission::new(
        NodeId(this_node),
        receiver,
        Arc::new(BrokerControllerAdminRouter::new()),
        BrokerMetrics::new(),
    );

    let answer = RegistrationAdmission::admit(&admission, vec![registration(RESTARTED)]).await;

    check!(answer.ok() == expected);
}

/// The brokers an admission before bind may elect are the ones a new
/// controller's freshly seeded registry holds active, without the returning
/// broker: neither a fenced broker nor one in controlled shutdown is.
#[tokio::test]
async fn the_brokers_active_on_activation_are_the_ones_a_new_term_seeds_active() {
    let mut image = img_with_partition().call();
    register_brokers(&mut image, &[1, 2, 3, 4]);
    fence(&mut image, 2);
    image.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            in_controlled_shutdown: true,
            ..image.broker(NodeId(3)).expect("registered").clone()
        },
    ));
    let liveness = ControllerLivenessState::new(krabka_units::secs(10));
    liveness
        .seed_term(Some(1), replicated_registrations(&image))
        .await;
    let mut seeded = liveness.alive_node_ids().await;
    seeded.remove(&NodeId(4));

    check!(active_on_activation(&image, NodeId(4)) == seeded);
    check!(seeded == HashSet::from([NodeId(1)]));
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
/// when the batch that decided them has committed, and once this node's
/// broker has bound and so has a manager to hand them to. A broker that never
/// binds hands nothing over.
#[yare::parameterized(
    binds = { true },
    never_binds = { false },
)]
#[test_macro(tokio::test)]
async fn recoveries_are_handed_off_once_the_new_incarnation_is_registered_and_the_broker_bound(
    binds: bool,
) {
    let mut image = img_with_partition().call();
    register_brokers(&mut image, &[1, 2, 3]);
    let (images, receiver) = tokio::sync::watch::channel(Arc::new(image.clone()));
    let (sender, mut jobs) = tokio::sync::mpsc::channel(4);
    let (bind, bound) = tokio::sync::oneshot::channel();

    let task = hand_off_recoveries(
        receiver,
        async move { bound.await.ok() },
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
    tokio::task::yield_now().await;
    check!(jobs.try_recv().is_err(), "nothing before the broker binds");

    if binds {
        check!(
            bind.send(UncleanRecoveryHandle::for_tests(sender)).is_ok(),
            "the hand-off waits for the bind"
        );
    } else {
        drop((bind, sender));
    }
    task.await.expect("hand-off task");

    let job = jobs.try_recv().ok().map(|job| {
        (
            job.topic,
            job.partition,
            job.strategy,
            job.proposal.is_none(),
        )
    });
    check!(job == binds.then(|| ("t".to_string(), 0, RecoveryStrategy::Balanced, true)));
}

/// No recovery to hand off starts no task.
#[test]
fn no_recoveries_start_no_hand_off() {
    let (_images, receiver) = tokio::sync::watch::channel(Arc::new(img_with_partition().call()));
    let (sender, _jobs) = tokio::sync::mpsc::channel(1);

    check!(
        hand_off_recoveries(
            receiver,
            std::future::ready(Some(UncleanRecoveryHandle::for_tests(sender))),
            (NodeId(3), Uuid::from_u128(RESTARTED)),
            vec![],
        )
        .is_none()
    );
}
