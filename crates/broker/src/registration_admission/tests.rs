//! What the leader adds to a batch that registers a broker, against one
//! three-replica partition of topic `t` in which broker 3 is in the ISR.

use {
    super::*,
    crate::{
        leader_election::test_support::{
            expected_partition, img_with_partition, liveness_with_alive, register_brokers,
        },
        test_support::broker_registration,
    },
    assert2::check,
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
            expected: Admitted {
                records: vec![without_broker_3(1, 6), registration(RESTARTED)],
                new_incarnations: vec![(NodeId(3), Uuid::from_u128(RESTARTED))],
                recoveries: vec![],
            },
        },
        Case {
            what: "a restarted follower",
            leader: 1,
            registered: &[1, 2, 3],
            submitted: vec![registration(RESTARTED)],
            expected: Admitted {
                records: vec![without_broker_3(1, 5), registration(RESTARTED)],
                new_incarnations: vec![(NodeId(3), Uuid::from_u128(RESTARTED))],
                recoveries: vec![],
            },
        },
        Case {
            what: "a first registration",
            leader: 3,
            registered: &[1, 2],
            submitted: vec![registration(RESTARTED)],
            expected: Admitted {
                records: vec![registration(RESTARTED)],
                new_incarnations: vec![],
                recoveries: vec![],
            },
        },
        Case {
            what: "the same process registering again",
            leader: 3,
            registered: &[1, 2, 3],
            submitted: vec![registration(REGISTERED)],
            expected: Admitted {
                records: vec![registration(REGISTERED)],
                new_incarnations: vec![],
                recoveries: vec![],
            },
        },
        Case {
            what: "a batch whose earlier records already retired the broker",
            leader: 3,
            registered: &[1, 2, 3],
            submitted: vec![without_broker_3(1, 6), registration(RESTARTED)],
            expected: Admitted {
                records: vec![without_broker_3(1, 6), registration(RESTARTED)],
                new_incarnations: vec![(NodeId(3), Uuid::from_u128(RESTARTED))],
                recoveries: vec![],
            },
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
/// that registers for the first time, go through.
#[yare::parameterized(
    another_brokers_restart = { 1, &[1, 2, 3], true },
    first_registration = { 1, &[1, 2], false },
    this_nodes_restart = { 3, &[1, 2, 3], false },
)]
fn an_unbound_admission_refuses_only_another_brokers_restart(
    this_node: u64,
    registered: &[u64],
    refused: bool,
) {
    let mut image = img_with_partition().call();
    register_brokers(&mut image, registered);

    check!(
        retires_another_broker(&image, NodeId(this_node), &[registration(RESTARTED)]) == refused
    );
}
