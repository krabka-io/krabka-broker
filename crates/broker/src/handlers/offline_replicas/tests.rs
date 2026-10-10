//! Table-driven coverage of the offline-replica projection: an unregistered
//! broker, a fenced broker, a broker without the request's listener, a replica
//! on a directory the registration no longer lists, a registration left with
//! no online directory at all, and the two "online" sentinels.

use assert2::assert;
use krabka_metadata::{BrokerEndpoint, LeaderEpoch, MetadataRecord, TopicRecord};
use uuid::Uuid;

use super::*;

/// The listener that the requests in these tests arrive on, and that every
/// registration lists unless a test says otherwise.
const LISTENER: &str = "PLAINTEXT";

fn dir(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn registration(node_id: u64, log_dirs: Vec<Uuid>) -> MetadataRecord {
    registration_on(node_id, log_dirs, LISTENER)
}

/// A registration whose only endpoint is on `listener`.
fn registration_on(node_id: u64, log_dirs: Vec<Uuid>, listener: &str) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        incarnation_id: Uuid::from_u128(u128::from(node_id)),
        host: format!("broker-{node_id}"),
        endpoints: vec![BrokerEndpoint {
            name: listener.to_owned(),
            host: format!("broker-{node_id}"),
            port: 9092,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }],
        log_dirs,
        ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
    })
}

fn partition(replicas: &[u64], directories: &[Uuid]) -> PartitionRecord {
    PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(replicas[0]),
        replicas: replicas.iter().copied().map(NodeId).collect(),
        isr: replicas.iter().copied().map(NodeId).collect(),
        leader_epoch: LeaderEpoch(3),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: directories.to_vec(),
        partition_epoch: 0,
    }
}

/// Image with `t-0` on `replicas`/`directories` and a registration for every
/// `(node_id, online_dirs)` pair in `registrations`.
fn image(
    registrations: &[(u64, Vec<Uuid>)],
    replicas: &[u64],
    directories: &[Uuid],
) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(0x70b1c),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).unwrap(),
    }));
    for (node_id, online) in registrations {
        img.apply(&registration(*node_id, online.clone()));
    }
    img.apply(&MetadataRecord::V1Partition(partition(
        replicas,
        directories,
    )));
    img
}

struct Case<E> {
    name: &'static str,
    registrations: Vec<(u64, Vec<Uuid>)>,
    replicas: Vec<u64>,
    directories: Vec<Uuid>,
    unavailable: Vec<u64>,
    expected: E,
}

fn replica_case<E>(
    name: &'static str,
    registrations: Vec<(u64, Vec<Uuid>)>,
    (replicas, directories): (Vec<u64>, Vec<Uuid>),
    unavailable: Vec<u64>,
    expected: E,
) -> Case<E> {
    Case {
        name,
        registrations,
        replicas,
        directories,
        unavailable,
        expected,
    }
}

impl<E> Case<E> {
    fn image(&self) -> MetadataImage {
        image(&self.registrations, &self.replicas, &self.directories)
    }

    fn unavailable(&self) -> HashSet<u64> {
        self.unavailable.iter().copied().collect()
    }
}

/// Keep the projection cases independent while sharing their image/lookup/assertion driver.
fn check_replica_cases<E: std::fmt::Debug + PartialEq>(
    cases: Vec<Case<E>>,
    project: impl Fn(&MetadataImage, &PartitionRecord, &HashSet<u64>, &str) -> E,
) {
    for case in cases {
        let img = case.image();
        let record = img.partition("t", 0).expect("partition in image");
        let unavailable: HashSet<u64> = case.unavailable();
        let actual = project(&img, record, &unavailable, LISTENER);
        assert!(actual == case.expected, "case {}", case.name);
    }
}

#[test]
fn offline_replicas_matches_kafka_replica_state_rules() {
    let (good, bad) = (dir(0x600d), dir(0xbad));
    let cases = vec![
        replica_case(
            "every replica registered, online dir, unfenced",
            vec![(1, vec![good, bad]), (2, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![],
            vec![],
        ),
        replica_case(
            "replica on a dir the registration no longer lists",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![bad, good]),
            vec![],
            vec![1],
        ),
        replica_case(
            "fenced broker",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![2],
            vec![2],
        ),
        replica_case(
            "unregistered broker",
            vec![(1, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![],
            vec![2],
        ),
        replica_case(
            "unassigned directory id is online",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![Uuid::nil(), Uuid::nil()]),
            vec![],
            vec![],
        ),
        replica_case(
            "registration whose last online dir was retired offlines its replicas",
            vec![(1, vec![]), (2, vec![good])],
            (vec![1, 2], vec![bad, good]),
            vec![],
            vec![1],
        ),
        replica_case(
            "registration with no online dir keeps an unassigned replica online",
            vec![(1, vec![]), (2, vec![good])],
            (vec![1, 2], vec![Uuid::nil(), good]),
            vec![],
            vec![],
        ),
        replica_case(
            "missing directory slot is online",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![]),
            vec![],
            vec![],
        ),
        replica_case(
            "offline dir and fenced peer are both reported, in replica order",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![2, 1], vec![good, bad]),
            vec![2],
            vec![2, 1],
        ),
    ];

    check_replica_cases(cases, offline_replicas);
}

/// A replica on a directory its broker no longer lists online neither leads
/// nor stays in the reported ISR -- and nothing else changes.
///
/// The first row is the one `kafka-topics --describe
/// --unavailable-partitions` opened this gap on: a sole replica on a failed
/// log directory. Apache Kafka 4.3.1 answers that shape `Leader: none
/// Replicas: 1 Isr:`, and it is the `Leader: none` and the empty ISR, not the
/// offline list, that the tool's two health filters read.
///
/// The last two rows are the boundary. A fenced broker and an unregistered
/// one are both reported offline, and both keep their ISR seat here, because
/// Kafka's `KRaftMetadataCache` passes the image's ISR through and krabka's
/// controller is able to shrink it for those two edges itself.
#[test]
fn a_replica_on_a_dead_log_dir_neither_leads_nor_stays_in_the_isr() {
    let (good, bad) = (dir(0x600d), dir(0xbad));
    let cases = vec![
        replica_case(
            "sole replica on a failed log dir",
            vec![(1, vec![good])],
            (vec![1], vec![bad]),
            vec![],
            availability(NO_LEADER_ID, vec![], vec![1]),
        ),
        replica_case(
            "leader on a failed log dir, follower healthy",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![bad, good]),
            vec![],
            availability(NO_LEADER_ID, vec![2], vec![1]),
        ),
        replica_case(
            "sole replica on a directory nobody has assigned yet",
            vec![(1, vec![good])],
            (vec![1], vec![Uuid::nil()]),
            vec![],
            availability(1, vec![1], vec![]),
        ),
        replica_case(
            "fenced follower keeps its ISR seat",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![2],
            availability(1, vec![1, 2], vec![2]),
        ),
        replica_case(
            "unregistered follower keeps its ISR seat",
            vec![(1, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![],
            availability(1, vec![1, 2], vec![2]),
        ),
        replica_case(
            "healthy partition is untouched",
            vec![(1, vec![good]), (2, vec![good])],
            (vec![1, 2], vec![good, good]),
            vec![],
            availability(1, vec![1, 2], vec![]),
        ),
    ];

    check_replica_cases(cases, partition_availability);
}

/// Kafka's `KRaftMetadataCache` finds a leader's endpoint with
/// `getAliveEndpoint`, which is empty for a broker with no registration and
/// for one with no endpoint on the request's listener, and it reports a
/// replica on such a broker offline (`isReplicaOffline`). The leader is then
/// `-1`, and the ISR and the replica list are left as they are.
#[test]
fn a_broker_without_the_request_listener_is_offline_and_cannot_be_the_leader() {
    let good = dir(0x600d);
    // Node 1 lists `PLAINTEXT`, node 2 lists only `EXTERNAL`, and node 3 has
    // no registration.
    let mut img = image(&[(1, vec![good]), (2, vec![good])], &[1, 2], &[good, good]);
    img.apply(&registration_on(2, vec![good], "EXTERNAL"));
    let cases = [
        (
            "the leader lists the listener and the follower does not",
            vec![1, 2],
            availability(1, vec![1, 2], vec![2]),
        ),
        (
            "the leader does not list the listener",
            vec![2, 1],
            availability(NO_LEADER_ID, vec![2, 1], vec![2]),
        ),
        (
            "the leader has no registration",
            vec![3, 1],
            availability(NO_LEADER_ID, vec![3, 1], vec![3]),
        ),
    ];

    for (name, replicas, expected) in cases {
        let mut img = img.clone();
        img.apply(&MetadataRecord::V1Partition(partition(
            &replicas,
            &[good, good],
        )));
        let record = img.partition("t", 0).expect("partition in image");

        let actual = partition_availability(&img, record, &HashSet::new(), LISTENER);

        assert!(actual == expected, "case {name}");
    }
    // The same partition on the other listener: node 2 is the one in place.
    let record = img.partition("t", 0).expect("partition in image");
    assert!(
        partition_availability(&img, record, &HashSet::new(), "EXTERNAL")
            == availability(NO_LEADER_ID, vec![1, 2], vec![1])
    );
}

/// The set an operator election may elect from, which every node computes the
/// same way so that a rotating `controllerId` cannot change the answer.
///
/// A registration is what makes a broker electable and the unavailable set is
/// what takes it back, including for a broker whose heartbeat this node has
/// never seen -- the case that made `ElectLeaders` depend on where it landed.
#[test]
fn electable_is_the_registered_brokers_the_unavailable_set_does_not_name() {
    let good = dir(0x600d);
    let img = image(
        &[(1, vec![good]), (2, vec![good]), (3, vec![good])],
        &[1, 2],
        &[good, good],
    );

    let cases: [(&str, Vec<u64>, Vec<u64>); 4] = [
        ("nothing unavailable", vec![], vec![1, 2, 3]),
        ("one fenced broker", vec![2], vec![1, 3]),
        ("every broker unavailable", vec![1, 2, 3], vec![]),
        (
            "an unavailable broker that never registered",
            vec![9],
            vec![1, 2, 3],
        ),
    ];

    for (name, unavailable, expected) in cases {
        let actual = electable(&img, &unavailable.into_iter().collect());

        assert!(actual == expected.into_iter().collect(), "case {name}");
    }
}

/// A broker the image does not carry a registration for is not electable, even
/// though nothing reports it unavailable.
#[test]
fn an_unregistered_broker_is_never_electable() {
    let good = dir(0x600d);
    let img = image(&[(1, vec![good])], &[1, 2], &[good, good]);

    assert!(electable(&img, &HashSet::new()) == HashSet::from([1]));
}

/// A partition the controller left without a leader keeps its last leader in
/// its record and publishes a one-member last-known ELR that names it (see
/// `crate::elr::state::is_leaderless`). Both APIs project it the way Kafka's
/// `leader = -1` reads: no leader, and the last leader out of the ISR. The
/// eligible set beside it does not change the answer, and a last-known ELR
/// that does not name the leader is not a marker.
#[test]
fn a_leaderless_partition_projects_no_leader_and_an_isr_without_its_last_leader() {
    let good = dir(0x600d);
    let cases: [(&str, Vec<u64>, Vec<u64>, PartitionAvailability); 5] = [
        (
            "sole replica, last-known ELR names it",
            vec![1],
            vec![1],
            availability(NO_LEADER_ID, vec![], vec![]),
        ),
        (
            "an ISR member other than the last leader stays",
            vec![1, 2],
            vec![1],
            availability(NO_LEADER_ID, vec![2], vec![]),
        ),
        (
            "no last-known ELR",
            vec![1, 2],
            vec![],
            availability(1, vec![1, 2], vec![]),
        ),
        (
            "a last-known ELR that names another replica is no marker",
            vec![1, 2],
            vec![2],
            availability(1, vec![1, 2], vec![]),
        ),
        (
            "a multi-member last-known ELR is no marker",
            vec![1, 2],
            vec![1, 2],
            availability(1, vec![1, 2], vec![]),
        ),
    ];
    for (name, replicas, last_known, expected) in cases {
        let mut img = image(
            &[(1, vec![good]), (2, vec![good])],
            &replicas,
            &vec![good; replicas.len()],
        );
        img.apply(&MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "t".into(),
                partition: 0,
                eligible_leader_replicas: vec![NodeId(2)],
                last_known_elr: last_known.into_iter().map(NodeId).collect(),
            },
        ));
        let record = img.partition("t", 0).expect("partition in image");

        assert!(
            partition_availability(&img, record, &HashSet::new(), LISTENER) == expected,
            "case {name}"
        );
    }
}

fn availability(
    leader_id: i32,
    isr_nodes: Vec<i32>,
    offline_replicas: Vec<i32>,
) -> PartitionAvailability {
    PartitionAvailability {
        leader_id,
        isr_nodes,
        offline_replicas,
    }
}
