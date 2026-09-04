//! Table-driven coverage of the offline-replica projection: an unregistered
//! broker, a fenced broker, a replica on a directory the registration no
//! longer lists, a registration left with no online directory at all, and the
//! two "online" sentinels.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, MetadataRecord, TopicRecord};
use uuid::Uuid;

use super::*;

fn dir(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn registration(node_id: u64, log_dirs: Vec<Uuid>) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        node_id: NodeId(node_id),
        broker_epoch: 0,
        incarnation_id: Uuid::from_u128(u128::from(node_id)),
        host: format!("broker-{node_id}"),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs,
        features: std::collections::BTreeMap::new(),
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

struct Case {
    name: &'static str,
    registrations: Vec<(u64, Vec<Uuid>)>,
    replicas: Vec<u64>,
    directories: Vec<Uuid>,
    unavailable: Vec<u64>,
    expected: Vec<i32>,
}

#[test]
fn offline_replicas_matches_kafka_replica_state_rules() {
    let (good, bad) = (dir(0x600d), dir(0xbad));
    let cases = vec![
        Case {
            name: "every replica registered, online dir, unfenced",
            registrations: vec![(1, vec![good, bad]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![],
            expected: vec![],
        },
        Case {
            name: "replica on a dir the registration no longer lists",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![bad, good],
            unavailable: vec![],
            expected: vec![1],
        },
        Case {
            name: "fenced broker",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![2],
            expected: vec![2],
        },
        Case {
            name: "unregistered broker",
            registrations: vec![(1, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![],
            expected: vec![2],
        },
        Case {
            name: "unassigned directory id is online",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![Uuid::nil(), Uuid::nil()],
            unavailable: vec![],
            expected: vec![],
        },
        Case {
            name: "registration whose last online dir was retired offlines its replicas",
            registrations: vec![(1, vec![]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![bad, good],
            unavailable: vec![],
            expected: vec![1],
        },
        Case {
            name: "registration with no online dir keeps an unassigned replica online",
            registrations: vec![(1, vec![]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![Uuid::nil(), good],
            unavailable: vec![],
            expected: vec![],
        },
        Case {
            name: "missing directory slot is online",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![],
            unavailable: vec![],
            expected: vec![],
        },
        Case {
            name: "offline dir and fenced peer are both reported, in replica order",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![2, 1],
            directories: vec![good, bad],
            unavailable: vec![2],
            expected: vec![2, 1],
        },
    ];

    for case in cases {
        let img = image(&case.registrations, &case.replicas, &case.directories);
        let record = img.partition("t", 0).expect("partition in image");
        let unavailable: HashSet<u64> = case.unavailable.iter().copied().collect();

        let actual = offline_replicas(&img, record, &unavailable);

        assert!(actual == case.expected, "case {}", case.name);
    }
}

/// One row of the availability table: the same image shape as above, plus the
/// whole partition row the projection must answer with.
struct AvailabilityCase {
    name: &'static str,
    registrations: Vec<(u64, Vec<Uuid>)>,
    replicas: Vec<u64>,
    directories: Vec<Uuid>,
    unavailable: Vec<u64>,
    expected: PartitionAvailability,
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
        AvailabilityCase {
            name: "sole replica on a failed log dir",
            registrations: vec![(1, vec![good])],
            replicas: vec![1],
            directories: vec![bad],
            unavailable: vec![],
            expected: PartitionAvailability {
                leader_id: NO_LEADER_ID,
                isr_nodes: vec![],
                offline_replicas: vec![1],
            },
        },
        AvailabilityCase {
            name: "leader on a failed log dir, follower healthy",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![bad, good],
            unavailable: vec![],
            expected: PartitionAvailability {
                leader_id: NO_LEADER_ID,
                isr_nodes: vec![2],
                offline_replicas: vec![1],
            },
        },
        AvailabilityCase {
            name: "sole replica on a directory nobody has assigned yet",
            registrations: vec![(1, vec![good])],
            replicas: vec![1],
            directories: vec![Uuid::nil()],
            unavailable: vec![],
            expected: PartitionAvailability {
                leader_id: 1,
                isr_nodes: vec![1],
                offline_replicas: vec![],
            },
        },
        AvailabilityCase {
            name: "fenced follower keeps its ISR seat",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![2],
            expected: PartitionAvailability {
                leader_id: 1,
                isr_nodes: vec![1, 2],
                offline_replicas: vec![2],
            },
        },
        AvailabilityCase {
            name: "unregistered follower keeps its ISR seat",
            registrations: vec![(1, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![],
            expected: PartitionAvailability {
                leader_id: 1,
                isr_nodes: vec![1, 2],
                offline_replicas: vec![2],
            },
        },
        AvailabilityCase {
            name: "healthy partition is untouched",
            registrations: vec![(1, vec![good]), (2, vec![good])],
            replicas: vec![1, 2],
            directories: vec![good, good],
            unavailable: vec![],
            expected: PartitionAvailability {
                leader_id: 1,
                isr_nodes: vec![1, 2],
                offline_replicas: vec![],
            },
        },
    ];

    for case in cases {
        let img = image(&case.registrations, &case.replicas, &case.directories);
        let record = img.partition("t", 0).expect("partition in image");
        let unavailable: HashSet<u64> = case.unavailable.iter().copied().collect();

        let actual = partition_availability(&img, record, &unavailable);

        assert!(actual == case.expected, "case {}", case.name);
    }
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
