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
