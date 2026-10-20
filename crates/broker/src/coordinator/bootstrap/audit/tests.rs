use {
    super::*,
    crate::{
        config::NodeRole,
        coordinator::AUDIT_TOPIC,
        test_support::{FakeMetadataSource, await_until, broker_registration},
    },
    assert2::{assert, check},
    krabka_metadata::BrokerRegistrationRecord,
    krabka_raft::{deserialize_metadata_snapshot, serialize_metadata_snapshot},
    std::path::PathBuf,
};

// The isolated controller of the Kafka system tests that surfaced the defect.
const CONTROLLER: NodeId = NodeId(3001);
const CONTROLLER_ONLY: &[NodeRole] = &[NodeRole::Controller];
const COMBINED: &[NodeRole] = &[NodeRole::Controller, NodeRole::Broker];
const BROKER_ONLY: &[NodeRole] = &[NodeRole::Broker];

krabka_macros::single_replica_partition_fixture!(partition_record);

fn node(node_id: NodeId, roles: &[NodeRole]) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(PathBuf::new());
    config.node_id = node_id;
    config.roles = roles.to_vec();
    config
}

fn registration(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: true,
        incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
        ..broker_registration(krabka_raft::NodeId(node_id))
    })
}

fn registrations(node_ids: &[u64]) -> Vec<MetadataRecord> {
    node_ids.iter().copied().map(registration).collect()
}

/// Audit partition `partition` on `replica`, as the bootstrap writes it.
fn partition(partition: i32, replica: u64) -> MetadataRecord {
    MetadataRecord::V1Partition(partition_record(
        AUDIT_TOPIC,
        krabka_ids::PartitionIndex(partition),
        NodeId(replica),
    ))
}

/// The audit topic's config record with `overrides`.
fn topic_config(overrides: &[(&str, &str)]) -> MetadataRecord {
    MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: AUDIT_TOPIC.into(),
        overrides: overrides
            .iter()
            .map(|&(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
    })
}

const MIN_ISR_ONE: (&str, &str) = ("min.insync.replicas", "1");

fn topic_config_with_epochs(overrides: &[(&str, &str)], nodes: &[u64]) -> MetadataRecord {
    let keys: Vec<_> = nodes
        .iter()
        .map(|node| format!("krabka.audit.placement.epoch.{node}"))
        .collect();
    let entries: Vec<_> = keys
        .iter()
        .map(|key| (key.as_str(), "0"))
        .chain(overrides.iter().copied())
        .collect();
    topic_config(&entries)
}

/// The batch a 1.0 broker wrote to create the audit topic: no topic config.
fn legacy_audit_batch(topic_id: uuid::Uuid, replicas: &[u64]) -> Vec<MetadataRecord> {
    let mut batch = vec![MetadataRecord::V1Topic(TopicRecord {
        name: AUDIT_TOPIC.into(),
        topic_id,
        partitions: i32::try_from(replicas.len()).unwrap(),
        replication_factor: 1,
    })];
    batch.extend(
        (0_i32..)
            .zip(replicas)
            .map(|(index, &replica)| partition(index, replica)),
    );
    batch
}

/// The batch this release writes to create the audit topic.
fn audit_batch(topic_id: uuid::Uuid, replicas: &[u64]) -> Vec<MetadataRecord> {
    let mut batch = legacy_audit_batch(topic_id, replicas);
    batch.push(topic_config_with_epochs(&[MIN_ISR_ONE], replicas));
    batch
}

// The id the code under test drew for the topic. It is random, so the
// expected batch borrows it, and every other field is compared as written.
fn submitted_topic_id(submitted: &[Vec<MetadataRecord>]) -> uuid::Uuid {
    match submitted.first().and_then(|batch| batch.first()) {
        Some(MetadataRecord::V1Topic(topic)) => topic.topic_id,
        _ => uuid::Uuid::nil(),
    }
}

struct Placement {
    case: &'static str,
    roles: &'static [NodeRole],
    node_id: NodeId,
    leader: NodeId,
    registered: &'static [u64],
    // The replica of each audit partition, in partition order, or `None` when
    // the node submits nothing.
    replicas: Option<&'static [u64]>,
}

// The image never carries the topic in these cases, so each submitting node
// waits out `audit_partition_wait_timeout`. The paused clock skips the wait.
#[tokio::test(start_paused = true)]
async fn the_audit_partitions_land_on_registered_brokers_only() {
    let cases = [
        Placement {
            case: "a controller-only leader with no registered broker",
            roles: CONTROLLER_ONLY,
            node_id: CONTROLLER,
            leader: CONTROLLER,
            registered: &[],
            replicas: None,
        },
        Placement {
            case: "a controller-only leader with registered brokers",
            roles: CONTROLLER_ONLY,
            node_id: CONTROLLER,
            leader: CONTROLLER,
            registered: &[2, 1],
            replicas: Some(&[1, 2]),
        },
        Placement {
            case: "a controller-only follower",
            roles: CONTROLLER_ONLY,
            node_id: NodeId(3002),
            leader: CONTROLLER,
            registered: &[1],
            replicas: None,
        },
        Placement {
            case: "a combined leader before its registration reaches the image",
            roles: COMBINED,
            node_id: NodeId(1),
            leader: NodeId(1),
            registered: &[],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a combined leader after its registration",
            roles: COMBINED,
            node_id: NodeId(1),
            leader: NodeId(1),
            registered: &[1],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a combined follower",
            roles: COMBINED,
            node_id: NodeId(2),
            leader: NodeId(1),
            registered: &[1, 2],
            replicas: None,
        },
        Placement {
            case: "a broker-only node under an isolated controller",
            roles: BROKER_ONLY,
            node_id: NodeId(1),
            leader: CONTROLLER,
            registered: &[1],
            replicas: Some(&[1]),
        },
        Placement {
            case: "a broker-only node that registered after two others",
            roles: BROKER_ONLY,
            node_id: NodeId(3),
            leader: CONTROLLER,
            registered: &[3, 1, 2],
            replicas: Some(&[1, 2, 3]),
        },
    ];
    for case in cases {
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(&registrations(case.registered))
                .leader(Some(case.leader))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();

        let result = bootstrap_audit_topic(&node(case.node_id, case.roles), &source).await;

        let submitted = fake.submitted();
        let expected: Vec<Vec<MetadataRecord>> = case
            .replicas
            .map(|replicas| audit_batch(submitted_topic_id(&submitted), replicas))
            .into_iter()
            .collect();
        check!(result.is_ok(), "{}", case.case);
        check!(submitted == expected, "{}", case.case);
    }
}

#[tokio::test(start_paused = true)]
async fn a_node_submits_nothing_when_the_audit_topic_exists_or_audit_is_off() {
    let existing = [
        registrations(&[1]),
        audit_batch(uuid::Uuid::from_u128(7), &[1]),
    ]
    .concat();
    let cases = [
        ("the topic exists", existing.as_slice(), true),
        ("audit is off", &[] as &[MetadataRecord], false),
    ];
    for (case, records, audit_enabled) in cases {
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(records)
                .leader(Some(NodeId(1)))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();
        let mut config = node(NodeId(1), COMBINED);
        config.audit_enabled = audit_enabled;

        let result = bootstrap_audit_topic(&config, &source).await;

        check!(result.is_ok(), "{case}");
        check!(
            fake.submitted() == Vec::<Vec<MetadataRecord>>::new(),
            "{case}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn only_an_existing_topic_excuses_a_refused_audit_batch() {
    struct Case {
        refusal: fn() -> RaftError,
        expected: Result<(), String>,
    }
    let cases: [Case; 2] = [
        Case {
            refusal: || RaftError::Metadata(MetadataError::TopicExists(String::new())),
            expected: Ok(()),
        },
        Case {
            refusal: || RaftError::ChangeRejected("refused".into()),
            expected: Err("startup failed: change rejected: refused".into()),
        },
    ];
    for Case { refusal, expected } in cases {
        let source: Arc<dyn MetadataSource> = Arc::new(
            FakeMetadataSource::builder()
                .records(&registrations(&[1]))
                .leader(Some(CONTROLLER))
                .on_submit(move |_| Err(refusal()))
                .build(),
        );

        let result = bootstrap_audit_topic(&node(NodeId(1), BROKER_ONLY), &source).await;

        check!(result.map_err(|error| error.to_string()) == expected);
    }
}

// A broker-only node sees its own forwarded batch only on its next metadata
// fetch. The audit pipeline reads the image right after this bootstrap, so the
// bootstrap holds startup until the topic arrives.
#[tokio::test(start_paused = true)]
async fn a_broker_only_node_waits_for_the_audit_topic_to_reach_its_image() {
    let fake = Arc::new(
        FakeMetadataSource::builder()
            .records(&registrations(&[1]))
            .leader(Some(CONTROLLER))
            .build(),
    );
    let source: Arc<dyn MetadataSource> = fake.clone();
    let config = node(NodeId(1), BROKER_ONLY);
    let timeout = config.audit_partition_wait_timeout.to_std();
    let started = tokio::time::Instant::now();
    let bootstrap = tokio::spawn(async move { bootstrap_audit_topic(&config, &source).await });
    while fake.submitted().is_empty() {
        tokio::task::yield_now().await;
    }

    tokio::time::sleep(timeout / 2).await;
    check!(!bootstrap.is_finished());
    fake.set_records(&[registrations(&[1]), fake.submitted_records()].concat());
    let result = bootstrap.await.expect("the bootstrap task does not panic");

    assert!(result.is_ok());
    assert!(started.elapsed() < timeout);
}

#[tokio::test(start_paused = true)]
async fn a_broker_only_node_starts_when_the_audit_topic_never_reaches_its_image() {
    let source: Arc<dyn MetadataSource> = Arc::new(
        FakeMetadataSource::builder()
            .records(&registrations(&[1]))
            .leader(Some(CONTROLLER))
            .build(),
    );
    let config = node(NodeId(1), BROKER_ONLY);
    let started = tokio::time::Instant::now();

    let result = bootstrap_audit_topic(&config, &source).await;

    assert!(result.is_ok());
    assert!(started.elapsed() >= config.audit_partition_wait_timeout.to_std());
}

/// Audit partition `index` on `replica` with no leader, as a restart leaves
/// it while the broker is still fenced. krabka spells "no leader" as node 0.
fn fenced_leaderless(replica: u64, index: i32) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        leader: NodeId(0),
        ..partition_record(
            AUDIT_TOPIC,
            krabka_ids::PartitionIndex(index),
            NodeId(replica),
        )
    })
}

#[test]
fn a_placement_pass_adds_only_what_the_audit_topic_lacks() {
    struct Case {
        name: &'static str,
        image: Vec<MetadataRecord>,
        expected: Vec<MetadataRecord>,
    }
    let topic_id = uuid::Uuid::from_u128(7);
    let cases = [
        Case {
            name: "no audit topic",
            image: registrations(&[1, 2]),
            expected: vec![],
        },
        Case {
            name: "every broker holds a partition",
            image: [registrations(&[1, 2]), audit_batch(topic_id, &[1, 2])].concat(),
            expected: vec![],
        },
        Case {
            name: "a broker that registered after the topic was created",
            image: [registrations(&[1, 2, 3]), audit_batch(topic_id, &[1, 2])].concat(),
            expected: vec![
                partition(2, 3),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2, 3]),
            ],
        },
        Case {
            name: "two late brokers, in ascending node-id order",
            image: [registrations(&[4, 1, 3]), audit_batch(topic_id, &[1])].concat(),
            expected: vec![
                partition(1, 3),
                partition(2, 4),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 3, 4]),
            ],
        },
        Case {
            name: "a restarted broker that leads nothing keeps its partition",
            image: [
                registrations(&[1, 2]),
                audit_batch(topic_id, &[1, 2]),
                vec![fenced_leaderless(2, 1)],
            ]
            .concat(),
            expected: vec![],
        },
        Case {
            name: "a new registration that keeps its replica only updates the epoch",
            image: [
                registrations(&[1, 2]),
                audit_batch(topic_id, &[1, 2]),
                vec![MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        broker_epoch: 1,
                        incarnation_id: uuid::Uuid::from_u128(20),
                        ..broker_registration(NodeId(2))
                    },
                )],
            ]
            .concat(),
            expected: vec![topic_config_with_epochs(
                &[MIN_ISR_ONE, ("krabka.audit.placement.epoch.2", "1")],
                &[1, 2],
            )],
        },
        Case {
            name: "a topic that a 1.0 broker created with only its leader",
            image: [
                registrations(&[1, 2, 3]),
                legacy_audit_batch(topic_id, &[2]),
            ]
            .concat(),
            expected: vec![
                partition(1, 1),
                partition(2, 3),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2, 3]),
            ],
        },
        Case {
            name: "other overrides survive the added minimum",
            image: [
                registrations(&[1]),
                legacy_audit_batch(topic_id, &[1]),
                vec![topic_config(&[("retention.ms", "-1")])],
            ]
            .concat(),
            expected: vec![topic_config_with_epochs(
                &[MIN_ISR_ONE, ("retention.ms", "-1")],
                &[1],
            )],
        },
        Case {
            name: "an operator's minimum is kept",
            image: [
                registrations(&[1]),
                legacy_audit_batch(topic_id, &[1]),
                vec![topic_config(&[("min.insync.replicas", "2")])],
            ]
            .concat(),
            expected: vec![topic_config_with_epochs(
                &[("min.insync.replicas", "2")],
                &[1],
            )],
        },
    ];
    for Case {
        name,
        image,
        expected,
    } in cases
    {
        let image = MetadataImage::from_records(uuid::Uuid::nil(), &image);

        check!(
            audit_placement_records(&image, AUDIT_TOPIC) == expected,
            "{name}"
        );
    }
}

// The bootstrap of a node that leads the quorum completes an existing topic in
// the same pass, before the audit pipeline reads the image. Any other node
// leaves that to the leader. The paused clock skips each broker's wait for a
// partition that the fake never applies.
#[tokio::test(start_paused = true)]
async fn only_the_quorum_leader_completes_an_existing_audit_topic_at_startup() {
    struct Case {
        name: &'static str,
        roles: &'static [NodeRole],
        node_id: u64,
        leads: bool,
        image: Vec<MetadataRecord>,
        expected: Vec<Vec<MetadataRecord>>,
    }
    let topic_id = uuid::Uuid::from_u128(7);
    // Broker 2 registered after the topic was created on broker 1.
    let late_broker = || [registrations(&[1, 2]), audit_batch(topic_id, &[1])].concat();
    let cases = [
        Case {
            name: "a combined leader that registered after the topic was created",
            roles: COMBINED,
            node_id: 2,
            leads: true,
            image: late_broker(),
            expected: vec![vec![
                partition(1, 2),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2]),
            ]],
        },
        Case {
            name: "a combined leader upgraded from 1.0",
            roles: COMBINED,
            node_id: 1,
            leads: true,
            image: [registrations(&[1]), legacy_audit_batch(topic_id, &[1])].concat(),
            expected: vec![vec![topic_config_with_epochs(&[MIN_ISR_ONE], &[1])]],
        },
        Case {
            name: "a controller-only leader under a late broker",
            roles: CONTROLLER_ONLY,
            node_id: CONTROLLER.0,
            leads: true,
            image: late_broker(),
            expected: vec![vec![
                partition(1, 2),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2]),
            ]],
        },
        Case {
            name: "a combined follower without a partition",
            roles: COMBINED,
            node_id: 2,
            leads: false,
            image: late_broker(),
            expected: vec![],
        },
        Case {
            name: "a broker-only node without a partition",
            roles: BROKER_ONLY,
            node_id: 2,
            leads: false,
            image: late_broker(),
            expected: vec![],
        },
    ];
    for Case {
        name,
        roles,
        node_id,
        leads,
        image,
        expected,
    } in cases
    {
        let leader = if leads { NodeId(node_id) } else { CONTROLLER };
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(&image)
                .leader(Some(leader))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();

        let result = bootstrap_audit_topic(&node(NodeId(node_id), roles), &source).await;

        check!(result.is_ok(), "{name}");
        check!(fake.submitted() == expected, "{name}");
    }
}

// Two placement passes that read the same image would both pick the next
// partition index. The guarded submit refuses the second one's stale image,
// and its retry reads the image the first one committed.
#[tokio::test(start_paused = true)]
async fn a_placement_pass_that_lost_a_race_places_after_the_winner() {
    let topic_id = uuid::Uuid::from_u128(7);
    let before = [registrations(&[1, 2, 3]), audit_batch(topic_id, &[1])].concat();
    // Another pass placed broker 2 between this pass's read and its submit.
    let after = [before.clone(), vec![partition(1, 2)]].concat();
    let fake = Arc::new(
        FakeMetadataSource::builder()
            .records(&before)
            .leader(Some(NodeId(1)))
            .before_guarded_submit(move |fake| fake.set_records(&after))
            .build(),
    );
    let deadline = Instant::now() + std::time::Duration::from_secs(10);

    place_audit_partitions(AUDIT_TOPIC, &*fake, deadline).await;

    assert!(
        fake.submitted()
            == vec![vec![
                partition(2, 3),
                topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2, 3])
            ]]
    );
}

// The leader's task places a broker that registers while it leads, and a node
// that becomes the leader places what its predecessor left.
#[tokio::test(start_paused = true)]
async fn the_leader_places_every_broker_that_registers_after_the_topic() {
    let topic_id = uuid::Uuid::from_u128(7);
    let created = [registrations(&[1, 2]), audit_batch(topic_id, &[1])].concat();
    let fake = Arc::new(
        FakeMetadataSource::builder()
            .records(&created)
            .leader(Some(NodeId(2)))
            .controller_node_id(NodeId(1))
            .build(),
    );
    let source: Arc<dyn MetadataSource> = fake.clone();
    let shutdown = CancellationToken::new();
    spawn_audit_placement(&node(NodeId(1), COMBINED), &source, shutdown.clone());
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let as_follower = fake.submitted();

    fake.set_leader(Some(NodeId(1)));
    await_until("the new leader places broker 2", || {
        fake.submitted().len() == 1
    })
    .await;
    fake.set_records(&[created, fake.submitted_records(), registrations(&[3])].concat());
    await_until("the leader places broker 3", || fake.submitted().len() == 2).await;
    shutdown.cancel();

    check!(as_follower == Vec::<Vec<MetadataRecord>>::new());
    check!(
        fake.submitted()
            == vec![
                vec![
                    partition(1, 2),
                    topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2])
                ],
                vec![
                    partition(2, 3),
                    topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2, 3])
                ],
            ]
    );
}

#[test]
fn audit_evacuation_survives_a_snapshot_and_new_registrations_still_get_a_partition() {
    let cluster_id = uuid::Uuid::nil();
    let mut image = MetadataImage::from_records(
        cluster_id,
        &[
            registrations(&[1, 2]),
            legacy_audit_batch(uuid::Uuid::from_u128(7), &[1, 2]),
        ]
        .concat(),
    );
    let initialized = audit_placement_records(&image, AUDIT_TOPIC);
    assert!(initialized == vec![topic_config_with_epochs(&[MIN_ISR_ONE], &[1, 2])]);
    for record in &initialized {
        image.apply(record);
    }
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        partition_epoch: 1,
        ..partition_record(AUDIT_TOPIC, PartitionIndex(1), NodeId(1))
    }));
    let snapshot = serialize_metadata_snapshot(&image, 0).unwrap();
    let mut recovered = MetadataImage::from_records(
        cluster_id,
        &deserialize_metadata_snapshot(&snapshot).unwrap(),
    );
    assert!(audit_placement_records(&recovered, AUDIT_TOPIC).is_empty());

    recovered.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            broker_epoch: 1,
            incarnation_id: uuid::Uuid::from_u128(20),
            ..broker_registration(NodeId(2))
        },
    ));
    let records = audit_placement_records(&recovered, AUDIT_TOPIC);
    assert!(
        records
            == vec![
                partition(2, 2),
                topic_config_with_epochs(
                    &[MIN_ISR_ONE, ("krabka.audit.placement.epoch.2", "1")],
                    &[1, 2]
                ),
            ]
    );
    for record in &records {
        recovered.apply(record);
    }
    recovered.apply(&MetadataRecord::V1Partition(PartitionRecord {
        partition_epoch: 1,
        ..partition_record(AUDIT_TOPIC, PartitionIndex(2), NodeId(1))
    }));
    assert!(audit_placement_records(&recovered, AUDIT_TOPIC).is_empty());
    recovered.apply(&registration(3));
    let later = audit_placement_records(&recovered, AUDIT_TOPIC);
    assert!(
        later
            == vec![
                partition(3, 3),
                topic_config_with_epochs(
                    &[MIN_ISR_ONE, ("krabka.audit.placement.epoch.2", "1")],
                    &[1, 2, 3]
                ),
            ]
    );
}

#[tokio::test(start_paused = true)]
async fn only_a_controller_with_audit_enabled_runs_the_placement_task() {
    let cases = [
        ("a broker-only node", BROKER_ONLY, true),
        ("a controller with audit disabled", COMBINED, false),
    ];
    for (case, roles, audit_enabled) in cases {
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(&[registrations(&[1, 2]), audit_batch(uuid::Uuid::nil(), &[1])].concat())
                .leader(Some(NodeId(1)))
                .build(),
        );
        let source: Arc<dyn MetadataSource> = fake.clone();
        let mut config = node(NodeId(1), roles);
        config.audit_enabled = audit_enabled;
        let shutdown = CancellationToken::new();

        spawn_audit_placement(&config, &source, shutdown.clone());
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();

        check!(
            fake.submitted() == Vec::<Vec<MetadataRecord>>::new(),
            "{case}"
        );
    }
}

// A broker writes the audit partition placed on it, so a restart that reads
// the image while the partition still has no leader finds it all the same.
#[yare::parameterized(
    led = { &[partition(0, 1), partition(1, 2)], 2, Some(1) },
    fenced_after_a_restart = { &[partition(0, 1), fenced_leaderless(2, 1)], 2, Some(1) },
    placed_on_another_broker = { &[partition(0, 1)], 2, None },
    no_topic = { &[], 2, None },
)]
fn a_broker_hosts_the_audit_partition_placed_on_it(
    partitions: &[MetadataRecord],
    node: u64,
    expected: Option<i32>,
) {
    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: AUDIT_TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: i32::try_from(partitions.len()).unwrap(),
        replication_factor: 1,
    });
    let records: Vec<MetadataRecord> = [
        registrations(&[1, 2]),
        if partitions.is_empty() {
            vec![]
        } else {
            vec![topic]
        },
        partitions.to_vec(),
    ]
    .concat();
    let image = MetadataImage::from_records(uuid::Uuid::nil(), &records);

    check!(
        hosted_audit_partition(&image, AUDIT_TOPIC, NodeId(node))
            == expected.map(krabka_ids::PartitionIndex)
    );
}
