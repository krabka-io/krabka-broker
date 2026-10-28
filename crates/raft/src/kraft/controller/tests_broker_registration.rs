//! Tests for KIP-903 broker registration: the broker epoch is the offset the
//! registration commits at, and a re-registration of an unchanged incarnation
//! keeps the epoch it was already assigned.

use {
    super::*,
    crate::kraft::controller::{
        recovery::replay_committed,
        test_support::{
            ControllerSetup, build, elect_leader_with_helper, submit_change_with_timeout,
            topic_record,
        },
    },
    assert2::{assert, check},
    krabka_protocol::records::metadata,
    std::{collections::BTreeMap, time::Duration},
    tokio::time,
};

#[test]
fn broker_registration_epoch_is_assigned_from_appended_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();

    let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record("anchor"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let base = engine.log.log_end_offset();
    let reg = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: false,
        log_dirs: vec![],
        ..new_registration()
    });
    let mut rx = super::test_support::submit_on_engine(&mut engine, &[reg]);

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert!(engine.image.broker_epoch(NodeId(7)) == Some(base.0));
}

#[test]
fn broker_registration_projection_preserves_existing_epoch() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let registration = BrokerRegistrationRecord {
        fenced: false,
        ..new_registration()
    };
    let mut rx = super::test_support::submit_on_engine(&mut engine, &[
        MetadataRecord::V1BrokerRegistration(registration),
    ]);
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let mut projection = engine.image.broker(NodeId(7)).unwrap().clone();
    let assigned_epoch = projection.broker_epoch;
    projection.log_dirs.clear();

    let mut rx = super::test_support::submit_on_engine(&mut engine, &[
        MetadataRecord::V1BrokerRegistration(projection),
    ]);

    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let stored = engine.image.broker(NodeId(7)).unwrap();
    assert2::assert!(stored.broker_epoch == assigned_epoch);
    assert2::assert!(stored.log_dirs.is_empty());
}

#[tokio::test]
async fn broker_registration_epoch_equals_commit_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

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
                features: BTreeMap::new(),
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
        features: BTreeMap::new(),
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

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let submit = |engine: &mut Engine, record: BrokerRegistrationRecord| {
        let mut rx =
            super::test_support::submit_on_engine(engine, &[MetadataRecord::V1BrokerRegistration(
                record,
            )]);
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

    let (ctrl, _dir) = build().call();
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    let commit = || async {
        super::test_support::commit_pending(&ctrl, NodeId(2)).await;
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
    time::sleep(Duration::from_millis(20)).await;
    commit().await;
    first.await.unwrap().unwrap();
    let registered = ctrl.current_image().broker(NodeId(7)).cloned().unwrap();
    let stale_unfence = BrokerRegistrationRecord {
        fenced: false,
        ..registered
    };

    let second = register();
    time::sleep(Duration::from_millis(20)).await;
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

/// A log value decoded with Kafka's metadata record schemas, and the version
/// of the schema it was written at.
#[derive(Debug, PartialEq)]
struct KafkaRecord {
    record: metadata::KraftMetadataRecord,
    version: i16,
}

/// Every value the log holds from `start`, decoded with Kafka's metadata
/// record schemas: what `kafka-dump-log --cluster-metadata-decoder` or a JVM
/// controller in a mixed quorum reads.
fn kafka_records_from(engine: &Engine, start: Offset) -> Vec<KafkaRecord> {
    engine
        .log
        .read_decoded(start, DEFAULT_METADATA_RAFT_FETCH_MAX)
        .expect("read the appended batches")
        .iter()
        .filter(|batch| !batch.attributes.is_control_batch())
        .flat_map(|batch| batch.records.iter())
        .filter_map(|record| record.value.as_ref())
        .map(|value| {
            let (record, version) = metadata::KraftMetadataRecord::decode_value(value)
                .expect("a Kafka metadata record");
            KafkaRecord { record, version }
        })
        .collect()
}

use rstest_bdd_macros::{given, scenarios, then, when};

struct RegistrationHistory {
    engine: Engine,
    _directory: tempfile::TempDir,
    records: Vec<KafkaRecord>,
    registration: Option<krabka_metadata::BrokerRegistrationRecord>,
    registered: Option<krabka_metadata::BrokerRegistrationRecord>,
}

#[rstest::fixture]
fn registration_history() -> RegistrationHistory {
    let (engine, directory) = super::test_support::single_voter_leader_engine();
    RegistrationHistory {
        engine,
        _directory: directory,
        records: vec![],
        registration: None,
        registered: None,
    }
}

#[given("a single voter controller leads the broker registration log")]
fn registration_leader(registration_history: &RegistrationHistory) {
    check!(
        registration_history
            .engine
            .image
            .broker(NodeId(7))
            .is_none()
    );
}

#[when("broker 7 performs registration transition {transition}")]
fn registration_transition(registration_history: &mut RegistrationHistory, transition: String) {
    use krabka_metadata::{
        BrokerRegistrationChangeRecord, FencingChange, MetadataRecord, UnregisterBrokerRecord,
    };
    let record = if transition == "register" {
        MetadataRecord::V1BrokerRegistration(new_registration())
    } else {
        let epoch = registration_history
            .registered
            .as_ref()
            .expect("broker registered")
            .broker_epoch;
        if transition == "unregister" {
            MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: NodeId(7),
                broker_epoch: epoch,
            })
        } else {
            let (fenced, in_controlled_shutdown) = match transition.as_str() {
                "unfence" => (FencingChange::Unfence, false),
                "shutdown" => (FencingChange::None, true),
                "fence" => (FencingChange::Fence, false),
                _ => panic!("unknown registration transition {transition}"),
            };
            MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
                fenced,
                in_controlled_shutdown,
                ..BrokerRegistrationChangeRecord::no_change(NodeId(7), epoch)
            })
        }
    };
    let start = registration_history.engine.log.log_end_offset();
    let mut receiver =
        super::test_support::submit_on_engine(&mut registration_history.engine, &[record]);
    receiver
        .try_recv()
        .expect("submission completed")
        .expect("registration transition committed");
    registration_history.records = kafka_records_from(&registration_history.engine, start);
    registration_history.registration =
        registration_history.engine.image.broker(NodeId(7)).cloned();
    if transition == "register" {
        registration_history.registered = registration_history.registration.clone();
    }
}

#[then("Kafka records a newly fenced broker with its assigned epoch")]
fn registered_kafka_record(registration_history: &RegistrationHistory) {
    use krabka_protocol::records::metadata::KraftMetadataRecord;
    let epoch = registration_history
        .registered
        .as_ref()
        .expect("broker registered")
        .broker_epoch;
    assert!(let [KafkaRecord { record: KraftMetadataRecord::RegisterBroker(written), .. }] = registration_history.records.as_slice());
    check!(
        (
            written.broker_epoch,
            written.fenced,
            written.in_controlled_shutdown
        ) == (epoch, true, false)
    );
}

#[then(
    "Kafka records fencing {fencing} shutdown {shutdown} at schema version {version} and the broker is fenced {fenced} shutting down {shutting_down}"
)]
fn registration_change_record(
    registration_history: &RegistrationHistory,
    fencing: i8,
    shutdown: i8,
    version: i16,
    fenced: bool,
    shutting_down: bool,
) {
    use krabka_protocol::{
        owned::broker_registration_change_record::BrokerRegistrationChangeRecord,
        records::metadata::KraftMetadataRecord,
    };
    let registered = registration_history
        .registered
        .as_ref()
        .expect("broker registered");
    check!(
        registration_history.records
            == vec![KafkaRecord {
                record: KraftMetadataRecord::BrokerRegistrationChange(
                    BrokerRegistrationChangeRecord {
                        broker_id: 7,
                        broker_epoch: registered.broker_epoch,
                        fenced: fencing,
                        in_controlled_shutdown: shutdown,
                        ..BrokerRegistrationChangeRecord::default()
                    }
                ),
                version,
            }]
    );
    check!(
        registration_history.registration
            == Some(krabka_metadata::BrokerRegistrationRecord {
                fenced,
                in_controlled_shutdown: shutting_down,
                ..registered.clone()
            })
    );
}

#[then("Kafka records unregistration at the assigned epoch and removes broker 7")]
fn unregistered_kafka_record(registration_history: &RegistrationHistory) {
    use krabka_protocol::{
        owned::unregister_broker_record::UnregisterBrokerRecord,
        records::metadata::KraftMetadataRecord,
    };
    let epoch = registration_history
        .registered
        .as_ref()
        .expect("broker registered")
        .broker_epoch;
    check!(
        registration_history.records
            == vec![KafkaRecord {
                record: KraftMetadataRecord::UnregisterBroker(UnregisterBrokerRecord {
                    broker_id: 7,
                    broker_epoch: epoch,
                    ..UnregisterBrokerRecord::default()
                }),
                version: 0,
            }]
    );
    check!(registration_history.registration.is_none());
}

scenarios!("tests/features/migrated/src/kraft/controller/tests_broker_registration.feature", fixtures = [registration_history: RegistrationHistory]);

/// krabka-io/krabka-broker#1009: a `BrokerRegistrationChangeRecord` built
/// before the broker registered again names an epoch the broker is no longer
/// registered at. Kafka's `replayRegistrationChange` would refuse it, so the
/// leader drops it instead of appending it: nothing reaches the log, the new
/// registration is untouched, and the batch it travels with still commits.
#[test]
fn a_registration_change_at_a_replaced_epoch_is_dropped() {
    use krabka_metadata::{BrokerRegistrationChangeRecord, FencingChange, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let submit = |engine: &mut Engine, records: &[MetadataRecord]| {
        let mut rx = super::test_support::submit_on_engine(engine, records);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    };
    submit(&mut engine, &[MetadataRecord::V1BrokerRegistration(
        new_registration(),
    )]);
    let first_epoch = engine.image.broker_epoch(NodeId(7)).expect("registered");
    submit(&mut engine, &[MetadataRecord::V1BrokerRegistration(
        new_registration(),
    )]);
    let reregistered = engine.image.broker(NodeId(7)).cloned();
    let start = engine.log.log_end_offset();

    let stale_unfence =
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            fenced: FencingChange::Unfence,
            ..BrokerRegistrationChangeRecord::no_change(NodeId(7), first_epoch)
        });
    let mut batch = topic_record("travels-with-the-stale-change");
    batch.push(stale_unfence);
    submit(&mut engine, &batch);

    // Only the topic's `TopicRecord` (2) and `PartitionRecord` (3) are
    // written, and no `BrokerRegistrationChangeRecord` (17).
    let written: Vec<_> = kafka_records_from(&engine, start)
        .into_iter()
        .map(|decoded| decoded.record.api_key())
        .collect();
    check!(written == vec![2, 3]);
    check!(
        engine
            .image
            .topic("travels-with-the-stale-change")
            .is_some()
    );
    check!(engine.image.broker(NodeId(7)).cloned() == reregistered);
    check!(reregistered.is_some_and(|registration| registration.fenced));
}

/// krabka-io/krabka-broker#825: a controller that restarts recovers every
/// broker's fence and controlled shutdown from the log, as Kafka replays
/// `RegisterBrokerRecord` and `BrokerRegistrationChangeRecord` into
/// `ClusterControlManager`.
#[test]
fn replay_recovers_fencing_and_controlled_shutdown() {
    use krabka_metadata::{
        BrokerRegistrationChangeRecord, BrokerRegistrationRecord, FencingChange, MetadataImage,
        MetadataRecord,
    };

    let registration = |node: u64| {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(node),
            incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
            ..new_registration()
        })
    };
    // Broker 7 unfences, broker 8 unfences and enters controlled shutdown,
    // broker 9 stays fenced.
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    for node in [7, 8, 9] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &[registration(node)]);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    let changes = [
        (7, FencingChange::Unfence, false),
        (8, FencingChange::Unfence, false),
        (8, FencingChange::None, true),
    ];
    for (node, fenced, in_controlled_shutdown) in changes {
        let epoch = engine.image.broker_epoch(NodeId(node)).expect("registered");
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(
            &[MetadataRecord::V1BrokerRegistrationChange(
                BrokerRegistrationChangeRecord {
                    fenced,
                    in_controlled_shutdown,
                    ..BrokerRegistrationChangeRecord::no_change(NodeId(node), epoch)
                },
            )],
            reply,
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    let fetch_cap = MetadataRaftFetchMax::default();
    replay_committed(&engine.log, &mut recovered, Offset(0), fetch_cap).expect("replay");

    let registrations = |image: &MetadataImage| {
        let mut brokers: Vec<BrokerRegistrationRecord> = image.brokers().cloned().collect();
        brokers.sort_by_key(|broker| broker.node_id);
        brokers
    };
    let flags: Vec<_> = registrations(&recovered)
        .iter()
        .map(|broker| {
            (
                broker.node_id.0,
                broker.fenced,
                broker.in_controlled_shutdown,
            )
        })
        .collect();
    check!(flags == vec![(7, false, false), (8, false, true), (9, true, false)]);
    check!(registrations(&recovered) == registrations(&engine.image));
}
