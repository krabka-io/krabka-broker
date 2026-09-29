//! Table-driven coverage of the existence check (#691), driven against the
//! real `handle` entry point through the dispatch registry and a real
//! in-process broker/metadata image, not against `existence::unknown_partitions`
//! in isolation.
//!
//! Each case seeds the image with one real topic, `a`, with one partition
//! (partition 0), the way KIP-516 existence checks expect it: a `V1Topic`
//! record **and** a matching `V1Partition` record, since `V1Topic.partitions`
//! does not survive the wire round trip on its own (#716). `missing` never
//! gets a record, so it stands in for an authorized topic the image does not
//! hold. Partition 5 of `a` stands in for a partition the image does not hold
//! even though its topic exists.

use std::{collections::HashSet, sync::Arc};

use assert2::check;
use krabka_ids::PartitionIndex;
use krabka_protocol::owned::{
    txn_offset_commit_request::{
        self, TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    },
    txn_offset_commit_response::TxnOffsetCommitResponse,
};

use crate::{
    codes,
    coordinator::{
        bootstrap::OFFSETS_TOPIC,
        partitioner::partition_for_group,
        persistence::OffsetCommitValue,
        unified::{
            GroupSeed,
            actor::GroupActorMessage,
            persistence_next_gen::{
                AssignedTopicPartitions, CurrentMemberAssignmentValue, CurrentTopicPartitions,
                MemberAssignmentState, MemberMetadataValue, TargetAssignmentMemberValue,
            },
        },
    },
    test_support::{
        GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer, principal,
        request_context, start_broker_with,
    },
};

const READ_ON_STAR: &str = "TransactionalId:Write+Group:Read+Topic:Read";
const NO_TOPIC_READ: &str = "TransactionalId:Write+Group:Read";

/// Adds topic `a` with one partition, led by this broker, to the metadata
/// image.
pub(super) async fn seed_topic_a(broker: &crate::broker::Broker) {
    seed_topic(broker, "a").await;
}

/// Adds `name` with one partition, led by this broker, to the metadata image.
/// `V1Topic` alone would not give the image a partition count or a partition
/// record after the wire round trip (#716), so this seeds both.
async fn seed_topic(broker: &crate::broker::Broker, name: &str) {
    let records = vec![
        krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: name.to_string(),
            topic_id: uuid::Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }),
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: name.to_string(),
            partition: 0,
            leader: broker.config.node_id,
            replicas: vec![broker.config.node_id],
            isr: vec![broker.config.node_id],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil()],
            partition_epoch: 0,
        }),
    ];
    broker
        .controller
        .submit_change(records)
        .await
        .unwrap_or_else(|error| panic!("seed topic {name}: {error}"));
}

/// Starts a broker that grants what the principal name says, waits until its
/// group and transaction coordinators serve, and seeds topic `a`.
async fn start_seeded_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
            GrantsInPrincipalName,
        ));
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    wait_for_coordinators(&handle).await;
    seed_topic_a(&handle.broker_arc_for_test()).await;
    (handle, dir)
}

/// Waits until the group coordinator serves `__consumer_offsets` and the
/// transaction coordinator has loaded `__transaction_state`, which KIP-890
/// makes every `TxnOffsetCommit` ask.
pub(super) async fn wait_for_coordinators(handle: &crate::broker::BrokerHandle) {
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    handle.wait_until_transaction_coordinator_ready().await;
    handle.wait_until_group_coordinator_ready().await;
}

/// Gives `transactional_id` an ongoing transaction at `(producer_id,
/// producer_epoch)` that already holds `group_id`'s `__consumer_offsets`
/// partition, the state `AddOffsetsToTxn` leaves. KIP-890 verifies a
/// `TxnOffsetCommit` against it before any offset is written.
pub(super) async fn open_transaction_for_group(
    broker: &crate::broker::Broker,
    transactional_id: &str,
    (producer_id, producer_epoch): (i64, i16),
    group_id: &str,
) {
    seed_transaction(
        broker,
        transactional_id,
        (producer_id, producer_epoch),
        (crate::txn::state::TxnState::Ongoing, Some(group_id)),
    )
    .await;
}

/// Gives `transactional_id` an entry in `state` at `(producer_id,
/// producer_epoch)`, holding `group_id`'s `__consumer_offsets` partition when
/// a group is named.
async fn seed_transaction(
    broker: &crate::broker::Broker,
    transactional_id: &str,
    (producer_id, producer_epoch): (i64, i16),
    (state, group_id): (crate::txn::state::TxnState, Option<&str>),
) {
    let image = broker.controller.current_image();
    let now_ms = crate::txn::util::now_millis();
    let mut entry = crate::txn::state::TxnEntry::new_empty(
        transactional_id.to_owned(),
        krabka_log::ProducerId(producer_id),
        producer_epoch,
        60_000,
        now_ms,
    );
    entry.state = state;
    entry.start_ms = now_ms;
    if let Some(group_id) = group_id {
        entry.partitions.insert(crate::txn::state::TopicPartition {
            topic: OFFSETS_TOPIC.to_owned(),
            partition: PartitionIndex(partition_for_group(&image, group_id)),
        });
    }
    broker
        .txn_coordinator
        .put(entry, crate::txn::version::resolve_txn_version(&image))
        .await
        .unwrap_or_else(|error| panic!("seed a transaction for {transactional_id}: {error}"));
}

pub(super) fn topic(name: &str, partitions: &[i32]) -> TxnOffsetCommitRequestTopic {
    TxnOffsetCommitRequestTopic {
        name: name.to_string(),
        partitions: partitions
            .iter()
            .map(|&partition_index| TxnOffsetCommitRequestPartition {
                partition_index,
                committed_offset: 100,
                committed_leader_epoch: 0,
                committed_metadata: None,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Whether `group_id`'s `__consumer_offsets` partition holds a record keyed
/// to `(group_id, topic, partition)`. Reads the real log the handler wrote
/// to, rather than trusting the response alone, so the test also catches a
/// response that claims success without a durable append (or the reverse).
pub(super) fn log_holds_key(
    broker: &crate::broker::Broker,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> bool {
    let image = broker.controller.current_image();
    let offsets_partition = partition_for_group(&image, group_id);
    let Some(part) = broker
        .partitions
        .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
    else {
        return false;
    };
    let log = part.log.lock().expect("lock offsets log");
    let Ok(read) = log.read(krabka_log::Offset(0), krabka_units::mebibytes(4)) else {
        return false;
    };
    let wanted = OffsetCommitValue::encode_key(group_id, topic, partition);
    read.batches
        .iter()
        .flat_map(|batch| batch.records.iter())
        .any(|record| record.key.as_ref() == Some(&wanted))
}

struct Case {
    name: &'static str,
    topics: Vec<TxnOffsetCommitRequestTopic>,
    grants: &'static str,
    expected: Vec<(&'static str, i32, i16)>,
    appended: Vec<(&'static str, i32)>,
}

/// The issue's table: one authorized existing partition, one authorized
/// partition the image does not hold (out of the topic's range), one
/// authorized topic the image does not hold at all, the same missing topic
/// denied outright, and a mixed request with one row of each outcome.
#[tokio::test]
async fn txn_offset_commit_runs_the_existence_check_after_the_topic_read_gate() {
    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();

    let cases = [
        Case {
            name: "authorized_existing_partition_commits",
            topics: vec![topic("a", &[0])],
            grants: READ_ON_STAR,
            expected: vec![("a", 0, codes::NONE)],
            appended: vec![("a", 0)],
        },
        Case {
            name: "authorized_partition_out_of_the_topics_range_is_unknown",
            topics: vec![topic("a", &[5])],
            grants: READ_ON_STAR,
            expected: vec![("a", 5, codes::UNKNOWN_TOPIC_OR_PARTITION)],
            appended: vec![],
        },
        Case {
            name: "authorized_topic_the_image_does_not_hold_is_unknown",
            topics: vec![topic("missing", &[0])],
            grants: READ_ON_STAR,
            expected: vec![("missing", 0, codes::UNKNOWN_TOPIC_OR_PARTITION)],
            appended: vec![],
        },
        Case {
            name: "denied_topic_is_topic_authorization_failed_not_unknown",
            topics: vec![topic("missing", &[0])],
            grants: NO_TOPIC_READ,
            expected: vec![("missing", 0, codes::TOPIC_AUTHORIZATION_FAILED)],
            appended: vec![],
        },
        Case {
            name: "mixed_request_appends_only_the_existing_partition",
            topics: vec![topic("a", &[0]), topic("missing", &[0])],
            grants: READ_ON_STAR,
            // Kafka's response builder holds the swept rows first.
            expected: vec![
                ("missing", 0, codes::UNKNOWN_TOPIC_OR_PARTITION),
                ("a", 0, codes::NONE),
            ],
            appended: vec![("a", 0)],
        },
    ];

    let address = peer();
    let version = 3; // flexible; a verify-only check against the open transaction
    for (case_index, case) in cases.into_iter().enumerate() {
        let group_id = format!("group-{case_index}");
        let user = principal(case.grants);
        let ctx = request_context(&user, &address, "txn-offset-commit-existence");
        let producer_id = 42 + i64::try_from(case_index).expect("small");
        let transactional_id = format!("tid-{case_index}");
        open_transaction_for_group(&broker, &transactional_id, (producer_id, 0), &group_id).await;
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: 0,
            topics: case.topics.clone(),
            ..Default::default()
        };
        let bytes = dispatch_context(
            &broker,
            txn_offset_commit_request::API_KEY,
            version,
            &encode_request(&request, version),
            &ctx,
        )
        .await;
        let response: TxnOffsetCommitResponse = decode_response(&bytes, version);

        let got: Vec<(String, i32, i16)> = response
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(|p| (t.name.clone(), p.partition_index, p.error_code))
            })
            .collect();
        let expected: Vec<(String, i32, i16)> = case
            .expected
            .iter()
            .map(|&(name, partition, code)| (name.to_string(), partition, code))
            .collect();
        check!(got == expected, "{}: response rows", case.name);

        // Every (topic, partition) the request named, whether it was
        // expected to append or not, so a wrongly-appended row is caught
        // too.
        let appended: HashSet<(&str, i32)> = case.appended.iter().copied().collect();
        for req_topic in &case.topics {
            for part in &req_topic.partitions {
                let key = (req_topic.name.as_str(), part.partition_index);
                let holds = log_holds_key(&broker, &group_id, key.0, key.1);
                check!(
                    holds == appended.contains(&key),
                    "{}: log holds {:?} == {}",
                    case.name,
                    key,
                    appended.contains(&key)
                );
            }
        }
    }

    handle.shutdown().await;
}

/// A v3+ request that both names an unknown row and fails group fencing (a
/// generation for a group the coordinator does not hold, which Kafka's
/// `validateTransactionalOffsetCommit` answers `ILLEGAL_GENERATION` below v6)
/// must keep `UNKNOWN_TOPIC_OR_PARTITION` on the unknown row rather than have
/// the fencing error overwrite it, and the valid row must still get the
/// fencing error and skip the append.
#[tokio::test]
async fn unknown_rows_survive_a_group_fencing_failure() {
    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();

    let address = peer();
    let version = 3;
    let group_id = "group-fencing";
    let user = principal(READ_ON_STAR);
    let ctx = request_context(&user, &address, "txn-offset-commit-fencing");
    open_transaction_for_group(&broker, "tid-fencing", (42, 0), group_id).await;
    let request = TxnOffsetCommitRequest {
        transactional_id: "tid-fencing".to_string(),
        group_id: group_id.to_string(),
        producer_id: 42,
        producer_epoch: 0,
        member_id: "never-registered-member".to_string(),
        generation_id_or_member_epoch: 0,
        topics: vec![topic("a", &[0]), topic("missing", &[0])],
        ..Default::default()
    };
    let bytes = dispatch_context(
        &broker,
        txn_offset_commit_request::API_KEY,
        version,
        &encode_request(&request, version),
        &ctx,
    )
    .await;
    let response: TxnOffsetCommitResponse = decode_response(&bytes, version);

    let got: Vec<(String, i32, i16)> = response
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(|p| (t.name.clone(), p.partition_index, p.error_code))
        })
        .collect();
    let expected = vec![
        ("missing".to_string(), 0, codes::UNKNOWN_TOPIC_OR_PARTITION),
        ("a".to_string(), 0, codes::ILLEGAL_GENERATION),
    ];
    check!(got == expected, "fenced response preserves unknown rows");

    check!(!log_holds_key(&broker, group_id, "a", 0));
    check!(!log_holds_key(&broker, group_id, "missing", 0));

    handle.shutdown().await;
}

/// The `OffsetCommitValue` the log holds for `(group_id, topic, partition)`.
fn logged_value(
    broker: &crate::broker::Broker,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Option<OffsetCommitValue> {
    let image = broker.controller.current_image();
    let part = broker.partitions.get(
        OFFSETS_TOPIC,
        PartitionIndex(partition_for_group(&image, group_id)),
    )?;
    let log = part.log.lock().expect("lock offsets log");
    let read = log
        .read(krabka_log::Offset(0), krabka_units::mebibytes(4))
        .ok()?;
    let wanted = OffsetCommitValue::encode_key(group_id, topic, partition);
    read.batches
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter(|record| record.key.as_ref() == Some(&wanted))
        .find_map(|record| OffsetCommitValue::decode_value(record.value.as_ref()?).ok())
}

/// Allows every request except `Read` on the topic `b`, so a case can tell
/// the topic `Read` gate apart from the others by the topic's name.
#[derive(Debug)]
struct DeniesReadOnB;

impl crate::authorizer::Authorizer for DeniesReadOnB {
    fn authorize(
        &self,
        _source: &dyn crate::authorizer::AclSource,
        request: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        if request.resource_type == krabka_metadata::ResourceType::Topic
            && request.operation == krabka_metadata::AclOperation::Read
            && request.resource_name == "b"
        {
            crate::authorizer::AuthorizationResult::Deny
        } else {
            crate::authorizer::AuthorizationResult::Allow
        }
    }
}

/// How one request topic names its topic.
#[derive(Clone, Copy)]
enum TopicRef {
    /// By name, as v0 to v5 do.
    Name(&'static str),
    /// By the id the image holds for this topic, as v6 does.
    IdOf(&'static str),
    /// By an id the image does not hold.
    Id(uuid::Uuid),
}

struct V6Case {
    name: &'static str,
    version: i16,
    topics: Vec<(TopicRef, &'static [i32])>,
    /// The response topics in order: who the topic is and its rows.
    expected: Vec<(TopicRef, Vec<(i32, i16)>)>,
    /// The `(topic, partition)` rows the log holds after the request, each
    /// with the topic id its offset records: the id the image holds at v6, and
    /// none below it, as Kafka 4.3.1 records.
    logged: Vec<(&'static str, i32)>,
}

/// #867, KIP-1319, against Kafka trunk's `KafkaApis.handleTxnOffsetCommitRequest`
/// and `KafkaApisTest.testHandleTxnOffsetCommitRequestTopicsAndPartitionsValidation`.
/// v6 names each topic by id. An id the image holds is authorized and checked
/// for existence under the topic's name, and its offset records the id. An id
/// the image does not hold, and the zero id, answer `UNKNOWN_TOPIC_ID` ahead
/// of the `Read` gate. The v6 response carries the topic ids, and the rows the
/// topic sweep settled lead the rows that reached the coordinator. v5 still
/// names the topic, and its offset records no id, as Kafka 4.3.1 records the
/// zero id for the versions it has.
#[tokio::test]
async fn v6_resolves_topic_ids_before_the_read_gate_and_the_existence_check() {
    use krabka_protocol::{
        owned::txn_offset_commit_response::{
            TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    };

    let (handle, _dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(DeniesReadOnB);
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    wait_for_coordinators(&handle).await;
    let broker = handle.broker_arc_for_test();
    seed_topic(&broker, "a").await;
    seed_topic(&broker, "b").await;
    let image = broker.controller.current_image();
    let id_of = |name: &str| image.topic(name).expect("seeded topic").topic_id;
    let dead = uuid::Uuid::from_u128(0xDEAD);
    let address = peer();
    let user = principal("user");
    let ctx = request_context(&user, &address, "txn-offset-commit-v6");

    let cases = [
        V6Case {
            name: "v5_names_the_topic_and_logs_no_id",
            version: 5,
            topics: vec![(TopicRef::Name("a"), &[0])],
            expected: vec![(TopicRef::Name("a"), vec![(0, codes::NONE)])],
            logged: vec![("a", 0)],
        },
        V6Case {
            name: "v6_known_id_commits_under_the_topic_name",
            version: 6,
            topics: vec![(TopicRef::IdOf("a"), &[0])],
            expected: vec![(TopicRef::IdOf("a"), vec![(0, codes::NONE)])],
            logged: vec![("a", 0)],
        },
        V6Case {
            name: "v6_unknown_id_is_unknown_topic_id",
            version: 6,
            topics: vec![(TopicRef::Id(dead), &[0])],
            expected: vec![(TopicRef::Id(dead), vec![(0, codes::UNKNOWN_TOPIC_ID)])],
            logged: vec![],
        },
        V6Case {
            name: "v6_zero_id_is_unknown_topic_id",
            version: 6,
            topics: vec![(TopicRef::Id(uuid::Uuid::nil()), &[0])],
            expected: vec![(
                TopicRef::Id(uuid::Uuid::nil()),
                vec![(0, codes::UNKNOWN_TOPIC_ID)],
            )],
            logged: vec![],
        },
        V6Case {
            name: "v6_known_id_missing_partition_is_unknown_topic_or_partition",
            version: 6,
            topics: vec![(TopicRef::IdOf("a"), &[5])],
            expected: vec![(
                TopicRef::IdOf("a"),
                vec![(5, codes::UNKNOWN_TOPIC_OR_PARTITION)],
            )],
            logged: vec![],
        },
        V6Case {
            name: "v6_read_is_authorized_by_the_resolved_name",
            version: 6,
            topics: vec![(TopicRef::IdOf("b"), &[0])],
            expected: vec![(
                TopicRef::IdOf("b"),
                vec![(0, codes::TOPIC_AUTHORIZATION_FAILED)],
            )],
            logged: vec![],
        },
        V6Case {
            name: "v6_swept_rows_lead_the_committed_ones",
            version: 6,
            topics: vec![
                (TopicRef::IdOf("a"), &[0, 5]),
                (TopicRef::IdOf("b"), &[0]),
                (TopicRef::Id(dead), &[0]),
            ],
            expected: vec![
                (
                    TopicRef::IdOf("a"),
                    vec![(5, codes::UNKNOWN_TOPIC_OR_PARTITION), (0, codes::NONE)],
                ),
                (
                    TopicRef::IdOf("b"),
                    vec![(0, codes::TOPIC_AUTHORIZATION_FAILED)],
                ),
                (TopicRef::Id(dead), vec![(0, codes::UNKNOWN_TOPIC_ID)]),
            ],
            logged: vec![("a", 0)],
        },
    ];

    // The request's name and id for `topic`, and the response's as `version`
    // decodes them: v6 carries only the id and v5 only the name.
    let request_key = |topic: TopicRef| match topic {
        TopicRef::Name(name) => (name.to_string(), WireUuid::default()),
        TopicRef::IdOf(name) => (String::new(), WireUuid(id_of(name).into_bytes())),
        TopicRef::Id(id) => (String::new(), WireUuid(id.into_bytes())),
    };
    for (row, case) in cases.into_iter().enumerate() {
        let group_id = format!("group-v6-{row}");
        let transactional_id = format!("tid-v6-{row}");
        let producer_id = 42 + i64::try_from(row).expect("small");
        open_transaction_for_group(&broker, &transactional_id, (producer_id, 0), &group_id).await;
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: 0,
            generation_id_or_member_epoch: -1,
            topics: case
                .topics
                .iter()
                .map(|&(topic_ref, partitions)| {
                    let (name, topic_id) = request_key(topic_ref);
                    TxnOffsetCommitRequestTopic {
                        topic_id,
                        ..topic(&name, partitions)
                    }
                })
                .collect(),
            ..Default::default()
        };
        let bytes = dispatch_context(
            &broker,
            txn_offset_commit_request::API_KEY,
            case.version,
            &encode_request(&request, case.version),
            &ctx,
        )
        .await;
        let response: TxnOffsetCommitResponse = decode_response(&bytes, case.version);

        let expected = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: case
                .expected
                .iter()
                .map(|(topic_ref, rows)| {
                    let (name, topic_id) = request_key(*topic_ref);
                    TxnOffsetCommitResponseTopic {
                        name,
                        topic_id,
                        partitions: rows
                            .iter()
                            .map(|&(partition_index, error_code)| {
                                TxnOffsetCommitResponsePartition {
                                    partition_index,
                                    error_code,
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        };
        check!(response == expected, "{}", case.name);

        let logged: Vec<(&str, i32, Option<uuid::Uuid>)> = ["a", "b"]
            .into_iter()
            .flat_map(|name| [0, 5].map(|partition| (name, partition)))
            .filter_map(|(name, partition)| {
                logged_value(&broker, &group_id, name, partition)
                    .map(|value| (name, partition, value.topic_id))
            })
            .collect();
        let want: Vec<(&str, i32, Option<uuid::Uuid>)> = case
            .logged
            .iter()
            .map(|&(name, partition)| (name, partition, (case.version >= 6).then(|| id_of(name))))
            .collect();
        check!(logged == want, "{}: logged offsets", case.name);
    }

    handle.shutdown().await;
}

/// KIP-1319 changes two answers of `validateTransactionalOffsetCommit` at v6:
/// a generation for a group the coordinator does not hold is
/// `GROUP_ID_NOT_FOUND` rather than `ILLEGAL_GENERATION`, and a member epoch
/// the consumer group refuses is `STALE_MEMBER_EPOCH` rather than
/// `ILLEGAL_GENERATION`.
#[tokio::test]
async fn v6_answers_group_id_not_found_where_older_versions_answer_illegal_generation() {
    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();
    let address = peer();
    let user = principal(READ_ON_STAR);
    let ctx = request_context(&user, &address, "txn-offset-commit-missing-group");
    let a_id = broker
        .controller
        .current_image()
        .topic("a")
        .expect("topic a")
        .topic_id;

    for (version, want) in [
        (3, codes::ILLEGAL_GENERATION),
        (5, codes::ILLEGAL_GENERATION),
        (6, codes::GROUP_ID_NOT_FOUND),
    ] {
        let group_id = format!("missing-group-v{version}");
        let transactional_id = format!("tid-missing-{version}");
        let producer_id = 42 + i64::from(version);
        open_transaction_for_group(&broker, &transactional_id, (producer_id, 0), &group_id).await;
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: 0,
            member_id: "member".into(),
            generation_id_or_member_epoch: 3,
            topics: vec![TxnOffsetCommitRequestTopic {
                topic_id: krabka_protocol::primitives::uuid::Uuid(a_id.into_bytes()),
                ..topic("a", &[0])
            }],
            ..Default::default()
        };
        let bytes = super::handle(
            &broker,
            version,
            1,
            &encode_request(&request, version),
            &ctx,
        )
        .await
        .expect("handle");
        let response: TxnOffsetCommitResponse = decode_response(&bytes, version);
        check!(
            response.topics[0].partitions[0].error_code == want,
            "version {version}"
        );
        check!(
            !log_holds_key(&broker, &group_id, "a", 0),
            "version {version}"
        );
    }

    handle.shutdown().await;
}

/// A consumer group whose one native member, `m`, is at member epoch 7 and
/// was assigned partition 0 of `topic_id` at epoch 5.
fn kip_1251_seed(topic_id: krabka_protocol::primitives::uuid::Uuid) -> GroupSeed {
    GroupSeed {
        group_epoch: 7,
        target_epoch: 7,
        members: [(
            "m".to_string(),
            MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "client".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["a".into()],
                subscribed_topic_regex: None,
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: None,
            },
        )]
        .into(),
        target_per_member: [(
            "m".to_string(),
            TargetAssignmentMemberValue {
                topic_partitions: vec![AssignedTopicPartitions {
                    topic_id,
                    partitions: vec![0],
                }],
            },
        )]
        .into(),
        current_per_member: [(
            "m".to_string(),
            CurrentMemberAssignmentValue {
                member_epoch: 7,
                previous_member_epoch: 6,
                state: MemberAssignmentState::Stable,
                assigned_partitions: vec![CurrentTopicPartitions {
                    topic_id,
                    partitions: vec![0],
                    assignment_epochs: Some(vec![5]),
                }],
                partitions_pending_revocation: vec![],
            },
        )]
        .into(),
        ..GroupSeed::default()
    }
}

/// Kafka's `commitTransactionalOffset` runs the KIP-1251 per-partition
/// validator: an older member epoch commits a partition assigned at or before
/// it. A refusal is `STALE_MEMBER_EPOCH` at v6 and `ILLEGAL_GENERATION`
/// below.
#[tokio::test]
async fn an_older_member_epoch_commits_a_partition_assigned_before_it() {
    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();
    let address = peer();
    let user = principal(READ_ON_STAR);
    let ctx = request_context(&user, &address, "txn-offset-commit-kip-1251");
    let a_id = krabka_protocol::primitives::uuid::Uuid(
        broker
            .controller
            .current_image()
            .topic("a")
            .expect("topic a")
            .topic_id
            .into_bytes(),
    );

    let rows = [
        (5, 7, codes::NONE),
        (5, 5, codes::NONE),
        (5, 4, codes::ILLEGAL_GENERATION),
        (6, 4, codes::STALE_MEMBER_EPOCH),
        (5, 8, codes::ILLEGAL_GENERATION),
        (6, 8, codes::STALE_MEMBER_EPOCH),
    ];
    let mut actual = Vec::new();
    for (i, &(version, epoch, _)) in rows.iter().enumerate() {
        let group_id = format!("kip-1251-{i}");
        let actor = broker.group_coordinator.get_or_create_consumer(&group_id);
        actor
            .tx
            .send(GroupActorMessage::Seed(kip_1251_seed(a_id)))
            .await
            .expect("seed");
        let transactional_id = format!("tid-kip-1251-{i}");
        let producer_id = 42 + i64::try_from(i).expect("small");
        open_transaction_for_group(&broker, &transactional_id, (producer_id, 0), &group_id).await;
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: 0,
            member_id: "m".into(),
            generation_id_or_member_epoch: epoch,
            topics: vec![TxnOffsetCommitRequestTopic {
                topic_id: a_id,
                ..topic("a", &[0])
            }],
            ..Default::default()
        };
        let bytes = super::handle(
            &broker,
            version,
            1,
            &encode_request(&request, version),
            &ctx,
        )
        .await
        .expect("handle");
        let response: TxnOffsetCommitResponse = decode_response(&bytes, version);
        actual.push((version, epoch, response.topics[0].partitions[0].error_code));
    }
    check!(actual == rows);

    handle.shutdown().await;
}

/// The transaction the coordinator holds for one verification case: its state,
/// its producer epoch, and whether it holds the group's offsets partition.
type HeldTransaction = (crate::txn::state::TxnState, i16, bool);

struct VerificationCase {
    name: &'static str,
    version: i16,
    /// `None` leaves the transactional id unknown to the coordinator.
    held: Option<HeldTransaction>,
    request_epoch: i16,
    expected: i16,
}

/// #1228, KIP-890 part 1: Kafka verifies the producer with the transaction
/// coordinator for every `TxnOffsetCommit` version, before it writes an offset
/// (`CoordinatorPartitionWriter.maybeStartTransactionVerification`). Below v5
/// it only checks that `AddOffsetsToTxn` added the group's offsets partition;
/// from v5 it adds the partition itself. A stale epoch is
/// `INVALID_PRODUCER_EPOCH`, as `AddPartitionsToTxnManager` translates the
/// coordinator's `PRODUCER_FENCED`.
#[tokio::test]
async fn txn_offset_commit_verifies_the_producer_with_the_transaction_coordinator() {
    use crate::txn::state::TxnState::{CompleteCommit, Empty, Ongoing, PrepareCommit};

    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();
    let address = peer();
    let user = principal(READ_ON_STAR);
    let ctx = request_context(&user, &address, "txn-offset-commit-verification");

    let cases = [
        VerificationCase {
            name: "v3 for a transactional id the coordinator does not know",
            version: 3,
            held: None,
            request_epoch: 5,
            expected: codes::INVALID_PRODUCER_ID_MAPPING,
        },
        VerificationCase {
            name: "v3 from a fenced producer",
            version: 3,
            held: Some((Ongoing, 6, true)),
            request_epoch: 5,
            expected: codes::INVALID_PRODUCER_EPOCH,
        },
        VerificationCase {
            name: "v3 when AddOffsetsToTxn never added the partition",
            version: 3,
            held: Some((Ongoing, 5, false)),
            request_epoch: 5,
            expected: codes::INVALID_TXN_STATE,
        },
        VerificationCase {
            name: "v4 knows TRANSACTION_ABORTABLE",
            version: 4,
            held: Some((Ongoing, 5, false)),
            request_epoch: 5,
            expected: codes::TRANSACTION_ABORTABLE,
        },
        VerificationCase {
            name: "v3 after the transaction completed",
            version: 3,
            held: Some((CompleteCommit, 5, false)),
            request_epoch: 5,
            expected: codes::INVALID_TXN_STATE,
        },
        VerificationCase {
            name: "v3 while the transaction is ending",
            version: 3,
            held: Some((PrepareCommit, 5, true)),
            request_epoch: 5,
            expected: codes::CONCURRENT_TRANSACTIONS,
        },
        VerificationCase {
            name: "v3 from the producer that added the partition",
            version: 3,
            held: Some((Ongoing, 5, true)),
            request_epoch: 5,
            expected: codes::NONE,
        },
        VerificationCase {
            name: "v5 from a fenced producer",
            version: 5,
            held: Some((Ongoing, 6, true)),
            request_epoch: 5,
            expected: codes::INVALID_PRODUCER_EPOCH,
        },
        VerificationCase {
            name: "v5 adds the partition to an empty transaction",
            version: 5,
            held: Some((Empty, 5, false)),
            request_epoch: 5,
            expected: codes::NONE,
        },
    ];

    for (row, case) in cases.into_iter().enumerate() {
        let group_id = format!("group-verified-{row}");
        let transactional_id = format!("tid-verified-{row}");
        let producer_id = 900 + i64::try_from(row).expect("small");
        if let Some((state, epoch, holds_offsets)) = case.held {
            seed_transaction(
                &broker,
                &transactional_id,
                (producer_id, epoch),
                (state, holds_offsets.then_some(group_id.as_str())),
            )
            .await;
        }
        let request = TxnOffsetCommitRequest {
            transactional_id,
            group_id: group_id.clone(),
            producer_id,
            producer_epoch: case.request_epoch,
            generation_id_or_member_epoch: -1,
            topics: vec![topic("a", &[0])],
            ..Default::default()
        };
        let bytes = dispatch_context(
            &broker,
            txn_offset_commit_request::API_KEY,
            case.version,
            &encode_request(&request, case.version),
            &ctx,
        )
        .await;
        let response: TxnOffsetCommitResponse = decode_response(&bytes, case.version);
        check!(
            response.topics[0].partitions[0].error_code == case.expected,
            "{}",
            case.name
        );
        check!(
            log_holds_key(&broker, &group_id, "a", 0) == (case.expected == codes::NONE),
            "{}: only an admitted producer writes",
            case.name
        );
    }

    handle.shutdown().await;
}

/// From v5 the verification adds the offsets partition to the transaction
/// whatever `transaction.version` the cluster finalized, as
/// `txnOffsetCommitRequestVersionToTransactionSupportedOperation` gives
/// `ADD_PARTITION` above v4. The coordinator records `TV_2` for the add.
#[tokio::test]
async fn a_v5_commit_adds_the_offsets_partition_on_a_transaction_version_1_cluster() {
    let (handle, _dir) = start_seeded_broker().await;
    let broker = handle.broker_arc_for_test();
    broker
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: krabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE.into(),
                level: 1,
            },
        )])
        .await
        .expect("finalize transaction.version 1");
    let address = peer();
    let user = principal(READ_ON_STAR);
    let ctx = request_context(&user, &address, "txn-offset-commit-tv1");

    let group_id = "group-tv1";
    seed_transaction(
        &broker,
        "tid-tv1",
        (700, 5),
        (crate::txn::state::TxnState::Ongoing, None),
    )
    .await;
    let request = TxnOffsetCommitRequest {
        transactional_id: "tid-tv1".to_string(),
        group_id: group_id.to_string(),
        producer_id: 700,
        producer_epoch: 5,
        generation_id_or_member_epoch: -1,
        topics: vec![topic("a", &[0])],
        ..Default::default()
    };
    let bytes = dispatch_context(
        &broker,
        txn_offset_commit_request::API_KEY,
        5,
        &encode_request(&request, 5),
        &ctx,
    )
    .await;
    let response: TxnOffsetCommitResponse = decode_response(&bytes, 5);
    check!(response.topics[0].partitions[0].error_code == codes::NONE);
    check!(log_holds_key(&broker, group_id, "a", 0));

    let entry = broker
        .txn_coordinator
        .get("tid-tv1")
        .expect("the transaction")
        .lock()
        .await
        .clone();
    let image = broker.controller.current_image();
    check!(
        entry.partitions
            == [crate::txn::state::TopicPartition {
                topic: OFFSETS_TOPIC.to_owned(),
                partition: PartitionIndex(partition_for_group(&image, group_id)),
            }]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    check!(entry.client_transaction_version == 2);

    handle.shutdown().await;
}

/// Kafka trunk records the topic id of a transactional commit at every
/// version, and 4.3.1 records none. The topic id follows the request version
/// and `unstable.api.versions.enable`, and a v5 request names its topic.
#[tokio::test]
async fn a_v5_commit_records_the_topic_id_only_under_unstable_api_versions() {
    // (unstable api versions on, the topic id the offset records)
    for (unstable, recorded) in [(false, false), (true, true)] {
        let (handle, _dir) = start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
                GrantsInPrincipalName,
            ));
            cfg.transaction_state_num_partitions = 1;
            cfg.transaction_state_replication_factor = 1;
            if unstable {
                cfg.features.unstable_api_versions =
                    crate::api_catalog::UnstableApiVersions::Enabled;
            }
        })
        .await;
        wait_for_coordinators(&handle).await;
        let broker = handle.broker_arc_for_test();
        seed_topic_a(&broker).await;
        let address = peer();
        let user = principal(READ_ON_STAR);
        let ctx = request_context(&user, &address, "txn-offset-commit-topic-id");

        let group_id = "group-topic-id";
        open_transaction_for_group(&broker, "tid-topic-id", (800, 0), group_id).await;
        let request = TxnOffsetCommitRequest {
            transactional_id: "tid-topic-id".to_string(),
            group_id: group_id.to_string(),
            producer_id: 800,
            producer_epoch: 0,
            generation_id_or_member_epoch: -1,
            topics: vec![topic("a", &[0])],
            ..Default::default()
        };
        let bytes = dispatch_context(
            &broker,
            txn_offset_commit_request::API_KEY,
            5,
            &encode_request(&request, 5),
            &ctx,
        )
        .await;
        let response: TxnOffsetCommitResponse = decode_response(&bytes, 5);
        check!(
            response.topics[0].partitions[0].error_code == codes::NONE,
            "unstable={unstable}"
        );

        let topic_id = broker
            .controller
            .current_image()
            .topic("a")
            .expect("topic a")
            .topic_id;
        check!(
            logged_value(&broker, group_id, "a", 0).map(|value| value.topic_id)
                == Some(recorded.then_some(topic_id)),
            "unstable={unstable}"
        );

        handle.shutdown().await;
    }
}
