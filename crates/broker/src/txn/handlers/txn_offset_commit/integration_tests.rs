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
/// group coordinator serves `__consumer_offsets`, and seeds topic `a`.
async fn start_seeded_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
            GrantsInPrincipalName,
        ));
    })
    .await;
    handle.wait_until_group_coordinator_ready().await;
    seed_topic_a(&handle.broker_arc_for_test()).await;
    (handle, dir)
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
    let version = 3; // flexible, no KIP-890 offsets-partition registration (v5+ only)
    for (case_index, case) in cases.into_iter().enumerate() {
        let group_id = format!("group-{case_index}");
        let user = principal(case.grants);
        let ctx = request_context(&user, &address, "txn-offset-commit-existence");
        let request = TxnOffsetCommitRequest {
            transactional_id: format!("tid-{case_index}"),
            group_id: group_id.clone(),
            producer_id: 42,
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

/// Finalizes `transaction.version` 1, so a v5+ commit takes no KIP-890
/// offsets-partition registration and needs no open transaction.
async fn transaction_version_1(broker: &crate::broker::Broker) {
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
    /// with the topic id its offset records.
    logged: Vec<(&'static str, i32)>,
}

/// #867, KIP-1319, against Kafka trunk's `KafkaApis.handleTxnOffsetCommitRequest`
/// and `KafkaApisTest.testHandleTxnOffsetCommitRequestTopicsAndPartitionsValidation`.
/// v6 names each topic by id. An id the image holds is authorized and checked
/// for existence under the topic's name, and its offset records the id. An id
/// the image does not hold, and the zero id, answer `UNKNOWN_TOPIC_ID` ahead
/// of the `Read` gate. The v6 response carries the topic ids, and the rows the
/// topic sweep settled lead the rows that reached the coordinator. v5 still
/// names the topic, and its offset records the id the image holds for that
/// name.
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
    })
    .await;
    handle.wait_until_group_coordinator_ready().await;
    let broker = handle.broker_arc_for_test();
    seed_topic(&broker, "a").await;
    seed_topic(&broker, "b").await;
    transaction_version_1(&broker).await;
    let image = broker.controller.current_image();
    let id_of = |name: &str| image.topic(name).expect("seeded topic").topic_id;
    let dead = uuid::Uuid::from_u128(0xDEAD);
    let address = peer();
    let user = principal("user");
    let ctx = request_context(&user, &address, "txn-offset-commit-v6");

    let cases = [
        V6Case {
            name: "v5_names_the_topic_and_logs_its_id",
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
        let request = TxnOffsetCommitRequest {
            transactional_id: format!("tid-v6-{row}"),
            group_id: group_id.clone(),
            producer_id: 42,
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
            .map(|&(name, partition)| (name, partition, Some(id_of(name))))
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
    transaction_version_1(&broker).await;
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
        let request = TxnOffsetCommitRequest {
            transactional_id: format!("tid-missing-{version}"),
            group_id: group_id.clone(),
            producer_id: 42,
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
    transaction_version_1(&broker).await;
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
        let request = TxnOffsetCommitRequest {
            transactional_id: format!("tid-kip-1251-{i}"),
            group_id: group_id.clone(),
            producer_id: 42 + i64::try_from(i).expect("small"),
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
