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
        bootstrap::OFFSETS_TOPIC, partitioner::partition_for_group, persistence::OffsetCommitValue,
    },
    test_support::{
        GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer, principal,
        request_context, start_broker_with,
    },
};

const READ_ON_STAR: &str = "TransactionalId:Write+Group:Read+Topic:Read";
const NO_TOPIC_READ: &str = "TransactionalId:Write+Group:Read";

/// Adds topic `a` with one partition, led by this broker, to the metadata
/// image. `V1Topic` alone would not give the image a partition count or a
/// partition record after the wire round trip (#716), so this seeds both.
async fn seed_topic_a(broker: &crate::broker::Broker) {
    let records = vec![
        krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "a".to_string(),
            topic_id: uuid::Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }),
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: "a".to_string(),
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
        .unwrap_or_else(|error| panic!("seed topic a: {error}"));
}

fn topic(name: &str, partitions: &[i32]) -> TxnOffsetCommitRequestTopic {
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
fn log_holds_key(
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
    let (handle, _dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(GrantsInPrincipalName);
    })
    .await;
    let broker = handle.broker_arc_for_test();
    seed_topic_a(&broker).await;

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
            expected: vec![
                ("a", 0, codes::NONE),
                ("missing", 0, codes::UNKNOWN_TOPIC_OR_PARTITION),
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

/// A v3+ request that both names an unknown row and fails KIP-447 group
/// fencing (a non-empty, never-registered `member_id` against a fresh
/// classic group) must keep `UNKNOWN_TOPIC_OR_PARTITION` on the unknown row
/// rather than have the fencing error overwrite it, and the valid row must
/// still get the fencing error and skip the append.
#[tokio::test]
async fn unknown_rows_survive_a_group_fencing_failure() {
    let (handle, _dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(GrantsInPrincipalName);
    })
    .await;
    let broker = handle.broker_arc_for_test();
    seed_topic_a(&broker).await;

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
        generation_id: 0,
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
        ("a".to_string(), 0, codes::UNKNOWN_MEMBER_ID),
        ("missing".to_string(), 0, codes::UNKNOWN_TOPIC_OR_PARTITION),
    ];
    check!(got == expected, "fenced response preserves unknown rows");

    check!(!log_holds_key(&broker, group_id, "a", 0));
    check!(!log_holds_key(&broker, group_id, "missing", 0));

    handle.shutdown().await;
}
