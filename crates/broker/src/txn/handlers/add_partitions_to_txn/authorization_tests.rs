//! The authorization and partition checks of `AddPartitionsToTxn` (#686,
//! #849).
//!
//! Kafka's `KafkaApis.handleAddPartitionsToTxnRequest`:
//! - v4 and later come from brokers. The request needs `ClusterAction` on the
//!   cluster, else a top-level `CLUSTER_AUTHORIZATION_FAILED`. No
//!   transactional id or topic ACL is checked.
//! - v0 to v3 come from clients: `Write` on the transactional id, then
//!   `Write` on each non-internal topic. An internal topic is never
//!   authorized.
//! - A partition that is not authorized or does not exist fails the whole
//!   transaction: nothing is added, and the other partitions answer
//!   `OPERATION_NOT_ATTEMPTED`.
//!
//! Every case runs against one broker that coordinates every transactional
//! id and has one open transaction for each case.

use std::{collections::BTreeSet, sync::Arc};

use assert2::check;
use krabka_ids::PartitionIndex;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
};

use super::{
    handle,
    test_support::{seed_topic, topic, topic_result},
};
use crate::{
    authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
    coordinator::bootstrap::OFFSETS_TOPIC,
    test_support::{decode_response, encode_request, peer, principal, request_context},
    txn::state::TxnEntry,
};

/// A broker principal: `ClusterAction` on the cluster and nothing else.
const BROKER: &str = "broker";
/// A client principal: `Write` on every transactional id, and `Write` on the
/// topics in [`CLIENT_TOPICS`] only.
const CLIENT: &str = "client";
/// The topics [`CLIENT`] may write, an internal topic included.
const CLIENT_TOPICS: [&str; 3] = ["a", "missing", OFFSETS_TOPIC];

#[derive(Debug)]
struct Grants;

impl Authorizer for Grants {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let allowed = match (
            request.principal.name.as_str(),
            request.resource_type,
            request.operation,
        ) {
            (BROKER, ResourceType::Cluster, AclOperation::ClusterAction) => true,
            (CLIENT, ResourceType::TransactionalId, AclOperation::Write) => true,
            (CLIENT, ResourceType::Topic, AclOperation::Write) => {
                CLIENT_TOPICS.contains(&request.resource_name)
            }
            _ => false,
        };
        if allowed {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    }
}

/// What one case expects back.
#[derive(Debug)]
enum Expect {
    /// The top-level error code, and no rows.
    TopLevel(i16),
    /// These `(topic, partition, code)` rows.
    Rows(&'static [(&'static str, i32, i16)]),
}

struct Case {
    name: &'static str,
    version: i16,
    caller: &'static str,
    partitions: &'static [(&'static str, i32)],
    verify_only: bool,
    expect: Expect,
    /// The partitions in the transaction after the request.
    enlisted: &'static [(&'static str, i32)],
}

fn request(case: &Case, tid: &str, producer_id: i64) -> AddPartitionsToTxnRequest {
    let mut topics: Vec<(&str, Vec<i32>)> = Vec::new();
    for &(name, partition) in case.partitions {
        match topics.iter_mut().find(|(topic, _)| *topic == name) {
            Some((_, partitions)) => partitions.push(partition),
            None => topics.push((name, vec![partition])),
        }
    }
    let topics = topics
        .iter()
        .map(|(name, partitions)| topic(name, partitions))
        .collect();
    if case.version >= 4 {
        AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: tid.to_owned(),
                producer_id,
                producer_epoch: 2,
                verify_only: case.verify_only,
                topics,
                ..Default::default()
            }],
            ..Default::default()
        }
    } else {
        AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: tid.to_owned(),
            v3_and_below_producer_id: producer_id,
            v3_and_below_producer_epoch: 2,
            v3_and_below_topics: topics,
            ..Default::default()
        }
    }
}

fn expected(case: &Case, tid: &str) -> AddPartitionsToTxnResponse {
    let rows = match case.expect {
        Expect::TopLevel(error_code) => {
            return AddPartitionsToTxnResponse {
                error_code,
                ..Default::default()
            };
        }
        Expect::Rows(rows) => rows,
    };
    let mut topics: Vec<(&str, Vec<(i32, i16)>)> = Vec::new();
    for &(name, partition, code) in rows {
        match topics.iter_mut().find(|(topic, _)| *topic == name) {
            Some((_, partitions)) => partitions.push((partition, code)),
            None => topics.push((name, vec![(partition, code)])),
        }
    }
    let topic_results = topics
        .iter()
        .map(|(name, rows)| topic_result(name, rows))
        .collect();
    if case.version >= 4 {
        AddPartitionsToTxnResponse {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: tid.to_owned(),
                topic_results,
                ..Default::default()
            }],
            ..Default::default()
        }
    } else {
        AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: topic_results,
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn add_partitions_to_txn_authorizes_by_version_and_fails_the_whole_transaction() {
    use codes::{
        CLUSTER_AUTHORIZATION_FAILED as CLUSTER_DENIED, NONE,
        OPERATION_NOT_ATTEMPTED as NOT_ATTEMPTED, TOPIC_AUTHORIZATION_FAILED as TOPIC_DENIED,
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED as TID_DENIED, UNKNOWN_TOPIC_OR_PARTITION as UNKNOWN,
    };

    let cases = [
        Case {
            name: "v4, client grants, no ClusterAction",
            version: 4,
            caller: CLIENT,
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::TopLevel(CLUSTER_DENIED),
            enlisted: &[],
        },
        Case {
            name: "v4, ClusterAction only",
            version: 4,
            caller: BROKER,
            partitions: &[("a", 0), ("b", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NONE), ("b", 0, NONE)]),
            enlisted: &[("a", 0), ("b", 0)],
        },
        Case {
            name: "v5 verify-only, ClusterAction only, the partition is enlisted",
            version: 5,
            caller: BROKER,
            partitions: &[("a", 0)],
            verify_only: true,
            expect: Expect::Rows(&[("a", 0, NONE)]),
            enlisted: &[("a", 0), ("b", 0)],
        },
        Case {
            name: "v4, ClusterAction only, a partition that does not exist",
            version: 4,
            caller: BROKER,
            partitions: &[("a", 0), ("missing", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NOT_ATTEMPTED), ("missing", 0, UNKNOWN)]),
            enlisted: &[],
        },
        Case {
            name: "v3, ClusterAction only",
            version: 3,
            caller: BROKER,
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, TID_DENIED)]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on a and not on b",
            version: 3,
            caller: CLIENT,
            partitions: &[("a", 0), ("b", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NOT_ATTEMPTED), ("b", 0, TOPIC_DENIED)]),
            enlisted: &[],
        },
        Case {
            name: "v3, a partition that does not exist",
            version: 3,
            caller: CLIENT,
            partitions: &[("a", 0), ("missing", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NOT_ATTEMPTED), ("missing", 0, UNKNOWN)]),
            enlisted: &[],
        },
        Case {
            name: "v3, an internal topic with Write on it",
            version: 3,
            caller: CLIENT,
            partitions: &[(OFFSETS_TOPIC, 0)],
            verify_only: false,
            expect: Expect::Rows(&[(OFFSETS_TOPIC, 0, TOPIC_DENIED)]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on the transactional id and the topic",
            version: 3,
            caller: CLIENT,
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NONE)]),
            enlisted: &[("a", 0)],
        },
    ];

    let (handle_, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(Grants);
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = handle_.broker_arc_for_test();
    handle_.wait_until_controller_leader().await;
    handle_.wait_until_brokers_registered(1).await;
    crate::txn::bootstrap::ensure_topic(
        &broker.controller,
        1,
        1,
        &crate::txn::bootstrap::topic_configs(
            broker.config.transaction_state_segment_bytes,
            broker.config.transaction_state_min_isr,
        ),
    )
    .await
    .expect("bootstrap __transaction_state");
    seed_topic(&broker, "a", 1).await;
    seed_topic(&broker, "b", 1).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while broker
            .partitions
            .get(crate::txn::bootstrap::TOPIC, PartitionIndex(0))
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("__transaction_state-0 becomes local");
    let txnv = crate::txn::version::resolve_txn_version(&broker.controller.current_image());

    let address = peer();
    // The verify-only case reads the transaction that the case before it
    // filled, so the two share a transactional id.
    let mut tid = String::new();
    let mut producer_id = 0;
    for (index, case) in (0_i64..).zip(cases.iter()) {
        if !case.verify_only {
            tid = format!("tid-{index}");
            producer_id = 100 + index;
            broker
                .txn_coordinator
                .put(
                    TxnEntry::new_empty(
                        tid.clone(),
                        krabka_log::ProducerId(producer_id),
                        2,
                        30_000,
                        0,
                    ),
                    txnv,
                )
                .await
                .expect("seed the open transaction");
        }
        let user = principal(case.caller);
        let ctx = request_context(&user, &address, "add-partitions-authorization");
        let bytes = handle(
            &broker,
            case.version,
            1,
            &encode_request(&request(case, &tid, producer_id), case.version),
            &ctx,
        )
        .await
        .expect("handle");
        check!(
            decode_response::<AddPartitionsToTxnResponse>(&bytes, case.version)
                == expected(case, &tid),
            "{}",
            case.name
        );
        let enlisted: BTreeSet<(String, i32)> = broker
            .txn_coordinator
            .get(&tid)
            .expect("open transaction")
            .lock()
            .await
            .partitions
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition.get()))
            .collect();
        let want: BTreeSet<(String, i32)> = case
            .enlisted
            .iter()
            .map(|&(topic, partition)| (topic.to_owned(), partition))
            .collect();
        check!(enlisted == want, "{}", case.name);
    }
    handle_.shutdown().await;
}
