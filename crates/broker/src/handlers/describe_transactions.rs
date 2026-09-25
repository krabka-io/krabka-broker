//! `DescribeTransactions` (`api_key=65`, KIP-664). This admin RPC returns the
//! full state of every requested transactional id: the producer id and epoch,
//! the current state, the txn timeout, the start time, and the set of
//! `(topic, partition)` tuples enrolled in the current transaction.
//!
//! ## ACL
//!
//! `Describe` on `TransactionalId(name)`, for each tid. A deny gives that row
//! `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)` and clears every
//! other field. An unknown tid gives that row `TRANSACTIONAL_ID_NOT_FOUND
//! (105)`. This matches the JVM `KafkaApis` shape.
//!
//! Once a tid is authorized, each entry in its `topics` list still needs its
//! own `Describe` on `Topic(name)`: `KafkaApis.handleDescribeTransactionsRequest`
//! drops every topic the principal may not describe from the row, rather than
//! failing the whole request, and krabka matches that per-topic filter.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::{
            DescribeTransactionsResponse, TopicData, TransactionState,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    txn::state::{TxnEntry, TxnState},
};

fn txn_state_str(s: TxnState) -> &'static str {
    match s {
        TxnState::Empty => "Empty",
        TxnState::Ongoing => "Ongoing",
        TxnState::PrepareCommit => "PrepareCommit",
        TxnState::PrepareAbort => "PrepareAbort",
        TxnState::CompleteCommit => "CompleteCommit",
        TxnState::CompleteAbort => "CompleteAbort",
        TxnState::Dead => "Dead",
    }
}

/// Builds the `topics` list for one txn entry. It groups the entry's
/// `(topic, partition)` set by topic name. Topics come out in alphabetical
/// order, and partitions in ascending order. JVM clients do not depend on that
/// order, but deterministic output keeps the wire-snapshot tests stable.
fn topics_for(entry: &TxnEntry) -> Vec<TopicData> {
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for tp in &entry.partitions {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition.get());
    }
    by_topic
        .into_iter()
        .map(|(topic, mut parts)| {
            parts.sort_unstable();
            TopicData {
                topic,
                partitions: parts,
                ..Default::default()
            }
        })
        .collect()
}

/// The row `DescribeTransactions` answers for one authorized transactional id.
///
/// `entry` is the coordinator's live entry for `tid`, or `None` when the
/// coordinator holds none. An absent entry is Kafka's
/// `TRANSACTIONAL_ID_NOT_FOUND`: the tid was never initialized here, or the
/// KIP-98 expiry sweep tombstoned it out of `__transaction_state` and dropped
/// it. The two are indistinguishable to a client, in Kafka and here.
pub(crate) fn transaction_state_row(tid: &str, entry: Option<&TxnEntry>) -> TransactionState {
    // Kafka reports a `Dead` transaction as not found: the id and its metadata
    // are being expired.
    let Some(entry) = entry.filter(|entry| entry.state != TxnState::Dead) else {
        return TransactionState {
            error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
            transactional_id: tid.to_owned(),
            ..Default::default()
        };
    };
    TransactionState {
        error_code: codes::NONE,
        transactional_id: entry.transactional_id.clone(),
        transaction_state: txn_state_str(entry.state).to_string(),
        transaction_timeout_ms: entry.txn_timeout_ms,
        transaction_start_time_ms: entry.start_ms,
        // Unwrap into the raw-`i64` wire field.
        producer_id: entry.producer_id.get(),
        producer_epoch: entry.producer_epoch,
        topics: topics_for(entry),
        ..Default::default()
    }
}

#[tracing::instrument(
    name = "handle_describe_transactions",
    level = "info",
    skip_all,
    fields(api = "DescribeTransactions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeTransactionsRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image before
    // checking coordinator-ness, as EndTxn, AddPartitionsToTxn and
    // AddOffsetsToTxn do. Otherwise a stale `leader_partitions` cache can
    // answer TRANSACTIONAL_ID_NOT_FOUND, or stale transaction details, from a
    // broker that already lost leadership instead of NOT_COORDINATOR.
    let image = broker.controller.current_image();
    drop(
        broker
            .txn_coordinator
            .refresh_leader_partitions(&image)
            .await,
    );

    let mut rows: Vec<TransactionState> = Vec::with_capacity(req.transactional_ids.len());
    for tid in &req.transactional_ids {
        // ACL gate: per-tid `Describe` on `TransactionalId`.
        let allow = broker.config.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::TransactionalId,
                resource_name: tid.as_str(),
                operation: AclOperation::Describe,
            },
        );
        if allow == AuthorizationResult::Deny {
            rows.push(TransactionState {
                error_code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                transactional_id: tid.clone(),
                ..Default::default()
            });
            continue;
        }

        // Kafka `TransactionCoordinator.handleDescribeTransactions` refuses
        // an empty id, and `getTransactionState` answers the coordinator error
        // of the partition the id belongs to.
        if tid.is_empty() {
            rows.push(TransactionState {
                error_code: codes::INVALID_REQUEST,
                transactional_id: tid.clone(),
                ..Default::default()
            });
            continue;
        }
        if let Some(error_code) = broker.txn_coordinator.coordinator_error(tid.as_str()).await {
            rows.push(TransactionState {
                error_code,
                transactional_id: tid.clone(),
                ..Default::default()
            });
            continue;
        }

        // Look up the coordinator's local entry. Unknown → 105.
        let mut row = match broker.txn_coordinator.get(tid.as_str()) {
            None => transaction_state_row(tid, None),
            Some(handle) => {
                let entry = handle.lock().await;
                transaction_state_row(tid, Some(&entry))
            }
        };

        // Kafka's `handleDescribeTransactionsRequest` removes every topic the
        // principal may not `Describe`, even though the tid itself is
        // authorized. Batch-check the row's topics in one pass.
        let topic_decisions: std::collections::HashMap<String, AuthorizationResult> =
            authorize_topics(
                broker.config.authorizer.as_ref(),
                &*image,
                ctx.principal,
                ctx.peer,
                AclOperation::Describe,
                row.topics.iter().map(|t| t.topic.as_str()),
            )
            .into_iter()
            .map(|(topic, decision)| (topic.to_owned(), decision))
            .collect();
        row.topics
            .retain(|t| topic_decisions.get(&t.topic).copied() == Some(AuthorizationResult::Allow));

        rows.push(row);
    }

    let resp = DescribeTransactionsResponse {
        throttle_time_ms: 0,
        transaction_states: rows,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::{
        test_support::{peer, principal, start_broker_with_authorizer_no_audit as start_broker},
        txn::state::TopicPartition,
    };

    fn entry() -> TxnEntry {
        let mut e = TxnEntry::new_empty("tx".into(), krabka_log::ProducerId(100), 0, 60_000, 1_000);
        e.partitions.insert(TopicPartition {
            topic: "b".into(),
            partition: krabka_ids::PartitionIndex(2),
        });
        e.partitions.insert(TopicPartition {
            topic: "b".into(),
            partition: krabka_ids::PartitionIndex(0),
        });
        e.partitions.insert(TopicPartition {
            topic: "a".into(),
            partition: krabka_ids::PartitionIndex(1),
        });
        e
    }

    #[test]
    fn topics_for_groups_and_sorts() {
        let e = entry();
        let t = topics_for(&e);
        // Alphabetical topics, ascending partitions.
        let expected = vec![
            TopicData {
                topic: "a".to_string(),
                partitions: vec![1],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            TopicData {
                topic: "b".to_string(),
                partitions: vec![0, 2],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
        ];
        assert!(t == expected);
    }

    crate::test_support::wire_helpers!(
        DescribeTransactionsRequest,
        DescribeTransactionsResponse,
        client_id = "admin-client"
    );

    /// Kafka `TransactionCoordinator.handleDescribeTransactions` per requested
    /// id: `INVALID_REQUEST` for an empty id, the coordinator error of the id's
    /// partition, `TRANSACTIONAL_ID_NOT_FOUND` for an id this broker
    /// coordinates but holds nothing for, or holds a `Dead` entry for, and the
    /// row otherwise.
    #[tokio::test]
    async fn each_row_carries_kafkas_code() {
        use krabka_log::ProducerId;

        let version = krabka_protocol::owned::describe_transactions_response::MAX_VERSION;
        let (broker_handle, dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = &broker.txn_coordinator;
        let principal = principal("admin");
        let peer = peer();
        let context = test_context(&principal, &peer);

        // Two ids this broker coordinates, and one it does not.
        let (ongoing_id, dead_id) = ("tx-ongoing", "tx-dead");
        for (tid, state) in [(ongoing_id, TxnState::Ongoing), (dead_id, TxnState::Dead)] {
            let partition = coordinator.partition_for(tid);
            let partition_dir = crate::log_dir::partition_dir(
                dir.path(),
                crate::txn::bootstrap::TOPIC,
                partition.get(),
            );
            std::fs::create_dir_all(&partition_dir).expect("create the state directory");
            let log = krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
                .expect("open the state log");
            broker.partitions.insert(
                crate::txn::bootstrap::TOPIC.into(),
                partition,
                crate::broker::spawn_partition(
                    crate::txn::bootstrap::TOPIC.to_string(),
                    partition,
                    dir.path().to_path_buf(),
                    log,
                    crate::log_dir_status::LogDirRegistry::default(),
                    Arc::new(crate::producer_state::ProducerState::new()),
                    false,
                ),
            );
            coordinator.lead_state_partition_for_test(partition).await;
            let mut entry = TxnEntry::new_empty(
                tid.to_string(),
                ProducerId(if state == TxnState::Dead { 200 } else { 100 }),
                3,
                60_000,
                0,
            );
            entry.state = state;
            entry.start_ms = 7;
            coordinator
                .put(entry, crate::txn::version::TxnVersion::Classic)
                .await
                .expect("seed the transaction");
        }
        // An id this broker coordinates and holds nothing for.
        let empty_id = (0..1_000)
            .map(|n| format!("tx-empty-{n}"))
            .find(|tid| coordinator.partition_for(tid) == coordinator.partition_for(ongoing_id))
            .expect("an id in a led partition");
        // An id in a partition this broker does not lead.
        let foreign_id = (0..1_000)
            .map(|n| format!("tx-foreign-{n}"))
            .find(|tid| {
                let p = coordinator.partition_for(tid);
                p != coordinator.partition_for(ongoing_id)
                    && p != coordinator.partition_for(dead_id)
            })
            .expect("an id in an unled partition");

        let request = encode_request(
            &DescribeTransactionsRequest {
                transactional_ids: vec![
                    String::new(),
                    empty_id.clone(),
                    foreign_id.clone(),
                    dead_id.to_string(),
                    ongoing_id.to_string(),
                ],
                ..Default::default()
            },
            version,
        );
        let response: DescribeTransactionsResponse = decode_response(
            &handle(&broker, version, 1, &request, &context)
                .await
                .expect("describe"),
            version,
        );

        let not_found = |tid: &str| TransactionState {
            error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
            transactional_id: tid.to_owned(),
            ..Default::default()
        };
        assert!(
            response.transaction_states
                == vec![
                    TransactionState {
                        error_code: codes::INVALID_REQUEST,
                        ..Default::default()
                    },
                    not_found(&empty_id),
                    TransactionState {
                        error_code: codes::NOT_COORDINATOR,
                        transactional_id: foreign_id.clone(),
                        ..Default::default()
                    },
                    not_found(dead_id),
                    TransactionState {
                        error_code: codes::NONE,
                        transactional_id: ongoing_id.to_owned(),
                        transaction_state: "Ongoing".to_owned(),
                        transaction_timeout_ms: 60_000,
                        transaction_start_time_ms: 7,
                        producer_id: 100,
                        producer_epoch: 3,
                        ..Default::default()
                    },
                ]
        );
        broker_handle.shutdown().await;
    }

    /// One case of [`topics_are_filtered_by_per_topic_describe`]: the topic
    /// grant `user` gets (`None` means no topic ACL at all) and the topics
    /// the response row should keep.
    struct TopicFilterCase {
        user: &'static str,
        topic_grant: Option<(&'static str, AclOperation)>,
        expected_topics: Vec<TopicData>,
    }

    /// Kafka's `handleDescribeTransactionsRequest` removes every topic the
    /// principal may not `Describe` from the row, even though `Describe` on
    /// the transactional id itself is already granted. Table-driven over the
    /// topic grant, for one transaction holding `a-0` and `b-1`.
    #[tokio::test]
    async fn topics_are_filtered_by_per_topic_describe() {
        use krabka_log::ProducerId;

        let version = krabka_protocol::owned::describe_transactions_response::MAX_VERSION;
        let (broker_handle, dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = &broker.txn_coordinator;
        let peer = peer();
        let tid = "tx";

        let partition = coordinator.partition_for(tid);
        let partition_dir = crate::log_dir::partition_dir(
            dir.path(),
            crate::txn::bootstrap::TOPIC,
            partition.get(),
        );
        std::fs::create_dir_all(&partition_dir).expect("create the state directory");
        let log = krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
            .expect("open the state log");
        broker.partitions.insert(
            crate::txn::bootstrap::TOPIC.into(),
            partition,
            crate::broker::spawn_partition(
                crate::txn::bootstrap::TOPIC.to_string(),
                partition,
                dir.path().to_path_buf(),
                log,
                crate::log_dir_status::LogDirRegistry::default(),
                Arc::new(crate::producer_state::ProducerState::new()),
                false,
            ),
        );
        coordinator.lead_state_partition_for_test(partition).await;

        let mut entry = TxnEntry::new_empty(tid.to_string(), ProducerId(100), 3, 60_000, 0);
        entry.state = TxnState::Ongoing;
        entry.start_ms = 7;
        entry.partitions.insert(TopicPartition {
            topic: "a".into(),
            partition: krabka_ids::PartitionIndex(0),
        });
        entry.partitions.insert(TopicPartition {
            topic: "b".into(),
            partition: krabka_ids::PartitionIndex(1),
        });
        coordinator
            .put(entry, crate::txn::version::TxnVersion::Classic)
            .await
            .expect("seed the transaction");

        let topic_a = TopicData {
            topic: "a".to_string(),
            partitions: vec![0],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        let topic_b = TopicData {
            topic: "b".to_string(),
            partitions: vec![1],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };

        let cases = [
            TopicFilterCase {
                user: "star",
                topic_grant: Some(("*", AclOperation::Describe)),
                expected_topics: vec![topic_a.clone(), topic_b.clone()],
            },
            TopicFilterCase {
                user: "topic-a",
                topic_grant: Some(("a", AclOperation::Describe)),
                expected_topics: vec![topic_a.clone()],
            },
            TopicFilterCase {
                // Read implies Describe (see issue #649 for the separate
                // DENY-vs-ALLOW-implication bug; this relies only on the
                // already-correct ALLOW side of the implication table).
                user: "read-b",
                topic_grant: Some(("b", AclOperation::Read)),
                expected_topics: vec![topic_b.clone()],
            },
            TopicFilterCase {
                user: "none",
                topic_grant: None,
                expected_topics: vec![],
            },
        ];

        for TopicFilterCase {
            user,
            topic_grant,
            expected_topics,
        } in cases
        {
            if let Some((grant_topic, operation)) = topic_grant {
                crate::test_support::grant_topic_operation(
                    &broker_handle,
                    user,
                    grant_topic,
                    operation,
                )
                .await;
            }
            // `grant_topic_operation` grants on the `Topic` resource; grant
            // `Describe` on `TransactionalId(tid)` too, since that gate runs
            // first.
            broker
                .controller
                .submit_change(vec![krabka_metadata::MetadataRecord::V1AccessControlEntry(
                    krabka_metadata::AclEntry {
                        resource_type: krabka_metadata::ResourceType::TransactionalId,
                        resource_name: tid.to_string(),
                        pattern_type: krabka_metadata::PatternType::Literal,
                        principal: format!("User:{user}"),
                        host: "*".to_string(),
                        operation: AclOperation::Describe,
                        permission_type: krabka_metadata::PermissionType::Allow,
                    },
                )])
                .await
                .expect("commit transactional id acl");

            let principal = principal(user);
            let context = test_context(&principal, &peer);
            let request = encode_request(
                &DescribeTransactionsRequest {
                    transactional_ids: vec![tid.to_string()],
                    ..Default::default()
                },
                version,
            );
            let response: DescribeTransactionsResponse = decode_response(
                &handle(&broker, version, 1, &request, &context)
                    .await
                    .expect("describe"),
                version,
            );

            let expected = TransactionState {
                error_code: codes::NONE,
                transactional_id: tid.to_owned(),
                transaction_state: "Ongoing".to_owned(),
                transaction_timeout_ms: 60_000,
                transaction_start_time_ms: 7,
                producer_id: 100,
                producer_epoch: 3,
                topics: expected_topics,
                ..Default::default()
            };
            assert!(response.transaction_states == vec![expected], "user={user}");
        }
        broker_handle.shutdown().await;
    }
}
