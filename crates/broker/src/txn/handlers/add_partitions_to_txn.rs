//! `AddPartitionsToTxn` (`api_key=24`). It registers one or more
//! (topic, partition) pairs with an ongoing transaction.
//!
//! Wire-format versions:
//!  - v0-3: one `(transactional_id, producer_id, producer_epoch, topics)` on
//!    the request, and `results_by_topic_v3_and_below` on the response.
//!  - v4-5: a batched `transactions` array on the request, and
//!    `results_by_transaction` on the response.
//!
//! This broker handles only the single-tid case, which is the only shape a
//! producer client ever sends. When a v4+ request carries more than one
//! transaction entry, the handler processes them all in sequence.
//!
//! ## ACL preamble
//!
//! For each transaction in the request:
//! * `Write` on `TransactionalId(tid)`. On a deny, every topic row in that
//!   transaction's results emits `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`
//!   on every partition.
//! * For each topic, `Write` on `Topic(name)`. On a deny, that topic's
//!   partition rows emit `TOPIC_AUTHORIZATION_FAILED (29)`.
//!
//! ## Write-freeze gate
//!
//! A topic that a KFC-9 write freeze covers never joins the transaction's
//! partition set, and every one of its partition rows emits
//! `POLICY_VIOLATION (44)`. This is the cheapest place to stop a transaction
//! from ever reaching a frozen topic.
//!
//! A producer that enlisted the partition before the freeze landed keeps its
//! ability to commit or abort. The gate refuses the *next* enlistment, and a
//! freeze never stops an open transaction from completing.
//!
//! The gate runs after both ACL checks and after the coordinator check. A
//! caller learns that it is unauthorized, or that it reached the wrong broker,
//! before it learns anything about the topic's freeze state.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use krabka_ids::PartitionIndex;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        add_partitions_to_txn_request::AddPartitionsToTxnRequest,
        add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
        common::{
            add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
            add_partitions_to_txn_response::{
                add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
                add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
            },
        },
    },
};
use krabka_security::Principal;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    freeze::resolve::resolve_topic_freeze,
    txn::state::TopicPartition,
};

#[tracing::instrument(
    name = "handle_add_partitions_to_txn",
    level = "info",
    skip_all,
    fields(api = "AddPartitionsToTxn", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let authorizer = broker.config.authorizer.as_ref();
    let mut cur: &[u8] = req_bytes;
    let req = AddPartitionsToTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness, to avoid a race.
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coord.refresh_leader_partitions(&image).await;

    let dependencies = HandlerDependencies {
        coord: &coord,
        image: &image,
        txnv,
        authorizer,
        principal: ctx.principal,
        peer: ctx.peer,
    };
    if version >= 4 {
        handle_v4(&dependencies, version, &req).await
    } else {
        handle_v3(&dependencies, version, &req).await
    }
}

#[derive(Clone, Copy)]
struct HandlerDependencies<'a> {
    coord: &'a crate::txn::coordinator::TxnCoordinator,
    image: &'a MetadataImage,
    txnv: crate::txn::version::TxnVersion,
    authorizer: &'a dyn Authorizer,
    principal: &'a Principal,
    peer: &'a SocketAddr,
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

async fn handle_v4(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    let &HandlerDependencies {
        coord,
        image,
        txnv,
        authorizer,
        principal,
        peer,
    } = dependencies;
    let mut results_by_transaction: Vec<AddPartitionsToTxnResult> =
        Vec::with_capacity(req.transactions.len());

    for txn in &req.transactions {
        // ── ACL preamble: per-txn Write on TransactionalId ─────
        let tid_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: txn.transactional_id.as_str(),
            operation: AclOperation::Write,
        };
        let topic_results = if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
            topic_error(&txn.topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        } else {
            // Per-topic Write check, then the per-topic freeze read.
            let denied = denied_topics(authorizer, image, principal, peer, &txn.topics);
            let frozen = frozen_topics(image, &txn.topics);
            process_one_txn(
                coord,
                TransactionRequest {
                    transactional_id: txn.transactional_id.as_str(),
                    producer_id: krabka_log::ProducerId(txn.producer_id),
                    producer_epoch: txn.producer_epoch,
                    topics: &txn.topics,
                    denied: &denied,
                    frozen: &frozen,
                    txnv,
                    verify_only: txn.verify_only,
                },
            )
            .await
        };
        results_by_transaction.push(AddPartitionsToTxnResult {
            transactional_id: txn.transactional_id.clone(),
            topic_results,
            ..Default::default()
        });
    }

    let resp = AddPartitionsToTxnResponse {
        results_by_transaction,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── v0-3 path ─────────────────────────────────────────────────────────────────

async fn handle_v3(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    let &HandlerDependencies {
        coord,
        image,
        txnv,
        authorizer,
        principal,
        peer,
    } = dependencies;
    // ── ACL preamble: Write on TransactionalId ────────────────
    let tid_req = AuthorizationRequest {
        principal,
        host: peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: req.v3_and_below_transactional_id.as_str(),
        operation: AclOperation::Write,
    };
    let topic_results = if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
        topic_error(
            &req.v3_and_below_topics,
            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        )
    } else {
        let denied = denied_topics(authorizer, image, principal, peer, &req.v3_and_below_topics);
        let frozen = frozen_topics(image, &req.v3_and_below_topics);
        process_one_txn(
            coord,
            TransactionRequest {
                transactional_id: req.v3_and_below_transactional_id.as_str(),
                producer_id: krabka_log::ProducerId(req.v3_and_below_producer_id),
                producer_epoch: req.v3_and_below_producer_epoch,
                topics: &req.v3_and_below_topics,
                denied: &denied,
                frozen: &frozen,
                txnv,
                // v0-3 has no `verify_only` field (predates KIP-890); always add.
                verify_only: false,
            },
        )
        .await
    };

    let resp = AddPartitionsToTxnResponse {
        results_by_topic_v3_and_below: topic_results,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── shared per-transaction logic ──────────────────────────────────────────────

/// Builds the set of topic names that the authorizer denies `Write` on
/// `Topic(name)` for this principal and host. The caller uses the set to stamp
/// `TOPIC_AUTHORIZATION_FAILED` on every partition row of a denied topic.
fn denied_topics(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    principal: &Principal,
    peer: &SocketAddr,
    topics: &[AddPartitionsToTxnTopic],
) -> std::collections::HashSet<String> {
    let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
    let map = authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Write,
        names,
    );
    map.into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Builds the set of topic names that a KFC-9 write freeze covers.
///
/// It runs once per transaction entry, beside [`denied_topics`] and for the
/// same reason: a freeze is a property of the topic, not of a partition, so
/// each partition row then costs one set lookup. On a cluster with no freeze
/// the image answers every topic in two emptiness tests and the set stays
/// empty.
fn frozen_topics(
    image: &MetadataImage,
    topics: &[AddPartitionsToTxnTopic],
) -> std::collections::HashSet<String> {
    topics
        .iter()
        .filter(|t| resolve_topic_freeze(image, &t.name).is_some())
        .map(|t| t.name.clone())
        .collect()
}

/// The refusal that one topic's partition rows carry before the transaction
/// state machine has any say, or `None` when the topic is free to proceed.
///
/// The `Write` ACL deny outranks the freeze. An unauthorized principal learns
/// that it is unauthorized and nothing more: answering `POLICY_VIOLATION`
/// instead would leak the topic's freeze state to a caller with no right to
/// read it.
fn topic_refusal(
    name: &str,
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
) -> Option<i16> {
    if denied.contains(name) {
        Some(codes::TOPIC_AUTHORIZATION_FAILED)
    } else if frozen.contains(name) {
        Some(codes::POLICY_VIOLATION)
    } else {
        None
    }
}

/// Processes one `transactional_id`, `producer_id`, and `producer_epoch`
/// triple. It returns per-topic and per-partition result entries. A topic
/// named in `denied` short-circuits with `TOPIC_AUTHORIZATION_FAILED`, and one
/// named in `frozen` with `POLICY_VIOLATION`. Every other topic goes through
/// the state-machine check and the partition registration.
// cargo-mutants: I/O over live txn state + partition registration
struct TransactionRequest<'a> {
    transactional_id: &'a str,
    producer_id: krabka_log::ProducerId,
    producer_epoch: i16,
    topics: &'a [AddPartitionsToTxnTopic],
    denied: &'a std::collections::HashSet<String>,
    frozen: &'a std::collections::HashSet<String>,
    txnv: crate::txn::version::TxnVersion,
    verify_only: bool,
}

#[cfg_attr(test, mutants::skip)]
async fn process_one_txn(
    coord: &crate::txn::coordinator::TxnCoordinator,
    request: TransactionRequest<'_>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let TransactionRequest {
        transactional_id: tid,
        producer_id,
        producer_epoch,
        topics,
        denied,
        frozen,
        txnv,
        verify_only,
    } = request;
    // Topics allowed to proceed past the per-topic Write ACL gate and the
    // write-freeze gate. A frozen topic never joins the partition set, which
    // is what keeps the transaction from ever reaching its log.
    let allowed_topics: Vec<&AddPartitionsToTxnTopic> = topics
        .iter()
        .filter(|t| !denied.contains(&t.name) && !frozen.contains(&t.name))
        .collect();

    // 1. Coordinator check (applies only to non-denied topics — for
    //    denied topics we always emit TOPIC_AUTHORIZATION_FAILED).
    //
    //    It runs ahead of the freeze gate, so this path passes no freeze set
    //    down. A client that reached the wrong broker has to learn that first:
    //    it then retries at the real coordinator, which is the broker that
    //    owns the decision and answers the freeze.
    if !coord.is_coordinator_for(tid).await {
        let unread = std::collections::HashSet::new();
        return per_topic_with_refusals(topics, denied, &unread, codes::NOT_COORDINATOR);
    }

    // 2. Look up entry for the TV_2 verify-only path.
    let Some(entry_mutex) = coord.get(tid) else {
        return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_PRODUCER_ID_MAPPING);
    };
    if txnv.verified() && verify_only {
        let entry = entry_mutex.lock().await;
        if entry.has_staged_producer_identity() {
            return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_TXN_STATE);
        }
        if entry.producer_id != producer_id || entry.producer_epoch != producer_epoch {
            return per_topic_with_refusals(topics, denied, frozen, codes::INVALID_PRODUCER_EPOCH);
        }
        return verify_partitions(&entry, topics, denied, frozen);
    }
    drop(entry_mutex);

    let partitions = allowed_topics
        .into_iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|&partition| TopicPartition {
                topic: topic.name.clone(),
                partition: PartitionIndex(partition),
            })
        })
        .collect();
    let code = coord
        .register_partitions(tid, producer_id, producer_epoch, partitions, txnv)
        .await;
    per_topic_with_refusals(topics, denied, frozen, code)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// KIP-890 `TV_2` verify-only per-partition decision. It gives `NONE (0)` when
/// the partition is already part of the ongoing transaction, and
/// `TRANSACTION_ABORTABLE (120)` in every other case. This matches the
/// verify-only path in cp-kafka 4.0:
/// `if txnMetadata.topicPartitions.contains(part) NONE else TRANSACTION_ABORTABLE`.
fn verify_partition_code(entry: &crate::txn::state::TxnEntry, tp: &TopicPartition) -> i16 {
    if entry.partitions.contains(tp) {
        codes::NONE
    } else {
        codes::TRANSACTION_ABORTABLE
    }
}

/// Builds the verify-only response. It has the same shape as
/// `per_topic_with_refusals` on the add path, but each partition carries its
/// own verify result instead of one shared code. A denied or frozen topic
/// still short-circuits to its refusal on every partition row.
fn verify_partitions(
    entry: &crate::txn::state::TxnEntry,
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let refusal = topic_refusal(&t.name, denied, frozen);
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| {
                        let row_code = refusal.unwrap_or_else(|| {
                            verify_partition_code(
                                entry,
                                &TopicPartition {
                                    topic: t.name.clone(),
                                    partition: PartitionIndex(p),
                                },
                            )
                        });
                        AddPartitionsToTxnPartitionResult {
                            partition_index: p,
                            partition_error_code: row_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Builds a per-topic and per-partition result list. A topic named in `denied`
/// gets `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row, and one
/// named in `frozen` gets `POLICY_VIOLATION (44)`. Every other topic gets
/// `code`.
fn per_topic_with_refusals(
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let row_code = topic_refusal(&t.name, denied, frozen).unwrap_or(code);
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| AddPartitionsToTxnPartitionResult {
                        partition_index: p,
                        partition_error_code: row_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Builds a per-topic and per-partition result list in which every partition
/// carries `error_code`. Whole-transaction errors use it, such as the txn-id
/// ACL deny path.
fn topic_error(
    topics: &[AddPartitionsToTxnTopic],
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| AddPartitionsToTxnTopicResult {
            name: t.name.clone(),
            results_by_partition: t
                .partitions
                .iter()
                .map(|&p| AddPartitionsToTxnPartitionResult {
                    partition_index: p,
                    partition_error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

fn encode_response(resp: &AddPartitionsToTxnResponse, version: i16) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert2::{assert, check};
    use krabka_metadata::{MetadataRecord, PatternType, TopicFreezeRecord};
    use krabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnTransaction;
    use krabka_security::Principal;
    use uuid::Uuid;

    use super::*;
    use crate::{
        test_support::{DenyAll, peer},
        txn::state::TxnEntry,
    };

    #[test]
    fn verify_only_codes_present_vs_absent() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        let present = TopicPartition {
            topic: "a".into(),
            partition: PartitionIndex(0),
        };
        e.partitions.insert(present.clone());
        let absent = TopicPartition {
            topic: "b".into(),
            partition: PartitionIndex(0),
        };
        assert!(verify_partition_code(&e, &present) == codes::NONE);
        assert!(verify_partition_code(&e, &absent) == codes::TRANSACTION_ABORTABLE);
    }

    fn topic(name: &str, partitions: &[i32]) -> AddPartitionsToTxnTopic {
        AddPartitionsToTxnTopic {
            name: name.into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }
    }

    /// Builds a fully pinned expected topic-result row. Every field is
    /// explicit, so that whole-value comparisons kill field-drop mutants.
    fn topic_result(name: &str, rows: &[(i32, i16)]) -> AddPartitionsToTxnTopicResult {
        AddPartitionsToTxnTopicResult {
            name: name.into(),
            results_by_partition: rows
                .iter()
                .map(
                    |&(partition_index, partition_error_code)| AddPartitionsToTxnPartitionResult {
                        partition_index,
                        partition_error_code,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                )
                .collect(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    #[test]
    fn verify_partitions_preserves_topic_and_partition_rows() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        e.partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: PartitionIndex(1),
        });
        let topics = vec![topic("alpha", &[1, 2]), topic("denied", &[3])];
        let denied = HashSet::from(["denied".to_string()]);

        let frozen = HashSet::from(["frozen".to_string()]);
        let topics = [topics, vec![topic("frozen", &[4])]].concat();

        let rows = verify_partitions(&e, &topics, &denied, &frozen);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NONE), (2, codes::TRANSACTION_ABORTABLE)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(4, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn per_topic_with_refusals_preserves_rows_and_overrides_refused_topics() {
        let topics = vec![
            topic("alpha", &[1, 2]),
            topic("denied", &[3]),
            topic("frozen", &[4]),
        ];
        let denied = HashSet::from(["denied".to_string()]);
        let frozen = HashSet::from(["frozen".to_string()]);

        let rows = per_topic_with_refusals(&topics, &denied, &frozen, codes::NOT_COORDINATOR);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NOT_COORDINATOR), (2, codes::NOT_COORDINATOR)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(4, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn topic_error_preserves_each_requested_partition() {
        let topics = vec![topic("alpha", &[4, 5])];

        let rows = topic_error(&topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

        let expected = vec![topic_result(
            "alpha",
            &[
                (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                (5, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            ],
        )];
        assert!(rows == expected);
    }

    crate::test_support::wire_helpers!(
        AddPartitionsToTxnRequest,
        AddPartitionsToTxnResponse,
        client_id = "producer-client"
    );

    #[test]
    fn encode_response_round_trips_v4_transaction_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: topic_error(&[topic("alpha", &[1])], codes::INVALID_TXN_STATE),
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = encode_response(&resp, 4).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 4);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: vec![topic_result("alpha", &[(1, codes::INVALID_TXN_STATE)])],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }

    #[test]
    fn encode_response_round_trips_v3_topic_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: topic_error(&[topic("alpha", &[7])], codes::NONE),
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 3);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result("alpha", &[(7, codes::NONE)])],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }

    fn principal() -> Principal {
        crate::test_support::principal("ANONYMOUS")
    }

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    #[tokio::test]
    async fn handle_v4_transactional_id_deny_returns_transaction_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let req = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: "tid-4".into(),
                producer_id: 11,
                producer_epoch: 2,
                verify_only: false,
                topics: vec![topic("alpha", &[1, 2])],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, 4);

        let bytes = handle(
            &broker_handle.broker_arc_for_test(),
            4,
            123,
            &req_bytes,
            &ctx,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, 4);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: vec![topic_result(
                    "alpha",
                    &[
                        (1, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                        (2, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                    ],
                )],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_v3_transactional_id_deny_returns_topic_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let req = AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: "tid-3".into(),
            v3_and_below_producer_id: 11,
            v3_and_below_producer_epoch: 2,
            v3_and_below_topics: vec![topic("alpha", &[3, 4])],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, 3);

        let bytes = handle(
            &broker_handle.broker_arc_for_test(),
            3,
            123,
            &req_bytes,
            &ctx,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, 3);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result(
                "alpha",
                &[
                    (3, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                    (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                ],
            )],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    // ── KFC-9 topic write freeze ─────────────────────────────────────

    /// The transactional id every freeze case drives.
    const TID: &str = "tid-freeze";
    /// The frozen topic in every freeze case, and the unfrozen control that
    /// travels in the same request.
    const FROZEN_TOPIC: &str = "tenant-a.orders";
    const UNFROZEN_TOPIC: &str = "events";

    fn freeze_record(scope: &str, pattern_type: PatternType) -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }
    }

    fn image_with_freezes(scopes: &[(&str, PatternType)]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(0x5150));
        for &(scope, pattern_type) in scopes {
            image.apply(&MetadataRecord::V1TopicFreeze(freeze_record(
                scope,
                pattern_type,
            )));
        }
        image
    }

    #[test]
    fn frozen_topics_keeps_the_covered_names_and_nothing_else() {
        let image = image_with_freezes(&[
            ("orders", PatternType::Literal),
            ("tenant-a.", PatternType::Prefixed),
        ]);

        for (label, names, want) in [
            (
                "a literal freeze covers the one topic it names",
                vec!["orders"],
                vec!["orders"],
            ),
            (
                "a prefix freeze covers every topic under it",
                vec!["tenant-a.billing"],
                vec!["tenant-a.billing"],
            ),
            ("an unfrozen topic is left out", vec!["events"], vec![]),
            (
                "an internal topic is never frozen",
                vec!["__consumer_offsets"],
                vec![],
            ),
            (
                "one request mixing all of them keeps only the covered names",
                vec!["orders", "tenant-a.billing", "events"],
                vec!["orders", "tenant-a.billing"],
            ),
        ] {
            let topics: Vec<_> = names.iter().map(|name| topic(name, &[0])).collect();
            let expected: HashSet<String> =
                want.iter().map(|name| (*name).to_string()).collect();
            check!(frozen_topics(&image, &topics) == expected, "{label}");
        }
    }

    #[test]
    fn frozen_topics_is_empty_on_a_cluster_with_no_freeze() {
        let image = image_with_freezes(&[]);
        let topics = vec![topic("orders", &[0]), topic("tenant-a.billing", &[1])];

        check!(frozen_topics(&image, &topics) == HashSet::new());
    }

    #[test]
    fn topic_refusal_ranks_the_acl_deny_above_the_freeze() {
        let denied = HashSet::from(["denied".to_string(), "both".to_string()]);
        let frozen = HashSet::from(["frozen".to_string(), "both".to_string()]);

        for (label, name, want) in [
            ("a topic under neither refusal proceeds", "clean", None),
            (
                "a denied topic reports the ACL deny",
                "denied",
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                "a frozen topic reports the freeze",
                "frozen",
                Some(codes::POLICY_VIOLATION),
            ),
            (
                "a denied and frozen topic never leaks the freeze state",
                "both",
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
        ] {
            check!(topic_refusal(name, &denied, &frozen) == want, "{label}");
        }
    }

    /// Allows every operation except `Write` on a `Topic`. The
    /// transactional-id preamble passes, so a test that installs it reaches
    /// the per-topic gates that follow.
    #[derive(Debug)]
    struct DenyTopicWrites;

    impl Authorizer for DenyTopicWrites {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if req.resource_type == ResourceType::Topic && req.operation == AclOperation::Write {
                AuthorizationResult::Deny
            } else {
                AuthorizationResult::Allow
            }
        }
    }

    async fn wait_until(label: &str, mut ready: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready() {
            assert!(std::time::Instant::now() <= deadline, "timed out: {label}");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// A broker that coordinates every transactional id, holds one open
    /// transaction under [`TID`], and carries one live freeze in its image.
    ///
    /// `transaction_state_num_partitions = 1` puts every tid on partition 0,
    /// so the coordinator check passes and the freeze gate behind it runs.
    async fn start_frozen_coordinator(
        authorizer: Arc<dyn crate::authorizer::Authorizer>,
        freeze: (&str, PatternType),
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (handle, dir) = crate::test_support::start_broker_with(move |cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
            cfg.transaction_state_num_partitions = 1;
            cfg.transaction_state_replication_factor = 1;
        })
        .await;
        let broker = handle.broker_arc_for_test();
        wait_until("the broker becomes controller leader", || {
            broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|node| node == broker.config.node_id)
        })
        .await;
        wait_until("the broker registers itself", || {
            broker.controller.current_image().brokers().next().is_some()
        })
        .await;
        crate::txn::bootstrap::ensure_topic(&broker.controller, 1, 1)
            .await
            .expect("bootstrap __transaction_state");
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1TopicFreeze(freeze_record(
                freeze.0, freeze.1,
            ))])
            .await
            .expect("submit the freeze");
        wait_until("__transaction_state-0 becomes local", || {
            broker
                .partitions
                .get(crate::txn::bootstrap::TOPIC, PartitionIndex(0))
                .is_some()
        })
        .await;
        let txnv =
            crate::txn::version::resolve_txn_version(&broker.controller.current_image());
        broker
            .txn_coordinator
            .put(
                TxnEntry::new_empty(TID.to_owned(), krabka_log::ProducerId(11), 2, 30_000, 0),
                txnv,
            )
            .await
            .expect("seed the open transaction");
        (handle, dir)
    }

    /// The two-topic request every freeze case sends, in the shape the given
    /// wire version carries it.
    fn freeze_case_request(version: i16) -> AddPartitionsToTxnRequest {
        let topics = vec![topic(FROZEN_TOPIC, &[0, 1]), topic(UNFROZEN_TOPIC, &[0])];
        if version >= 4 {
            AddPartitionsToTxnRequest {
                transactions: vec![AddPartitionsToTxnTransaction {
                    transactional_id: TID.into(),
                    producer_id: 11,
                    producer_epoch: 2,
                    verify_only: false,
                    topics,
                    ..Default::default()
                }],
                ..Default::default()
            }
        } else {
            AddPartitionsToTxnRequest {
                v3_and_below_transactional_id: TID.into(),
                v3_and_below_producer_id: 11,
                v3_and_below_producer_epoch: 2,
                v3_and_below_topics: topics,
                ..Default::default()
            }
        }
    }

    /// The topic rows a response carries, whichever half of the wire format
    /// the version puts them in.
    fn topic_rows(
        resp: &AddPartitionsToTxnResponse,
        version: i16,
    ) -> Vec<AddPartitionsToTxnTopicResult> {
        if version >= 4 {
            resp.results_by_transaction
                .iter()
                .flat_map(|txn| txn.topic_results.clone())
                .collect()
        } else {
            resp.results_by_topic_v3_and_below.clone()
        }
    }

    #[tokio::test]
    async fn handle_refuses_every_partition_row_of_a_frozen_topic_and_adds_the_unfrozen_one() {
        // (label, wire version, freeze scope, pattern type). A prefix scope
        // covering the topic has to refuse exactly what a literal one does,
        // on both the v0-3 and the v4+ path.
        for (label, version, scope, pattern_type) in [
            ("v3, a literal freeze", 3, FROZEN_TOPIC, PatternType::Literal),
            ("v3, a prefix freeze", 3, "tenant-a.", PatternType::Prefixed),
            ("v4, a literal freeze", 4, FROZEN_TOPIC, PatternType::Literal),
            ("v4, a prefix freeze", 4, "tenant-a.", PatternType::Prefixed),
        ] {
            let (broker_handle, _dir) = start_frozen_coordinator(
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                (scope, pattern_type),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer = peer();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&freeze_case_request(version), version);

            let bytes = handle(&broker, version, 123, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&bytes, version);

            let expected = vec![
                topic_result(
                    FROZEN_TOPIC,
                    &[(0, codes::POLICY_VIOLATION), (1, codes::POLICY_VIOLATION)],
                ),
                topic_result(UNFROZEN_TOPIC, &[(0, codes::NONE)]),
            ];
            check!(topic_rows(&resp, version) == expected, "{label}");

            // The refusal is the enlistment itself and not only the code: no
            // partition of the frozen topic joins the transaction, while the
            // unfrozen one does.
            let entry_mutex = broker.txn_coordinator.get(TID).expect("open transaction");
            let enrolled: HashSet<String> = entry_mutex
                .lock()
                .await
                .partitions
                .iter()
                .map(|tp| tp.topic.clone())
                .collect();
            check!(
                enrolled == HashSet::from([UNFROZEN_TOPIC.to_string()]),
                "{label}"
            );

            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn handle_tells_an_unauthorized_principal_it_is_unauthorized_and_not_that_it_is_frozen() {
        for (label, version) in [("v3", 3), ("v4", 4)] {
            let (broker_handle, _dir) = start_frozen_coordinator(
                Arc::new(DenyTopicWrites),
                (FROZEN_TOPIC, PatternType::Literal),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer = peer();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&freeze_case_request(version), version);

            let bytes = handle(&broker, version, 123, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&bytes, version);

            // Both topics report the ACL deny. The frozen one reports 29 and
            // not 44: its freeze state never reaches a caller with no right
            // to read it.
            let expected = vec![
                topic_result(
                    FROZEN_TOPIC,
                    &[
                        (0, codes::TOPIC_AUTHORIZATION_FAILED),
                        (1, codes::TOPIC_AUTHORIZATION_FAILED),
                    ],
                ),
                topic_result(UNFROZEN_TOPIC, &[(0, codes::TOPIC_AUTHORIZATION_FAILED)]),
            ];
            check!(topic_rows(&resp, version) == expected, "{label}");

            broker_handle.shutdown().await;
        }
    }
}
