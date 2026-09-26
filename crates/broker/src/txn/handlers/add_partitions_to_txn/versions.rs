//! The two request shapes of `AddPartitionsToTxn` and the authorization each
//! of them runs.
//!
//! v0-3 carries a single transaction inline on the request and answers with
//! `results_by_topic_v3_and_below`. It comes from a client, which needs
//! `Write` on the transactional id and on each topic. v4-5 carries a
//! `transactions` array and answers with `results_by_transaction`. It comes
//! from a broker, which [`super::handle`] has already checked for
//! `ClusterAction`, and it runs no client check. Below the authorization
//! the work is identical, so both paths funnel into
//! [`process_one_txn`](super::registration::process_one_txn).

use std::{collections::HashSet, net::SocketAddr};

use bytes::Bytes;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::{
    add_partitions_to_txn_request::AddPartitionsToTxnRequest,
    add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
    common::{
        add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};
use krabka_security::Principal;

use super::{
    authz::{TopicAuthorization, failed_partitions},
    registration::{TransactionRequest, process_one_txn},
    results::{dedup_topics, topic_error},
    wire::encode_response,
    write_freeze::frozen_topics,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
    error::BrokerError,
};

/// The request-independent collaborators both version paths need: the
/// coordinator they drive, the metadata image the checks read, the
/// negotiated transaction version, and the caller's identity.
#[derive(Clone, Copy)]
pub(super) struct HandlerDependencies<'a> {
    pub(super) coord: &'a crate::txn::coordinator::TxnCoordinator,
    pub(super) image: &'a MetadataImage,
    pub(super) txnv: crate::txn::version::TxnVersion,
    pub(super) authorizer: &'a dyn Authorizer,
    pub(super) principal: &'a Principal,
    pub(super) peer: &'a SocketAddr,
    /// The broker config, which names the internal topics a client can never
    /// add.
    pub(super) config: &'a crate::config::BrokerConfig,
}

/// One transaction of a request, in the form both version paths share.
struct Transaction<'a> {
    transactional_id: &'a str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &'a [AddPartitionsToTxnTopic],
    verify_only: bool,
}

/// Runs the checks of Kafka's `KafkaApis.handleAddPartitionsToTxnRequest` for
/// one transaction, then hands it to the coordinator.
///
/// A client (v0-3) needs `Write` on the transactional id, else every
/// partition answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. Then every
/// partition must be authorized and exist, else the whole transaction fails
/// and nothing is added.
async fn process_transaction(
    dependencies: &HandlerDependencies<'_>,
    client: bool,
    txn: &Transaction<'_>,
    version: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let &HandlerDependencies {
        coord,
        image,
        txnv,
        authorizer,
        principal,
        peer,
        config,
    } = dependencies;
    // Kafka's response schema keys its per-partition results by (topic,
    // partition), so a request that names either one twice must still get
    // one response row for it (#883). Deduping here, before either
    // authorization check, means every downstream step -- the ACL sweep, the
    // freeze gate, and the coordinator call -- already works from the
    // collapsed list.
    let topics = dedup_topics(txn.topics);
    let authorization = if client {
        let tid_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: txn.transactional_id,
            operation: AclOperation::Write,
        };
        if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
            return topic_error(&topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        }
        TopicAuthorization::Client {
            authorizer,
            principal,
            peer,
            config,
        }
    } else {
        TopicAuthorization::Broker
    };
    if let Some(rows) = failed_partitions(image, authorization, &topics) {
        return rows;
    }
    let authorized = HashSet::new();
    let frozen = frozen_topics(image, &topics, &authorized);
    process_one_txn(
        coord,
        TransactionRequest {
            transactional_id: txn.transactional_id,
            producer_id: krabka_log::ProducerId(txn.producer_id),
            producer_epoch: txn.producer_epoch,
            topics: &topics,
            denied: &authorized,
            frozen: &frozen,
            txnv,
            verify_only: txn.verify_only,
            version,
        },
    )
    .await
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

pub(super) async fn handle_v4(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    // `TransactionalId` is a `mapKey` field of `AddPartitionsToTxnResponse`,
    // and Kafka's `KafkaApis` keeps its per-transaction results in an
    // `AddPartitionsToTxnResultCollection` keyed by it. A request that names
    // the same transactional id twice gets one response row, keyed by first
    // occurrence and holding the last-processed result (#883).
    let mut order: Vec<String> = Vec::with_capacity(req.transactions.len());
    let mut by_tid: std::collections::HashMap<String, AddPartitionsToTxnResult> =
        std::collections::HashMap::with_capacity(req.transactions.len());

    for txn in &req.transactions {
        let topic_results = process_transaction(
            dependencies,
            false,
            &Transaction {
                transactional_id: txn.transactional_id.as_str(),
                producer_id: txn.producer_id,
                producer_epoch: txn.producer_epoch,
                topics: &txn.topics,
                verify_only: txn.verify_only,
            },
            version,
        )
        .await;
        if !by_tid.contains_key(&txn.transactional_id) {
            order.push(txn.transactional_id.clone());
        }
        by_tid.insert(
            txn.transactional_id.clone(),
            AddPartitionsToTxnResult {
                transactional_id: txn.transactional_id.clone(),
                topic_results,
                ..Default::default()
            },
        );
    }
    let results_by_transaction = order
        .into_iter()
        .map(|tid| {
            by_tid
                .remove(&tid)
                .expect("every ordered id was inserted into the map above")
        })
        .collect();

    let resp = AddPartitionsToTxnResponse {
        results_by_transaction,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── v0-3 path ─────────────────────────────────────────────────────────────────

pub(super) async fn handle_v3(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    let topic_results = process_transaction(
        dependencies,
        true,
        &Transaction {
            transactional_id: req.v3_and_below_transactional_id.as_str(),
            producer_id: req.v3_and_below_producer_id,
            producer_epoch: req.v3_and_below_producer_epoch,
            topics: &req.v3_and_below_topics,
            // v0-3 has no `verify_only` field (predates KIP-890); always add.
            verify_only: false,
        },
        version,
    )
    .await;

    let resp = AddPartitionsToTxnResponse {
        results_by_topic_v3_and_below: topic_results,
        ..Default::default()
    };
    encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnTransaction;

    use super::*;
    use crate::{
        test_support::{DenyAll, peer, start_broker_with_authorizer_no_audit as start_broker},
        txn::handlers::add_partitions_to_txn::{
            handle,
            test_support::{topic, topic_result},
        },
    };

    crate::test_support::wire_helpers!(
        AddPartitionsToTxnRequest,
        AddPartitionsToTxnResponse,
        client_id = "producer-client"
    );

    fn principal() -> Principal {
        crate::test_support::principal("ANONYMOUS")
    }

    /// A v4+ request comes from a broker, so a principal without
    /// `ClusterAction` gets Kafka's top-level `CLUSTER_AUTHORIZATION_FAILED`
    /// and no transaction row.
    #[tokio::test]
    async fn handle_v4_without_cluster_action_returns_the_top_level_error() {
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
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// #883: `TransactionalId` is a `mapKey` field, so a v4+ request that
    /// names the same transactional id twice must answer with exactly one
    /// `results_by_transaction` row for it, holding the last-processed
    /// result.
    #[tokio::test]
    async fn handle_v4_duplicate_transactional_id_collapses_to_one_result() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::test_support::GrantsInPrincipalName)).await;
        let principal = crate::test_support::principal("Cluster:ClusterAction");
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let req = AddPartitionsToTxnRequest {
            transactions: vec![
                AddPartitionsToTxnTransaction {
                    transactional_id: "dup-tid".into(),
                    producer_id: 11,
                    producer_epoch: 2,
                    verify_only: false,
                    topics: vec![topic("first-topic", &[0])],
                    ..Default::default()
                },
                AddPartitionsToTxnTransaction {
                    transactional_id: "dup-tid".into(),
                    producer_id: 11,
                    producer_epoch: 2,
                    verify_only: false,
                    topics: vec![topic("second-topic", &[0])],
                    ..Default::default()
                },
            ],
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

        assert!(resp.results_by_transaction.len() == 1);
        let expected = AddPartitionsToTxnResult {
            transactional_id: "dup-tid".into(),
            topic_results: vec![topic_result(
                "second-topic",
                &[(0, codes::UNKNOWN_TOPIC_OR_PARTITION)],
            )],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp.results_by_transaction[0] == expected);
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
}
