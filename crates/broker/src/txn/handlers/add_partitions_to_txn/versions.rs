//! The two request shapes of `AddPartitionsToTxn` and the whole-transaction
//! `TransactionalId` ACL gate each of them runs.
//!
//! v0-3 carries a single transaction inline on the request and answers with
//! `results_by_topic_v3_and_below`; v4-5 carries a `transactions` array and
//! answers with `results_by_transaction`. Below the ACL gate the work is
//! identical, so both paths funnel into
//! [`process_one_txn`](super::registration::process_one_txn).

use std::net::SocketAddr;

use bytes::Bytes;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::{
    add_partitions_to_txn_request::AddPartitionsToTxnRequest,
    add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
};
use krabka_security::Principal;

use super::{
    authz::denied_topics,
    registration::{TransactionRequest, process_one_txn},
    results::topic_error,
    wire::encode_response,
    write_freeze::frozen_topics,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
    error::BrokerError,
};

/// The request-independent collaborators both version paths need: the
/// coordinator they drive, the metadata image the ACL checks read, the
/// negotiated transaction version, and the caller's identity.
#[derive(Clone, Copy)]
pub(super) struct HandlerDependencies<'a> {
    pub(super) coord: &'a crate::txn::coordinator::TxnCoordinator,
    pub(super) image: &'a MetadataImage,
    pub(super) txnv: crate::txn::version::TxnVersion,
    pub(super) authorizer: &'a dyn Authorizer,
    pub(super) principal: &'a Principal,
    pub(super) peer: &'a SocketAddr,
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

pub(super) async fn handle_v4(
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

pub(super) async fn handle_v3(
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
}
