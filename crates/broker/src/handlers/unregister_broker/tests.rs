//! Unit tests for the `UnregisterBroker` handler.
//!
//! Two kinds sit here. The wire tests drive `handle` against a live in-process
//! broker and compare the whole decoded response, because the shape of a
//! refusal is as much the contract as its error code. The gate tests call
//! `unregister_records` directly, where the break-glass decision is a pure
//! function of the metadata image, the approver set, and the clock.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_metadata::{
    BreakGlassProposalRecord, MetadataImage, MetadataRecord, UnregisterBrokerRecord,
};
use krabka_protocol::owned::unregister_broker_response::{self, UnregisterBrokerResponse};
use krabka_security::Principal;
use uuid::Uuid;

use super::*;
use crate::{
    authorizer::Authorizer, break_glass::gate::tests::approval, broker::BrokerHandle,
    config::BreakGlassConfig, test_support::DenyAll,
};

fn encode_request(req: &UnregisterBrokerRequest, version: i16) -> Bytes {
    crate::test_support::encode_request(req, version)
}

fn decode_response(bytes: &Bytes) -> UnregisterBrokerResponse {
    crate::test_support::decode_response(bytes, unregister_broker_response::MAX_VERSION)
}

fn principal() -> Principal {
    crate::test_support::principal("admin")
}

fn context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "unregister-client")
}

async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

#[test]
fn response_preserves_error_fields_and_throttle() {
    let resp = response(codes::UNKNOWN_SERVER_ERROR, Some("submit failed".into()));

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::UNKNOWN_SERVER_ERROR,
        error_message: Some("submit failed".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_with_message_and_throttle() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("unregister-broker denied".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_negative_broker_id_before_casting() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: -1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::INVALID_REQUEST,
        error_message: Some("broker_id must be non-negative, got -1".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_treats_zero_as_non_negative_unknown_broker() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 0,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::INVALID_REQUEST,
        error_message: Some("broker 0 is not registered".into()),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unregisters_registered_broker_with_success_shape() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
const DOOMED: NodeId = NodeId(7);

fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

/// A proposal that two people approved, and that has not expired.
fn approved_proposal(target: &str) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: PROPOSAL,
        action: BreakGlassAction::UnregisterBroker,
        target: target.to_owned(),
        proposer: "User:carol".to_owned(),
        reason: "broker 7 is never coming back".to_owned(),
        created_at_ms: 1_000,
        expires_at_ms: 600_000,
        approvals: vec![approval("User:alice"), approval("User:bob")],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

fn image_of(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}

const NOW_MS: i64 = 60_000;

#[test]
fn an_unregistration_with_no_proposal_appends_nothing() {
    let image = image_of(&[]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, NOW_MS)
        .expect_err("no proposal covers broker 7");

    check!(denial.action == BreakGlassAction::UnregisterBroker);
    check!(denial.target == "7");
    check!(
        denial.to_string()
            == "break-glass refused unregister_broker on 7: no approved proposal covers the request"
    );
}

#[test]
fn an_approved_unregistration_appends_the_consume_beside_the_unregister() {
    let proposal = approved_proposal("7");
    let image = image_of(std::slice::from_ref(&proposal));

    let records = unregister_records(&image, &gated_config(), DOOMED, NOW_MS)
        .expect("the proposal authorizes the unregistration");

    let expected = vec![
        MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
            consumed_at_ms: NOW_MS,
            ..proposal
        }),
        MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id: DOOMED }),
    ];
    assert!(records == expected);
}

#[test]
fn a_proposal_for_another_broker_does_not_cover_this_one() {
    let image = image_of(&[approved_proposal("8")]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, NOW_MS)
        .expect_err("a proposal for broker 8 authorizes nothing about broker 7");

    check!(denial.proposal_id() == None);
}

#[test]
fn a_broker_with_no_approver_set_gates_nothing() {
    let records = unregister_records(&image_of(&[]), &BreakGlassConfig::default(), DOOMED, NOW_MS)
        .expect("an ungated broker unregisters with no proposal");

    assert!(
        records
            == vec![MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: DOOMED
            })]
    );
}

/// The `break_glass_refusals` count for this action.
fn refusals(metrics: &crate::metrics::BrokerMetrics) -> u64 {
    metrics
        .break_glass_refusals
        .get_or_create(&crate::metrics::BreakGlassActionLabel {
            action: crate::metrics::BreakGlassAction(BreakGlassAction::UnregisterBroker),
        })
        .get()
}

#[tokio::test]
async fn the_wire_handler_refuses_an_unregistration_that_no_proposal_covers() {
    let version = unregister_broker_response::MAX_VERSION;
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.break_glass = gated_config();
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = UnregisterBrokerRequest {
        broker_id: 1,
        ..Default::default()
    };

    let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    check!(resp.error_code == codes::POLICY_VIOLATION);
    check!(
        resp.error_message
            == Some(
                "break-glass refused unregister_broker on 1: no approved proposal covers the request"
                    .to_owned()
            )
    );
    // The refusal refused: broker 1 is still registered.
    check!(
        broker
            .controller
            .current_image()
            .broker(NodeId(1))
            .is_some()
    );
    // The refusal reached the series an operator reads.
    check!(refusals(&broker.metrics) == 1);
    broker_handle.shutdown().await;
}
