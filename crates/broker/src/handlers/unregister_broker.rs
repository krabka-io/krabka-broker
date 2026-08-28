//! `UnregisterBroker` (`api_key=64`).
//!
//! This is the admin RPC an operator uses to drop a permanently dead broker
//! from the cluster's metadata image. Once the change lands through Raft,
//! `Metadata` responses no longer advertise the broker's endpoints, and
//! clients stop routing to it.
//!
//! ## ACL
//!
//! The handler needs `Alter` on `Cluster("kafka-cluster")`. On Deny, the whole
//! response carries `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Idempotency
//!
//! An unknown `broker_id` returns `INVALID_REQUEST (42)` with an explanatory
//! message. This matches the shape of the JVM
//! `KafkaApis.handleUnregisterBroker`, which reports
//! `BrokerIdNotRegisteredException` as `INVALID_REQUEST`.
//!
//! ## KFC-9: dropping a broker needs two people
//!
//! Unregistering a broker is one of the transitions the break-glass two-person
//! rule gates. KIP-631 defines the request and it gains no field for this: an
//! operator gets an approval out of band through `krabka-guard`, targeted at
//! the broker id, and then runs the ordinary tool. A request that no approved
//! proposal covers answers `POLICY_VIOLATION (44)` at the top level, which is
//! where this response carries every other whole-request refusal.
//!
//! The consumed proposal rides the same `submit_change` call as the unregister
//! record, so the approval and the transition it authorized commit together.
//! The gate is active only when `[break_glass]` names an approver set.

use bytes::Bytes;
use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{
    BreakGlassAction, MetadataImage, MetadataRecord, NodeId, UnregisterBrokerRecord,
};
use krabka_protocol::{
    Decode,
    owned::{
        unregister_broker_request::UnregisterBrokerRequest,
        unregister_broker_response::UnregisterBrokerResponse,
    },
};
use uuid::Uuid;

use crate::{
    break_glass::{
        action_name,
        gate::{self, BreakGlassDenial},
        handlers::{PrivilegedAudit, audit_privileged},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    config::BreakGlassConfig,
    error::BrokerError,
    handlers::{RequestContext, cluster_alter_denied},
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
};

#[tracing::instrument(
    name = "handle_unregister_broker",
    level = "info",
    skip_all,
    fields(api = "UnregisterBroker", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = UnregisterBrokerRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Cluster:Alter gate.
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let resp = response(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("unregister-broker denied".into()),
        );
        return encode_resp(version, &resp);
    }

    // The request broker_id is signed but node ids are non-negative;
    // refuse negatives up front rather than silently `as u64`.
    if req.broker_id < 0 {
        let resp = response(
            codes::INVALID_REQUEST,
            Some(format!(
                "broker_id must be non-negative, got {}",
                req.broker_id
            )),
        );
        return encode_resp(version, &resp);
    }

    let node_id = NodeId(u64::try_from(req.broker_id).expect("non-negative"));

    // Existence check. Unknown id → INVALID_REQUEST with a clear message,
    // matching JVM's `BrokerIdNotRegisteredException → INVALID_REQUEST`
    // surface. It runs before the break-glass gate so that a typo in the id
    // does not spend an approval that a real unregistration still needs.
    if image.broker(node_id).is_none() {
        let resp = response(
            codes::INVALID_REQUEST,
            Some(format!("broker {node_id} is not registered")),
        );
        return encode_resp(version, &resp);
    }

    // KFC-9: the two-person rule, and the records it makes this append carry.
    let target = broker_target(node_id);
    let records = match unregister_records(&image, &broker.config.break_glass, node_id, now_ms()) {
        Ok(records) => records,
        Err(denial) => {
            let message = denial.to_string();
            break_glass_metrics::record_refusal(&broker.metrics, denial.action);
            audit_unregister(
                broker,
                ctx,
                &target,
                PrivilegedPhase::Refused,
                denial.proposal_id(),
                &message,
            );
            let resp = response(codes::POLICY_VIOLATION, Some(message));
            return encode_resp(version, &resp);
        }
    };
    let proposal_id = records.first().and_then(consumed_proposal_id);

    // Submit the unregister record through Raft. The image apply is
    // idempotent (the `apply` arm calls `brokers.remove`).
    if let Err(e) = broker.controller.submit_change(records).await {
        let resp = response(
            codes::UNKNOWN_SERVER_ERROR,
            Some(format!("controller submit failed: {e}")),
        );
        return encode_resp(version, &resp);
    }
    audit_unregister(
        broker,
        ctx,
        &target,
        PrivilegedPhase::Applied,
        proposal_id,
        "broker registration removed",
    );

    let resp = response(codes::NONE, None);
    encode_resp(version, &resp)
}

/// The records one unregistration appends.
///
/// The consumed break-glass proposal goes first, and the unregister record
/// follows it, so one raft append carries both. That single append is what
/// stops an approval from being spent twice across a crash: a broker that
/// committed the transition has committed the consume with it.
///
/// A broker whose `[break_glass]` names no approver gates nothing, and the
/// answer is then the unregister record alone.
///
/// # Errors
///
/// Returns the [`BreakGlassDenial`] when no approved proposal covers this
/// broker id. The caller answers `POLICY_VIOLATION (44)` with its text.
fn unregister_records(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    node_id: NodeId,
    now_ms: i64,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    let record = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id });
    if !gate::is_gated(config) {
        return Ok(vec![record]);
    }
    let consumed = gate::authorize(
        image,
        config,
        BreakGlassAction::UnregisterBroker,
        &broker_target(node_id),
        now_ms,
    )?;
    Ok(vec![consumed, record])
}

/// The break-glass target of one broker: its id in decimal, as an operator
/// spells it on `krabka-guard break-glass propose --target`.
///
/// `UnregisterBroker` names no partition, so the gate takes this target
/// exactly and no wider proposal covers it.
fn broker_target(node_id: NodeId) -> String {
    node_id.0.to_string()
}

/// The proposal that a consumed record names.
///
/// [`gate::authorize`] only ever answers with a proposal record, so the `None`
/// arm costs one match rather than a panic.
fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

/// Emit one `PrivilegedAction` event for an unregistration.
///
/// `counterparties` stays empty for the reason the freeze events give: the
/// approvers are named on the proposal's own approve events, and the proposal
/// id joins those rows to this one.
fn audit_unregister(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    target: &str,
    phase: PrivilegedPhase,
    proposal_id: Option<Uuid>,
    reason: &str,
) {
    // A broker that gates nothing has no two-person evidence to record, and
    // this event exists to carry that evidence. The ordinary administrative
    // event already reports the transition itself, so a stock cluster's audit
    // stream is unchanged.
    if !gate::is_gated(&broker.config.break_glass) {
        return;
    }
    audit_privileged(
        &broker.audit_log,
        ctx,
        approver_set_fingerprint(&broker.config.break_glass.approvers),
        &PrivilegedAudit {
            outcome: if matches!(phase, PrivilegedPhase::Refused) {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            },
            phase,
            action: action_name(BreakGlassAction::UnregisterBroker),
            target,
            proposal_id,
            counterparties: &[],
            key_id: "",
            signature: &[],
            signature_verified: false,
            reason,
        },
    );
}

fn response(error_code: i16, error_message: Option<String>) -> UnregisterBrokerResponse {
    UnregisterBrokerResponse {
        error_code,
        error_message,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &UnregisterBrokerResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::{assert, check};
    use krabka_metadata::BreakGlassProposalRecord;
    use krabka_protocol::owned::unregister_broker_response::{self, UnregisterBrokerResponse};
    use krabka_security::Principal;

    use super::*;
    use crate::{
        authorizer::Authorizer, break_glass::gate::tests::approval, broker::BrokerHandle,
        test_support::DenyAll,
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
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
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
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
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
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
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
        let records =
            unregister_records(&image_of(&[]), &BreakGlassConfig::default(), DOOMED, NOW_MS)
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
                action: crate::metrics::BreakGlassAction::UnregisterBroker,
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
}
