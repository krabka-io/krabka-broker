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
//!
//! This file holds the request flow. The two-person gate and the records it
//! builds live in `gate`, and the response shape in `wire`.

use bytes::Bytes;
use krabka_audit::PrivilegedPhase;
use krabka_metadata::{BreakGlassAction, NodeId};
use krabka_protocol::{Decode, owned::unregister_broker_request::UnregisterBrokerRequest};

use self::{
    gate::{broker_target, consumed_proposal_id, unregister_records},
    wire::{encode_resp, response},
};
use crate::{
    break_glass::{
        handlers::audit::{GatedTransition, audit_transition},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, cluster_alter_denied},
    time_util::now_ms,
};

mod gate;
mod wire;

#[cfg(test)]
mod tests;

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
            audit_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: BreakGlassAction::UnregisterBroker,
                    target: &target,
                    phase: PrivilegedPhase::Refused,
                    proposal_id: denial.proposal_id(),
                    reason: &message,
                },
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
    audit_transition(
        &broker.audit_log,
        &broker.config.break_glass,
        ctx,
        &GatedTransition {
            action: BreakGlassAction::UnregisterBroker,
            target: &target,
            phase: PrivilegedPhase::Applied,
            proposal_id,
            reason: "broker registration removed",
        },
    );

    let resp = response(codes::NONE, None);
    encode_resp(version, &resp)
}
