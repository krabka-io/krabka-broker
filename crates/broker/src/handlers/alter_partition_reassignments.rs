//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! This file holds the wire handler: it authorizes the whole request, walks
//! every requested row, and submits the accepted records in one batch. The
//! pure planning that turns one alter row into a `PartitionRecord`, or into a
//! wire error code, lives in `plan`, the KFC-9 break-glass gate over a cancel
//! and the batch one request accumulates live in `cancel_approval`, and the
//! result rows and the encode step live in `response`.
//!
//! # KFC-9: a cancel needs two people, and a start does not
//!
//! A cancel reverts a reassignment that is already under way, drops every
//! adding replica, and can move leadership off one. It is one of the
//! transitions the break-glass two-person rule gates. A start is not: it adds
//! replicas and removes none until the new ones catch up. **The completion path
//! in [`crate::reassignment`] is not a cancel either, and it is not gated.**
//!
//! KIP-455 defines the request that `kafka-reassign-partitions` sends and it
//! gains no field for this. An operator gets the approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and the broker looks it up in its own metadata image. A refused row
//! answers `POLICY_VIOLATION (44)` with the refusal text, and the gate is
//! active only when `[break_glass]` names an approver set.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_metadata::ResourceType;
use krabka_protocol::owned::{
    alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
    alter_partition_reassignments_response::{
        AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
        ReassignableTopicResponse,
    },
};

mod cancel_approval;
mod plan;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::plan::process_one_partition;
use self::{
    cancel_approval::{ReassignBatch, ReassignEnv, alter_one},
    response::{encode_response, encode_whole_request_error, mark_submit_failed},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::CLUSTER_AUTHORIZATION_FAILED,
    handlers::RequestContext,
};

#[tracing::instrument(
    name = "handle_alter_partition_reassignments",
    level = "info",
    skip_all,
    fields(api = "AlterPartitionReassignments"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterPartitionReassignmentsRequest,
    ctx: &RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    // Whole-request Cluster Alter authorize.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "alter-reassignment denied",
            api_version,
        );
    }

    let env = ReassignEnv {
        broker,
        image: &image,
        ctx,
        allow_rf_change: req.allow_replication_factor_change,
    };
    let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
    let mut batch = ReassignBatch::default();
    for topic in &req.topics {
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            rows.push(alter_one(&env, &mut batch, &topic.name, p));
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    let mut submit_failure = None;
    if !batch.records.is_empty()
        && let Err(e) = broker
            .controller
            .submit_change(std::mem::take(&mut batch.records))
            .await
    {
        tracing::warn!(error = %e, "alter-reassignment submit failed");
        submit_failure = Some(format!("submit failed: {e}"));
        mark_submit_failed(&mut by_topic, &format!("submit failed: {e}"));
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    let responses: Vec<ReassignableTopicResponse> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopicResponse {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}
