//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! This file holds the wire handler: it authorizes the whole request, walks
//! every requested row, and submits the accepted records in one batch. The
//! pure planning that turns one alter row into a `PartitionRecord`, or into a
//! wire error code, lives in `plan`, the KFC-9 break-glass gate over a cancel
//! and the batch one request accumulates live in `cancel_approval`, and the
//! result rows and the encode step live in `response`.
//!
//! # KFC-9: a cancel needs two people, and every alter respects a freeze
//!
//! A cancel reverts a reassignment that is already under way, drops every
//! adding replica, and can move leadership off one. It is one of the
//! transitions the break-glass two-person rule gates. A start does not need an
//! approval, but a live topic freeze refuses starts and cancels alike. The
//! background completion path in [`crate::reassignment`] uses the same freeze
//! admission and classifies completion as work the cluster already accepted.
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
use krabka_verified::FreezeMutationKind;

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
    codes::{CLUSTER_AUTHORIZATION_FAILED, POLICY_VIOLATION},
    freeze::resolve::resolve_freeze_mutation,
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
        let freeze = resolve_freeze_mutation(
            &image,
            &topic.name,
            true,
            FreezeMutationKind::ReassignmentAlter,
        );
        for p in &topic.partitions {
            rows.push(alter_one(&env, &mut batch, &topic.name, p, freeze));
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    // KIP-966: starting or cancelling a reassignment rewrites the replica set
    // and the ISR with it, so the eligible-leader state the controller keeps
    // rides the same batch. Without it a cancel can leave a replica the
    // partition no longer has in the published ELR.
    crate::elr::ElrPublisher::new(&image).extend(&mut batch.records);

    let mut submit_failure = None;
    if !batch.records.is_empty() {
        match batch.require_audit(broker, ctx).await {
            Ok(()) => {
                if let Err(error) = broker
                    .controller
                    .submit_change(std::mem::take(&mut batch.records))
                    .await
                {
                    let message = format!("submit failed: {error}");
                    tracing::warn!(%error, "alter-reassignment submit failed");
                    mark_submit_failed(&mut by_topic, &message);
                    submit_failure = Some(message);
                }
            }
            Err(error) => {
                let message = format!("privileged action refused: {error}");
                for rows in by_topic.values_mut() {
                    for row in rows.iter_mut().filter(|row| row.error_code == 0) {
                        row.error_code = POLICY_VIOLATION;
                        row.error_message = Some(message.clone());
                    }
                }
                submit_failure = Some(message);
            }
        }
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    // A cancel audits itself as a `PrivilegedAction` through the two-person
    // gate. An ordinary start spends no approval, so every partition the
    // request actually altered is audited here.
    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "AlterPartitionReassignments",
        by_topic
            .iter()
            .flat_map(|(topic, rows)| {
                rows.iter()
                    .filter(|row| row.error_code == crate::codes::NONE)
                    .map(move |row| {
                        crate::handlers::audit_resource(
                            "Partition",
                            format!("{topic}-{}", row.partition_index),
                        )
                    })
            })
            .collect(),
    );

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
