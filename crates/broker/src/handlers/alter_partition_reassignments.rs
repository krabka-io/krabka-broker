//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! This file holds the wire handler: it authorizes the whole request, walks
//! every requested row, and submits the accepted records in one batch. The
//! pure planning that turns one alter row into a `PartitionRecord`, or into a
//! wire error code, lives in `plan`, and the result rows and the encode step
//! live in `response`.

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

mod plan;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::plan::process_one_partition;
use self::response::{
    encode_response, encode_whole_request_error, err_row, mark_submit_failed, ok_row,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::CLUSTER_AUTHORIZATION_FAILED,
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
    ctx: &crate::handlers::RequestContext<'_>,
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

    let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
    let mut to_submit: Vec<krabka_metadata::MetadataRecord> = Vec::new();
    for topic in &req.topics {
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            let target_slice: Option<&[i32]> = p.replicas.as_deref();
            match process_one_partition(
                &image,
                &topic.name,
                p.partition_index,
                target_slice,
                req.allow_replication_factor_change,
            ) {
                Ok(Some(record)) => {
                    to_submit.push(krabka_metadata::MetadataRecord::V1Partition(record));
                    rows.push(ok_row(p.partition_index));
                }
                Ok(None) => rows.push(ok_row(p.partition_index)),
                Err((code, msg)) => rows.push(err_row(p.partition_index, code, msg)),
            }
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "alter-reassignment submit failed");
        mark_submit_failed(&mut by_topic, &format!("submit failed: {e}"));
    }

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
