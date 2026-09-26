//! `OffsetDelete` (`api_key=47`, KIP-496).
//!
//! The handler deletes committed offsets for specific (topic, partition)
//! tuples inside a consumer group. `kafka-consumer-groups --delete-offsets`
//! calls it.
//!
//! It follows Kafka's `KafkaApis.handleOffsetDeleteRequest`,
//! `GroupCoordinatorService.deleteOffsets` and
//! `OffsetMetadataManager.deleteOffsets`:
//!   - `Delete` on `Group(group_id)` denied → `GROUP_AUTHORIZATION_FAILED`
//!   - an empty `group_id` → `INVALID_GROUP_ID`
//!   - coordinator routing → `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR`
//!   - missing group → `GROUP_ID_NOT_FOUND`
//!   - a non-empty classic group that does not use the consumer protocol →
//!     `NON_EMPTY_GROUP`
//!
//! Each of those is a group-level refusal with only the top-level code.
//! Otherwise every requested partition gets a row: `Read` on `Topic(name)`
//! denied → `TOPIC_AUTHORIZATION_FAILED`; a missing topic or partition →
//! `UNKNOWN_TOPIC_OR_PARTITION`; a topic the group subscribes to →
//! `GROUP_SUBSCRIBED_TO_TOPIC`; otherwise a tombstone (key =
//! `OffsetCommitKey`, value = null) goes to the group's `__consumer_offsets`
//! partition, the entry leaves the group's committed offsets, and the row
//! answers `NONE`.
//!
//! This file is the module root and holds the wire entry point: decode,
//! authorize, consult the group actor, then delegate. Each child holds one
//! concern: `rows` the per-partition decision table, `response` the
//! group-level response and the encoder, and `tombstone` the
//! `__consumer_offsets` append.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        offset_delete_request::OffsetDeleteRequest, offset_delete_response::OffsetDeleteResponse,
    },
    records::RecordBatch,
};
use tokio::sync::oneshot;

mod response;
mod rows;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod tombstone;

use self::{
    response::{encode, whole_error},
    rows::{Rows, build_response_rows},
    tombstone::{append_tombstones, now_ms},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        partitioner::{GroupRoutingError, local_partition_for_group},
        unified::actor::GroupActorMessage,
    },
    error::BrokerError,
};

#[tracing::instrument(
    name = "handle_offset_delete",
    level = "info",
    skip_all,
    fields(api = "OffsetDelete", version, req_bytes = req_bytes.len()),
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
    let req = OffsetDeleteRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Group `Delete` ACL — `OffsetDeleteRequest.getErrorResponse` on Deny.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: req.group_id.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode(version, &whole_error(codes::GROUP_AUTHORIZATION_FAILED));
    }

    // `GroupCoordinatorService.deleteOffsets` answers an empty group id
    // before it routes the request.
    if req.group_id.is_empty() {
        return encode(version, &whole_error(codes::INVALID_GROUP_ID));
    }

    let offsets_partition =
        match local_partition_for_group(&image, broker.config.node_id, &req.group_id) {
            Ok(partition) => partition,
            Err(GroupRoutingError::Unavailable) => {
                return encode(version, &whole_error(codes::COORDINATOR_NOT_AVAILABLE));
            }
            Err(GroupRoutingError::NotCoordinator) => {
                return encode(version, &whole_error(codes::NOT_COORDINATOR));
            }
        };

    // The group must exist and pass Kafka's `validateOffsetDelete`; its
    // answer also names the topics it subscribes to.
    let Some(group_handle) = broker.group_coordinator.find(&req.group_id) else {
        return encode(version, &whole_error(codes::GROUP_ID_NOT_FOUND));
    };
    let subscribed_topics = {
        let (tx, rx) = oneshot::channel();
        let sent = group_handle
            .tx
            .send(GroupActorMessage::OffsetDeleteGuard { reply: tx })
            .await
            .is_ok();
        // An actor that stopped holds no group any more.
        let guard = if sent {
            rx.await.unwrap_or(Err(codes::GROUP_ID_NOT_FOUND))
        } else {
            Err(codes::GROUP_ID_NOT_FOUND)
        };
        match guard {
            Ok(topics) => topics,
            Err(code) => return encode(version, &whole_error(code)),
        }
    };

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on Deny.
    let topic_decisions = {
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };

    let topic_partition_counts: std::collections::HashMap<&str, i32> = req
        .topics
        .iter()
        .filter_map(|t| {
            image
                .topic(&t.name)
                .map(|tr| (t.name.as_str(), tr.partitions))
        })
        .collect();
    let Rows {
        topics,
        tombstones,
        to_remove,
    } = build_response_rows(
        &req.group_id,
        &req.topics,
        &topic_decisions,
        &subscribed_topics,
        &topic_partition_counts,
    );

    if !tombstones.is_empty() {
        let last_offset_delta =
            i32::try_from(tombstones.len().saturating_sub(1)).unwrap_or(i32::MAX);
        let batch = RecordBatch {
            max_timestamp: now_ms(),
            last_offset_delta,
            records: tombstones,
            ..RecordBatch::default()
        };
        // A failed coordinator write replaces the whole response, as
        // `OffsetDeleteResponse.Builder.merge` does with a top-level error.
        if let Err(code) = append_tombstones(broker, offsets_partition, batch).await {
            return encode(version, &whole_error(code));
        }
        let (tx, rx) = oneshot::channel();
        if group_handle
            .tx
            .send(GroupActorMessage::RemoveCommitted {
                keys: to_remove,
                reply: tx,
            })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
    }

    let resp = OffsetDeleteResponse {
        error_code: codes::NONE,
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    };
    encode(version, &resp)
}
