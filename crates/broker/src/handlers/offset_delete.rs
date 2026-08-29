//! `OffsetDelete` (`api_key=47`, KIP-496).
//!
//! The handler deletes committed offsets for specific (topic, partition)
//! tuples inside a consumer group. `kafka-consumer-groups --delete-offsets`
//! calls it.
//!
//! Authorization, per KIP-496:
//!   - `Delete` on `Group(group_id)` for the whole response
//!   - `Read` on `Topic(name)` for each topic. On Deny, every partition of
//!     that topic gets `TOPIC_AUTHORIZATION_FAILED`.
//!
//! Semantics:
//!   - missing group → whole-response `GROUP_ID_NOT_FOUND`
//!   - missing topic / partition out of range → per-partition
//!     `UNKNOWN_TOPIC_OR_PARTITION`
//!   - group has live members AND any member's consumer-protocol
//!     subscription contains the topic → per-partition
//!     `GROUP_SUBSCRIBED_TO_TOPIC` (86)
//!   - otherwise: append a tombstone (key = `OffsetCommitKey`, value =
//!     null) to the group's `__consumer_offsets` partition, remove the entry from
//!     `Group.committed_offsets`, per-partition `NONE`.
//!
//! This file is the module root and holds the wire entry point: decode,
//! authorize, consult the group actor, then delegate. Each child holds one
//! concern: `rows` the per-partition decision table, `response` the
//! whole-response shapes and the encoder, `tombstone` the
//! `__consumer_offsets` append, and `subscription` the member-metadata decode.

use std::collections::HashSet;

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
mod subscription;
#[cfg(test)]
mod test_support;
mod tombstone;

use self::{
    response::{encode, rewrite_success_as, whole_error},
    rows::build_response_rows,
    subscription::decode_subscribed_topics,
    tombstone::{append_tombstones, now_ms},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        partitioner::{GroupRoutingError, local_partition_for_group},
        unified::{actor::GroupActorMessage, classic_state::GroupState},
    },
    error::BrokerError,
};

// ACL preamble + subscription guard + tombstone pipeline; splitting hurts readability
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

    // Group `Delete` ACL — whole-response on Deny.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: req.group_id.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode(
            version,
            &whole_error(&req, codes::GROUP_AUTHORIZATION_FAILED),
        );
    }

    let offsets_partition =
        match local_partition_for_group(&image, broker.config.node_id, &req.group_id) {
            Ok(partition) => partition,
            Err(GroupRoutingError::Unavailable) => {
                return encode(
                    version,
                    &whole_error(&req, codes::COORDINATOR_NOT_AVAILABLE),
                );
            }
            Err(GroupRoutingError::NotCoordinator) => {
                return encode(version, &whole_error(&req, codes::NOT_COORDINATOR));
            }
        };

    // Group must exist.
    let Some(group_handle) = broker.group_coordinator.find(&req.group_id) else {
        return encode(version, &whole_error(&req, codes::GROUP_ID_NOT_FOUND));
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

    // Snapshot live subscriptions. KIP-496 only blocks deletion when a
    // *consumer-protocol* classic group with live members still subscribes to
    // the topic; Empty/Dead groups, non-`"consumer"` protocol_type groups, and
    // next-gen consumer groups skip the guard.
    // `ClassicInspect` dispatches on the actor's LIVE `group.kind`: it replies
    // ONLY for a classic-kind group and drops the sender for a consumer-kind
    // group, so the `&& let Ok(view) = rx.await` guard yields the empty set for
    // a consumer group (including an UPGRADED one) without consulting the stale
    // spawn-time `handle.kind`. A KIP-848 downgrade leaves the group classic, so
    // its `ClassicInspect` view is the one that matters.
    let subscribed_topics: HashSet<String> = {
        let (tx, rx) = oneshot::channel();
        if group_handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .is_ok()
            && let Ok(view) = rx.await
            && view.state != GroupState::Empty
            && view.protocol_type.as_deref() == Some("consumer")
        {
            view.members
                .iter()
                .flat_map(|m| decode_subscribed_topics(&m.protocol_metadata))
                .collect()
        } else {
            HashSet::new()
        }
    };

    // Build per-topic/per-partition result rows and queue the tombstone
    // batch for the rows that should actually delete.
    let topic_partition_counts: std::collections::HashMap<&str, i32> = req
        .topics
        .iter()
        .filter_map(|t| {
            image
                .topic(&t.name)
                .map(|tr| (t.name.as_str(), tr.partitions))
        })
        .collect();
    let (topics_out, tombstone_records, to_remove) = build_response_rows(
        &req.group_id,
        &req.topics,
        &topic_decisions,
        &subscribed_topics,
        &topic_partition_counts,
    );

    if !tombstone_records.is_empty() {
        let last_offset_delta =
            i32::try_from(tombstone_records.len().saturating_sub(1)).unwrap_or(i32::MAX);
        let tombstones = RecordBatch {
            max_timestamp: now_ms(),
            last_offset_delta,
            records: tombstone_records,
            ..RecordBatch::default()
        };
        if let Err(code) = append_tombstones(broker, offsets_partition, tombstones).await {
            return encode(version, &rewrite_success_as(topics_out, code));
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
        topics: topics_out,
        ..Default::default()
    };
    encode(version, &resp)
}
