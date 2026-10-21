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
//! A streams group (KIP-1071) keeps its members and topology in its streams
//! actor and its committed offsets in a group actor of the same id. The
//! streams actor decides the subscribed topics, as
//! `StreamsGroup.isSubscribedToTopic`, and a streams group that has never
//! committed an offset is found all the same.
//!
//! Each of those is a group-level refusal with only the top-level code.
//! Otherwise every requested partition gets a row: `Read` on `Topic(name)`
//! denied → `TOPIC_AUTHORIZATION_FAILED`; a missing topic or partition →
//! `UNKNOWN_TOPIC_OR_PARTITION`; a topic the group subscribes to →
//! `GROUP_SUBSCRIBED_TO_TOPIC`; otherwise the row answers `NONE`. For such a
//! partition the group actor writes a tombstone (key = `OffsetCommitKey`,
//! value = null) to the group's `__consumer_offsets` partition and drops the
//! entry only when it holds a committed or a pending transactional offset
//! for it, as Kafka's `deleteOffsets` checks `hasCommittedOffset` and
//! `hasPendingTransactionalOffsets`. When it holds none, nothing is written.
//! A group with no offsets actor holds no offset, so nothing is written for
//! it either.
//!
//! This file is the module root and holds the wire entry point: decode,
//! authorize, consult the group actor, then delegate. Each child holds one
//! concern: `rows` the per-partition decision table, `response` the
//! group-level response and the encoder, and `tombstone` the
//! `__consumer_offsets` write through the group actor.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, group_coordinator_error, group_delete_denied,
            topic_decisions,
        },
    },
    krabka_metadata::AclOperation,
    krabka_protocol::owned::{
        offset_delete_request::OffsetDeleteRequest, offset_delete_response::OffsetDeleteResponse,
    },
    tokio::sync::oneshot,
};

mod response;
mod rows;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod tombstone;

use {
    self::{
        rows::{Rows, build_response_rows},
        tombstone::delete_held_offsets,
    },
    crate::{
        codes,
        coordinator::unified::{
            actor::{GroupActorHandle, GroupActorMessage, SubscribedTopics},
            streams::actor::streams_offset_delete_guard,
        },
        handlers::ErrorCodeResponse as _,
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    req: OffsetDeleteRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<OffsetDeleteResponse, BrokerError> {
    let image = broker.controller.current_image();

    // Group `Delete` ACL — `OffsetDeleteRequest.getErrorResponse` on Deny.
    if group_delete_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        req.group_id.as_str(),
    ) {
        return Ok(OffsetDeleteResponse::error(
            codes::GROUP_AUTHORIZATION_FAILED,
        ));
    }

    // `GroupCoordinatorService.deleteOffsets` answers an empty group id
    // before it routes the request.
    if req.group_id.is_empty() {
        return Ok(OffsetDeleteResponse::error(codes::INVALID_GROUP_ID));
    }

    if let Some(code) = group_coordinator_error(broker, &req.group_id) {
        return Ok(OffsetDeleteResponse::error(code));
    }

    // The group must exist and pass Kafka's `validateOffsetDelete`; its
    // answer also names the topics it subscribes to. A streams group answers
    // from its streams actor, whose offsets actor may not exist yet.
    let offsets_home = broker.group_coordinator.find(&req.group_id);
    let guard = match (
        broker.group_coordinator.find_live_streams(&req.group_id),
        &offsets_home,
    ) {
        (Some(streams), _) => streams_offset_delete_guard(&streams).await,
        (None, Some(group_handle)) => group_offset_delete_guard(group_handle).await,
        (None, None) => Err(codes::GROUP_ID_NOT_FOUND),
    };
    let subscribed_topics = match guard {
        Ok(topics) => topics,
        Err(code) => return Ok(OffsetDeleteResponse::error(code)),
    };

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on Deny.
    let topic_decisions = topic_decisions(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        AclOperation::Read,
        req.topics.iter().map(|t| t.name.as_str()),
    );

    let topic_partition_counts: std::collections::HashMap<&str, i32> = req
        .topics
        .iter()
        .filter_map(|t| {
            image
                .topic(&t.name)
                .map(|tr| (t.name.as_str(), tr.partitions))
        })
        .collect();
    let Rows { topics, to_delete } = build_response_rows(
        &req.topics,
        &topic_decisions,
        &subscribed_topics,
        &topic_partition_counts,
    );

    // A failed coordinator write replaces the whole response, as
    // `OffsetDeleteResponse.Builder.merge` does with a top-level error. The
    // answer waits for the tombstones to commit, as Kafka's
    // `CoordinatorRuntime` completes the write only then. A group with no
    // offsets actor holds no offset to tombstone.
    if let Some(group_handle) = offsets_home
        && !to_delete.is_empty()
        && let Err(code) = delete_held_offsets(&group_handle, to_delete).await
    {
        return Ok(OffsetDeleteResponse::error(code));
    }

    Ok(OffsetDeleteResponse {
        error_code: codes::NONE,
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}

/// Asks a classic or consumer group actor for Kafka's `validateOffsetDelete`
/// and its subscribed topics. An actor that stopped holds no group any more.
async fn group_offset_delete_guard(
    group_handle: &GroupActorHandle,
) -> Result<SubscribedTopics, i16> {
    let (tx, rx) = oneshot::channel();
    if group_handle
        .tx
        .send(GroupActorMessage::OffsetDeleteGuard { reply: tx })
        .await
        .is_err()
    {
        return Err(codes::GROUP_ID_NOT_FOUND);
    }
    rx.await.unwrap_or(Err(codes::GROUP_ID_NOT_FOUND))
}
