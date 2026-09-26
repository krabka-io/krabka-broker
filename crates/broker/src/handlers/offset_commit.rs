//! `OffsetCommit` (`api_key=8`). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to the group-id's
//! `__consumer_offsets` partition
//! via the partition writer, then updates the group's committed offsets
//! through its actor.
//!
//! KIP-211: a v2-v4 request may carry `retention_time_ms`, which overrides the
//! broker's `offsets.retention.minutes` for the offsets in that one request.
//! The handler turns it into the absolute `expire_timestamp_ms` that the
//! record and the in-memory entry both carry, and
//! `coordinator::retention` honours it. The field was removed at v5,
//! where the decoder leaves it at its `-1` default, and `-1` at v2-v4 means
//! the same thing: take the broker-wide retention.

use std::sync::Arc;

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic},
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tokio::sync::oneshot;

use self::response::ResponseBuilder;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        partitioner::{GroupRoutingError, local_partition_for_group},
        persistence::OffsetCommitValue,
        unified::{
            actor::{GroupActorHandle, GroupActorMessage, GroupKindTag, validate_offset_commit},
            classic_state::OffsetEntry,
            streams::actor::validate_streams_group_commit,
        },
    },
    error::BrokerError,
};

mod response;

#[cfg(test)]
mod group_validation_tests;
#[cfg(test)]
mod topic_resolution_tests;

/// The first `OffsetCommit` version that names each topic by `topic_id` only
/// (KIP-848). The request schema carries `name` at versions 0-9 and
/// `topic_id` from this version on.
const FIRST_TOPIC_ID_VERSION: i16 = 10;

/// The first `OffsetCommit` version that answers `GROUP_ID_NOT_FOUND` for a
/// group that does not exist. Earlier versions answer `ILLEGAL_GENERATION`.
const FIRST_GROUP_ID_NOT_FOUND_VERSION: i16 = 9;

/// Serves one `OffsetCommit` request.
///
/// The order of the checks is the order of Kafka's
/// `KafkaApis.handleOffsetCommitRequest`:
///
/// 1. `Read` on `Group(group_id)`. A denial answers
///    `GROUP_AUTHORIZATION_FAILED` on every partition row.
/// 2. At v10 and later, each `topic_id` resolves to a name. A row whose name
///    stays empty answers `UNKNOWN_TOPIC_ID` on every partition row. The zero
///    id is such a row.
/// 3. `Read` on each `Topic(name)`. A denied topic answers
///    `TOPIC_AUTHORIZATION_FAILED` on every partition row.
/// 4. A topic or a partition that the image does not hold answers
///    `UNKNOWN_TOPIC_OR_PARTITION` on its partition rows.
/// 5. The group coordinator commits the rows that remain, and a coordinator
///    error goes on those rows only.
///
/// The error rows come first in the response and the committed rows follow,
/// as `OffsetCommitResponse.Builder.merge` puts them. The handler writes no
/// offset for a row that steps 2 to 4 refuse.
#[tracing::instrument(
    name = "handle_offset_commit",
    level = "info",
    skip_all,
    fields(api = "OffsetCommit", version, req_bytes = req_bytes.len()),
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
    let mut req = OffsetCommitRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();

    let group_request = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: req.group_id.as_str(),
        operation: AclOperation::Read,
    };
    if broker.config.authorizer.authorize(&*image, &group_request) == AuthorizationResult::Deny {
        return encode(
            version,
            &build_response_all(&req, codes::GROUP_AUTHORIZATION_FAILED),
        );
    }

    let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    if use_topic_ids {
        resolve_topic_names(&mut req, &image);
    }

    let allowed: Vec<bool> = {
        let decisions = authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            req.topics
                .iter()
                .filter(|topic| !(use_topic_ids && topic.name.is_empty()))
                .map(|topic| topic.name.as_str()),
        );
        req.topics
            .iter()
            .map(|topic| {
                decisions.get(topic.name.as_str()).copied() == Some(AuthorizationResult::Allow)
            })
            .collect()
    };

    let mut response = ResponseBuilder::new(use_topic_ids);
    let mut accepted = Vec::with_capacity(req.topics.len());
    for (topic, allowed) in std::mem::take(&mut req.topics).into_iter().zip(allowed) {
        if use_topic_ids && topic.name.is_empty() {
            response.add_topic(&topic, codes::UNKNOWN_TOPIC_ID);
        } else if !allowed {
            response.add_topic(&topic, codes::TOPIC_AUTHORIZATION_FAILED);
        } else if let Some(topic) = existing_partitions(topic, &image, &mut response) {
            accepted.push(topic);
        }
    }
    if accepted.is_empty() {
        return encode(version, &response.build());
    }

    req.topics = accepted;
    let error_code = commit(broker, &req, version).await;
    response.merge(build_response_all(&req, error_code).topics);
    encode(version, &response.build())
}

/// Sets the name of each topic row whose `topic_id` the image knows.
///
/// A row whose id the image does not know keeps its empty name. That includes
/// the zero id, which names no topic.
fn resolve_topic_names(request: &mut OffsetCommitRequest, image: &krabka_metadata::MetadataImage) {
    for topic in &mut request.topics {
        if topic.topic_id == WireUuid::ZERO {
            continue;
        }
        if let Some(name) = image.topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0)) {
            topic.name = name.to_string();
        }
    }
}

/// Keeps the partitions of an authorized `topic` that the image holds.
///
/// A topic that the image does not hold answers `UNKNOWN_TOPIC_OR_PARTITION`
/// on every partition row, with the zero id. A partition that the image does
/// not hold answers `UNKNOWN_TOPIC_OR_PARTITION` on its own row. This is the
/// existence check of Kafka's `KafkaApis.handleOffsetCommitRequest`, which
/// runs after the topic `Read` check and keeps every refused row away from
/// the group coordinator. It returns `None` when no partition remains.
fn existing_partitions(
    mut topic: OffsetCommitRequestTopic,
    image: &krabka_metadata::MetadataImage,
    response: &mut ResponseBuilder,
) -> Option<OffsetCommitRequestTopic> {
    if image.topic(&topic.name).is_none() {
        topic.topic_id = WireUuid::ZERO;
        response.add_topic(&topic, codes::UNKNOWN_TOPIC_OR_PARTITION);
        return None;
    }
    let (present, missing): (Vec<_>, Vec<_>) = std::mem::take(&mut topic.partitions)
        .into_iter()
        .partition(|partition| {
            image
                .partition(&topic.name, partition.partition_index)
                .is_some()
        });
    for partition in missing {
        response.add_partition(
            topic.topic_id,
            &topic.name,
            partition.partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        );
    }
    topic.partitions = present;
    (!topic.partitions.is_empty()).then_some(topic)
}

/// Commits every row of `req` through the group coordinator, and returns the
/// error code that goes on each of those rows.
///
/// The group routing, the membership and epoch check, and the append each
/// answer with one code for the whole commit, as Kafka's
/// `GroupCoordinatorService.commitOffsets` does with
/// `OffsetCommitRequest.getErrorResponse`.
async fn commit(broker: &Broker, req: &OffsetCommitRequest, version: i16) -> i16 {
    let image = broker.controller.current_image();
    match local_partition_for_group(&image, broker.config.node_id, &req.group_id) {
        Ok(_) => {}
        Err(GroupRoutingError::Unavailable) => return codes::COORDINATOR_NOT_AVAILABLE,
        Err(GroupRoutingError::NotCoordinator) => return codes::NOT_COORDINATOR,
    }

    let now_ms = now_ms();
    let expire_timestamp_ms = expire_timestamp_ms(req.retention_time_ms, now_ms);
    let handle = match validate(broker, req, version).await {
        Ok(handle) => handle,
        Err(code) => return code,
    };

    let commit = Commit {
        now_ms,
        expire_timestamp_ms,
        image: &image,
    };
    match commit_through_actor(&handle, req, commit).await {
        Ok(()) => codes::NONE,
        Err(code) => code,
    }
}

/// The wire value of `retention_time_ms` that asks for the broker's own
/// `offsets.retention.minutes`.
const DEFAULT_RETENTION_TIME_MS: i64 = -1;

/// The values every record of one commit shares.
#[derive(Clone, Copy)]
struct Commit<'a> {
    /// Commit time, stamped on the batch and on every `OffsetCommitValue`.
    now_ms: i64,
    /// The KIP-211 per-commit expiry, when the request asked for one.
    expire_timestamp_ms: Option<i64>,
    /// The image the commit was validated against. It gives each offset the
    /// topic id of its topic name, as `KafkaApis.handleOffsetCommitRequest`
    /// sets it from the metadata cache.
    image: &'a krabka_metadata::MetadataImage,
}

/// KIP-211: resolve the absolute expiry that this commit asked for.
///
/// `OffsetCommitRequest` carries `retention_time_ms` at v2-v4 only. The
/// decoder leaves the field at its schema default of `-1` for every other
/// version, and `-1` is also how a v2-v4 client says "use the broker's
/// `offsets.retention.minutes`". This mirrors
/// `OffsetMetadataManager.expireTimestampMs`.
fn expire_timestamp_ms(retention_time_ms: i64, now_ms: i64) -> Option<i64> {
    (retention_time_ms != DEFAULT_RETENTION_TIME_MS)
        .then(|| now_ms.saturating_add(retention_time_ms))
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// Finds the group of `req` and validates the commit against its membership,
/// as Kafka's `OffsetMetadataManager.validateOffsetCommit` does. It returns
/// the actor that holds the group's offsets, or the error code for every row.
///
/// A group that does not exist is created as a simple group when the
/// generation is negative, which is the admin client or a consumer that does
/// not use group management. Otherwise it answers `GROUP_ID_NOT_FOUND` from
/// v9 on and `ILLEGAL_GENERATION` before.
///
/// A classic or consumer group validates inside its actor, on the actor's
/// LIVE protocol: a KIP-848 migration may have flipped the protocol in place
/// after spawn. A streams group (KIP-1071) validates in its streams actor, and
/// its offsets live in a group actor of the same id, as `TxnOffsetCommit`
/// keeps them.
async fn validate(
    broker: &Broker,
    req: &OffsetCommitRequest,
    version: i16,
) -> Result<Arc<GroupActorHandle>, i16> {
    let coordinator = &broker.group_coordinator;
    let generation = req.generation_id_or_member_epoch;
    let code = if let Some(streams) = coordinator.find_streams(&req.group_id) {
        validate_streams_group_commit(&streams, &req.member_id, generation).await
    } else if let Some(handle) = coordinator.find(&req.group_id) {
        let code = validate_offset_commit(
            &handle,
            &req.member_id,
            generation,
            req.group_instance_id.as_deref(),
            version,
        )
        .await;
        return code.map_or(Ok(handle), Err);
    } else if generation < 0 {
        None
    } else if version >= FIRST_GROUP_ID_NOT_FOUND_VERSION {
        Some(codes::GROUP_ID_NOT_FOUND)
    } else {
        Some(codes::ILLEGAL_GENERATION)
    };
    match code {
        Some(code) => Err(code),
        None => Ok(coordinator.get_or_create_group(&req.group_id, GroupKindTag::Classic)),
    }
}

/// One commit's two halves: the `__consumer_offsets` records for every
/// `(topic, partition)` in the request, and the in-memory entries that mirror
/// them.
///
/// They are built together and travel to the group's actor together, because
/// the actor is what orders them against the KIP-211 retention sweep.
struct CommitRecords {
    batch: RecordBatch,
    entries: Vec<((String, i32), OffsetEntry)>,
}

/// Build both halves of one commit.
fn commit_records(req: &OffsetCommitRequest, commit: Commit<'_>) -> CommitRecords {
    let mut batch = RecordBatch {
        max_timestamp: commit.now_ms,
        ..RecordBatch::default()
    };
    let mut entries = Vec::new();
    let mut delta: i32 = 0;
    for topic in &req.topics {
        for part in &topic.partitions {
            let value = OffsetCommitValue {
                offset: krabka_log::Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: commit.now_ms,
                expire_timestamp_ms: commit.expire_timestamp_ms,
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            entries.push((
                (topic.name.clone(), part.partition_index),
                OffsetEntry {
                    offset: krabka_log::Offset(part.committed_offset),
                    leader_epoch: part.committed_leader_epoch,
                    metadata: part.committed_metadata.clone().unwrap_or_default(),
                    commit_timestamp_ms: commit.now_ms,
                    expire_timestamp_ms: commit.expire_timestamp_ms,
                    topic_id: commit.image.topic(&topic.name).map(|t| t.topic_id),
                },
            ));
            delta += 1;
        }
    }
    batch.last_offset_delta = (delta - 1).max(0);
    CommitRecords { batch, entries }
}

/// Append the commit and apply it to the group, both inside the group's actor.
///
/// The append cannot run outside the mailbox. `coordinator::retention` decides
/// what to tombstone from the actor's in-memory offsets and writes the
/// tombstones in the same turn, so a commit that appended its record first and
/// queued the in-memory update afterwards could be acknowledged and then
/// deleted by a sweep that never saw it — the sweep would read the stale map,
/// tombstone the offset behind the newer record, and stop the actor with the
/// queued update still in flight. Sending both halves as one message puts the
/// commit and the sweep in one order.
///
/// Returns `Err(error_code)` when the append failed or the actor is gone.
async fn commit_through_actor(
    handle: &Arc<GroupActorHandle>,
    req: &OffsetCommitRequest,
    commit: Commit<'_>,
) -> Result<(), i16> {
    let CommitRecords { batch, entries } = commit_records(req, commit);
    let (reply, result) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::CommitOffsets {
            batch,
            entries,
            reply,
        })
        .await
        .is_err()
    {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    match result.await {
        Ok(result) => result,
        Err(_) => Err(codes::UNKNOWN_SERVER_ERROR),
    }
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    OffsetCommitResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &OffsetCommitResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// KIP-211: `-1` is the "use the broker's own retention" sentinel, and it
    /// is also what the decoder leaves behind for a version that does not
    /// carry the field at all. Every other value becomes an absolute deadline.
    #[test]
    fn per_commit_retention_becomes_an_absolute_deadline_unless_it_is_the_sentinel() {
        let cases = [
            (DEFAULT_RETENTION_TIME_MS, 1_000, None),
            (0, 1_000, Some(1_000)),
            (86_400_000, 1_000, Some(86_401_000)),
            // A nonsense value still becomes a deadline rather than being read
            // as the sentinel, which is what Kafka's `== DEFAULT_RETENTION_TIME`
            // check does.
            (-5, 1_000, Some(995)),
            // Arithmetic that would overflow saturates rather than panicking.
            (i64::MAX, 1_000, Some(i64::MAX)),
        ];
        for (retention_time_ms, now_ms, want) in cases {
            check!(
                expire_timestamp_ms(retention_time_ms, now_ms) == want,
                "retention_time_ms={retention_time_ms} now_ms={now_ms}"
            );
        }
    }
}
