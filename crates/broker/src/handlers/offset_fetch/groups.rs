//! The KIP-516 batched `groups[]` `OffsetFetch` shape, version 8 and above.
//!
//! From version 8 one request carries several groups, each with its own topic
//! list and its own error code, and from version 10 the topics are keyed by
//! `topic_id` rather than by name. Internal offset storage stays keyed by
//! name, so this module resolves each id to a name at the wire boundary and
//! echoes the id back on the response. The legacy single-group shape lives in
//! `legacy`.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_metadata::AclOperation;
use krabka_protocol::{
    owned::{
        offset_fetch_request::OffsetFetchRequest,
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{authz::group_authorized, committed::fetch_offsets, unstable};
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::unified::group::GroupOffsets,
    error::BrokerError,
};

/// Per-group fetch for v8 and above.
///
/// It processes `req.groups` into `resp.groups` and leaves `resp.topics`
/// empty, because the encoder writes `resp.topics` only for v < 8.
///
/// The offset storage keys by name, so at v10 this function resolves each
/// requested `topic_id` to a name and echoes the id back. An unknown id gives
/// `UNKNOWN_TOPIC_ID` for each partition.
///
/// `require_stable` is a top-level request field, not a per-group one, so one
/// request either asks every group it names for stable offsets or none of
/// them.
// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_groups(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut groups_out: Vec<OffsetFetchResponseGroup> = Vec::with_capacity(req.groups.len());

    for grp in &req.groups {
        // ── ACL: `Describe` on `Group(group_id)` ────────────────
        {
            if !group_authorized(broker, ctx, &grp.group_id) {
                groups_out.push(OffsetFetchResponseGroup {
                    group_id: grp.group_id.clone(),
                    topics: Vec::new(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                });
                continue;
            }
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &grp.group_id) {
            groups_out.push(OffsetFetchResponseGroup {
                group_id: grp.group_id.clone(),
                topics: Vec::new(),
                error_code,
                ..Default::default()
            });
            continue;
        }

        // Fetch the group's offset state from its actor. An unknown id reads
        // as a group with no offsets and creates nothing. A consumer group
        // checks the v9+ member id and epoch first, and a refusal is the
        // group's error code with no topics (Kafka's
        // `OffsetFetchResponse.groupError`).
        let offsets = match fetch_offsets(
            broker,
            &grp.group_id,
            grp.member_id.as_deref(),
            grp.member_epoch,
        )
        .await
        {
            Ok(offsets) => offsets,
            Err(error_code) => {
                groups_out.push(OffsetFetchResponseGroup {
                    group_id: grp.group_id.clone(),
                    topics: Vec::new(),
                    error_code,
                    ..Default::default()
                });
                continue;
            }
        };
        let image = broker.controller.current_image();

        // Named/id'd topics: resolve id→name (v10) and read each requested
        // partition from the name-keyed store. `None` topics → fetch-all.
        let topics_out: Vec<OffsetFetchResponseTopics> =
            if let Some(req_topics) = grp.topics.as_deref() {
                group_named_topics(
                    broker,
                    ctx,
                    version,
                    &image,
                    req_topics,
                    &offsets,
                    req.require_stable,
                )
            } else {
                group_fetch_all(broker, ctx, version, &image, &offsets, req.require_stable)
            };

        groups_out.push(OffsetFetchResponseGroup {
            group_id: grp.group_id.clone(),
            topics: topics_out,
            error_code: codes::NONE,
            ..Default::default()
        });
    }

    let resp = OffsetFetchResponse {
        topics: Vec::new(),
        error_code: codes::NONE,
        throttle_time_ms: 0,
        groups: groups_out,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// The first `OffsetFetch` version that names each topic by `topic_id` only.
/// The request schema carries `name` at versions 8-9 and `topic_id` from this
/// version on (Kafka's `OffsetFetchRequest.TOPIC_ID_MIN_VERSION`).
const FIRST_TOPIC_ID_VERSION: i16 = 10;

/// Builds one group's rows for an explicit topic list on the KIP-516 shape.
///
/// The precedence is the one in Kafka's `KafkaApis.fetchOffsetsForGroup`. At
/// v10 each `topic_id` resolves to a name, and a row whose name stays empty
/// answers `UNKNOWN_TOPIC_ID` on every partition. The zero id is such a row.
/// A topic without a `Describe` grant then answers
/// `TOPIC_AUTHORIZATION_FAILED`.
/// Within an allowed topic, a partition that an unresolved transaction has
/// written reports `UNSTABLE_OFFSET_COMMIT` under `require_stable` before any
/// offset is read.
///
/// The allowed topics come first, in request order, and the refused topics
/// follow them, as Kafka appends `errorTopics` after the coordinator's rows.
fn group_named_topics(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    requested: &[krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopics],
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopics> {
    let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    let resolved: Vec<_> = requested
        .iter()
        .map(|topic| {
            let name = if !use_topic_ids {
                topic.name.clone()
            } else if topic.topic_id == WireUuid::ZERO {
                String::new()
            } else {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .map(str::to_string)
                    .unwrap_or_default()
            };
            (topic, name)
        })
        .collect();
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Describe,
        resolved
            .iter()
            .filter(|(_, name)| !(use_topic_ids && name.is_empty()))
            .map(|(_, name)| name.as_str()),
    );

    let mut allowed = Vec::with_capacity(resolved.len());
    let mut refused = Vec::new();
    for (topic, name) in &resolved {
        let refusal = if use_topic_ids && name.is_empty() {
            Some(codes::UNKNOWN_TOPIC_ID)
        } else if decisions.get(name.as_str()).copied() == Some(AuthorizationResult::Allow) {
            None
        } else {
            Some(codes::TOPIC_AUTHORIZATION_FAILED)
        };
        let partitions = topic
            .partition_indexes
            .iter()
            .map(|&partition| match refusal {
                Some(error_code) => missing_offset_row(partition, error_code),
                None => committed_row(name, partition, offsets, require_stable),
            });
        let row = OffsetFetchResponseTopics {
            name: name.clone(),
            topic_id: topic.topic_id,
            partitions: partitions.collect(),
            ..Default::default()
        };
        if refusal.is_some() {
            refused.push(row);
        } else {
            allowed.push(row);
        }
    }
    allowed.extend(refused);
    allowed
}

/// Builds one group's rows for the fetch-all sentinel (a null topic list) on
/// the KIP-516 shape.
///
/// Kafka's `KafkaApis.fetchAllOffsetsForGroup` keeps only the topics that the
/// principal may `Describe`, and leaves every other topic out of the response
/// rather than answering it with an error. The coordinator sends no topic id,
/// so the id comes from the metadata image, and at v10, where the wire carries
/// only the id, a topic that the image does not hold is left out too because
/// it cannot be written without one. The topics come in name order.
fn group_fetch_all(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopics> {
    let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    let mut by_topic: BTreeMap<&str, Vec<OffsetFetchResponsePartitions>> = BTreeMap::new();
    for (topic, partition) in offsets.committed.keys() {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(committed_row(topic, *partition, offsets, require_stable));
    }
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Describe,
        by_topic.keys().copied(),
    );
    by_topic
        .into_iter()
        .filter(|(name, _)| decisions.get(name).copied() == Some(AuthorizationResult::Allow))
        .filter_map(|(name, mut partitions)| {
            let topic_id = image
                .topic(name)
                .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));
            if use_topic_ids && topic_id == WireUuid::ZERO {
                return None;
            }
            partitions.sort_by_key(|p| p.partition_index);
            Some(OffsetFetchResponseTopics {
                name: name.to_string(),
                topic_id,
                partitions,
                ..Default::default()
            })
        })
        .collect()
}

/// The row of one partition of an allowed topic.
fn committed_row(
    topic: &str,
    partition_index: i32,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> OffsetFetchResponsePartitions {
    let key = (topic.to_string(), partition_index);
    if require_stable && offsets.pending_txn.contains(&key) {
        return unstable::group_row(partition_index);
    }
    offsets.committed.get(&key).map_or_else(
        || missing_offset_row(partition_index, codes::NONE),
        |entry| OffsetFetchResponsePartitions {
            partition_index,
            committed_offset: entry.offset.0,
            committed_leader_epoch: entry.leader_epoch,
            metadata: Some(entry.metadata.clone()),
            error_code: codes::NONE,
            ..Default::default()
        },
    )
}

/// A partition row that carries no committed offset.
///
/// Kafka's `OffsetMetadataManager.fetchOffsets` builds this row for a
/// partition with no offset, and `KafkaApis.fetchOffsetsForGroup` builds it
/// for a refused topic: offset -1, leader epoch -1, and the empty metadata
/// string. The empty string is the schema default of `Metadata`, so the row
/// carries it, not null.
fn missing_offset_row(partition_index: i32, error_code: i16) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code,
        ..Default::default()
    }
}
