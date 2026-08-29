//! The KIP-516 batched `groups[]` `OffsetFetch` shape, version 8 and above.
//!
//! From version 8 one request carries several groups, each with its own topic
//! list and its own error code, and from version 10 the topics are keyed by
//! `topic_id` rather than by name. Internal offset storage stays keyed by
//! name, so this module resolves each id to a name at the wire boundary and
//! echoes the id back on the response. The legacy single-group shape lives in
//! `legacy`.

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

use super::{authz::group_authorized, committed::fetch_committed};
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
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
// per-group loop: ACL + id→name resolve + named/fetch-all branches
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

        // Fetch the group's committed offsets from its actor (a classic actor
        // is created for an unknown id; offsets are protocol-agnostic, so an
        // existing actor of either kind serves `FetchCommitted` the same way).
        let committed = fetch_committed(broker, &grp.group_id).await;
        let image = broker.controller.current_image();

        // Named/id'd topics: resolve id→name (v10) and read each requested
        // partition from the name-keyed store. `None` topics → fetch-all.
        let topics_out: Vec<OffsetFetchResponseTopics> =
            if let Some(req_topics) = grp.topics.as_deref() {
                group_named_topics(broker, ctx, &image, req_topics, &committed)
            } else {
                // fetch-all: every committed offset for the group, grouped by
                // topic name. Echo each topic's id (required at v10, where the
                // name is dropped from the wire) and authorize Read per topic.
                let mut by_topic: std::collections::HashMap<
                    String,
                    Vec<OffsetFetchResponsePartitions>,
                > = std::collections::HashMap::new();
                for ((topic, pid), entry) in &committed {
                    by_topic.entry(topic.clone()).or_default().push(
                        OffsetFetchResponsePartitions {
                            partition_index: *pid,
                            committed_offset: entry.offset.0,
                            committed_leader_epoch: entry.leader_epoch,
                            metadata: Some(entry.metadata.clone()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                    );
                }

                let discovered: Vec<String> = by_topic.keys().cloned().collect();
                let decisions = authorize_topics(
                    broker.config.authorizer.as_ref(),
                    &*image,
                    ctx.principal,
                    ctx.peer,
                    AclOperation::Read,
                    discovered.iter().map(String::as_str),
                );

                by_topic
                    .into_iter()
                    .map(|(name, partitions)| {
                        let topic_id = image
                            .topic(&name)
                            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));
                        let denied = decisions
                            .get(name.as_str())
                            .copied()
                            .unwrap_or(AuthorizationResult::Deny)
                            == AuthorizationResult::Deny;
                        if denied {
                            OffsetFetchResponseTopics {
                                name,
                                topic_id,
                                partitions: partitions
                                    .into_iter()
                                    .map(|p| OffsetFetchResponsePartitions {
                                        partition_index: p.partition_index,
                                        committed_offset: -1,
                                        committed_leader_epoch: -1,
                                        metadata: None,
                                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            }
                        } else {
                            OffsetFetchResponseTopics {
                                name,
                                topic_id,
                                partitions,
                                ..Default::default()
                            }
                        }
                    })
                    .collect()
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

fn group_named_topics(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    image: &krabka_metadata::MetadataImage,
    requested: &[krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopics],
    committed: &std::collections::HashMap<
        (String, i32),
        crate::coordinator::unified::classic_state::OffsetEntry,
    >,
) -> Vec<OffsetFetchResponseTopics> {
    let resolved: Vec<_> = requested
        .iter()
        .map(|topic| {
            let name = if topic.topic_id == WireUuid::ZERO {
                Some(topic.name.clone())
            } else {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .map(str::to_string)
            };
            (topic, name)
        })
        .collect();
    let names: Vec<_> = resolved
        .iter()
        .filter_map(|(_, name)| name.clone())
        .collect();
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Read,
        names.iter().map(String::as_str),
    );
    resolved
        .into_iter()
        .map(|(topic, name)| {
            let error = match name.as_deref() {
                None => codes::UNKNOWN_TOPIC_ID,
                Some(name) if decisions.get(name).copied() != Some(AuthorizationResult::Allow) => {
                    codes::TOPIC_AUTHORIZATION_FAILED
                }
                Some(_) => codes::NONE,
            };
            let partitions = topic
                .partition_indexes
                .iter()
                .map(|partition| {
                    let entry = if error == codes::NONE {
                        name.as_ref()
                            .and_then(|name| committed.get(&(name.clone(), *partition)))
                    } else {
                        None
                    };
                    OffsetFetchResponsePartitions {
                        partition_index: *partition,
                        committed_offset: entry.map_or(-1, |value| value.offset.0),
                        committed_leader_epoch: entry.map_or(-1, |value| value.leader_epoch),
                        metadata: entry.map(|value| value.metadata.clone()),
                        error_code: error,
                        ..Default::default()
                    }
                })
                .collect();
            OffsetFetchResponseTopics {
                name: name.unwrap_or_default(),
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}
