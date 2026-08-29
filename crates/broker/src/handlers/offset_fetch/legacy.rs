//! The legacy single-group `OffsetFetch` shape, versions 0 through 7.
//!
//! Before KIP-516 the request carried one `group_id` and one optional
//! `topics` list, and the response carried one flat `topics` array. This
//! module keeps that shape whole: the group gate, the coordinator check, the
//! fetch-all sentinel, and the named-topic rows the client asked for. The v8
//! and above `groups[]` shape lives in `groups` and shares nothing but the
//! group gate and the offset read.

use bytes::Bytes;
use krabka_metadata::AclOperation;
use krabka_protocol::owned::{
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    },
};

use super::{authz::group_authorized, committed::fetch_committed};
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
};

/// Serves an `OffsetFetch` request in the pre-KIP-516 single-group shape.
///
/// It gates the group, resolves the group's committed offsets, and fills
/// `resp.topics`, which the encoder writes only for versions below 8.
// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_legacy(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // ── ACL preamble ────────────────────────────────────────────
    // Step 1: `Describe` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    {
        if !group_authorized(broker, ctx, &req.group_id) {
            let resp = OffsetFetchResponse {
                topics: Vec::new(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                throttle_time_ms: 0,
                ..Default::default()
            };
            return crate::handlers::encode_response(&resp, version);
        }
    }

    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
        return crate::handlers::encode_response(
            &OffsetFetchResponse {
                topics: Vec::new(),
                error_code,
                throttle_time_ms: 0,
                ..Default::default()
            },
            version,
        );
    }

    // Fetch the group's committed offsets from its actor (a classic actor is
    // created for an unknown id; offsets are protocol-agnostic, so an existing
    // actor of either kind serves `FetchCommitted` the same way).
    let committed = fetch_committed(broker, &req.group_id).await;

    // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
    // return every committed offset stored for this group.
    let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
        legacy_fetch_all(broker, ctx, &committed)
    } else {
        legacy_named_topics(
            broker,
            ctx,
            req.topics.as_deref().unwrap_or(&[]),
            &committed,
        )
    };

    let resp = OffsetFetchResponse {
        topics: topics_out,
        error_code: codes::NONE,
        throttle_time_ms: 0,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Builds the response rows for an explicit topic list on the legacy shape.
///
/// Each requested topic is gated with `Read`; a denial replaces every one of
/// its partitions with `TOPIC_AUTHORIZATION_FAILED` and the `-1` sentinels,
/// and an offset the group never committed reports `-1` with no error, which
/// is what the JVM consumer expects for an unset partition.
fn legacy_named_topics(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    req_topics: &[OffsetFetchRequestTopic],
    committed: &std::collections::HashMap<
        (String, i32),
        crate::coordinator::unified::classic_state::OffsetEntry,
    >,
) -> Vec<OffsetFetchResponseTopic> {
    // ── ACL preamble ─────────────────────────────────────
    // Step 2 (named topics): `Read` on each requested topic. On Deny →
    // per-topic `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
    let topic_decisions = {
        let image = broker.controller.current_image();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            req_topics.iter().map(|t| t.name.as_str()),
        )
    };

    req_topics
        .iter()
        .map(|t| {
            let denied = topic_decisions
                .get(t.name.as_str())
                .copied()
                .unwrap_or(AuthorizationResult::Deny)
                == AuthorizationResult::Deny;
            if denied {
                // Return all partitions with TOPIC_AUTHORIZATION_FAILED.
                let partitions = t
                    .partition_indexes
                    .iter()
                    .map(|&pid| OffsetFetchResponsePartition {
                        partition_index: pid,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: None,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        ..Default::default()
                    })
                    .collect();
                OffsetFetchResponseTopic {
                    name: t.name.clone(),
                    partitions,
                    ..Default::default()
                }
            } else {
                let partitions = t
                    .partition_indexes
                    .iter()
                    .map(|&pid| match committed.get(&(t.name.clone(), pid)) {
                        Some(entry) => OffsetFetchResponsePartition {
                            partition_index: pid,
                            committed_offset: entry.offset.0,
                            committed_leader_epoch: entry.leader_epoch,
                            metadata: Some(entry.metadata.clone()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                        None => OffsetFetchResponsePartition {
                            partition_index: pid,
                            committed_offset: -1,
                            committed_leader_epoch: -1,
                            metadata: None,
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                    })
                    .collect();
                OffsetFetchResponseTopic {
                    name: t.name.clone(),
                    partitions,
                    ..Default::default()
                }
            }
        })
        .collect()
}

fn legacy_fetch_all(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    committed: &std::collections::HashMap<
        (String, i32),
        crate::coordinator::unified::classic_state::OffsetEntry,
    >,
) -> Vec<OffsetFetchResponseTopic> {
    let mut by_topic: std::collections::HashMap<String, Vec<OffsetFetchResponsePartition>> =
        std::collections::HashMap::new();
    for ((topic, partition), entry) in committed {
        by_topic
            .entry(topic.clone())
            .or_default()
            .push(OffsetFetchResponsePartition {
                partition_index: *partition,
                committed_offset: entry.offset.0,
                committed_leader_epoch: entry.leader_epoch,
                metadata: Some(entry.metadata.clone()),
                error_code: codes::NONE,
                ..Default::default()
            });
    }
    let names: Vec<_> = by_topic.keys().cloned().collect();
    let image = broker.controller.current_image();
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        context.principal,
        context.peer,
        AclOperation::Read,
        names.iter().map(String::as_str),
    );
    by_topic
        .into_iter()
        .map(|(name, mut partitions)| {
            if decisions.get(name.as_str()).copied() != Some(AuthorizationResult::Allow) {
                for partition in &mut partitions {
                    partition.committed_offset = -1;
                    partition.committed_leader_epoch = -1;
                    partition.metadata = None;
                    partition.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
                }
            }
            OffsetFetchResponseTopic {
                name,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}
