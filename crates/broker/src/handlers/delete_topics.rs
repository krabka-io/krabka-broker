//! `DeleteTopics` (`api_key=20`). This handler routes through
//! `Controller::submit_change`, so the metadata quorum records every topic
//! deletion before the broker tears down the partition dirs and the in-memory
//! state.
//!
//! This file holds the request flow. Request resolution lives in `request`,
//! the `Delete` ACL check in `authz`, the response shapes in `wire`, the
//! post-commit local tear-down in `teardown`, the remote-tier snapshot and
//! cascade in `tiering`, and the audit record in `audit`.

use bytes::Bytes;
use krabka_metadata::{DeleteTopicRecord, MetadataRecord};
use krabka_protocol::{
    Decode,
    owned::{
        delete_topics_request::DeleteTopicsRequest, delete_topics_response::DeletableTopicResult,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::RaftError;
use krabka_units::{Time, convert::TimeExt};

use crate::{broker::Broker, codes, error::BrokerError};

mod audit;
mod authz;
mod request;
mod teardown;
mod tiering;
mod wire;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    audit::{audit_deleted_topics, deleted_topic_resources},
    authz::denied_topic_names,
    request::resolve_topic_names,
    teardown::remove_local_partitions,
    tiering::{spawn_remote_cascades, tiered_partitions},
    wire::{delete_topic_result, delete_topics_response},
};

/// KIP-599: a zero delay means the request was never throttled, so the
/// response path must not sleep at all.
fn should_wait_for_quota_delay(delay: Time) -> bool {
    delay > <Time as TimeExt>::ZERO
}

#[tracing::instrument(
    name = "handle_delete_topics",
    level = "info",
    skip_all,
    fields(api = "DeleteTopics", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = &broker.controller;
    let partitions = broker.partitions.clone();
    let log_dirs = broker.config.all_log_dirs();

    let mut cur: &[u8] = req_bytes;
    let req = DeleteTopicsRequest::decode(&mut cur, version)?;

    let image = controller.current_image();
    // (resolved_name, requested_by_id, requested_topic_id)
    let name_list = resolve_topic_names(&req, &image);

    // KIP-599: count partition mutations before running the delete logic.
    // Nonexistent topics (name_opt = None) contribute 0 partitions.
    let mutation_count: u64 = name_list
        .iter()
        .map(|(name_opt, _, _)| {
            name_opt
                .as_deref()
                .map_or(0, |name| image.partitions_of(name).count() as u64)
        })
        .sum();
    let quota = crate::quota::apply_controller_mutation_quota_mode(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
        broker.config.controller_mutation_quota_window,
        broker.config.quota_throttle_max,
        version >= 5,
    );
    if quota.is_rejected() {
        let results = name_list
            .iter()
            .map(|(name, _, topic_id)| {
                delete_topic_result(name.clone(), *topic_id, codes::THROTTLING_QUOTA_EXCEEDED)
            })
            .collect();
        return crate::handlers::encode_response(
            &delete_topics_response(results, crate::quota::throttle_time_ms(quota.delay())),
            version,
        );
    }

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the delete loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let denied_topics = denied_topic_names(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
        &name_list,
    );

    let mut results: Vec<DeletableTopicResult> = Vec::with_capacity(name_list.len());

    for (name_opt, requested_by_id, req_topic_id) in name_list {
        let Some(name) = name_opt else {
            // topic not found in image — choose error code by how it was requested.
            let error_code = if requested_by_id {
                codes::UNKNOWN_TOPIC_ID
            } else {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            };
            results.push(delete_topic_result(None, req_topic_id, error_code));
            continue;
        };

        // Per-topic ACL check.
        if denied_topics.contains(&name) {
            results.push(delete_topic_result(
                Some(name),
                WireUuid::ZERO,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ));
            continue;
        }

        // Snapshot every local partition before committing the metadata
        // deletion. The metadata image watcher can remove registry entries as
        // soon as the commit becomes visible; enumerating afterward races that
        // watcher and can leave the on-disk log directory behind. A later
        // create of the same topic name would then reopen the deleted topic's
        // WAL, including stale transactional visibility state.
        let local_partitions = partitions.partitions_of(&name);
        let topic_id = image.topic(&name).map(|topic| topic.topic_id);

        let tiered_to_cascade =
            tiered_partitions(broker, &partitions, &image, &name, &local_partitions);

        let res = controller
            .submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: name.clone(),
            })])
            .await;

        let error_code = match res {
            Ok(_) => {
                // Committed to quorum — tear down in-memory state and dirs.
                remove_local_partitions(
                    broker,
                    &partitions,
                    &log_dirs,
                    &name,
                    topic_id,
                    local_partitions,
                );
                // Now that the local tear-down is done, cascade the remote tier.
                spawn_remote_cascades(broker, tiered_to_cascade);
                codes::NONE
            }
            Err(RaftError::Metadata(krabka_metadata::MetadataError::UnknownTopic(_))) => {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => codes::NOT_CONTROLLER,
            Err(e) => {
                tracing::error!(topic = %name, error = %e, "DeleteTopics submit_change failed");
                codes::UNKNOWN_SERVER_ERROR
            }
        };

        results.push(delete_topic_result(Some(name), WireUuid::ZERO, error_code));
    }

    // Audit: emit one AdminOperation record for the successfully-deleted topics.
    audit_deleted_topics(
        broker.audit_log.as_ref(),
        ctx,
        deleted_topic_resources(&results),
    );

    // KIP-599: apply controller_mutation_rate throttle after response assembly.
    let delay = quota.delay();
    let throttle_time_ms = crate::quota::throttle_time_ms(delay);
    if should_wait_for_quota_delay(delay) {
        tokio::time::sleep(delay.to_std()).await;
    }

    let resp = delete_topics_response(results, throttle_time_ms);
    crate::handlers::encode_response(&resp, version)
}
