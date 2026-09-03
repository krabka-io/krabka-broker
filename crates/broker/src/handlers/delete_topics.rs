//! `DeleteTopics` (`api_key=20`). This handler routes through
//! `Controller::submit_change`, so the metadata quorum records every topic
//! deletion before the broker tears down the partition dirs and the in-memory
//! state.
//!
//! # KFC-9: deleting a topic needs two people
//!
//! A topic deletion destroys every record the topic holds, so it is one of the
//! transitions the break-glass two-person rule gates. The request gains no
//! field for it: an operator gets an approval out of band through
//! `krabka-guard`, targeted at the topic name, and then runs
//! `kafka-topics --delete`.
//!
//! # KFC-9: a frozen topic is never deleted
//!
//! A write freeze refuses every operation that removes data from the topic it
//! covers, so a frozen topic answers `POLICY_VIOLATION (44)` here whatever the
//! caller holds. The check runs ahead of the two-person rule, because an
//! approval to delete does not defeat a freeze and a refusal must not spend
//! one.
//!
//! A refused topic answers `POLICY_VIOLATION (44)` on its own row, which is the
//! code Apache Kafka already returns from `CreateTopicPolicy` and
//! `AlterConfigPolicy`, so `AdminClient` surfaces a `PolicyViolationException`
//! per topic. The consumed proposal rides the same `submit_change` call as the
//! delete record, so the approval and the deletion commit together. The gate is
//! active only when `[break_glass]` names an approver set.
//!
//! This file holds the request flow. Request resolution lives in `request`,
//! the `Delete` ACL check in `authz`, the break-glass gate in `gate`, the
//! response shapes in `wire`, the post-commit local tear-down in `teardown`,
//! the remote-tier snapshot and cascade in `tiering`, and the audit record in
//! `audit`.

use bytes::Bytes;
use krabka_audit::PrivilegedPhase;
use krabka_metadata::BreakGlassAction;
use krabka_protocol::{
    Decode,
    owned::{
        delete_topics_request::DeleteTopicsRequest, delete_topics_response::DeletableTopicResult,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::RaftError;
use krabka_verified::FreezeMutationKind;

use crate::{
    break_glass::{
        handlers::audit::{GatedTransition, audit_transition, require_transition},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::RequestContext,
    time_util::now_ms,
};

mod audit;
mod authz;
mod gate;
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
    gate::{consumed_proposal_id, delete_topic_records},
    request::resolve_topic_names,
    teardown::remove_local_partitions,
    tiering::{spawn_remote_cascades, tiered_partitions},
    wire::{delete_topic_result, delete_topics_response, refused_topic_result},
};

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
    ctx: &RequestContext<'_>,
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

        // KFC-9: a write freeze refuses every operation that removes data
        // from the topic it covers, and it answers ahead of the two-person
        // rule. That order is the rule: a break-glass approval to delete does
        // not defeat a freeze, and a deletion the freeze refuses must not
        // spend an approval on its way to being refused.
        //
        // Like the produce gate, this refusal emits no privileged-action audit
        // event. A freeze is not a break-glass act, and the registry entry that
        // caused the refusal is already in the metadata log and in the audit
        // record of the freeze that set it.
        if let crate::freeze::resolve::FreezeMutationResolution::Frozen(record) =
            crate::freeze::resolve::resolve_freeze_mutation(
                &image,
                &name,
                true,
                FreezeMutationKind::DeleteTopic,
            )
        {
            let verdict = crate::freeze::resolve::FreezeVerdict::from(record);
            let message = verdict.removal_message();
            tracing::warn!(topic = %name, refusal = %message, "DeleteTopics refused by a freeze");
            results.push(refused_topic_result(name, codes::POLICY_VIOLATION, message));
            continue;
        }

        // KFC-9: the two-person rule, and the records this append carries. It
        // runs before the broker snapshots any partition state, because a
        // deletion the broker will refuse has no reason to walk the topic's
        // logs.
        let records =
            match delete_topic_records(&image, &broker.config.break_glass, &name, now_ms()) {
                Ok(records) => records,
                Err(denial) => {
                    let message = denial.to_string();
                    break_glass_metrics::record_refusal(&broker.metrics, denial.action);
                    audit_transition(
                        &broker.audit_log,
                        &broker.config.break_glass,
                        ctx,
                        &GatedTransition {
                            action: BreakGlassAction::DeleteTopic,
                            target: &name,
                            phase: PrivilegedPhase::Refused,
                            proposal_id: denial.proposal_id(),
                            reason: &message,
                        },
                    );
                    results.push(refused_topic_result(name, codes::POLICY_VIOLATION, message));
                    continue;
                }
            };
        let proposal_id = records.first().and_then(consumed_proposal_id);
        if let Err(error) = require_transition(
            &broker.audit_log,
            &broker.config.break_glass,
            ctx,
            &GatedTransition {
                action: BreakGlassAction::DeleteTopic,
                target: &name,
                phase: PrivilegedPhase::Applied,
                proposal_id,
                reason: "topic deletion admitted",
            },
        )
        .await
        {
            let message = format!("privileged action refused: {error}");
            results.push(refused_topic_result(name, codes::POLICY_VIOLATION, message));
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

        let res = controller.submit_change(records).await;

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
                audit_transition(
                    &broker.audit_log,
                    &broker.config.break_glass,
                    ctx,
                    &GatedTransition {
                        action: BreakGlassAction::DeleteTopic,
                        target: &name,
                        phase: PrivilegedPhase::Applied,
                        proposal_id,
                        reason: "topic deleted",
                    },
                );
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

    // KIP-599: report the controller_mutation_rate throttle in the response and
    // hand the window to the connection loop, which mutes the connection once
    // the response is written (KIP-219). The delay is the only throttle this
    // api applies — the dispatch loop marks it quota-exempt and never charges
    // it the request quota — so resolving it through the metric records the
    // throttle phase and the quota that caused it exactly once per request.
    let delay = broker.metrics.record_applied_throttle(
        krabka_protocol::api_key::ApiKey::DeleteTopics as i16,
        &[(crate::metrics::QuotaType::ControllerMutation, quota.delay()).into()],
    );
    let throttle_time_ms = crate::quota::throttle_time_ms(delay);
    ctx.record_throttle(delay);

    let resp = delete_topics_response(results, throttle_time_ms);
    crate::handlers::encode_response(&resp, version)
}
