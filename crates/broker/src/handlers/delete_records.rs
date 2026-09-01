//! `DeleteRecords` (`api_key=21`). Only the leader trims its local segments.
//!
//! The follower picks up the new `log_start_offset` on its next Fetch, through
//! the existing `OFFSET_OUT_OF_RANGE` recovery path. This matches the Apache
//! Kafka model.
//!
//! KFC-1 adds one bound: on a topic that schedules delivery, the trim stops at
//! the partition's delivery watermark. See [`delivery_capped`].
//!
//! # KFC-9: trimming a partition needs two people
//!
//! A trim destroys committed records, so it is one of the transitions the
//! break-glass two-person rule gates. KIP-107 defines the request and it gains
//! no field for this: an operator gets an approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and then runs `kafka-delete-records`.
//!
//! # KFC-9: a frozen partition is never trimmed
//!
//! A write freeze refuses every operation that removes data from the topic it
//! covers, so a frozen partition answers `POLICY_VIOLATION (44)` here whatever
//! the caller holds. The check runs ahead of the two-person rule, because an
//! approval to trim does not defeat a freeze and a refusal must not spend one.
//!
//! A refused partition answers `POLICY_VIOLATION (44)` on its own row. No
//! version of the `DeleteRecords` response carries an `error_message`, so the
//! refusal text reaches the operator through the audit log and the broker's own
//! log rather than the wire.
//!
//! This trim writes no metadata record, so there is nothing for the consumed
//! proposal to ride beside. The broker appends the consume on its own and only
//! then trims. Consume-then-transition is the safe order of the two: a crash
//! between them loses the approval, where the reverse order would leave an
//! approval that a second trim could spend again. The gate is active only when
//! `[break_glass]` names an approver set.
//!
//! This file is the module root and holds the request loop. The ACL reduction
//! lives in `authz`, the response constructors in `response`, the offset
//! boundary decisions in `offsets`, and the break-glass gate in `gate`.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_audit::PrivilegedPhase;
use krabka_log::Offset;
use krabka_metadata::{AclOperation, BreakGlassAction, MetadataImage};
use krabka_protocol::{
    Decode,
    owned::{
        delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest},
        delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    },
};
use krabka_verified::FreezeMutationKind;
use uuid::Uuid;

mod authz;
mod gate;
mod offsets;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    authz::denied_topic_names,
    gate::{authorize_trim, consumed_proposal_id, refuse_trim, spend_approval, trim_target},
    offsets::{delivery_capped, offset_out_of_range, target_offset},
    response::{delete_records_response, error_partition_result, partition_result, topic_result},
};
use crate::{
    authorizer::authorize_topics,
    break_glass::handlers::audit::{GatedTransition, audit_transition, require_transition},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::RequestContext,
};

#[tracing::instrument(
    name = "handle_delete_records",
    level = "info",
    skip_all,
    fields(api = "DeleteRecords", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteRecordsRequest::decode(&mut cur, version)?;

    let partitions = broker.partitions.clone();

    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the trim loop and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row for that topic.
    let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Delete,
        topic_names.iter().copied(),
    );
    let denied_topics = denied_topic_names(&acl_results);

    let env = TrimEnv {
        broker,
        image: &image,
        ctx,
        partitions: &partitions,
    };
    // KFC-9: the proposals this request already spent. One proposal on a bare
    // topic name covers every partition of it, and it is spent once.
    let mut spent: HashSet<Uuid> = HashSet::new();
    let mut topic_results: Vec<DeleteRecordsTopicResult> = Vec::with_capacity(req.topics.len());

    for topic in req.topics {
        // Per-topic ACL check: if denied, mark every partition in the topic.
        if denied_topics.contains(&topic.name) {
            let part_results: Vec<DeleteRecordsPartitionResult> = topic
                .partitions
                .iter()
                .map(|fp| {
                    error_partition_result(fp.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                })
                .collect();
            topic_results.push(topic_result(topic.name, part_results));
            continue;
        }

        let mut part_results: Vec<DeleteRecordsPartitionResult> =
            Vec::with_capacity(topic.partitions.len());

        for fp in topic.partitions {
            part_results.push(trim_one(&env, &mut spent, &topic.name, &fp).await);
        }

        topic_results.push(topic_result(topic.name, part_results));
    }

    let resp = delete_records_response(topic_results);
    crate::handlers::encode_response(&resp, version)
}

/// Everything one partition's trim reads, and nothing it writes.
pub(super) struct TrimEnv<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    pub(super) partitions: &'a crate::partition_registry::PartitionRegistry,
}

/// Trim one partition, and answer the row the response carries for it.
///
/// `spent` carries the proposals this request already consumed, so one approval
/// that covers a whole topic is spent once however many partitions the request
/// names.
async fn trim_one(
    env: &TrimEnv<'_>,
    spent: &mut HashSet<Uuid>,
    topic: &str,
    fp: &DeleteRecordsPartition,
) -> DeleteRecordsPartitionResult {
    let index = fp.partition_index;
    let part_opt = env.partitions.get(topic, krabka_ids::PartitionIndex(index));
    let Some(part) = part_opt else {
        return error_partition_result(index, codes::UNKNOWN_TOPIC_OR_PARTITION);
    };

    let cur_leader = part
        .current_leader
        .load(std::sync::atomic::Ordering::Acquire);
    if cur_leader != env.broker.config.node_id {
        return error_partition_result(index, codes::NOT_LEADER_OR_FOLLOWER);
    }

    // KFC-9: a write freeze refuses every operation that removes data from the
    // topic it covers, and it answers ahead of the two-person rule. That order
    // is the rule: a break-glass approval to trim does not defeat a freeze, and
    // a trim the freeze refuses must not spend an approval on its way to being
    // refused.
    //
    // No version of the `DeleteRecords` response carries an `error_message`, so
    // the reason reaches the operator through the broker's log alone. Like the
    // produce gate, this refusal emits no privileged-action audit event: a
    // freeze is not a break-glass act, and the registry entry that caused it is
    // already in the metadata log.
    if let crate::freeze::resolve::FreezeMutationResolution::Frozen(record) =
        crate::freeze::resolve::resolve_freeze_mutation(
            env.image,
            topic,
            true,
            FreezeMutationKind::DeleteRecords,
        )
    {
        let verdict = crate::freeze::resolve::FreezeVerdict::from(record);
        tracing::warn!(
            %topic,
            partition = index,
            refusal = %verdict.removal_message(),
            "DeleteRecords refused by a freeze"
        );
        return error_partition_result(index, codes::POLICY_VIOLATION);
    }

    // KFC-9: the two-person rule answers here, before the broker reads a single
    // offset. The approval is not spent yet: a request that then fails its
    // range check leaves the proposal usable, so an operator does not have to
    // gather two signatures again over a typo.
    let consumed = match authorize_trim(env.image, &env.broker.config.break_glass, topic, index) {
        Ok(consumed) => consumed,
        Err(denial) => return refuse_trim(env, topic, index, &denial),
    };

    // Translate offset == -1 → high_watermark per Kafka semantics.
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // `hw`/`leo` are `Offset`; the boundary helpers work in raw `i64`, so
    // unwrap at the seam and re-wrap `requested` for the `Offset`-typed
    // `trim_to_offset` call below.
    let requested = Offset(target_offset(fp.offset, hw.0));

    // The range check reads the offset the admin asked for. A target above the
    // log end is still out of range, and the KFC-1 cap below must not turn that
    // mistake into a silent partial trim.
    if offset_out_of_range(requested.0, leo.0) {
        return error_partition_result(index, codes::OFFSET_OUT_OF_RANGE);
    }

    // KFC-1: hold the trim at the delivery watermark. The recompute runs under
    // the log mutex against the partition's own clock, so it agrees with the
    // cap a fetch at this instant would apply, rather than with whatever the
    // last scheduler sweep published. It answers `None` on a topic that
    // delivers immediately, where the target stands.
    let target = delivery_capped(
        requested,
        part.delivery
            .publish_now(&part.log)
            .map(|delivery| delivery.watermark),
    );

    let audit_target = trim_target(topic, index);
    if let Err(error) = require_transition(
        &env.broker.audit_log,
        &env.broker.config.break_glass,
        env.ctx,
        &GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: &audit_target,
            phase: PrivilegedPhase::Applied,
            proposal_id: consumed.as_ref().and_then(consumed_proposal_id),
            reason: "record trim admitted",
        },
    )
    .await
    {
        tracing::warn!(%topic, partition = index, %error, "DeleteRecords refused by audit policy");
        return error_partition_result(index, codes::POLICY_VIOLATION);
    }

    // KFC-9: spend the approval before the trim removes anything.
    let proposal_id = match spend_approval(env.broker, spent, consumed, &audit_target).await {
        Ok(proposal_id) => proposal_id,
        Err(error) => {
            tracing::warn!(
                %topic, partition = index, %error,
                "DeleteRecords could not spend the break-glass approval"
            );
            return error_partition_result(index, codes::COORDINATOR_NOT_AVAILABLE);
        }
    };

    match part.trim_to_offset(target).await {
        Ok(new_start) => {
            audit_transition(
                &env.broker.audit_log,
                &env.broker.config.break_glass,
                env.ctx,
                &GatedTransition {
                    action: BreakGlassAction::DeleteRecords,
                    target: &audit_target,
                    phase: PrivilegedPhase::Applied,
                    proposal_id,
                    reason: "records deleted below the trim point",
                },
            );
            // Unwrap the `Offset` into the wire `i64` `low_watermark`.
            partition_result(index, new_start.0, codes::NONE)
        }
        Err(e) => {
            tracing::warn!(
                %topic, partition = index, error = %e,
                "DeleteRecords: trim_to_offset failed"
            );
            error_partition_result(index, codes::UNKNOWN_SERVER_ERROR)
        }
    }
}
