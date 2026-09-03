//! `DeleteRecords` (`api_key=21`). Only the leader trims its local segments.
//!
//! The follower picks up the new `log_start_offset` on its next Fetch, through
//! the existing `OFFSET_OUT_OF_RANGE` recovery path. This matches the Apache
//! Kafka model.
//!
//! A trim is bounded by the current high watermark and, on a topic that
//! schedules delivery, by the partition's delivery watermark. See
//! [`offsets::trim_decision`].
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
use krabka_units::convert::TimeExt as _;
use krabka_verified::{DeleteRecordsTrimDecision, FreezeMutationKind};
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
    offsets::trim_decision,
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

/// Record the trim as the partition's diskless `DeleteRecords` floor, durably.
///
/// A diskless partition's local log start is the flusher's trim frontier, not
/// a delete point, so the object tier cannot read the floor off the log. The
/// index topic carries it instead, as a keyed record the projection replays
/// like any other: a cold read below the floor misses from here on,
/// `ListOffsets(EARLIEST)` answers the floor, and the flusher's next tick
/// tombstones every index range that ends below it.
///
/// The range tombstones could not stand in for the record. A range that
/// straddles the floor keeps live records, so retention must not expire it,
/// and neither may the newest range; both would come back out of a replay
/// still covering the offsets below the floor. So this runs *before* the trim
/// is acknowledged, and a failure fails the row: a client told its records are
/// gone must not see them again after a restart or a leadership move.
///
/// A partition with no object tier behind it has nothing to record.
///
/// # Errors
///
/// Returns an error when the floor cannot be published or does not reach the
/// committed projection in time.
async fn publish_diskless_delete_floor(
    env: &TrimEnv<'_>,
    topic: &str,
    part: &crate::partition::Partition,
    floor: Offset,
) -> Result<(), crate::error::BrokerError> {
    let Some((handle, topic_id)) = diskless_index(env, topic, part) else {
        return Ok(());
    };
    // A rebuilding projection is the one case that must not be mistaken for
    // "nothing to record": there is a floor to persist and nowhere to put it.
    let Some(index_log) = handle.index_log() else {
        return Err(crate::error::BrokerError::Txn(
            "diskless WAL index log is rebuilding; the delete floor cannot be recorded".into(),
        ));
    };
    index_log
        .publish_delete_floor(
            topic_id,
            part.index.get(),
            floor.0,
            env.broker
                .config
                .diskless_wal_index_projection_timeout
                .to_std(),
        )
        .await
}

/// The offset a diskless partition actually starts at: the lower of the local
/// log start and the first offset the object tier still answers for, which is
/// what [`crate::handlers::list_offsets`] reports for `EARLIEST`.
///
/// `None` for a partition with no object tier behind it, which trims against
/// its local log start like any other.
async fn diskless_logical_start(
    env: &TrimEnv<'_>,
    topic: &str,
    part: &crate::partition::Partition,
) -> Option<i64> {
    let (handle, topic_id) = diskless_index(env, topic, part)?;
    let covered = handle
        .index
        .lock()
        .await
        .earliest_covered(topic_id, part.index.get())?;
    Some(covered.min(part.log_start_offset().0))
}

/// The diskless read handle and topic id behind a diskless partition.
fn diskless_index<'a>(
    env: &'a TrimEnv<'_>,
    topic: &str,
    part: &crate::partition::Partition,
) -> Option<(&'a crate::diskless::read::DisklessReadHandle, Uuid)> {
    if !part.diskless {
        return None;
    }
    env.broker
        .diskless_read
        .as_deref()
        .zip(env.image.topic(topic).map(|topic| topic.topic_id))
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

    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Recompute delivery under the log mutex against the partition's own
    // clock. This agrees with the cap a fetch at this instant would apply,
    // rather than with whatever the last scheduler sweep published. It
    // answers `None` on a topic that delivers immediately. The verified
    // decision rejects malformed state, preserves stale retries, and caps
    // every admitted target at both HWM and delivery.
    let delivery_watermark = part
        .delivery
        .publish_now(&part.log)
        .map(|delivery| delivery.watermark);
    // A diskless partition's local log start is the flusher's trim frontier:
    // the records below it are in the object store, not gone. The trim has to
    // measure against the offset the partition actually starts at, which is
    // the same one `ListOffsets(EARLIEST)` answers, or every request below the
    // trim frontier would be a no-op that deletes nothing.
    let current_start = diskless_logical_start(env, topic, &part)
        .await
        .map_or_else(|| part.log_start_offset(), Offset);
    let target = match trim_decision(fp.offset, hw, leo, current_start, delivery_watermark) {
        DeleteRecordsTrimDecision::Noop { frontier }
        | DeleteRecordsTrimDecision::Apply { frontier } => Offset(frontier),
        DeleteRecordsTrimDecision::RejectMalformed
        | DeleteRecordsTrimDecision::RejectOutOfRange => {
            return error_partition_result(index, codes::OFFSET_OUT_OF_RANGE);
        }
    };

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

    // Ahead of the trim, so the acknowledgement below can never outrun the
    // durable record of what was deleted.
    if let Err(error) = publish_diskless_delete_floor(env, topic, &part, target).await {
        tracing::warn!(
            %topic, partition = index, %error,
            "DeleteRecords could not record the diskless delete floor"
        );
        return error_partition_result(index, codes::UNKNOWN_SERVER_ERROR);
    }

    match part.trim_to_offset(target).await {
        Ok(new_start) => {
            // On a diskless partition the local trim frontier is already past
            // `target` in the steady state, so `new_start` says nothing about
            // what a client can still read. The floor does, and it is also the
            // low watermark the response carries.
            let low_watermark = if part.diskless { target.0 } else { new_start.0 };
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
            partition_result(index, low_watermark, codes::NONE)
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
