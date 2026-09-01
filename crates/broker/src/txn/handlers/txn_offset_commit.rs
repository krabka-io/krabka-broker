//! `TxnOffsetCommit` (`api_key=28`). The consumer side of the
//! consume-process-produce pattern. A transactional producer that also
//! reads commits its consumed offsets atomically with its transaction by
//! appending them to `__consumer_offsets` with `is_transactional=true` +
//! the producer's (pid, epoch). The offsets are held under the partition's
//! LSO until a `WriteTxnMarkers` commit or abort marker arrives.
//!
//! Versions 0 to 2 are non-flexible and carry no `generation_id` or
//! `member_id` field. Versions 3 to 5 are flexible, carry tagged fields, and
//! add `generation_id`, `member_id`, and `group_instance_id`.
//!
//! On v3 and above, the shared `validate_group_commit` validates the
//! consumer-group metadata against the classic generation or the KIP-848
//! next-gen member epoch. KIP-447 requires fencing that is "consistent with
//! normal offset fencing".
//!
//! ## ACL preamble
//!
//! Three gates run in order:
//! * `Write` on `TransactionalId(transactional_id)`. A deny gives the whole
//!   response `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `Read` on `Group(group_id)`. A deny gives the whole response
//!   `GROUP_AUTHORIZATION_FAILED (30)`.
//! * `Read` on `Topic(name)` for each topic. A deny gives every partition row
//!   of that topic `TOPIC_AUTHORIZATION_FAILED (29)`.

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{Decode, owned::txn_offset_commit_request::TxnOffsetCommitRequest};

mod batch;
mod response;

#[cfg(test)]
mod test_support;

use self::{
    batch::{AppendedTxnOffsets, append_txn_batch},
    response::{build_response, encode_err_all, encode_resp},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        partitioner::{GroupRoutingError, local_partition_for_group},
        unified::{
            actor::{GroupActorMessage, GroupKindTag, validate_group_commit},
            streams::actor::validate_streams_group_commit,
        },
    },
    error::BrokerError,
    txn::util::now_millis,
};

#[tracing::instrument(
    name = "handle_txn_offset_commit",
    level = "info",
    skip_all,
    fields(api = "TxnOffsetCommit", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let partitions = broker.partitions.clone();
    let mut cur: &[u8] = req_bytes;
    let req = TxnOffsetCommitRequest::decode(&mut cur, version)?;

    // ── ACL preamble: Write on TransactionalId ────────────────
    {
        let image = broker.controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        let tid_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: req.transactional_id.as_str(),
            operation: AclOperation::Write,
        };
        if authorizer.authorize(&*image, &tid_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        }
        // Group Read gate.
        let group_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if authorizer.authorize(&*image, &group_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::GROUP_AUTHORIZATION_FAILED);
        }
    }

    // ── ACL preamble: per-topic Read ──────────────────────────
    let topic_decisions = {
        let image = broker.controller.current_image();
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
    let denied_topics: std::collections::HashSet<String> = topic_decisions
        .into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect();

    if let Some(entry) = broker.txn_coordinator.get(&req.transactional_id)
        && entry.lock().await.has_staged_producer_identity()
    {
        return encode_err_all(version, &req, codes::INVALID_TXN_STATE);
    }

    // 1. Verify that this broker leads the group's offsets partition before
    //    creating or accessing its actor.
    let (offsets_partition, txnv) = {
        let image = broker.controller.current_image();
        match local_partition_for_group(&image, broker.config.node_id, &req.group_id) {
            Ok(partition) => (partition, crate::txn::version::resolve_txn_version(&image)),
            Err(GroupRoutingError::Unavailable) => {
                return encode_err_all(version, &req, codes::COORDINATOR_NOT_AVAILABLE);
            }
            Err(GroupRoutingError::NotCoordinator) => {
                return encode_err_all(version, &req, codes::NOT_COORDINATOR);
            }
        }
    };
    let handle = broker
        .group_coordinator
        .find(&req.group_id)
        .unwrap_or_else(|| {
            broker
                .group_coordinator
                .get_or_create_group(&req.group_id, GroupKindTag::Classic)
        });

    // 2. KIP-447 / KIP-1319 fencing — identical to a regular OffsetCommit
    //    (KIP-447: "consistent with normal offset fencing"). For a classic
    //    group this checks member id + group.instance.id + generation
    //    (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / FENCED_INSTANCE_ID); for a
    //    KIP-848 next-gen group the `generation_id` field carries the member
    //    epoch and we return STALE_MEMBER_EPOCH / FENCED_MEMBER_EPOCH /
    //    UNKNOWN_MEMBER_ID. A producer that supplies no metadata (empty
    //    member_id, generation_id = -1) is a simple consumer and is not fenced.
    //    The fields only exist on v3+, so older requests carry the
    //    simple-consumer defaults and no-op. `validate_group_commit` dispatches
    //    on the actor's LIVE `group.kind`, so a KIP-848-flipped group is fenced
    //    against its current protocol, not the stale spawn-time `handle.kind`.
    // KIP-1071: a streams-group consumer's membership lives in the STREAMS
    // group actor, not the classic one. Route its fencing there (member_epoch
    // check) — `validate_group_commit` only knows the classic/consumer actor,
    // so validating a streams member against the freshly-created empty classic
    // actor would wrongly reject every EOS offset commit with UNKNOWN_MEMBER_ID.
    if version >= 3 {
        let code = if let Some(streams) = broker.group_coordinator.find_streams(&req.group_id) {
            validate_streams_group_commit(&streams, &req.member_id, req.generation_id).await
        } else {
            validate_group_commit(
                &handle,
                &req.member_id,
                req.generation_id,
                req.group_instance_id.as_deref(),
            )
            .await
        };
        if let Some(code) = code {
            return encode_err_all(version, &req, code);
        }
    }

    // KIP-890 transaction protocol v2 folds AddOffsetsToTxn into v5+
    // TxnOffsetCommit. Enroll the group's offsets partition with the
    // transaction coordinator before appending the transactional records.
    if version >= 5
        && txnv.verified()
        && req
            .topics
            .iter()
            .any(|topic| !denied_topics.contains(&topic.name) && !topic.partitions.is_empty())
    {
        let code = broker
            .txn_coordinator
            .register_offsets_partition(
                &req.transactional_id,
                krabka_log::ProducerId(req.producer_id),
                req.producer_epoch,
                PartitionIndex(offsets_partition),
                txnv,
            )
            .await;
        if code != codes::NONE {
            return encode_resp(version, &build_response(&req, code, &denied_topics));
        }
    }

    // 3. Append a transactional RecordBatch to __consumer_offsets.
    //    We reuse the OffsetCommitKey/Value layout but stamp the batch with
    //    is_transactional=true + (producer_id, producer_epoch) so the log's
    //    LSO machinery holds the offsets until EndTxn commits/aborts.
    //    Topics denied by the per-topic Read ACL are skipped from the
    //    batch and surfaced as TOPIC_AUTHORIZATION_FAILED in the response.
    let now_ms = now_millis();
    let appended = match append_txn_batch(
        &req,
        &partitions,
        offsets_partition,
        now_ms,
        &denied_topics,
    )
    .await
    {
        Ok(appended) => appended,
        Err(code) => return encode_resp(version, &build_response(&req, code, &denied_topics)),
    };

    // 4. KIP-447: mark those offsets pending on the group actor, so that an
    //    `OffsetFetch` with `require_stable = true` answers
    //    UNSTABLE_OFFSET_COMMIT for them until the transaction's marker
    //    resolves. Marking after the durable append is what guarantees the
    //    marker path can rediscover the same keys in the log and clear them;
    //    a mark placed before a failed append would never be cleared. The
    //    append's log position travels with the mark, because the marker for
    //    this very transaction can be resolved on the actor in between, and
    //    the log order is what tells the actor that it was.
    if let Some(appended) = appended
        && let Err(code) =
            mark_offsets_pending(&handle, req.producer_id, appended, &req.group_id).await
    {
        return encode_resp(version, &build_response(&req, code, &denied_topics));
    }

    // 5. Success — per-(topic, partition) error_code = NONE for allowed,
    //    TOPIC_AUTHORIZATION_FAILED for denied.
    encode_resp(version, &build_response(&req, codes::NONE, &denied_topics))
}

/// Marks the appended offsets as belonging to an unresolved transaction.
///
/// A group actor that cannot take the mark would leave the group answering a
/// `require_stable` fetch with the pre-transaction offset, which is the
/// rewind KIP-447 exists to prevent, so the commit reports
/// `COORDINATOR_NOT_AVAILABLE` rather than claiming a success the fetch path
/// cannot honour.
async fn mark_offsets_pending(
    handle: &crate::coordinator::unified::actor::GroupActorHandle,
    producer_id: i64,
    appended: AppendedTxnOffsets,
    group_id: &str,
) -> Result<(), i16> {
    let (reply, ack) = tokio::sync::oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::AddPendingTxnOffsets {
            producer_id,
            written_at: appended.written_at,
            keys: appended.keys,
            reply,
        })
        .await
        .is_err()
        || ack.await.is_err()
    {
        tracing::warn!(
            group = %group_id,
            producer_id,
            "TxnOffsetCommit: group actor could not record the pending transactional offsets"
        );
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::check;
    use tokio::sync::oneshot;

    use super::*;
    use crate::coordinator::unified::test_support::make_coord;

    /// The mark is what makes a later `require_stable` `OffsetFetch` answer
    /// `UNSTABLE_OFFSET_COMMIT`, so a group actor that cannot take it must not
    /// leave the commit reporting success: the consumer would read the
    /// pre-transaction offset and reprocess the records the transaction had
    /// already handled. A live actor takes it and reports it; a departed one
    /// makes the commit answer `COORDINATOR_NOT_AVAILABLE`.
    #[tokio::test]
    async fn a_live_actor_takes_the_mark_and_a_departed_one_fails_the_commit() {
        let coord = make_coord();
        let handle = coord.get_or_create_group("g", GroupKindTag::Classic);

        mark_offsets_pending(
            &handle,
            7,
            AppendedTxnOffsets {
                written_at: 4,
                keys: vec![("orders".to_string(), 0)],
            },
            "g",
        )
        .await
        .expect("a live actor takes the mark");

        let (reply, offsets) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .expect("send FetchOffsets");
        check!(
            offsets.await.expect("FetchOffsets reply").pending_txn
                == HashSet::from([("orders".to_string(), 0)])
        );

        let (reply, ack) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Shutdown(reply))
            .await
            .expect("send Shutdown");
        ack.await.expect("Shutdown ack");
        crate::coordinator::unified::test_support::await_until("the actor's handle closes", || {
            handle.tx.is_closed()
        })
        .await;

        let refused = mark_offsets_pending(
            &handle,
            7,
            AppendedTxnOffsets {
                written_at: 5,
                keys: vec![("orders".to_string(), 1)],
            },
            "g",
        )
        .await
        .expect_err("a departed actor cannot take the mark");
        check!(refused == codes::COORDINATOR_NOT_AVAILABLE);
    }
}
