//! `ShareFetch` (`api_key` 78), from KIP-932.
//!
//! This handler drives the per-`(group, topic, partition)`
//! [`AcquisitionState`] machine that
//! [`crate::share_partition::manager::SharePartitionLeaderManager`] owns. It
//! validates the share session and checks membership. Then, for every
//! requested partition that this broker leads, it applies any piggybacked
//! acknowledgement, expires stale locks, materializes newly produced records
//! up to the high watermark, acquires a batch of `Available` records under a
//! lock, and reads the verbatim bytes of the acquired offset range from the
//! log.
//!
//! On a KFC-1 scheduled topic this path delivers out of offset order, which is
//! the one read path that can. A classic group commits a single position per
//! partition, so a record it steps over is unreachable for it forever, and its
//! fetch stops at the delivery watermark. A share group tracks per-record
//! state, so this handler keeps its window at the high watermark and instead
//! marks the not-yet-due ranges `Deferred`, which acquisition steps over. The
//! group then gets what is due now and picks up the rest on a later pass, in
//! delivery-time order.
//!
//! When it acquired nothing and the client asked to wait, it long-polls on the
//! partitions' append and HW-advance notifies, and runs the acquire pass once
//! more.
//!
//! `network::dispatch` intercepts this request inline, not through the
//! `&Broker`-only handler table, so that the handler receives the
//! per-connection principal and the peer `SocketAddr` for the per-topic `Read`
//! ACL gate.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::share_fetch_request::{FetchPartition, ShareFetchRequest},
};

mod acknowledge;
mod acquire;
mod authorization;
mod long_poll;
mod pending;
mod records;
mod request;
mod response;

pub(crate) use self::acknowledge::apply_one_ack;
use self::{
    acquire::{AcquireContext, acquire_records},
    authorization::{member_is_valid, topic_read_denied},
    pending::PendingPartition,
    request::{collect_ack_batches, fetch_session_flags, session_release_phases},
    response::{
        acquisition_timeout_ms, encode_error_response, encode_success_response, group_responses,
        not_leader_response, partition_response,
    },
};
use crate::{broker::Broker, codes, error::BrokerError};

#[tracing::instrument(
    name = "handle_share_fetch",
    level = "info",
    skip_all,
    fields(api = "ShareFetch", version, req_bytes = req_bytes.len()),
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
    let req = ShareFetchRequest::decode(&mut cur, version)?;

    let cfg = broker.config.share_group.clone();
    let lock_timeout_ms = acquisition_timeout_ms(&cfg);

    if !cfg.enable {
        return encode_error_response(version, codes::UNSUPPORTED_VERSION, lock_timeout_ms);
    }
    let group = req.group_id.clone().unwrap_or_default();
    let member = req.member_id.clone().unwrap_or_default();

    // Best-effort membership check: if the group has a live share actor, the
    // member must be present in its describe view. When no actor exists yet
    // (e.g. the group was never joined) we are lenient and skip the check —
    // the Task-7 tests always join via `ShareGroupHeartbeat` first, so a
    // present actor with an absent member is the only hard failure.
    if !member_is_valid(broker, &group, &member).await {
        return encode_error_response(version, codes::UNKNOWN_MEMBER_ID, lock_timeout_ms);
    }

    let mgr = broker.share_partition_leaders.clone();
    let image = broker.controller.current_image();

    let mut requested = HashSet::new();
    let mut requested_order = Vec::new();
    let mut request_rows: HashMap<(uuid::Uuid, i32), FetchPartition> = HashMap::new();
    let (has_acknowledgements, final_has_additions) = fetch_session_flags(&req);
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        for partition in &topic.partitions {
            let key = (topic_id, partition.partition_index);
            if requested.insert(key) {
                requested_order.push(key);
            }
            request_rows.insert(key, partition.clone());
        }
    }
    let forgotten: HashSet<(uuid::Uuid, i32)> = req
        .forgotten_topics_data
        .iter()
        .flat_map(|topic| {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            topic
                .partitions
                .iter()
                .copied()
                .map(move |partition| (topic_id, partition))
        })
        .collect();
    let session = match mgr.update_fetch_session(
        &group,
        &member,
        ctx.connection_id,
        req.share_session_epoch,
        &requested,
        &forgotten,
        has_acknowledgements,
        final_has_additions,
    ) {
        Ok(session) => session,
        Err(code) => return encode_error_response(version, code, lock_timeout_ms),
    };
    let (release_before_acquire, release_after_acquire) =
        session_release_phases(session.final_request);
    if release_before_acquire {
        mgr.release_session_partitions(&group, &member, &session.released)
            .await;
    }

    let mut effective_order = requested_order;
    let mut cached_only: Vec<_> = session
        .partitions
        .iter()
        .copied()
        .filter(|partition| !requested.contains(partition))
        .collect();
    cached_only.sort_unstable();
    effective_order.extend(cached_only);

    // Resolve the complete effective session subscription plus request-only
    // acknowledgement rows into pending partitions.
    let mut pending: Vec<PendingPartition> = Vec::new();
    for (topic_id, partition_index) in effective_order {
        let topic_name = mgr.topic_name_for(topic_id);
        let request_row = request_rows.get(&(topic_id, partition_index));
        let fetchable = session.partitions.contains(&(topic_id, partition_index));

        // Per-topic `Read` ACL — mirrors `fetch::handle`'s authorize call.
        let denied = topic_read_denied(broker, &image, ctx, topic_name.as_deref());

        let mut out = partition_response(partition_index);
        let ack_batches = request_row.map_or_else(Vec::new, collect_ack_batches);
        let partition_max_bytes = request_row.map_or(0, |row| row.partition_max_bytes);

        if denied {
            out.error_code = if topic_name.is_some() {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            };
            pending.push(PendingPartition {
                topic_id,
                topic_name,
                partition_index,
                partition_max_bytes,
                leadable: false,
                fetchable,
                ack_batches,
                out,
            });
            continue;
        }

        if !mgr.topic_leader_is_self(topic_id, partition_index) {
            let (leader_id, leader_epoch) = mgr.current_leader_of(topic_id, partition_index);
            out = not_leader_response(partition_index, leader_id, leader_epoch);
            pending.push(PendingPartition {
                topic_id,
                topic_name,
                partition_index,
                partition_max_bytes,
                leadable: false,
                fetchable,
                ack_batches,
                out,
            });
            continue;
        }

        pending.push(PendingPartition {
            topic_id,
            topic_name,
            partition_index,
            partition_max_bytes,
            leadable: true,
            fetchable,
            ack_batches,
            out,
        });
    }

    let acquire = AcquireContext {
        broker,
        manager: &mgr,
        group: &group,
        member: &member,
        max_records: req.max_records,
        max_bytes: req.max_bytes,
        is_renew_ack: req.is_renew_ack,
        config: &cfg,
    };

    let max_wait_ms = if session.final_request {
        0
    } else {
        req.max_wait_ms
    };
    let acquire_result = acquire_records(&acquire, &mut pending, max_wait_ms).await;
    if release_after_acquire {
        mgr.release_session_partitions(&group, &member, &session.released)
            .await;
    }
    acquire_result?;

    // Group pending rows back into per-topic responses, preserving first-seen
    // topic order.
    let responses = group_responses(pending);

    encode_success_response(version, lock_timeout_ms, responses)
}
