//! `ShareFetch` (`api_key` 78), from KIP-932.
//!
//! This handler drives the per-`(group, topic, partition)`
//! [`AcquisitionState`] machine that
//! [`crate::share_partition::manager::SharePartitionLeaderManager`] owns. It
//! validates the group id, the member id and the share session. Then, for every
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
//! per-connection principal and the peer `SocketAddr` for the group `Read` and
//! per-topic `Read` ACL gates.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        share_fetch_request::{FetchPartition, ShareFetchRequest},
        share_fetch_response::{LeaderIdAndEpoch, NodeEndpoint},
    },
};

mod acknowledge;
mod acquire;
mod authorization;
mod leader_hint;
mod long_poll;
mod pending;
mod records;
mod request;
mod resolve;
mod response;

#[cfg(test)]
mod ack_validation_tests;
#[cfg(test)]
mod byte_limit_tests;
#[cfg(test)]
mod group_authorization_tests;
#[cfg(test)]
mod log_start_lockout_tests;
#[cfg(test)]
mod node_endpoints_tests;
#[cfg(test)]
mod persister_error_tests;
#[cfg(test)]
mod renew_tests;
#[cfg(test)]
mod request_validation_tests;
#[cfg(test)]
mod session_tests;
#[cfg(test)]
mod topic_resolution_tests;

pub(crate) use self::{
    acknowledge::{
        AckApplication, Renewal, acknowledgement_batches_are_valid, apply_acknowledgements,
        renew_acknowledge_enabled,
    },
    leader_hint::{current_leader, leader_endpoints, names_the_leader},
};
use self::{
    acquire::{AcquireContext, acquire_records},
    pending::PendingPartition,
    request::has_acknowledgements,
    resolve::{RowContext, resolve_row},
    response::{
        acquisition_timeout_ms, encode_error_response, encode_success_response, group_responses,
    },
};
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::group_read_denied,
    share_partition::session::{FetchPartitions, ResponseRow},
};

/// The longest member id that Kafka accepts: a human-readable UUID.
const MAX_MEMBER_ID_LEN: usize = 36;

/// Kafka's `KafkaApis.isMemberIdValid`: a member id is non-empty and at most
/// 36 characters long. The length is Java's `String.length`, that is UTF-16
/// code units.
pub(crate) fn member_id_is_valid(member_id: &str) -> bool {
    !member_id.is_empty() && member_id.encode_utf16().count() <= MAX_MEMBER_ID_LEN
}

/// Whether a renew-ack `ShareFetch` asks for no records and no wait:
/// `MaxBytes`, `MinBytes`, `MaxRecords` and `MaxWaitMs` are all 0.
fn renew_fetch_fields_are_zero(req: &ShareFetchRequest) -> bool {
    req.max_bytes == 0 && req.min_bytes == 0 && req.max_records == 0 && req.max_wait_ms == 0
}

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
        return encode_error_response(version, codes::UNSUPPORTED_VERSION);
    }
    // Kafka's `KafkaApis.handleShareFetchRequest` refuses a null group id
    // after the feature gate, then checks `Read` on the group, then the
    // member id format. It asks the group coordinator nothing about the
    // member: the share session and the acquisition locks are keyed by the
    // member id alone.
    let Some(group) = req.group_id.clone() else {
        return encode_error_response(version, codes::INVALID_REQUEST);
    };
    let image = broker.controller.current_image();
    if group_read_denied(broker.config.authorizer.as_ref(), &image, ctx, &group) {
        return encode_error_response(version, codes::GROUP_AUTHORIZATION_FAILED);
    }
    let Some(member) = req.member_id.clone().filter(|id| member_id_is_valid(id)) else {
        return encode_error_response(version, codes::INVALID_REQUEST);
    };

    // KIP-1222: a renew-ack fetch renews locks and fetches no records, so
    // Kafka's `KafkaApis.handleShareFetchRequest` refuses one that asks for
    // records or a wait. The error response carries no message.
    let renew_only = version >= 2 && req.is_renew_ack;
    if renew_only && !renew_fetch_fields_are_zero(&req) {
        return encode_error_response(version, codes::INVALID_REQUEST);
    }

    let mgr = broker.share_partition_leaders.clone();

    let mut requested = HashSet::new();
    let mut requested_order = Vec::new();
    let mut request_rows: HashMap<(uuid::Uuid, i32), FetchPartition> = HashMap::new();
    let has_acknowledgements = has_acknowledgements(&req);
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
        (&group, &member),
        ctx.connection_id,
        req.share_session_epoch,
        FetchPartitions {
            requested: &requested_order,
            forgotten: &forgotten,
        },
        has_acknowledgements,
    ) {
        Ok(session) => session,
        Err(codes::SHARE_SESSION_LIMIT_REACHED) => {
            // Kafka's `createIdleShareFetchTimerTask`: the answer waits out
            // `MaxWaitMs`, so a client that cannot get a session does not
            // spin on the broker.
            let wait = u64::try_from(req.max_wait_ms).unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
            return encode_error_response(version, codes::SHARE_SESSION_LIMIT_REACHED);
        }
        Err(code) => return encode_error_response(version, code),
    };

    // The session's partitions in the session's order, then the request
    // partitions that only acknowledge: those that a final request names and
    // those that a request forgets.
    let fetched: HashSet<(uuid::Uuid, i32)> = session.partitions.iter().copied().collect();
    let mut effective_order = session.partitions.clone();
    effective_order.extend(
        requested_order
            .iter()
            .copied()
            .filter(|partition| !fetched.contains(partition)),
    );

    // Resolve the complete effective session subscription plus request-only
    // acknowledgement rows into pending partitions.
    let row_context = RowContext {
        broker,
        manager: &mgr,
        image: &image,
        ctx,
        has_acknowledgements,
        supports_renew: version >= 2,
        is_renew_ack: renew_only,
    };
    let mut pending: Vec<PendingPartition> = effective_order
        .into_iter()
        .map(|key| {
            // A renew-ack fetch acquires nothing: a fetch that took longer
            // than the renewed lock would let the lock run out before the
            // response arrives.
            let fetchable = !renew_only && fetched.contains(&key);
            resolve_row(&row_context, key, fetchable, request_rows.get(&key))
        })
        .collect();

    let acquire = AcquireContext {
        broker,
        manager: &mgr,
        group: &group,
        member: &member,
        max_records: req.max_records,
        max_bytes: req.max_bytes,
        renewal: Renewal {
            requested: req.is_renew_ack,
            enabled: renew_acknowledge_enabled(&image, &group),
            lock_duration: cfg.record_lock_duration,
        },
        config: &cfg,
    };

    let max_wait_ms = if session.final_request || renew_only {
        0
    } else {
        req.max_wait_ms
    };
    let acquire_result = acquire_records(&acquire, &mut pending, max_wait_ms).await;
    // Kafka's `releaseSession` runs after the final request's response is
    // built: the member gives its records back.
    if session.final_request {
        mgr.release_session_partitions(&group, &member, &session.released)
            .await;
    }
    acquire_result?;

    // Kafka answers a fetch row for each partition of the share session that
    // it fetched, and an acknowledge row for each request partition when the
    // request carries acknowledgements. A renew-ack fetch runs no fetch, so it
    // answers only the acknowledge rows.
    pending.retain(|p| p.fetchable || (p.in_request && has_acknowledgements));
    // An incremental response carries only the session partitions that have
    // something to say, and every acknowledge row.
    if session.incremental {
        let rows: Vec<ResponseRow> = pending
            .iter()
            .filter(|p| p.fetchable)
            .map(|p| ResponseRow {
                key: (p.topic_id, p.partition_index),
                has_records: !p.out.acquired_records.is_empty(),
                has_error: p.out.error_code != codes::NONE,
            })
            .collect();
        let mut carried = mgr.prune_fetch_response(&group, &member, &rows).into_iter();
        pending.retain(|p| {
            let carried = !p.fetchable || carried.next().unwrap_or(true);
            carried || (p.in_request && has_acknowledgements)
        });
    }

    // Kafka's `processShareFetchResponse`: every row that names another
    // leader carries the current leader and its endpoint.
    for p in &mut pending {
        if names_the_leader(p.out.error_code) {
            let (leader_id, leader_epoch) = current_leader(&mgr, p.topic_id, p.partition_index);
            p.out.current_leader = LeaderIdAndEpoch {
                leader_id,
                leader_epoch,
                ..Default::default()
            };
        }
    }
    let node_endpoints = leader_endpoints(
        &image,
        ctx.connection_listener_name,
        &broker.config.inter_broker_listener_name,
        pending
            .iter()
            .filter(|p| names_the_leader(p.out.error_code))
            .map(|p| p.out.current_leader.leader_id),
    )
    .into_iter()
    .map(|endpoint| NodeEndpoint {
        node_id: endpoint.node_id,
        host: endpoint.host,
        port: endpoint.port,
        rack: endpoint.rack,
        ..Default::default()
    })
    .collect();

    // Group pending rows back into per-topic responses, preserving first-seen
    // topic order.
    let responses = group_responses(pending);

    encode_success_response(version, lock_timeout_ms, responses, node_endpoints)
}
