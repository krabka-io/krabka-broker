//! KIP-227 incremental fetch sessions: the classification the cache made
//! becomes the response session id here, and the per-partition state the
//! response just carried is snapshotted, filtered and written back.

use krabka_protocol::{
    owned::fetch_response::{AbortedTransaction, FetchableTopicResponse},
    primitives::uuid::Uuid as WireUuid,
};

use super::request::{EffectivePartition, EffectiveTopic};
use crate::{
    broker::Broker,
    fetch_session::{CachedPartitionState, FetchSessionKey, INVALID_SESSION_ID, SessionDecision},
};

pub(super) fn finalize_fetch_session(
    broker: &Broker,
    decision: &SessionDecision,
    effective_topics: &[EffectiveTopic],
    responses: &mut Vec<FetchableTopicResponse>,
    is_follower_fetch: bool,
    principal_name: &str,
) -> i32 {
    let session_id = match decision {
        SessionDecision::Sessionless => INVALID_SESSION_ID,
        SessionDecision::Close { session_id } => {
            broker.fetch_session_cache.close(*session_id);
            INVALID_SESSION_ID
        }
        SessionDecision::NewSession => {
            let snapshot = snapshot_response_state(effective_topics, responses);
            broker.fetch_session_cache.try_allocate(
                is_follower_fetch,
                principal_name.to_owned(),
                snapshot,
            )
        }
        SessionDecision::Incremental {
            session_id,
            partitions,
            ..
        } => {
            let cached: std::collections::HashMap<FetchSessionKey, CachedPartitionState> =
                partitions.iter().cloned().collect();
            let sent = filter_incremental_response(responses, &cached);
            broker
                .fetch_session_cache
                .finalize_incremental(*session_id, &sent);
            *session_id
        }
        SessionDecision::Error { .. } => unreachable!("returned above"),
    };
    refresh_fetch_session_metrics(broker);
    session_id
}

fn refresh_fetch_session_metrics(broker: &Broker) {
    broker
        .metrics
        .incremental_fetch_sessions
        .set(i64::try_from(broker.fetch_session_cache.len()).unwrap_or(i64::MAX));
    broker.metrics.incremental_fetch_partitions_cached.set(
        i64::try_from(broker.fetch_session_cache.total_partitions_cached()).unwrap_or(i64::MAX),
    );
    let current = broker.fetch_session_cache.evictions_total();
    let previous = broker
        .metrics
        .incremental_fetch_session_evictions_total
        .get();
    if current > previous {
        broker
            .metrics
            .incremental_fetch_session_evictions_total
            .inc_by(current - previous);
    }
}

/// Walk `responses` and snapshot every `(topic, partition)` row into a
/// `CachedPartitionState`.
///
/// Each snapshot describes what the handler just emitted, in the `last_*`
/// fields. It merges that with the client's wanted state for the partition
/// from `effective`, which is `fetch_offset`, `max_bytes`, and `leader_epoch`.
/// The caller uses the result to seed a brand-new session.
fn snapshot_response_state(
    effective: &[EffectiveTopic],
    responses: &[FetchableTopicResponse],
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    use std::collections::HashMap;
    // Pre-index the desired state. Topic identity differs by wire
    // version: v ≤ 12 carries topic name and zero topic_id, v ≥ 13
    // carries topic_id and empty name. The server-side response always
    // has the resolved name *and* the id, but `effective` (built from
    // `req.topics`) may have only one or the other. Index by both so
    // lookup succeeds in either direction.
    let mut by_name: HashMap<(String, i32), &EffectivePartition> = HashMap::new();
    let mut by_id: HashMap<(WireUuid, i32), &EffectivePartition> = HashMap::new();
    for et in effective {
        for ep in &et.partitions {
            if !et.topic.is_empty() {
                by_name.insert((et.topic.clone(), ep.partition), ep);
            }
            if et.topic_id != WireUuid::ZERO {
                by_id.insert((et.topic_id, ep.partition), ep);
            }
        }
    }
    let mut out = Vec::new();
    for tr in responses {
        for p in &tr.partitions {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let mut state = CachedPartitionState {
                last_high_watermark: p.high_watermark,
                last_last_stable_offset: p.last_stable_offset,
                last_log_start_offset: p.log_start_offset,
                last_preferred_read_replica: p.preferred_read_replica,
                last_aborted_txns_hash: hash_aborted_transactions(p.aborted_transactions.as_ref()),
                last_error_code: p.error_code,
                ..Default::default()
            };
            let ep = by_id
                .get(&(tr.topic_id, p.partition_index))
                .or_else(|| by_name.get(&(tr.topic.clone(), p.partition_index)));
            if let Some(ep) = ep {
                state.fetch_offset = ep.fetch_offset;
                state.max_bytes = ep.partition_max_bytes;
                state.current_leader_epoch = ep.current_leader_epoch;
                state.last_fetched_epoch = ep.last_fetched_epoch;
            }
            out.push((key, state));
        }
    }
    out
}

/// KIP-227 incremental-response filter.
///
/// The function drops the partitions whose outgoing state matches the cached
/// `last_*` snapshot. The broker already told the client these values, and a
/// second send would waste bytes. The function returns the
/// `(key, sent_state)` list for the partitions that survived. The caller uses
/// that list to update the cache's `last_*` fields to what it just emitted.
fn filter_incremental_response(
    responses: &mut Vec<FetchableTopicResponse>,
    cached: &std::collections::HashMap<FetchSessionKey, CachedPartitionState>,
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    let mut sent: Vec<(FetchSessionKey, CachedPartitionState)> = Vec::new();
    for tr in responses.iter_mut() {
        tr.partitions.retain(|p| {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let aborted_hash = hash_aborted_transactions(p.aborted_transactions.as_ref());
            let records_present = p.records.as_ref().is_some_and(|b| b.payload_len() > 0);
            let changed = match cached.get(&key) {
                Some(prev) => {
                    records_present
                        || p.error_code != prev.last_error_code
                        || p.high_watermark != prev.last_high_watermark
                        || p.last_stable_offset != prev.last_last_stable_offset
                        || p.log_start_offset != prev.last_log_start_offset
                        || p.preferred_read_replica != prev.last_preferred_read_replica
                        || aborted_hash != prev.last_aborted_txns_hash
                        || p.diverging_epoch.end_offset >= 0
                }
                // Partition not in the cached set — newly added by this
                // request. Always send it once so the client sees its
                // initial state.
                None => true,
            };
            if changed {
                sent.push((
                    key,
                    CachedPartitionState {
                        last_high_watermark: p.high_watermark,
                        last_last_stable_offset: p.last_stable_offset,
                        last_log_start_offset: p.log_start_offset,
                        last_preferred_read_replica: p.preferred_read_replica,
                        last_aborted_txns_hash: aborted_hash,
                        last_error_code: p.error_code,
                        ..Default::default()
                    },
                ));
            }
            changed
        });
    }
    // Drop topics that ended up with no partitions.
    responses.retain(|tr| !tr.partitions.is_empty());
    sent
}

/// Stable hash of the aborted-transaction list for the "did anything change?"
/// comparison.
///
/// The iteration order within a single response is deterministic, because
/// `do_read` produces the list in offset order. A plain `DefaultHasher` over
/// the sequence is therefore enough.
fn hash_aborted_transactions(list: Option<&Vec<AbortedTransaction>>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match list {
        None => 0_u8.hash(&mut h),
        Some(v) => {
            1_u8.hash(&mut h);
            (v.len() as u64).hash(&mut h);
            for tx in v {
                tx.producer_id.hash(&mut h);
                tx.first_offset.hash(&mut h);
            }
        }
    }
    h.finish()
}
