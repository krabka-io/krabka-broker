//! The follower fetch loop.
//!
//! Each round reads the local end offset, asks the throttle for a budget,
//! builds one single-partition `Fetch` request, and hands the response to the
//! response handler. The loop reconnects on transport failure and returns when
//! the task is cancelled or the replication target moves.

use krabka_client_core::ClientError;
use krabka_log::Offset;
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
    fetch_response::FetchResponse,
};
use krabka_units::{
    ByteSize,
    convert::{ByteSizeExt, TimeExt},
};
use tracing::{info, warn};

use super::{
    Config,
    connection::connect_with_backoff,
    follower_throttle::{FetchThrottleDecision, follower_partition_fetch_cap},
    replication_target_changed,
    response::{LoopAction, handle_response},
};

pub(super) async fn run_inner(cfg: &Config) -> Result<(), String> {
    let mut client = connect_with_backoff(cfg).await?;

    loop {
        if cfg.shutdown.is_cancelled() || replication_target_changed(cfg) {
            return Ok(());
        }

        // Read the local log's next offset so the leader knows where to
        // resume from. Cheap: takes the partition's log mutex briefly.
        let fetch_offset = {
            let entry = cfg
                .partitions
                .get(&cfg.topic, cfg.partition)
                .ok_or_else(|| "local partition missing".to_string())?;
            entry.log_end_offset()
        };

        // KIP-73: follower-side throttle. Check the current metadata image
        // to see if this (partition, node) pair is in the follower throttled
        // replicas list. If so, cap the request size via the follower-in
        // token bucket.
        //
        // The replicator already issues one Fetch per (topic, partition), so
        // throttled-partition Fetch isolation is free — no need to split
        // requests here. We set `partition_max_bytes` on the single partition
        // in the request to the bucket-granted amount.
        let partition_max_cap = match follower_partition_fetch_cap(cfg) {
            FetchThrottleDecision::Fetch(cap) => cap,
            FetchThrottleDecision::Sleep => {
                tracing::debug!(
                    topic = %cfg.topic,
                    partition = cfg.partition.get(),
                    "follower throttle: skip fetch this round (bucket exhausted)"
                );
                // Bucket exhausted — yield and retry next loop iteration.
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(cfg.replication.throttle_exhausted_backoff.to_std()) => {}
                }
                continue;
            }
        };

        let req = build_fetch_request(cfg, fetch_offset, partition_max_cap);
        // Fence the response against the epoch that was actually sent. The
        // metadata image may advance while this request is in flight.
        let request_leader_epoch = req.topics[0].partitions[0].current_leader_epoch;

        let send = tokio::select! {
            () = cfg.shutdown.cancelled() => return Ok(()),
            r = client.send(req) => r,
        };

        let resp: FetchResponse = match send {
            Ok(r) => r,
            // Transport / framing failure: drop the client and reconnect.
            Err(ClientError::Disconnected | ClientError::Io(_)) => {
                client = connect_with_backoff(cfg).await?;
                continue;
            }
            Err(e) => {
                warn!(error = %e,
                    "replicator: client.send unexpected error; retrying after backoff");
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(cfg.replication.send_error_backoff.to_std()) => {}
                }
                client = connect_with_backoff(cfg).await?;
                continue;
            }
        };

        match handle_response(resp, cfg, request_leader_epoch).await {
            LoopAction::Continue => {}
            LoopAction::StopNotLeader => {
                info!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator.not_leader; supervisor will re-evaluate");
                return Ok(());
            }
        }
    }
}

/// Builds a single-partition Fetch request for the partition of this
/// replicator.
///
/// `replica_id` holds the local broker, so the leader treats the request as a
/// follower fetch and not as a consumer fetch. The high-watermark semantics of
/// Kafka differ between the two.
///
/// KIP-101: the request includes `current_leader_epoch`, so the leader can
/// detect a stale or fenced replica and return `FENCED_LEADER_EPOCH` or
/// `UNKNOWN_LEADER_EPOCH`.
///
/// `partition_max_cap` is the KIP-73 follower-throttle cap for
/// `partition_max_bytes`. Pass the configured fetch maximum when the partition
/// is not throttled.
fn build_fetch_request(
    cfg: &Config,
    fetch_offset: Offset,
    partition_max_cap: ByteSize,
) -> FetchRequest {
    let leader_epoch = cfg
        .partitions
        .get(&cfg.topic, cfg.partition)
        .map_or(-1, |entry| {
            entry
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        });
    // KIP-320: the leader epoch of our last appended record. Sent so the
    // leader can detect divergence in-band and answer with `diverging_epoch`.
    let last_fetched_epoch = cfg
        .partitions
        .get(&cfg.topic, cfg.partition)
        .and_then(|entry| {
            let log = entry.log.lock().expect("log mutex poisoned");
            log.epoch_checkpoint().latest_epoch()
        })
        // Unwrap the log-layer `LeaderEpoch` into the raw wire `last_fetched_epoch`.
        .map_or(-1, |e| e.0);
    // `replica_id` is the wire field on Fetch v0-14. KIP-903 (Kafka 3.5) moved
    // it into a tagged `replica_state` struct on v15+; the codegen serializes
    // whichever the negotiated version requires. Populate BOTH so the request
    // is correct regardless of which version the leader negotiates.
    let rid = i32::try_from(cfg.node_id.0).unwrap_or(-1);
    // Truncate rather than round: `max_wait_ms` is a wire field, and a
    // fractional millisecond rounded up would ask the leader to hold the Fetch
    // open past the configured budget. A negative budget means "do not wait".
    let max_wait_ms =
        i32::try_from(cfg.replication.fetch_max_wait.millis_i64_trunc().max(0)).unwrap_or(i32::MAX);
    FetchRequest {
        replica_id: rid,
        replica_state: ReplicaState {
            replica_id: rid,
            ..ReplicaState::default()
        },
        max_wait_ms,
        min_bytes: cfg.replication.fetch_min.bytes_i32(),
        max_bytes: cfg.replication.fetch_max.bytes_i32(),
        topics: vec![FetchTopic {
            topic: cfg.topic.to_string(),
            topic_id: cfg.topic_id,
            partitions: vec![FetchPartition {
                partition: cfg.partition.get(),
                // Unwrap the `Offset` into the wire `i64` field.
                fetch_offset: fetch_offset.0,
                current_leader_epoch: leader_epoch,
                last_fetched_epoch,
                partition_max_bytes: partition_max_cap.bytes_i32(),
                ..FetchPartition::default()
            }],
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;
    use krabka_raft::NodeId;
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::replicator::test_support::{
        LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, image_with_leader, test_config,
    };

    #[test]
    fn build_fetch_request_populates_replica_and_partition_fields() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.replication.fetch_max = bytes(2_345_678);
        cfg.replication.fetch_max_wait = millis(321);
        cfg.replication.fetch_min = bytes(17);

        let req = build_fetch_request(&cfg, Offset(123), bytes(456));

        let rid = i32::try_from(NODE_ID.0).unwrap();
        let expected = FetchRequest {
            replica_id: rid,
            max_wait_ms: 321,
            min_bytes: 17,
            max_bytes: 2_345_678,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: TOPIC.into(),
                topic_id: WIRE_TOPIC_ID,
                partitions: vec![FetchPartition {
                    partition: PARTITION,
                    current_leader_epoch: -1,
                    fetch_offset: 123,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 456,
                    replica_directory_id: WireUuid::ZERO,
                    high_watermark: i64::MAX,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            forgotten_topics_data: Vec::new(),
            rack_id: String::new(),
            cluster_id: None,
            replica_state: ReplicaState {
                replica_id: rid,
                replica_epoch: -1,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn build_fetch_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_fetch_request(&cfg, Offset(0), cfg.replication.fetch_max);

        assert!(req.replica_id == -1);
        assert!(req.replica_state.replica_id == -1);
    }

    #[tokio::test]
    async fn run_inner_reports_cancelled_before_first_connection() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.shutdown.cancel();

        let err = run_inner(&cfg).await.unwrap_err();

        assert!(err == "cancelled");
    }
}
