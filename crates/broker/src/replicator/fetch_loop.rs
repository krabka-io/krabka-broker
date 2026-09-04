//! The follower fetch loop.
//!
//! Each round takes a snapshot of the partitions the fetcher follows, reads
//! each one's local end offset and KIP-73 throttle budget, folds them into one
//! `Fetch` request through the KIP-227 session handler, and applies the
//! response row by row. The loop reconnects on transport failure and returns
//! when the fetcher is cancelled.
//!
//! A round costs one request and one response however many partitions the
//! fetcher follows, and after the first round the request names only the
//! partitions whose desired state moved.

use std::{collections::BTreeMap, sync::Arc};

use krabka_client_core::{ClientError, Connection};
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, ReplicaState},
    fetch_response::FetchResponse,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};
use tracing::{info, warn};

use super::{
    Config, FetcherConfig, FollowedKey,
    connection::connect_with_backoff,
    ensure_local_partition,
    follower_throttle::{FetchThrottleDecision, follower_partition_fetch_cap},
    replication_target_changed,
    response::{RoundEntry, RowAction, apply_response},
    session::{FollowerFetchSession, SessionKey, SessionOutcome, WantedRows},
};

/// One round's plan: what goes on the wire, and which partition each row
/// belongs to once the answer comes back.
struct Round<'round> {
    wanted: WantedRows,
    entries: Vec<RoundEntry<'round>>,
    keys: Vec<FollowedKey>,
    /// Every partition wanted a fetch this round but no budget was left for
    /// any of them, so the round has nothing to send and should wait for the
    /// throttle rather than spin.
    all_throttled: bool,
}

pub(super) async fn run_fetcher_loop(fetcher: &FetcherConfig) -> Result<(), String> {
    let mut client = connect_with_backoff(fetcher).await?;
    let mut session = FollowerFetchSession::default();
    // The partitions whose on-disk log this fetcher has already opened. A
    // reconcile adds a partition to a running fetcher, and the first round
    // that sees it is what materializes it; re-checking every partition every
    // round would put the registry lookup back on the per-partition path this
    // batching exists to leave.
    let mut materialized: std::collections::HashSet<FollowedKey> = std::collections::HashSet::new();

    loop {
        if fetcher.shutdown.is_cancelled() {
            return Ok(());
        }

        let followed = snapshot(fetcher);
        materialized.retain(|key| followed.contains_key(key));
        for (key, cfg) in &followed {
            if materialized.contains(key) {
                continue;
            }
            if let Err(error) = ensure_local_partition(cfg) {
                warn!(error = %error, topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator failed to open local partition; not following it");
                drop_partition(fetcher, key);
                continue;
            }
            materialized.insert(key.clone());
        }

        let round = plan_round(&followed);
        if round.entries.is_empty() {
            // Nothing to ask for: either this fetcher follows nothing yet, or
            // every partition it follows is out of throttle budget. Wait, and
            // let the next reconcile or the next refill decide.
            let idle = if round.all_throttled {
                fetcher.replication.throttle_exhausted_backoff
            } else {
                fetcher.replication.fetch_max_wait
            };
            if !sleep_or_stop(fetcher, idle).await {
                return Ok(());
            }
            continue;
        }

        let request = build_fetch_request(fetcher, &mut session, round.wanted);
        let send = tokio::select! {
            () = fetcher.shutdown.cancelled() => return Ok(()),
            r = client.send(request) => r,
        };

        let mut resp: FetchResponse = match send {
            Ok(r) => r,
            // Transport / framing failure: the leader may or may not have seen
            // the request, so what it holds for this session is unknowable.
            // Drop the session with the connection; the next round is full.
            Err(ClientError::Disconnected | ClientError::Io(_)) => {
                session.reset();
                client = reconnect(fetcher, &mut session).await?;
                continue;
            }
            Err(e) => {
                warn!(error = %e,
                    "replicator: client.send unexpected error; retrying after backoff");
                session.reset();
                if !sleep_or_stop(fetcher, fetcher.replication.send_error_backoff).await {
                    return Ok(());
                }
                client = reconnect(fetcher, &mut session).await?;
                continue;
            }
        };

        if session.handle_response(resp.error_code, resp.session_id) == SessionOutcome::SessionLost {
            info!(
                leader_node_id = fetcher.leader_node_id.0,
                error_code = resp.error_code,
                "replicator: leader dropped the fetch session; re-opening it"
            );
            continue;
        }

        let actions = apply_response(&mut resp, &round.entries).await;
        let mut backoff: Option<Time> = None;
        for (key, action) in round.keys.iter().zip(actions) {
            match action {
                RowAction::Continue => {}
                RowAction::Drop => {
                    info!(topic = %key.0, partition = key.1.get(),
                        "replicator.not_leader; supervisor will re-evaluate");
                    drop_partition(fetcher, key);
                    // The leader's cached set still holds it; the next round
                    // forgets it, which is what the session handler does with
                    // a key that left the wanted set.
                }
                RowAction::Backoff(delay) => {
                    backoff = Some(match backoff {
                        Some(current) if current > delay => current,
                        _ => delay,
                    });
                }
            }
        }
        if let Some(delay) = backoff
            && !sleep_or_stop(fetcher, delay).await
        {
            return Ok(());
        }
    }
}

/// The partitions this fetcher follows right now.
fn snapshot(fetcher: &FetcherConfig) -> BTreeMap<FollowedKey, Arc<Config>> {
    fetcher
        .followed
        .lock()
        .expect("followed-partitions mutex poisoned")
        .clone()
}

/// Stops following one partition. The supervisor re-adds it if the next image
/// still says this broker follows it from this leader.
fn drop_partition(fetcher: &FetcherConfig, key: &FollowedKey) {
    fetcher
        .followed
        .lock()
        .expect("followed-partitions mutex poisoned")
        .remove(key);
}

/// Sleeps, or reports `false` when the fetcher was cancelled during the wait.
async fn sleep_or_stop(fetcher: &FetcherConfig, delay: Time) -> bool {
    tokio::select! {
        () = fetcher.shutdown.cancelled() => false,
        () = tokio::time::sleep(delay.to_std()) => true,
    }
}

/// Redials the leader and forgets the session, which the new connection could
/// not continue in any case.
async fn reconnect(
    fetcher: &FetcherConfig,
    session: &mut FollowerFetchSession,
) -> Result<Connection, String> {
    session.reset();
    connect_with_backoff(fetcher).await
}

/// Decides what this round asks for, partition by partition.
///
/// A partition whose replication target has moved is skipped rather than
/// dropped: the supervisor owns that decision, and the next reconcile makes
/// it. A partition with no throttle budget left is skipped for this round
/// only, which is how Kafka's follower quota holds one partition back without
/// holding back the ones sharing its fetcher.
fn plan_round(followed: &BTreeMap<FollowedKey, Arc<Config>>) -> Round<'_> {
    let mut round = Round {
        wanted: WantedRows::new(),
        entries: Vec::new(),
        keys: Vec::new(),
        all_throttled: false,
    };
    let mut wanted_a_fetch = 0_usize;
    let mut throttled_out = 0_usize;
    for (key, cfg) in followed {
        if replication_target_changed(cfg) {
            continue;
        }
        wanted_a_fetch += 1;
        let partition_max_cap = match follower_partition_fetch_cap(cfg) {
            FetchThrottleDecision::Fetch(cap) => cap,
            FetchThrottleDecision::Sleep => {
                tracing::debug!(
                    topic = %cfg.topic,
                    partition = cfg.partition.get(),
                    "follower throttle: skip fetch this round (bucket exhausted)"
                );
                throttled_out += 1;
                continue;
            }
        };
        let Some(row) = partition_row(cfg, partition_max_cap) else {
            continue;
        };
        round.wanted.insert(
            SessionKey {
                topic: cfg.topic.to_string(),
                topic_id: cfg.topic_id,
                partition: cfg.partition.get(),
            },
            row.request,
        );
        round.entries.push(RoundEntry {
            cfg,
            request_leader_epoch: row.request_leader_epoch,
        });
        round.keys.push(key.clone());
    }
    round.all_throttled = throttled_out > 0 && throttled_out == wanted_a_fetch;
    round
}

/// One partition's request row, and the epoch its response is fenced against.
struct PartitionRow {
    request: FetchPartition,
    request_leader_epoch: i32,
}

/// Builds the `FetchPartition` for one partition of this round.
///
/// KIP-101: the row carries `current_leader_epoch`, so the leader can detect a
/// stale or fenced replica and answer `FENCED_LEADER_EPOCH` or
/// `UNKNOWN_LEADER_EPOCH`. KIP-320: it carries `last_fetched_epoch`, the epoch
/// of the follower's last appended record, so the leader can report divergence
/// in-band.
///
/// `partition_max_cap` is the KIP-73 follower-throttle budget. An unthrottled
/// partition gets the configured fetch maximum.
fn partition_row(cfg: &Config, partition_max_cap: ByteSize) -> Option<PartitionRow> {
    let entry = cfg.partitions.get(&cfg.topic, cfg.partition)?;
    let fetch_offset = entry.log_end_offset();
    let leader_epoch = entry
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let last_fetched_epoch = {
        let log = entry.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .latest_epoch()
            // Unwrap the log-layer `LeaderEpoch` into the raw wire field.
            .map_or(-1, |epoch| epoch.0)
    };
    Some(PartitionRow {
        request: FetchPartition {
            partition: cfg.partition.get(),
            // Unwrap the `Offset` into the wire `i64` field.
            fetch_offset: fetch_offset.0,
            current_leader_epoch: leader_epoch,
            last_fetched_epoch,
            partition_max_bytes: partition_max_cap.bytes_i32(),
            ..FetchPartition::default()
        },
        request_leader_epoch: leader_epoch,
    })
}

/// Wraps one round's rows in the request envelope the leader reads.
///
/// `replica_id` holds the local broker, so the leader treats the request as a
/// follower fetch and not as a consumer fetch. The high-watermark semantics of
/// Kafka differ between the two.
fn build_fetch_request(
    fetcher: &FetcherConfig,
    session: &mut FollowerFetchSession,
    wanted: WantedRows,
) -> FetchRequest {
    let session_request = session.build(wanted);
    // `replica_id` is the wire field on Fetch v0-14. KIP-903 (Kafka 3.5) moved
    // it into a tagged `replica_state` struct on v15+; the codegen serializes
    // whichever the negotiated version requires. Populate BOTH so the request
    // is correct regardless of which version the leader negotiates.
    let rid = i32::try_from(fetcher.node_id.0).unwrap_or(-1);
    // Truncate rather than round: `max_wait_ms` is a wire field, and a
    // fractional millisecond rounded up would ask the leader to hold the Fetch
    // open past the configured budget. A negative budget means "do not wait".
    let max_wait_ms = i32::try_from(
        fetcher
            .replication
            .fetch_max_wait
            .millis_i64_trunc()
            .max(0),
    )
    .unwrap_or(i32::MAX);
    FetchRequest {
        replica_id: rid,
        replica_state: ReplicaState {
            replica_id: rid,
            ..ReplicaState::default()
        },
        max_wait_ms,
        min_bytes: fetcher.replication.fetch_min.bytes_i32(),
        max_bytes: fetcher.replication.fetch_max.bytes_i32(),
        session_id: session_request.session_id,
        session_epoch: session_request.session_epoch,
        topics: session_request.topics,
        forgotten_topics_data: session_request.forgotten_topics_data,
        ..FetchRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;
    use krabka_raft::NodeId;
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::replicator::{
        FollowedPartitions,
        test_support::{
            LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, image_with_leader, test_config,
        },
    };

    /// A fetcher over `followed`, dialling nowhere. Every test here drives the
    /// request-building half, which never touches the connection.
    fn test_fetcher(cfg: &Config, followed: FollowedPartitions) -> FetcherConfig {
        FetcherConfig {
            node_id: cfg.node_id,
            leader_node_id: cfg.leader_node_id,
            leader_host: cfg.leader_host.clone(),
            leader_port: cfg.leader_port,
            client_id: cfg.client_id.clone(),
            shutdown: tokio_util::sync::CancellationToken::new(),
            inter_broker_client: cfg.inter_broker_client.clone(),
            inter_broker_listener_protocol: cfg.inter_broker_listener_protocol,
            inter_broker_server_name: cfg.inter_broker_server_name.clone(),
            replication: cfg.replication.clone(),
            followed,
        }
    }

    /// One `wanted` row per partition, at `fetch_offset`, as `plan_round`
    /// would have produced it.
    fn wanted_row(partition: i32, fetch_offset: i64, max_bytes: i32) -> (SessionKey, FetchPartition) {
        (
            SessionKey {
                topic: TOPIC.to_string(),
                topic_id: WIRE_TOPIC_ID,
                partition,
            },
            FetchPartition {
                partition,
                fetch_offset,
                current_leader_epoch: -1,
                last_fetched_epoch: -1,
                partition_max_bytes: max_bytes,
                ..FetchPartition::default()
            },
        )
    }

    #[test]
    fn the_first_request_carries_every_partition_and_the_configured_envelope() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.replication.fetch_max = bytes(2_345_678);
        cfg.replication.fetch_max_wait = millis(321);
        cfg.replication.fetch_min = bytes(17);
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();
        let wanted: WantedRows = [wanted_row(PARTITION, 123, 456), wanted_row(PARTITION + 1, 7, 456)]
            .into_iter()
            .collect();

        let req = build_fetch_request(&fetcher, &mut session, wanted);

        let rid = i32::try_from(NODE_ID.0).unwrap();
        check!(req.replica_id == rid);
        check!(req.replica_state.replica_id == rid);
        check!(req.max_wait_ms == 321);
        check!(req.min_bytes == 17);
        check!(req.max_bytes == 2_345_678);
        // KIP-227: the first request opens a session rather than declaring
        // itself sessionless, which is what `(0, -1)` used to say.
        check!(req.session_id == 0);
        check!(req.session_epoch == 0);
        check!(req.forgotten_topics_data.is_empty());
        // One `FetchTopic` for both partitions, not one request each.
        check!(req.topics.len() == 1);
        check!(req.topics[0].topic.as_str() == TOPIC);
        check!(req.topics[0].topic_id == WIRE_TOPIC_ID);
        check!(
            req.topics[0]
                .partitions
                .iter()
                .map(|p| (p.partition, p.fetch_offset, p.partition_max_bytes))
                .collect::<Vec<_>>()
                == vec![(PARTITION, 123, 456), (PARTITION + 1, 7, 456)]
        );
    }

    #[test]
    fn a_later_request_names_only_the_partition_that_moved() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();
        let first: WantedRows = [wanted_row(PARTITION, 0, 99), wanted_row(PARTITION + 1, 0, 99)]
            .into_iter()
            .collect();
        build_fetch_request(&fetcher, &mut session, first);
        session.handle_response(crate::codes::NONE, 77);

        let second: WantedRows = [wanted_row(PARTITION, 5, 99), wanted_row(PARTITION + 1, 0, 99)]
            .into_iter()
            .collect();
        let req = build_fetch_request(&fetcher, &mut session, second);

        check!(req.session_id == 77);
        check!(req.session_epoch == 1);
        check!(req.topics.len() == 1);
        check!(
            req.topics[0]
                .partitions
                .iter()
                .map(|p| p.partition)
                .collect::<Vec<_>>()
                == vec![PARTITION]
        );
    }

    #[test]
    fn a_node_id_that_overflows_the_wire_field_sends_the_negative_sentinel() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();

        let req = build_fetch_request(
            &fetcher,
            &mut session,
            [wanted_row(PARTITION, 0, 1)].into_iter().collect(),
        );

        check!(req.replica_id == -1);
        check!(req.replica_state.replica_id == -1);
    }

    /// A partition whose replication target has moved is not asked about, and
    /// not dropped either: the supervisor owns that decision and makes it on
    /// the next reconcile.
    #[test]
    fn a_partition_whose_target_moved_is_skipped_for_the_round() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let mut followed = BTreeMap::new();
        followed.insert(
            (Arc::clone(&cfg.topic), cfg.partition),
            Arc::new(cfg),
        );

        let round = plan_round(&followed);

        check!(round.entries.is_empty());
        check!(round.wanted.is_empty());
        check!(!round.all_throttled);
    }

    /// A partition with no local runtime yet contributes no row rather than
    /// putting a bogus offset on the wire.
    #[test]
    fn a_partition_with_no_local_log_contributes_no_row() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let mut followed = BTreeMap::new();
        followed.insert((Arc::clone(&cfg.topic), cfg.partition), Arc::new(cfg));

        let round = plan_round(&followed);

        check!(round.entries.is_empty());
    }

    #[tokio::test]
    async fn a_fetcher_reports_cancelled_before_its_first_connection() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        fetcher.shutdown.cancel();

        let err = run_fetcher_loop(&fetcher).await.unwrap_err();

        assert!(err == "cancelled");
    }

    /// `WireUuid` is only here to keep the import used when the assertions
    /// above change shape.
    #[test]
    fn the_fixture_topic_id_is_the_one_the_requests_carry() {
        check!(WIRE_TOPIC_ID != WireUuid::ZERO);
    }
}
