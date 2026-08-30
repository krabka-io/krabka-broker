//! Pull-based durable WAL follower for one diskless shard.

use std::{path::PathBuf, sync::Arc};

use krabka_client_core::ClientError;
use krabka_log::LogConfig;
use krabka_protocol::owned::fetch_response::FetchResponse;
use krabka_raft::NodeId;
use krabka_security::ListenerProtocol;
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

mod checkpoint;
mod connection;
mod fetch;
mod log;
mod promotion;

pub(crate) use self::promotion::hydrate_on_promotion;
use self::{
    connection::{connect_with_backoff, sleep_or_cancel},
    fetch::{
        FetchProgress, fetch_progress, response_partition, validate_fetch_frontiers,
        validate_reset_offset,
    },
    log::FollowerLog,
};
use super::{
    registry::ShardId,
    wire::{QuorumGroup, fetch_request},
};
use crate::{codes, config::ReplicationRuntimeConfig};

pub(crate) struct Config {
    pub(crate) node_id: NodeId,
    pub(crate) topic: String,
    pub(crate) shard: ShardId,
    pub(crate) leader_node_id: NodeId,
    pub(crate) leader_epoch: i32,
    pub(crate) leader_host: String,
    pub(crate) leader_port: u16,
    pub(crate) log_dirs: Vec<PathBuf>,
    pub(crate) storage: LogConfig,
    pub(crate) client_id: String,
    pub(crate) shutdown: CancellationToken,
    pub(crate) inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub(crate) inter_broker_listener_protocol: ListenerProtocol,
    pub(crate) inter_broker_server_name: String,
    pub(crate) replication: ReplicationRuntimeConfig,
}

pub(crate) async fn run(config: Config) {
    info!(
        topic = %config.topic,
        partition = config.shard.partition.0,
        leader = config.leader_node_id.0,
        "diskless WAL follower started"
    );
    loop {
        if config.shutdown.is_cancelled() {
            return;
        }
        match FollowerLog::open(&config) {
            Ok(follower) => match run_inner(&config, &follower).await {
                Ok(()) => return,
                Err(error) => {
                    if config.shutdown.is_cancelled() {
                        return;
                    }
                    warn!(error = %error, "diskless WAL follower stopped; retrying");
                }
            },
            Err(error) => {
                warn!(error = %error, "diskless WAL follower could not open its log; retrying");
            }
        }
        if sleep_or_cancel(
            &config.shutdown,
            config.replication.unexpected_error_backoff,
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

async fn run_inner(config: &Config, follower: &FollowerLog) -> Result<(), String> {
    let mut connection = connect_with_backoff(config).await?;
    loop {
        let requested = follower.end_offset();
        let mut request = fetch_request(
            QuorumGroup::diskless_wal(config.shard.topic_id, config.shard.partition),
            config.node_id,
            config.leader_epoch,
            follower.last_epoch(),
            requested.0,
            config.replication.fetch_max,
        );
        request.max_wait_ms =
            i32::try_from(config.replication.fetch_max_wait.millis_i64_trunc().max(0))
                .unwrap_or(i32::MAX);
        request.min_bytes = config.replication.fetch_min.bytes_i32();
        let response: FetchResponse = tokio::select! {
            () = config.shutdown.cancelled() => return Ok(()),
            response = connection.send(request) => match response {
                Ok(response) => response,
                Err(ClientError::Disconnected | ClientError::Io(_)) => {
                    connection = connect_with_backoff(config).await?;
                    continue;
                }
                Err(error) => {
                    warn!(error = %error, "diskless WAL fetch failed; reconnecting");
                    sleep_or_cancel(&config.shutdown, config.replication.send_error_backoff)
                        .await?;
                    connection = connect_with_backoff(config).await?;
                    continue;
                }
            },
        };
        let Some(partition) = response_partition(response, config.shard) else {
            sleep_or_cancel(
                &config.shutdown,
                config.replication.unexpected_error_backoff,
            )
            .await?;
            continue;
        };
        match partition.error_code {
            codes::NONE => {
                if partition.diverging_epoch.end_offset >= 0 {
                    let divergence = krabka_ids::Offset(partition.diverging_epoch.end_offset);
                    if !(follower.start_offset()..=requested).contains(&divergence) {
                        return Err("leader returned invalid WAL divergence offset".into());
                    }
                    follower
                        .truncate_to(divergence)
                        .await
                        .map_err(|error| error.to_string())?;
                    continue;
                }
                let frontiers = validate_fetch_frontiers(&partition)?;
                follower
                    .trim_to(frontiers.start)
                    .await
                    .map_err(|error| error.to_string())?;
                let appended = follower
                    .append(requested, frontiers.end, partition.records)
                    .await
                    .map_err(|error| error.to_string())?;
                match fetch_progress(requested, appended)? {
                    FetchProgress::Idle => {
                        sleep_or_cancel(
                            &config.shutdown,
                            config.replication.throttle_exhausted_backoff,
                        )
                        .await?;
                    }
                    FetchProgress::Advanced => {}
                }
            }
            codes::OFFSET_OUT_OF_RANGE => {
                let reset = validate_reset_offset(&partition)?;
                follower
                    .reset_to(reset)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                sleep_or_cancel(
                    &config.shutdown,
                    config.replication.unknown_topic_retry_delay,
                )
                .await?;
            }
            codes::NOT_LEADER_OR_FOLLOWER
            | codes::FENCED_LEADER_EPOCH
            | codes::UNKNOWN_LEADER_EPOCH => return Ok(()),
            error_code => {
                warn!(
                    error_code,
                    "diskless WAL follower received an unexpected error"
                );
                sleep_or_cancel(
                    &config.shutdown,
                    config.replication.unexpected_error_backoff,
                )
                .await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};

    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::{Offset, PartitionIndex};
    use krabka_log::Log;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;
    use crate::wal::{
        WalStore as _,
        quorum::{
            engine::WalShardEngine,
            registry::{WalPlacement, WalShardRegistry},
        },
    };

    fn test_config(root: &Path, shutdown: CancellationToken) -> Config {
        Config {
            node_id: NodeId(2),
            topic: "diskless".into(),
            shard: ShardId {
                topic_id: uuid::Uuid::from_u128(99),
                partition: PartitionIndex(0),
            },
            leader_node_id: NodeId(1),
            leader_epoch: 7,
            leader_host: "127.0.0.1".into(),
            leader_port: 0,
            log_dirs: vec![root.to_path_buf()],
            storage: LogConfig::default(),
            client_id: "wal-follower-test".into(),
            shutdown,
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
            inter_broker_listener_protocol: ListenerProtocol::Plaintext,
            inter_broker_server_name: "localhost".into(),
            replication: ReplicationRuntimeConfig::default(),
        }
    }

    #[tokio::test]
    async fn follower_run_retries_until_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        let config = test_config(dir.path(), shutdown.clone());
        let mut task = tokio::spawn(run(config));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn follower_inner_loop_propagates_connect_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let config = test_config(dir.path(), shutdown);
        let follower = FollowerLog::for_log(
            Log::open(dir.path().join("follower"), LogConfig::default()).unwrap(),
        );

        let error = run_inner(&config, &follower).await.unwrap_err();

        assert!(error == "cancelled");
    }

    #[tokio::test]
    async fn follower_fsync_ack_releases_the_leader_quorum_wait() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(leader_dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = Arc::new(
            super::super::QuorumWalStore::for_distributed_partition(
                uuid::Uuid::from_u128(42),
                PartitionIndex(0),
                source.clone(),
                None,
                3,
            )
            .unwrap(),
        );
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(42),
            partition: PartitionIndex(0),
        };
        let registry = WalShardRegistry::new(NodeId(1));
        registry.replace_placements(&maplit::hashmap! {shard => WalPlacement {
            voters: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: 0,
        }});
        registry.insert(shard, store.engine());
        let follower =
            FollowerLog::for_log(Log::open(follower_dir.path(), LogConfig::default()).unwrap());
        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        source.lock().unwrap().append(&mut batch).unwrap();
        let leo = source.lock().unwrap().log_end_offset();
        let syncing = Arc::clone(&store);
        let sync = tokio::spawn(async move { syncing.sync_durable(leo).await });

        let request = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            NodeId(2),
            0,
            -1,
            0,
            krabka_units::mebibytes(1),
        );
        let response = registry
            .route_fetch_request(&request, NodeId(2))
            .unwrap()
            .unwrap();
        let partition = response_partition(response, shard).unwrap();
        let follower_end = follower
            .append(
                Offset(0),
                Offset(partition.last_stable_offset),
                partition.records,
            )
            .await
            .unwrap();
        assert2::assert!((follower_end) == (leo));

        let acknowledgement = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            NodeId(2),
            0,
            0,
            follower_end.0,
            krabka_units::mebibytes(1),
        );
        registry
            .route_fetch_request(&acknowledgement, NodeId(2))
            .unwrap()
            .unwrap();

        assert2::assert!((sync.await.unwrap().unwrap()) == (leo));
        assert2::assert!((store.engine().durable_watermark()) == (leo));
    }

    #[tokio::test]
    async fn follower_truncates_a_divergent_epoch_and_replicates_the_leader_tail() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let leader = Arc::new(Mutex::new(
            Log::open(leader_dir.path(), LogConfig::default()).unwrap(),
        ));
        let mut shared = batch(0, b"shared");
        leader
            .lock()
            .unwrap()
            .append_at(&mut shared, Offset(0))
            .unwrap();
        let mut leader_tail = batch(1, b"leader");
        leader
            .lock()
            .unwrap()
            .append_at(&mut leader_tail, Offset(1))
            .unwrap();
        leader.lock().unwrap().sync().unwrap();

        let mut follower_log = Log::open(follower_dir.path(), LogConfig::default()).unwrap();
        follower_log
            .append_at(&mut batch(0, b"shared"), Offset(0))
            .unwrap();
        follower_log
            .append_at(&mut batch(0, b"divergent"), Offset(1))
            .unwrap();
        follower_log.sync().unwrap();
        let follower = FollowerLog::for_log(follower_log);

        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(100),
            partition: PartitionIndex(0),
        };
        let registry = WalShardRegistry::new(NodeId(1));
        registry.insert(
            shard,
            Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {NodeId(1) => Arc::clone(&leader)},
            )),
        );
        registry.replace_placements(&maplit::hashmap! {shard => WalPlacement {
            voters: vec![NodeId(1), NodeId(2)],
            leader_epoch: 1,
        }});

        let response = registry
            .route_fetch_request(&fetch_request(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                NodeId(2),
                1,
                follower.last_epoch(),
                follower.end_offset().0,
                krabka_units::mebibytes(1),
            ))
            .unwrap()
            .unwrap();
        let partition = response_partition(response, shard).unwrap();
        assert2::assert!((partition.diverging_epoch.end_offset) == (1));

        follower
            .truncate_to(Offset(partition.diverging_epoch.end_offset))
            .await
            .unwrap();
        let response = registry
            .route_fetch_request(&fetch_request(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                NodeId(2),
                1,
                follower.last_epoch(),
                follower.end_offset().0,
                krabka_units::mebibytes(1),
            ))
            .unwrap()
            .unwrap();
        let partition = response_partition(response, shard).unwrap();
        follower
            .append(
                Offset(1),
                Offset(partition.last_stable_offset),
                partition.records,
            )
            .await
            .unwrap();

        assert2::assert!((follower.end_offset()) == (Offset(2)));
        assert2::assert!((follower.last_epoch()) == (1));
        assert2::assert!(
            follower
                .log
                .lock()
                .read_raw(Offset(0), Offset(2), krabka_units::mebibytes(1))
                .unwrap()
                .bytes
                == leader
                    .lock()
                    .unwrap()
                    .read_raw(Offset(0), Offset(2), krabka_units::mebibytes(1))
                    .unwrap()
                    .bytes
        );
    }

    fn batch(epoch: i32, value: &'static [u8]) -> RecordBatch {
        RecordBatch {
            partition_leader_epoch: epoch,
            records: vec![Record {
                value: Some(Bytes::from_static(value)),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }
}
