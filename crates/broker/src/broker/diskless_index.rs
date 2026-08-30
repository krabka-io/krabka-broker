//! Bring-up of the diskless WAL index projection and the object flusher that
//! rides on it. The retry loop mirrors the topic-backed RLMM bootstrap beside
//! it but targets a different topic and hands off to the flusher, so it is a
//! module of its own.

use std::sync::{Arc, atomic::AtomicBool};

use tokio_util::sync::CancellationToken;

use crate::{
    broker::rlmm::{KafkaSwapKickoff, metadata_log_config, rlmm_bootstrap_backoff},
    partition_registry::PartitionRegistry,
};

pub(super) struct DisklessFlusherStartup {
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) image_rx: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    pub(super) object_store: Arc<dyn object_store::ObjectStore>,
    pub(super) node_id: krabka_metadata::NodeId,
    pub(super) broker_id: i32,
    pub(super) flush_config: crate::diskless::flusher::FlushConfig,
    pub(super) ready: Arc<AtomicBool>,
}

pub(super) async fn bootstrap_diskless_index_log(
    cache: Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
    config: KafkaSwapKickoff,
    flusher: DisklessFlusherStartup,
    shutdown: CancellationToken,
) {
    let log_config = metadata_log_config(
        &config.cfg,
        crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC.to_owned(),
        format!("krabka-diskless-index-broker-{}", config.broker_id),
    );
    let mut backoff = config.bootstrap_backoff_initial;
    loop {
        let started = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = krabka_remote_storage_topic::KafkaMetadataEventLog::start(log_config.clone()) => result,
        };
        let index_log = match started {
            Ok(log) => {
                let log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> = log;
                crate::diskless::index_log::DisklessIndexLog::start_with_cache(
                    log,
                    Arc::clone(&cache),
                )
                .await
            }
            Err(error) => Err(crate::error::BrokerError::Txn(format!(
                "diskless WAL index log start: {error}"
            ))),
        };
        match index_log {
            Ok(index_log) => {
                tracing::info!(
                    topic = crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC,
                    "diskless WAL index projection started; flusher waits for its replay"
                );
                // `flusher.ready` flips inside `run`, once the projection has
                // replayed the index topic and the first tick may fire.
                crate::diskless::flusher::run(
                    crate::diskless::flusher::FlusherContext {
                        partitions: flusher.partitions,
                        image_rx: flusher.image_rx,
                        object_store: flusher.object_store,
                        index_log,
                        node_id: flusher.node_id,
                        broker_id: flusher.broker_id,
                        ready: flusher.ready,
                    },
                    flusher.flush_config,
                    shutdown,
                )
                .await;
                return;
            }
            Err(error) => {
                tracing::warn!(%error, backoff_ms = backoff.as_millis(),
                    "diskless WAL index log start failed; retrying");
                if !rlmm_bootstrap_backoff(&mut backoff, config.bootstrap_backoff_max, &shutdown)
                    .await
                {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn diskless_index_bootstrap_retries_until_cancelled() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = listener.local_addr().unwrap().to_string();
        drop(listener);

        let snapshot_dir = tempdir().unwrap();
        let kickoff = KafkaSwapKickoff {
            cfg: crate::config::KafkaRlmmConfig {
                bootstrap,
                num_partitions: 1,
                replication: 1,
                snapshot_dir: snapshot_dir.path().to_path_buf(),
                ..crate::config::KafkaRlmmConfig::default()
            },
            broker_id: 1,
            bootstrap_backoff_initial: std::time::Duration::from_millis(10),
            bootstrap_backoff_max: std::time::Duration::from_secs(1),
            reconcile_tick: std::time::Duration::from_secs(1),
        };
        let (_image_tx, image_rx) = tokio::sync::watch::channel(Arc::new(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
        ));
        let flusher = DisklessFlusherStartup {
            partitions: Arc::new(PartitionRegistry::new()),
            image_rx,
            object_store: Arc::new(object_store::memory::InMemory::new()),
            node_id: krabka_raft::NodeId(7),
            broker_id: 1,
            flush_config: crate::diskless::flusher::FlushConfig::default(),
            ready: Arc::new(AtomicBool::new(false)),
        };
        let shutdown = CancellationToken::new();
        let bootstrap = bootstrap_diskless_index_log(
            Arc::new(tokio::sync::Mutex::new(
                crate::diskless::wal_index::WalIndexCache::default(),
            )),
            kickoff,
            flusher,
            shutdown.clone(),
        );
        tokio::pin!(bootstrap);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut bootstrap)
                .await
                .is_err()
        );
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), bootstrap)
            .await
            .expect("cancelled diskless index bootstrap returns promptly");
    }
}
