//! KIP-405 tiered-storage bring-up: the object-store backend, the remote-log
//! metadata manager (topic-backed or in-memory), the diskless cold-read
//! handle, and the remote-log-manager copy task. It is its own module because
//! the backend selection and its WORM layering are self-contained.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{config::BrokerConfig, error::BrokerError, partition_registry::PartitionRegistry};

pub(super) struct RemoteStorageStartup {
    pub(super) reader: Option<Arc<crate::remote_reader::RemoteReader>>,
    pub(super) swap_target: Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    pub(super) diskless_read: Option<Arc<crate::diskless::read::DisklessReadHandle>>,
}

fn build_diskless_read_handle(
    backend: &crate::config::RemoteStorageBackend,
) -> Result<Arc<crate::diskless::read::DisklessReadHandle>, BrokerError> {
    let store_config = match backend {
        crate::config::RemoteStorageBackend::Local { dir } => {
            krabka_object_store::ObjectStoreConfig::Local { root: dir.clone() }
        }
        crate::config::RemoteStorageBackend::S3(config) => {
            krabka_object_store::ObjectStoreConfig::S3(config.clone())
        }
        crate::config::RemoteStorageBackend::Gcs(config) => {
            krabka_object_store::ObjectStoreConfig::Gcs(config.clone())
        }
    };
    let store = krabka_object_store::build_object_store(&store_config).map_err(|error| {
        BrokerError::Startup(format!("diskless object store builder failed: {error}"))
    })?;
    Ok(Arc::new(crate::diskless::read::DisklessReadHandle::new(
        Arc::new(tokio::sync::Mutex::new(
            crate::diskless::wal_index::WalIndexCache::default(),
        )),
        store,
    )))
}

pub(super) fn start_remote_storage(
    config: &BrokerConfig,
    partitions: &Arc<PartitionRegistry>,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    shutdown: &CancellationToken,
) -> Result<RemoteStorageStartup, BrokerError> {
    let Some(backend) = config.remote_storage_backend.clone() else {
        return Ok(RemoteStorageStartup {
            reader: None,
            swap_target: None,
            diskless_read: None,
        });
    };
    let diskless_read = Some(build_diskless_read_handle(&backend)?);
    // WORM layers over whichever object store was selected; it is not a
    // backend of its own. `BrokerConfig::validate` already rejects the
    // local pairing, so the arm below only fires for a config built in code.
    let worm = config.remote_storage_worm.as_ref();
    let archive = crate::remote_log_manager::ArchiveMode::from_worm(worm);
    let with_worm = |store: krabka_remote_storage::S3RemoteStorage| match worm {
        Some(worm) => store.with_worm(worm).map_err(|error| {
            BrokerError::Startup(format!("remote_storage.worm setup failed: {error}"))
        }),
        None => Ok(store),
    };
    let storage: Arc<dyn krabka_remote_storage::RemoteStorageManager> = match backend {
        crate::config::RemoteStorageBackend::Local { dir } => {
            // Fail loudly rather than archive without manifests: a local
            // directory cannot enforce write-once, so a WORM deployment that
            // silently ran on one would produce an archive no auditor can
            // trust and nobody would find out until the audit.
            if worm.is_some() {
                return Err(BrokerError::Startup(
                    "remote_storage.worm requires an object-store backend; a local storage \
                     directory cannot enforce write-once"
                        .into(),
                ));
            }
            Arc::new(krabka_remote_storage::LocalTieredStorage::new(dir))
        }
        crate::config::RemoteStorageBackend::S3(s3) => Arc::new(with_worm(
            krabka_remote_storage::S3RemoteStorage::from_s3_config(&s3).map_err(|error| {
                BrokerError::Startup(format!("remote_storage.s3 builder failed: {error}"))
            })?,
        )?),
        crate::config::RemoteStorageBackend::Gcs(gcs) => Arc::new(with_worm(
            krabka_remote_storage::S3RemoteStorage::from_gcs_config(&gcs).map_err(|error| {
                BrokerError::Startup(format!("remote_storage.gcs builder failed: {error}"))
            })?,
        )?),
    };
    let (metadata, swap_target): (
        Arc<dyn krabka_remote_storage::RemoteLogMetadataManager>,
        Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    ) = match &config.remote_log_metadata {
        crate::config::RlmmKind::TopicBacked(_) => {
            let not_ready = Arc::new(krabka_remote_storage_topic::NotReadyRlmm::new());
            let swap = Arc::new(krabka_remote_storage_topic::SwappableRlmm::new(not_ready));
            (swap.clone(), Some(swap))
        }
        crate::config::RlmmKind::InMemory => (
            Arc::new(krabka_remote_storage::InmemoryRemoteLogMetadataManager::new()),
            None,
        ),
    };
    tokio::spawn(crate::remote_log_manager::run(
        crate::remote_log_manager::RemoteLogManagerContext {
            partitions: Arc::clone(partitions),
            controller: Arc::clone(controller),
            archive,
            rsm: Arc::clone(&storage),
            rlmm: Arc::clone(&metadata),
            node_id: config.node_id,
            broker_id: config.broker_id,
        },
        crate::remote_log_manager::RemoteLogManagerConfig {
            interval: config.remote_log_manager_interval,
        },
        shutdown.child_token(),
    ));
    Ok(RemoteStorageStartup {
        reader: Some(Arc::new(crate::remote_reader::RemoteReader::new(
            storage, metadata,
        ))),
        swap_target,
        diskless_read,
    })
}
