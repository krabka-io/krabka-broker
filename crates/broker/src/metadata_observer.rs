//! Broker-only metadata observer (Component B).
//!
//! A broker-only `KRaft` node is not an openraft voter. It keeps its
//! `MetadataImage` current by *fetching* the committed `__cluster_metadata`
//! log from the controller quorum over `API_KEY_METADATA_FETCH`. It decodes
//! each record batch through the `krabka_metadata` Kafka-record bridge, and
//! applies the records exactly as the controller state machine would.
//!
//! The controller prunes that log behind KIP-630 snapshots, so the observer
//! also speaks the two halves of snapshotting a follower does: a fetch below
//! the controller's log start is answered with a snapshot id, which the
//! observer installs over `FetchSnapshot` ([`snapshot`]), and what it has
//! applied is checkpointed under its own metadata directory ([`store`]) so a
//! restart resumes there instead of at offset 0.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use krabka_metadata::MetadataImage;
use krabka_raft::{NodeId, OutboundDialer, kraft::snapshot_fetch::MetadataSnapshotFetchMax};
use krabka_units::{ByteSize, Time};
use qubit_clock::Timer;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

mod fetch;
mod serve_loop;
mod snapshot;
mod store;
#[cfg(test)]
pub(crate) mod test_support;

use self::serve_loop::run_loop;

/// Static configuration for the observer.
#[derive(Clone)]
pub struct ObserverConfig {
    /// Capacity of each outbound observer connection.
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size of each outbound observer connection.
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    /// Controller-listener voter map `(id, "<host>:<port>")`, from
    /// `controller_quorum_voters`. The map carries the host verbatim. The
    /// dialer resolves it again on each connect, so it reaches the new pod IP
    /// of a rejoining peer.
    pub voters: Vec<(NodeId, String)>,
    /// Outbound dialer. It uses the same TLS and SASL path as the raft
    /// transport.
    pub dialer: Arc<dyn OutboundDialer>,
    /// `client_id` for the dial handshake.
    pub client_id: String,
    /// Cluster UUID for the initial empty image.
    pub cluster_id: uuid::Uuid,
    /// This node's id, sent as the `replica_id` of the `FetchSnapshot`
    /// requests that install a pruned-past metadata snapshot.
    pub node_id: NodeId,
    /// Directory holding this node's `__cluster_metadata` state: the KIP-630
    /// checkpoints the observer installs and writes. Same layout as a
    /// controller's data dir, so the two are readable by the same tools.
    pub data_dir: std::path::PathBuf,
    /// Records applied between the checkpoints the observer writes for itself.
    /// `0` disables them, leaving only the snapshots fetched from a controller
    /// on disk.
    pub snapshot_interval_records: u64,
    /// Ceiling on the bytes reassembled for one fetched metadata snapshot.
    pub snapshot_fetch_max: MetadataSnapshotFetchMax,
    /// Soft cap per fetch.
    pub max_bytes: ByteSize,
    /// Idle poll interval once caught up to the high watermark.
    pub poll_interval: Time,
    /// Timer that drives the idle poll cadence. Production uses
    /// [`qubit_clock::StdTimer`], which follows real time. Tests inject a
    /// timer from a [`qubit_clock::ManualMonotonicClock`], so the poll
    /// interval fires on a controlled manual timeline instead of wall-clock
    /// time.
    pub timer: Arc<dyn Timer>,
}

/// Handle to a running observer. It holds the image watch and the background
/// fetch task.
pub struct MetadataObserver {
    image: watch::Sender<Arc<MetadataImage>>,
    leader: watch::Sender<Option<NodeId>>,
    /// Highest metadata-log offset applied to `image`, or `-1` before the
    /// first record. This is the value sent in `BrokerHeartbeat`.
    metadata_offset: AtomicI64,
    /// Highest `__cluster_metadata` offset the controller quorum reported as
    /// committed in the last successful fetch, or `-1` before the observer has
    /// reached a controller. A catching-up observer trails it by the records it
    /// has yet to fetch, which is the gap the readiness probe bounds.
    quorum_committed_offset: AtomicI64,
    shutdown: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl MetadataObserver {
    /// The observer state alone, with no fetch loop behind it yet: an empty
    /// image for `cluster_id`, no leader hint, and no applied offset.
    ///
    /// [`Self::start`] spawns the loop over it. A test that drives
    /// [`run_loop`] itself, so that it can await the loop's own return rather
    /// than cancelling it, starts from this instead.
    fn new(cluster_id: uuid::Uuid, shutdown: CancellationToken) -> Arc<Self> {
        let (image_tx, _) = watch::channel(Arc::new(MetadataImage::new(cluster_id)));
        let (leader_tx, _) = watch::channel(None);
        Arc::new(Self {
            image: image_tx,
            leader: leader_tx,
            metadata_offset: AtomicI64::new(-1),
            quorum_committed_offset: AtomicI64::new(-1),
            shutdown,
            task: tokio::sync::Mutex::new(None),
        })
    }

    /// Starts the observer loop. The image watch begins at an empty image for
    /// `cluster_id`. Callers subscribe with [`Self::watch_image`].
    #[must_use]
    pub fn start(config: ObserverConfig) -> Arc<Self> {
        let shutdown = CancellationToken::new();
        let observer = Self::new(config.cluster_id, shutdown.clone());
        let task = tokio::spawn(run_loop(config, observer.clone(), shutdown));
        if let Ok(mut guard) = observer.task.try_lock() {
            *guard = Some(task);
        }
        observer
    }

    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.image.borrow().clone()
    }

    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image.subscribe()
    }

    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.subscribe()
    }

    #[must_use]
    pub fn current_metadata_offset(&self) -> i64 {
        self.metadata_offset.load(Ordering::Acquire)
    }

    /// Highest `__cluster_metadata` offset the quorum has committed, as the
    /// last successful fetch reported it, or `-1` before the observer has
    /// reached a controller.
    #[must_use]
    pub fn quorum_committed_offset(&self) -> i64 {
        self.quorum_committed_offset.load(Ordering::Acquire)
    }

    /// Stops the fetch loop and drains the task.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn task_drained_for_test(&self) -> bool {
        self.task.lock().await.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_raft::{BootstrapMode, Controller, ControllerConfig};
    use krabka_units::millis;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{test_support::observer_config, *};

    #[derive(Clone)]
    struct RecordingDialer {
        client_ids: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for RecordingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: krabka_client_core::ConnectionOptions,
        ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
            self.client_ids
                .lock()
                .unwrap()
                .push(options.client_id.clone());
            krabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    #[tokio::test]
    async fn cancel_drains_background_task() {
        let dir = TempDir::new().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            client_id: "cancel-test".into(),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
        });

        assert!(observer.current_metadata_offset() == -1);

        observer.cancel().await;

        assert!(observer.task.lock().await.is_none());
    }

    #[tokio::test]
    async fn observer_replicates_committed_topic() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(krabka_raft::NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        let ctrl_addr = ctrl.controller_bound_addr();
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "observed".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");
        let client_ids = Arc::new(std::sync::Mutex::new(Vec::new()));

        let observer_dir = TempDir::new().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(krabka_raft::NodeId(1), ctrl_addr.to_string())],
            dialer: Arc::new(RecordingDialer {
                client_ids: client_ids.clone(),
            }),
            poll_interval: millis(50),
            ..observer_config(Uuid::nil(), observer_dir.path().to_path_buf())
        });

        let mut img_rx = observer.watch_image();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if img_rx.borrow().topic("observed").is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "observer did not replicate topic within 5s"
            );
            let _ = tokio::time::timeout(Duration::from_millis(200), img_rx.changed()).await;
        }

        assert!(observer.current_image().topic("observed").is_some());
        let controller_offset = i64::try_from(ctrl.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1);
        assert!(observer.current_metadata_offset() == controller_offset);
        assert!(
            client_ids
                .lock()
                .unwrap()
                .iter()
                .any(|id| id == "test-observer")
        );

        observer.cancel().await;
        ctrl.shutdown().await;
    }
}
