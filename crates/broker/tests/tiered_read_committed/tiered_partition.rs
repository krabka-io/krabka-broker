//! One broker over a local archive, the tiered topic it hosts, and the waits
//! on the copy and local-retention passes of its remote-log manager.

use {
    crate::support::{
        client::connect,
        configs::{ConfigOperation, alter_topic_config},
        init_tracing,
        offsets::{list_offset_partition, single_partition_list_offsets},
        share::{bootstrap_share_state, broker_config, topic_id, wire},
        storage::local_segment_bases,
        tiered::create_configured_topic,
        topics::topic_configs,
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerHandle, RemoteStorageBackend, RlmmKind},
    krabka_client_core::Client,
    krabka_protocol::{owned::list_offsets_request::ListOffsetsRequest, primitives::uuid::Uuid},
    std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// The tiered topic every read in the suite goes to.
pub const TOPIC: &str = "tiered-read-committed";

/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP` (KIP-405).
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// How long the remote-log manager may take over one pass the suite waits on.
const TIER_DEADLINE: Duration = Duration::from_secs(30);

/// A broker that tiers `TOPIC` to a local archive, and its directories.
pub struct TieredPartition {
    pub broker: BrokerHandle,
    pub client: Arc<Client>,
    pub topic_id: Uuid,
    pub share_topic_id: uuid::Uuid,
    log_dir: TempDir,
    _remote_dir: TempDir,
}

/// Boots one broker over a local archive and creates `TOPIC` on it: 1 KiB
/// segments, tiered, and local retention unbounded so the local log keeps
/// every segment until [`TieredPartition::evict_local_segments`] bounds it.
pub async fn start_tiered_partition() -> TieredPartition {
    init_tracing();
    let log_dir = tempfile::tempdir().expect("log tempdir");
    let remote_dir = tempfile::tempdir().expect("remote tempdir");
    let mut config = broker_config(log_dir.path().to_path_buf());
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    config.remote_log_metadata = RlmmKind::InMemory;
    // The copy and local-retention passes run on this tick.
    config.remote_log_manager_interval = krabka_units::millis(200);
    let broker = Box::pin(Broker::start(config)).await.expect("broker start");
    let client = connect(&broker.listen_addr().to_string(), "tiered-read-committed").await;
    create_configured_topic(
        &client,
        TOPIC,
        topic_configs([
            ("remote.storage.enable", "true"),
            ("internal.segment.bytes", "1024"),
            ("local.retention.bytes", "-1"),
            ("retention.bytes", "-1"),
            ("retention.ms", "-1"),
        ]),
    )
    .await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    await_partition_config(&broker, |config| {
        config.remote_storage_enable && config.segment_size == krabka_units::kibibytes(1)
    })
    .await;
    broker.wait_until_transaction_coordinator_ready().await;
    // `bootstrap_share_state` also sets `share.auto.offset.reset` of a group
    // the suite never uses; the groups it reads with set their own.
    bootstrap_share_state(&broker, &client, "tiered-read-committed-unused").await;
    let share_topic_id = topic_id(&broker, TOPIC);
    TieredPartition {
        broker,
        client,
        topic_id: wire(share_topic_id),
        share_topic_id,
        log_dir,
        _remote_dir: remote_dir,
    }
}

/// Waits until the partition's `LogConfig` satisfies `ready`.
async fn await_partition_config(
    broker: &BrokerHandle,
    ready: impl Fn(&krabka_log::LogConfig) -> bool,
) {
    let deadline = Instant::now() + TIER_DEADLINE;
    // intentional: the reconcile loop applies a topic config to the
    // partition's `LogConfig` with no awaiter.
    loop {
        let config = broker.partition_log_config_for_test(TOPIC, 0);
        if config.as_ref().is_some_and(&ready) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the topic config never reached the partition: {config:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl TieredPartition {
    fn partition_dir(&self) -> PathBuf { self.log_dir.path().join(format!("{TOPIC}-0")) }

    /// The offset the remote tier must hold everything below once the copy
    /// pass has run: the base of the first local segment that does not end
    /// below `last_stable_offset`. Kafka's `RLMCopyTask.candidateLogSegments`
    /// copies a segment only when the next one starts at or below the LSO.
    pub fn copy_bound(&self, last_stable_offset: i64) -> i64 {
        local_segment_bases(&self.partition_dir())
            .into_iter()
            .filter(|base| *base <= last_stable_offset)
            .max()
            .unwrap_or(0)
    }

    /// Waits until the RLMM lists finished segments that cover `[0, end)`.
    pub async fn await_tiered_through(&self, end: i64) {
        let deadline = Instant::now() + TIER_DEADLINE;
        // intentional: the copy pass runs on the remote-log manager's timer.
        loop {
            let segments = self
                .broker
                .remote_log_segments_for_test(TOPIC, 0)
                .await
                .expect("the broker has a remote tier")
                .expect("the RLMM lists the partition");
            let tiered_end = segments.iter().map(|(_, last)| last + 1).max();
            if tiered_end == Some(end) {
                return;
            }
            assert!(
                Instant::now() <= deadline,
                "the tier never reached offset {end}: {segments:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Bounds local retention to one byte, so the local-retention pass deletes
    /// every segment the tier holds, and waits until the local log starts at
    /// `local_start`.
    pub async fn evict_local_segments(&self, local_start: i64) {
        let outcome = alter_topic_config()
            .client(&self.client)
            .topic(TOPIC)
            .name("local.retention.bytes")
            .operation(ConfigOperation::Set)
            .value("1")
            .call()
            .await;
        assert!(outcome.error_code == 0, "{outcome:?}");
        let deadline = Instant::now() + TIER_DEADLINE;
        // intentional: local retention runs on the remote-log manager's timer.
        loop {
            let start = self.local_log_start().await;
            if start == local_start {
                return;
            }
            assert!(
                Instant::now() <= deadline,
                "the local log start stayed at {start}, not {local_start}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `ListOffsets(EARLIEST_LOCAL)`: the first offset the local log holds.
    pub async fn local_log_start(&self) -> i64 {
        let mut response = self
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                timeout_ms: 5_000,
                ..single_partition_list_offsets(
                    TOPIC,
                    list_offset_partition(0, EARLIEST_LOCAL_TIMESTAMP),
                )
            })
            .await
            .expect("ListOffsets");
        let row = response.topics.remove(0).partitions.remove(0);
        assert!(row.error_code == 0, "{row:?}");
        row.offset
    }
}
