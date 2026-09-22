//! KIP-98 / KIP-939: background reaper that aborts timed-out transactions.
//!
//! [`Broker::start`] spawns this task on every broker. On each tick it
//! refreshes the coordinator's leader-partition view from the live metadata
//! image, then asks [`TxnCoordinator::sweep_expired`] to abort every
//! locally-coordinated, `Ongoing`, non-2PC transaction whose timeout has
//! elapsed.
//!
//! **KIP-939 invariant:** this task *never* reaps a two-phase-commit
//! transaction, which is persisted with the
//! [`crate::txn::two_pc::NO_TIMEOUT_MS`] sentinel. Its external transaction
//! manager owns the commit or abort decision. The skip lives in
//! [`crate::txn::two_pc::should_abort_idle_txn`], the exhaustively
//! model-checked decision core, so this task can never break the property.
//!
//! Every broker runs the loop, as Kafka's
//! `transaction.abort.timed.out.transaction.cleanup.interval.ms` sweep does,
//! but each one acts only on the transactions it coordinates.
//! `__transaction_state` persistence and the producer-epoch fence on
//! completion make a duplicate or late sweep on a moved partition a safe
//! no-op.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{metadata_source::MetadataSource, txn::coordinator::TxnCoordinator};

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
///
/// The cadence is
/// [`crate::config::BrokerConfig::txn_abort_cleanup_interval`], which mirrors
/// Kafka's `transaction.abort.timed.out.transaction.cleanup.interval.ms` and
/// defaults to 10s. The broker spawns this task only when that interval is
/// non-zero.
pub(crate) async fn run(
    coord: Arc<TxnCoordinator>,
    controller: Arc<dyn MetadataSource>,
    interval: Time,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => sweep_once(&coord, &*controller).await,
            () = shutdown.cancelled() => {
                info!("txn idle-transaction reaper shutting down");
                return;
            }
        }
    }
}

/// Runs one sweep. It resolves `transaction.version`, refreshes the
/// leader-partition view, then aborts any expired transactions.
async fn sweep_once(coord: &Arc<TxnCoordinator>, controller: &dyn MetadataSource) {
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    // The sweep is a background task, so it waits for the loads it starts.
    coord
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    let now_ms = crate::txn::util::now_millis();
    let aborted = coord.sweep_expired(now_ms, txnv).await;
    if aborted.is_empty() {
        debug!("txn reaper: no timed-out transactions");
    } else {
        info!(
            count = aborted.len(),
            "txn reaper: aborted timed-out transactions"
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};
    use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
    use krabka_units::{mebibytes, secs};
    use tempfile::{TempDir, tempdir};
    use uuid::Uuid;

    use super::*;
    use crate::{
        partition::Partition,
        partition_registry::PartitionRegistry,
        test_support::FakeMetadataSource,
        txn::{
            bootstrap,
            state::{TxnEntry, TxnState},
            version::TxnVersion,
        },
    };

    const TID: &str = "tid-expiration";

    fn image_with_leader(leader: NodeId, leader_epoch: i32) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(1));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: bootstrap::TOPIC.to_string(),
            topic_id: Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: bootstrap::TOPIC.to_string(),
            partition: 0,
            leader,
            replicas: vec![leader],
            isr: vec![leader],
            leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
            ..Default::default()
        }));
        image
    }

    fn transaction_state_partition(log_root: &std::path::Path) -> Arc<Partition> {
        let partition_dir = crate::log_dir::partition_dir(log_root, bootstrap::TOPIC, 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let log = Log::open(&partition_dir, LogConfig::default()).expect("open log");
        crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            PartitionIndex(0),
            log_root.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        )
    }

    async fn seeded_coordinator(entry: TxnEntry) -> (Arc<TxnCoordinator>, TempDir) {
        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            bootstrap::TOPIC.into(),
            PartitionIndex(0),
            transaction_state_partition(dir.path()),
        );
        let coordinator = Arc::new(TxnCoordinator::new(
            NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            mebibytes(1),
        ));
        coordinator
            .refresh_leader_partitions(&image_with_leader(NodeId(1), 0))
            .await
            .finished()
            .await;
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
        (coordinator, dir)
    }

    fn ongoing_entry() -> TxnEntry {
        let mut e = TxnEntry::new_empty(TID.to_string(), ProducerId(1000), 2, 60_000, 0);
        e.state = TxnState::Ongoing;
        e.start_ms = 0;
        e.last_update_ms = 0;
        e
    }

    #[tokio::test]
    async fn sweep_once_aborts_timed_out_transaction() {
        let (coordinator, _dir) = seeded_coordinator(ongoing_entry()).await;
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_leader(NodeId(1), 0))
                .build(),
        );

        let handle = coordinator.get(TID).expect("seeded entry exists");
        check!(handle.lock().await.state == TxnState::Ongoing);

        sweep_once(&coordinator, &*source).await;

        let updated = coordinator.get(TID).expect("entry exists");
        check!(updated.lock().await.state == TxnState::CompleteAbort);
    }

    #[tokio::test]
    async fn run_ticks_and_aborts_until_shutdown() {
        let (coordinator, _dir) = seeded_coordinator(ongoing_entry()).await;
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_leader(NodeId(1), 0))
                .build(),
        );
        let shutdown = CancellationToken::new();

        let handle = coordinator.get(TID).expect("seeded entry exists");
        check!(handle.lock().await.state == TxnState::Ongoing);

        let task = tokio::spawn(run(
            Arc::clone(&coordinator),
            Arc::clone(&source) as Arc<dyn MetadataSource>,
            secs(10),
            shutdown.clone(),
        ));

        let mut aborted = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if let Some(entry) = coordinator.get(TID)
                && entry.lock().await.state == TxnState::CompleteAbort
            {
                aborted = true;
                break;
            }
        }
        check!(aborted, "run should execute sweep and abort timed out txn");
        check!(
            !task.is_finished(),
            "run should stay active until cancelled"
        );

        shutdown.cancel();
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), task).await;
        assert!(res.is_ok(), "task should exit promptly on shutdown");
    }
}
