//! KIP-98: background sweep that expires idle and terminal transactional ids.
//!
//! [`Broker::start`](crate::Broker::start) spawns this task on every broker.
//! On each tick it refreshes the coordinator's leader-partition view from the
//! live metadata image, then asks
//! [`TxnCoordinator::expire_transactional_ids`] to tombstone every
//! locally-coordinated transactional id whose last transition is older than
//! `transactional.id.expiration.ms`. Without it `__transaction_state` keeps one
//! live entry per transactional id ever used.
//!
//! **KIP-939 invariant:** this task never expires a prepared two-phase-commit
//! transaction. The skip lives in
//! [`crate::txn::coordinator::expiry::should_expire_transactional_id`], which
//! refuses every `Prepare*` state, so this task cannot break the property.
//!
//! Every broker runs the loop, as Kafka's
//! `transaction.remove.expired.transaction.cleanup.interval.ms` sweep does, but
//! each one acts only on the transactional ids it coordinates. The tombstone is
//! idempotent -- a second one for an id already gone is a no-op on replay -- so
//! a duplicate or late sweep on a moved partition is safe.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{metadata_source::MetadataSource, txn::coordinator::TxnCoordinator};

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
///
/// The cadence is
/// [`crate::config::BrokerConfig::txn_id_expiration_cleanup_interval`], which
/// mirrors Kafka's
/// `transaction.remove.expired.transaction.cleanup.interval.ms` and defaults to
/// one hour. The broker spawns this task only when that interval is non-zero.
/// `expiration` is
/// [`crate::config::BrokerConfig::txn_id_expiration`], Kafka's
/// `transactional.id.expiration.ms`.
pub(crate) async fn run(
    coord: Arc<TxnCoordinator>,
    controller: Arc<dyn MetadataSource>,
    interval: Time,
    expiration: Time,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => sweep_once(&coord, &*controller, expiration).await,
            () = shutdown.cancelled() => {
                info!("transactional-id expiry sweep shutting down");
                return;
            }
        }
    }
}

/// Runs one sweep: refresh the leader-partition view, then expire.
///
/// This is the tick [`run`] drives, and the seam the expiry tests in
/// [`crate::txn::coordinator::expiry`] drive instead of waiting on a timer.
pub(in crate::txn) async fn sweep_once(
    coord: &Arc<TxnCoordinator>,
    controller: &dyn MetadataSource,
    expiration: Time,
) {
    let image = controller.current_image();
    // The sweep is a background task, so it waits for the loads it starts.
    coord
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    let now_ms = crate::txn::util::now_millis();
    let expired = coord
        .expire_transactional_ids(now_ms, expiration.millis_i64())
        .await;
    if expired.is_empty() {
        debug!("txn id expiry: no transactional ids to expire");
    } else {
        info!(
            count = expired.len(),
            "txn id expiry: tombstoned expired transactional ids"
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};
    use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
    use krabka_units::{mebibytes, millis, secs};
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

    const TID: &str = "tid-id-expiration";

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

    fn complete_commit_entry(last_update_ms: i64) -> TxnEntry {
        let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 3, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        entry.last_update_ms = last_update_ms;
        entry
    }

    #[tokio::test]
    async fn run_ticks_and_expires_until_shutdown() {
        let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0)).await;
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_leader(NodeId(1), 0))
                .build(),
        );
        let shutdown = CancellationToken::new();

        check!(coordinator.get(TID).is_some());

        let task = tokio::spawn(run(
            Arc::clone(&coordinator),
            Arc::clone(&source) as Arc<dyn MetadataSource>,
            secs(10),
            millis(1000),
            shutdown.clone(),
        ));

        let mut expired = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if coordinator.get(TID).is_none() {
                expired = true;
                break;
            }
        }
        check!(expired, "run should execute sweep and expire complete txn");
        check!(
            !task.is_finished(),
            "run should stay active until cancelled"
        );

        shutdown.cancel();
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), task).await;
        assert!(res.is_ok(), "task should exit promptly on shutdown");
    }
}
