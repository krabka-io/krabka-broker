//! KIP-460 auto preferred-replica rebalance.
//!
//! A background task on the controller leader scans every partition
//! periodically. For each partition where
//! `select_new_leader_for_partition(Preferred)` succeeds, the task queues a
//! `V1Partition` update and submits the batch.
//!
//! There is no cluster-wide imbalance ratio to cross first. That gate is the
//! `ZooKeeper` controller's `leader.imbalance.per.broker.percentage`; the
//! `KRaft` controller's `maybeBalancePartitionLeaders` has none, and restores every
//! partition whose preferred replica is back in the ISR. It bounds the work
//! by count instead: at most `MAX_ELECTIONS_PER_TICK` elections in one pass,
//! so a cluster that restarts a broker holding a hundred thousand partitions
//! does not put them all into one metadata batch. The remainder is picked up
//! by the next tick.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_units::{Time, convert::TimeExt as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::{ElectionType, select_new_leader_for_partition},
};

/// Minimal trait for the controller surface this module uses. It lets tests
/// inject a mock without a real raft cluster.
#[async_trait]
pub(crate) trait ControllerLike: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Kafka's `QuorumController.MAX_ELECTIONS_PER_IMBALANCE`: the most preferred
/// elections one balancing pass will submit.
pub(crate) const MAX_ELECTIONS_PER_TICK: usize = 1000;

#[derive(Debug, Clone)]
pub(crate) struct AutoRebalanceConfig {
    pub check_interval: Time,
}

/// Spawned task entry point.
pub(crate) async fn run(
    controller: Arc<dyn ControllerLike>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.check_interval.to_std());
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            () = shutdown.cancelled() => {
                info!("auto-rebalance task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("auto-rebalance tick skipped: not controller leader");
            continue;
        }
        rebalance_tick(&*controller, &liveness, &cfg).await;
    }
}

pub(crate) async fn rebalance_tick(
    controller: &dyn ControllerLike,
    liveness: &ControllerLivenessState,
    _cfg: &AutoRebalanceConfig,
) {
    let image = controller.current_image();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    let mut selected_keys = HashSet::new();
    let mut total: u64 = 0;
    // Witness nodes never lead. Build the set once per tick, not once per
    // partition, so the tick stays a single walk over the image.
    let witnesses = crate::config_keys::witness_node_ids(&image);
    // One lock acquisition for the whole tick rather than one per partition;
    // the set is exactly `is_alive` over every broker the registry knows.
    let alive = liveness.alive_snapshot().await;
    // Single O(P) walk over every partition.
    for pr in image.all_partitions() {
        let Some(next_total) = total.checked_add(1) else {
            warn!("auto-rebalance: partition count overflow; skipping tick");
            return;
        };
        total = next_total;
        if let Ok(new_pr) = select_new_leader_for_partition(
            &image,
            &alive,
            &witnesses,
            &pr.topic,
            pr.partition,
            ElectionType::Preferred,
        ) {
            // PreferredAlreadyLeader, PreferredIsWitness and any other Err are
            // silently skipped this tick.
            if !selected_keys.insert((new_pr.topic.clone(), new_pr.partition)) {
                warn!(
                    topic = %new_pr.topic,
                    partition = new_pr.partition,
                    "auto-rebalance: duplicate partition change; skipping tick"
                );
                return;
            }
            to_submit.push(MetadataRecord::V1Partition(new_pr));
            if to_submit.len() >= MAX_ELECTIONS_PER_TICK {
                debug!(
                    limit = MAX_ELECTIONS_PER_TICK,
                    "auto-rebalance: election cap reached; the rest waits for the next tick"
                );
                break;
            }
        }
    }
    let imbalanced = u64::try_from(to_submit.len()).unwrap_or(u64::MAX);
    if !krabka_verified::preferred_rebalance_admission(
        total,
        imbalanced,
        selected_keys.len() == to_submit.len(),
        true,
    ) {
        debug!(imbalanced, total, "auto-rebalance: batch admission denied");
        return;
    }
    info!(count = imbalanced, "auto-rebalance: submitting elections");
    if let Err(e) = controller.submit_change(to_submit).await {
        warn!(error = %e, "auto-rebalance submit failed");
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use assert2::assert;
    use krabka_metadata::{PartitionRecord, TopicRecord};
    use krabka_units::{millis, minutes, secs};
    use uuid::Uuid;

    use super::*;

    struct MockController {
        image: Arc<MetadataImage>,
        is_leader: bool,
        submitted: Mutex<Vec<MetadataRecord>>,
        submit_calls: std::sync::atomic::AtomicUsize,
        fail_submissions: std::sync::atomic::AtomicUsize,
    }

    impl MockController {
        fn new(image: Arc<MetadataImage>, is_leader: bool) -> Self {
            Self {
                image,
                is_leader,
                submitted: Mutex::new(Vec::new()),
                submit_calls: std::sync::atomic::AtomicUsize::new(0),
                fail_submissions: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn fail_next_submission(&self) {
            self.fail_submissions
                .store(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ControllerLike for MockController {
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            self.submit_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self
                .fail_submissions
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err("injected submit failure".into());
            }
            self.submitted.lock().unwrap().extend(records);
            Ok(())
        }
    }

    fn img_with_n_partitions(imbalanced: usize, balanced: usize) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: i32::try_from(imbalanced + balanced).expect("partition count fits i32"),
            replication_factor: 3,
        }));
        let mut p = 0i32;
        // Imbalanced: leader = 2 (not preferred). ISR has all three.
        for _ in 0..imbalanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: krabka_audit::NodeId(2),
                replicas: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                isr: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
            p += 1;
        }
        // Balanced: leader = 1 (preferred).
        for _ in 0..balanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: krabka_audit::NodeId(1),
                replicas: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                isr: vec![
                    krabka_audit::NodeId(1),
                    krabka_audit::NodeId(2),
                    krabka_audit::NodeId(3),
                ],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
            p += 1;
        }
        Arc::new(img)
    }

    async fn liveness_all_alive() -> ControllerLivenessState {
        let l = ControllerLivenessState::new(secs(10));
        for n in [1, 2, 3] {
            l.record_heartbeat(n).await;
        }
        l
    }

    #[tokio::test]
    async fn offline_or_out_of_isr_preferred_replica_is_not_submitted() {
        let offline = MockController::new(img_with_n_partitions(1, 0), true);
        let liveness = ControllerLivenessState::new(secs(10));
        for node in [2, 3] {
            liveness.record_heartbeat(node).await;
        }
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
        };
        rebalance_tick(&offline, &liveness, &cfg).await;
        assert!(offline.submitted.lock().unwrap().is_empty());

        let mut image =
            Arc::into_inner(img_with_n_partitions(1, 0)).expect("fixture has one Arc reference");
        let mut partition = image.partition("foo", 0).unwrap().clone();
        partition.isr = vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)];
        image.apply(&MetadataRecord::V1Partition(partition));
        let out_of_isr = MockController::new(Arc::new(image), true);
        rebalance_tick(&out_of_isr, &liveness_all_alive().await, &cfg).await;
        assert!(out_of_isr.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_submission_can_retry_the_same_nonempty_change() {
        let mock = MockController::new(img_with_n_partitions(1, 0), true);
        mock.fail_next_submission();
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
        };

        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 1);
        assert!(mock.submit_calls.load(std::sync::atomic::Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn a_small_imbalance_is_still_restored() {
        // 5 imbalanced out of 100. The ZooKeeper controller's 10% gate would
        // leave those five partitions led from the wrong broker until enough
        // others joined them; the KRaft controller restores them now, which
        // is what a rolling restart needs (#394).
        let mock = MockController::new(img_with_n_partitions(5, 95), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().len() == 5);
    }

    #[tokio::test]
    async fn every_imbalanced_partition_is_submitted_whatever_the_share() {
        // No ratio decides anything any more: the same tick submits 19 of 200
        // and 21 of 200 alike, where the retired 10% gate passed only the
        // second.
        for (imbalanced, balanced) in [(19_usize, 181_usize), (21, 179)] {
            let mock = MockController::new(img_with_n_partitions(imbalanced, balanced), true);
            let liveness = liveness_all_alive().await;
            let cfg = AutoRebalanceConfig {
                check_interval: minutes(5),
            };

            rebalance_tick(&mock, &liveness, &cfg).await;

            assert!(
                mock.submitted.lock().unwrap().len() == imbalanced,
                "{imbalanced}/{} imbalanced",
                imbalanced + balanced
            );
        }
    }

    /// Kafka's `MAX_ELECTIONS_PER_IMBALANCE`: one pass submits at most a
    /// thousand elections, so a broker that comes back holding far more
    /// partitions than that does not produce one enormous metadata batch.
    #[tokio::test]
    async fn a_tick_submits_at_most_the_election_cap() {
        let over_cap = MAX_ELECTIONS_PER_TICK + 25;
        let mock = MockController::new(img_with_n_partitions(over_cap, 0), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
        };

        rebalance_tick(&mock, &liveness, &cfg).await;

        assert!(mock.submitted.lock().unwrap().len() == MAX_ELECTIONS_PER_TICK);
    }

    #[tokio::test]
    async fn zero_imbalance_does_not_submit_empty_batch() {
        // Every partition is already balanced (leader == preferred). Even
        // with threshold 0% the tick must NOT call submit_change: an empty
        // batch still writes a spurious raft entry, which broadcasts the
        // metadata image and churns every broker's reconcile loop once per
        // tick (starving ISR re-admission of catching-up replicas).
        let mock = MockController::new(img_with_n_partitions(0, 5), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: secs(1),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(
            mock.submit_calls.load(std::sync::atomic::Ordering::SeqCst) == 0,
            "must not submit when there is nothing to rebalance"
        );
    }

    #[tokio::test]
    async fn every_submitted_record_promotes_the_preferred_replica() {
        let mock = MockController::new(img_with_n_partitions(20, 80), true);
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: minutes(5),
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert!(submitted.len() == 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert!(p.leader == krabka_audit::NodeId(1)),
                _ => panic!("unexpected record type"),
            }
        }
    }

    #[tokio::test]
    async fn run_submits_when_controller_is_leader() {
        let controller = Arc::new(MockController::new(img_with_n_partitions(1, 0), true));
        let controller_for_run: Arc<dyn ControllerLike> = controller.clone();
        let liveness = Arc::new(liveness_all_alive().await);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            controller_for_run,
            liveness,
            AutoRebalanceConfig {
                check_interval: millis(10),
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_millis(500), async {
            while controller
                .submit_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader auto-rebalance loop should submit");

        shutdown.cancel();
        task.await.unwrap();
        assert!(!controller.submitted.lock().unwrap().is_empty());
    }
}
