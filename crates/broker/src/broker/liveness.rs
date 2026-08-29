//! Controller liveness services: the heartbeat client, the unclean-recovery
//! manager, the broker-death failover ticker, and the leadership watcher that
//! reseeds the liveness registry. These are the event-driven halves of the
//! liveness phase, split from the periodic gauge sampler beside them.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use tokio_util::sync::CancellationToken;

use crate::config::BrokerConfig;

/// Drives broker-death failover from the liveness registry. Every
/// `tick_interval` it runs [`crate::leader_election::run_liveness_tick`]. That
/// tick discovers registered brokers, handles the `AliveToDead` edge at once,
/// and then sweeps for dead brokers that still lead a partition. The edge is
/// the fast path. The sweep guarantees convergence when an edge was lost. An
/// edge is lost when this node was not the controller leader at that instant,
/// when no ISR replica was alive at that instant, or when the commit stalled
/// and timed out.
fn spawn_liveness_ticker(
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    node_id: krabka_metadata::NodeId,
    metrics: crate::metrics::BrokerMetrics,
    recovery: crate::unclean_recovery::UncleanRecoveryHandle,
    tick_interval: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(tick_interval.to_std());
        let mut state = crate::leader_election::LivenessTickState::default();
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                () = shutdown.cancelled() => return,
            }
            crate::leader_election::run_liveness_tick(
                &controller,
                node_id,
                &liveness,
                &metrics,
                &recovery,
                &mut state,
            )
            .await;
        }
    });
}

/// Handles one wake of the leadership watcher. It counts a leadership change
/// and re-seeds the liveness registry when this node has just become the
/// controller leader. The seed sits inside the change branch on purpose. A
/// re-published identical leader must not reset every broker's death clock.
async fn on_leadership_wake(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &crate::heartbeat::controller_state::ControllerLivenessState,
    node_id: krabka_metadata::NodeId,
    metrics: &crate::metrics::BrokerMetrics,
    previous: &mut Option<krabka_metadata::NodeId>,
    current: Option<krabka_metadata::NodeId>,
) {
    if current == *previous {
        return;
    }
    metrics.controller_leader_changes_total.inc();
    *previous = current;
    if current == Some(node_id) {
        let broker_ids: Vec<u64> = controller
            .current_image()
            .brokers()
            .map(|broker| broker.node_id.0)
            .collect();
        liveness.seed_brokers(broker_ids).await;
    }
}

fn spawn_leadership_watcher(
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    node_id: krabka_metadata::NodeId,
    metrics: crate::metrics::BrokerMetrics,
    shutdown: CancellationToken,
) {
    let mut leaders = controller.watch_leader();
    let mut previous = *leaders.borrow();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = leaders.changed() => {}
                () = shutdown.cancelled() => return,
            }
            let current = *leaders.borrow();
            on_leadership_wake(
                &controller,
                &liveness,
                node_id,
                &metrics,
                &mut previous,
                current,
            )
            .await;
        }
    });
}

pub(super) struct LivenessStartup {
    pub(super) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    pub(super) want_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    pub(super) should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    pub(super) unclean_recovery: crate::unclean_recovery::UncleanRecoveryHandle,
}

pub(super) fn start_liveness_services(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    listener_protocol: krabka_security::ListenerProtocol,
    // The two places this subsystem reports to, paired the way `log_dirs`
    // pairs the two log-dir registries: the metric families, and the audit log
    // that a bypassed background unclean recovery writes its evidence to.
    observability: (&crate::metrics::BrokerMetrics, &Arc<krabka_audit::AuditLog>),
    shutdown: &CancellationToken,
    log_dirs: (
        &crate::log_dir_status::LogDirRegistry,
        &crate::log_dir_id::LogDirIds,
    ),
) -> LivenessStartup {
    let (metrics, audit_log) = observability;
    let liveness = Arc::new(
        crate::heartbeat::controller_state::ControllerLivenessState::new(config.heartbeat_timeout),
    );
    let (want_shutdown, want_shutdown_rx) = tokio::sync::watch::channel(false);
    let (should_shutdown, _) = tokio::sync::watch::channel(false);
    let want_shutdown = Arc::new(want_shutdown);
    let should_shutdown = Arc::new(should_shutdown);
    tokio::spawn(crate::heartbeat::client::run(
        crate::heartbeat::client::Config {
            broker_id: config.broker_id,
            interval: config.heartbeat_interval,
            controller: Arc::clone(controller),
            shutdown: shutdown.child_token(),
            inter_broker_client: Arc::clone(inter_broker_client),
            inter_broker_listener_protocol: listener_protocol,
            inter_broker_listener_name: config.inter_broker_listener_name.clone(),
            want_shutdown: want_shutdown_rx,
            should_shutdown: Arc::clone(&should_shutdown),
            log_dir_status: log_dirs.0.clone(),
            log_dir_ids: log_dirs.1.clone(),
            all_log_dirs: config.all_log_dirs(),
            supervisor_shutdown: shutdown.clone(),
        },
    ));
    let unclean_recovery = crate::unclean_recovery::UncleanRecoveryManager::spawn(
        Arc::clone(controller),
        Arc::clone(&liveness),
        config.node_id,
        Arc::clone(inter_broker_client),
        metrics.clone(),
        crate::unclean_recovery::RecoveryPolicy {
            aggressive_deadline: config.unclean_recovery_aggressive_deadline,
            balanced_deadline: config.unclean_recovery_balanced_deadline,
            queue_capacity: config.unclean_recovery_queue_capacity,
            listener_protocol,
            inter_broker_server_name: config.inter_broker_server_name.clone(),
            background: crate::unclean_recovery::BackgroundRecovery::new(
                &config.break_glass,
                Arc::clone(audit_log),
            ),
        },
        shutdown.child_token(),
    );
    spawn_liveness_ticker(
        Arc::clone(controller),
        Arc::clone(&liveness),
        config.node_id,
        metrics.clone(),
        unclean_recovery.clone(),
        config.liveness_tick_interval,
        shutdown.child_token(),
    );
    spawn_leadership_watcher(
        Arc::clone(controller),
        Arc::clone(&liveness),
        config.node_id,
        metrics.clone(),
        shutdown.child_token(),
    );
    LivenessStartup {
        liveness,
        want_shutdown,
        should_shutdown,
        unclean_recovery,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::broker::test_support::MockMetadataSource;

    fn image_with_registered_broker(node_id: u64) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_raft::NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
                host: "127.0.0.1".to_string(),
                port: 9_092,
                rack: None,
                endpoints: Vec::new(),
                log_dirs: Vec::new(),
                features: std::collections::BTreeMap::new(),
            },
        ));
        image
    }

    #[tokio::test]
    async fn leadership_wake_seeds_liveness_only_on_a_real_change() {
        use crate::heartbeat::controller_state::{
            ControllerLivenessState, LivenessTransition, TestClock,
        };
        let me = krabka_raft::NodeId(7);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            MockMetadataSource::new(image_with_registered_broker(1), Some(me)),
        );
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_test_clock(std::time::Duration::from_millis(10), &clock);
        let metrics = crate::metrics::BrokerMetrics::new();

        // Broker 1 dies while this node leads.
        liveness.record_heartbeat(1).await;
        clock.advance(std::time::Duration::from_millis(11));
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);
        let mut previous = Some(me);

        // An identical publish must not reset broker 1's death clock.
        on_leadership_wake(
            &controller,
            &liveness,
            me,
            &metrics,
            &mut previous,
            Some(me),
        )
        .await;
        assert!(liveness.dead_snapshot().await.contains(&1));
        assert!(metrics.controller_leader_changes_total.get() == 0);

        // A change to another leader counts, but does not seed here.
        let other = krabka_raft::NodeId(8);
        on_leadership_wake(
            &controller,
            &liveness,
            me,
            &metrics,
            &mut previous,
            Some(other),
        )
        .await;
        assert!(previous == Some(other));
        assert!(liveness.dead_snapshot().await.contains(&1));
        assert!(metrics.controller_leader_changes_total.get() == 1);

        // A change back to this node seeds every registered broker alive.
        on_leadership_wake(
            &controller,
            &liveness,
            me,
            &metrics,
            &mut previous,
            Some(me),
        )
        .await;
        assert!(previous == Some(me));
        assert!(metrics.controller_leader_changes_total.get() == 2);
        assert!(liveness.dead_snapshot().await.is_empty());
        assert!(liveness.is_alive(1).await);
    }

    /// The spawned watcher, not only its per-wake body, must react to a
    /// leadership change: count it and seed the registered brokers alive.
    #[tokio::test]
    async fn leadership_watcher_seeds_liveness_when_this_node_takes_the_lead() {
        use crate::heartbeat::controller_state::ControllerLivenessState;
        let me = krabka_raft::NodeId(7);
        let mock = Arc::new(MockMetadataSource::new(
            image_with_registered_broker(1),
            None,
        ));
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = mock.clone();
        let liveness = Arc::new(ControllerLivenessState::new(krabka_units::secs(60)));
        let metrics = crate::metrics::BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        spawn_leadership_watcher(
            controller,
            liveness.clone(),
            me,
            metrics.clone(),
            shutdown.clone(),
        );
        assert!(!liveness.is_alive(1).await);

        mock.leader_tx
            .send(Some(me))
            .expect("watcher holds a receiver");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !liveness.is_alive(1).await {
            assert!(
                std::time::Instant::now() < deadline,
                "leadership watcher did not seed the registered broker"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(metrics.controller_leader_changes_total.get() == 1);
        shutdown.cancel();
    }
}
