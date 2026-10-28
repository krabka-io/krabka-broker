//! Graceful failover of the active controller under production raft timings.
//!
//! A three-node combined cluster runs with the production controller timings:
//! the two-second `controller_fetch_timeout`, the one-second
//! `controller_election_timeout`, the default fetch-miss limit and the default
//! broker heartbeat cadence. The test stops the active
//! controller the way SIGTERM does, with `controlled_shutdown`, and measures
//! how long the two survivors take to elect a new leader and then to serve a
//! controller mutation through it.
//!
//! Kafka's `KafkaRaftClient` resigns on a graceful shutdown: the leader moves
//! to `Resigned` and sends `EndQuorumEpoch` to the voters, ordered by fetch
//! offset, so the first preferred successor elects at once instead of waiting
//! out its fetch timeout. A leader that stopped without resigning would leave
//! the survivors to notice it only when their two-second fetch timer expired,
//! and then to win an election.
#![cfg(test)]

use {
    crate::support::{
        client::{connect_client, send_topic_creation},
        poll::within,
        shutdown_cluster, start_n_node_customized_with_retry,
        topics::{
            CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor,
            configured_topic_request_from,
        },
        wait_for_all_brokers_registered,
    },
    assert2::assert,
    krabka_broker::BrokerConfig,
    std::time::{Duration, Instant},
    tokio::time,
};

mod support;

/// How long the survivors may take, from the start of the graceful stop, to
/// elect a new controller and serve a mutation through it.
///
/// It covers the leadership drain (a broker heartbeat and its 50 ms re-ask),
/// the resignation and one election, about half a second in all. It is below
/// one two-second `controller_fetch_timeout`, which is the least a stop
/// without a resignation takes: the survivors elect only once their fetch
/// timer expires.
const FAILOVER_BOUND: Duration = Duration::from_millis(1_800);

/// Give `config` the production controller and broker heartbeat timings.
fn production_timings(config: &mut BrokerConfig) {
    let defaults = BrokerConfig::default();
    config.controller_fetch_timeout = defaults.controller_fetch_timeout;
    config.controller_election_timeout = defaults.controller_election_timeout;
    config.controller_heartbeat_interval = defaults.controller_heartbeat_interval;
    config.controller_heartbeat_interval_explicit = defaults.controller_heartbeat_interval_explicit;
    config.controller_fetch_miss_limit = defaults.controller_fetch_miss_limit;
    config.heartbeat_interval = defaults.heartbeat_interval;
    config.heartbeat_timeout = defaults.heartbeat_timeout;
}

#[derive(Default)]
struct GracefulControllerFailover {
    lock: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(krabka_broker::BrokerHandle, BrokerConfig, tempfile::TempDir)>,
    old_leader: Option<krabka_broker::NodeId>,
    new_leader: Option<krabka_broker::NodeId>,
    drain_result: Option<Result<(), krabka_broker::BrokerError>>,
    served_after: Option<Duration>,
}
#[rstest::fixture]
fn graceful_controller_failover() -> GracefulControllerFailover {
    let lock = None;
    GracefulControllerFailover {
        lock,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "three combined brokers are registered using production controller fetch, election and heartbeat timings"
)]
async fn production_controller_quorum(
    graceful_controller_failover: &mut GracefulControllerFailover,
) {
    graceful_controller_failover.lock = Some(support::cluster_lock().lock().await);
    let cluster =
        start_n_node_customized_with_retry(3, |_, config| production_timings(config), "cluster")
            .await;
    wait_for_all_brokers_registered(&cluster, 3).await;
    graceful_controller_failover.old_leader = Some(
        cluster[0]
            .0
            .wait_until_controller_leader()
            .await
            .expect("active controller"),
    );
    graceful_controller_failover.cluster = cluster;
}
#[rstest_bdd_macros::when(
    "the active controller shuts down gracefully and both survivors create a one-partition topic replicated twice"
)]
async fn gracefully_replace_controller(
    graceful_controller_failover: &mut GracefulControllerFailover,
) {
    let mut cluster = mem::take(&mut graceful_controller_failover.cluster);
    let old = graceful_controller_failover
        .old_leader
        .expect("active controller");
    let index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id.0 == old.0)
        .expect("the controller leader is a cluster member");
    let (target, _, _dir) = cluster.remove(index);

    let started = Instant::now();
    let stop = tokio::spawn(async move {
        let drained = target.controlled_shutdown(Duration::from_secs(30)).await;
        (drained, started.elapsed())
    });

    let survivors: Vec<_> = cluster.iter().map(|(handle, _, _)| handle).collect();
    let new = within("a new controller leader", Duration::from_secs(60), async {
        loop {
            let views: Vec<_> = survivors
                .iter()
                .map(|handle| handle.controller_leader_id())
                .collect();
            if let Some(Some(leader)) = views.first()
                && leader.0 != old.0
                && views.iter().all(|view| *view == Some(*leader))
            {
                return *leader;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let elected = started.elapsed();

    // A controller mutation through each survivor reaches the new leader:
    // the broker forwards it to the controller it discovered.
    for (attempt, (handle, _, _)) in cluster.iter().enumerate() {
        let client = connect_client(handle.listen_addr().to_string(), None).await;
        let topic = format!("after-failover-{attempt}");
        within(
            "CreateTopics on the new controller",
            Duration::from_secs(60),
            async {
                loop {
                    let response = send_topic_creation(
                        &client,
                        configured_topic_request_from(CreateTopicSetup {
                            topic: &topic,
                            num_partitions: TopicPartitionCount(1),
                            replication_factor: TopicReplicationFactor(2),
                            ..Default::default()
                        }),
                    )
                    .await;
                    if response.topics[0].error_code == 0 {
                        return;
                    }
                    time::sleep(Duration::from_millis(50)).await;
                }
            },
        )
        .await;
    }
    let served = started.elapsed();

    let (drained, stopped) = within("the graceful stop", Duration::from_secs(60), stop)
        .await
        .expect("the stop task");
    eprintln!(
        "controller failover: node {} -> node {}: elected after {elected:?}, mutation served \
         after {served:?}, stop returned after {stopped:?} ({drained:?})",
        old.0, new.0
    );

    graceful_controller_failover.cluster = cluster;
    graceful_controller_failover.new_leader = Some(new);
    graceful_controller_failover.drain_result = Some(drained);
    graceful_controller_failover.served_after = Some(served);
}
#[rstest_bdd_macros::then(
    "the old controller drains successfully and both survivor mutations succeed within 1800ms of shutdown starting"
)]
async fn controller_failover_bound(graceful_controller_failover: &mut GracefulControllerFailover) {
    let drained = graceful_controller_failover
        .drain_result
        .as_ref()
        .expect("shutdown result");
    assert!(
        drained.is_ok(),
        "the leadership drain completes: {drained:?}"
    );
    let served = graceful_controller_failover
        .served_after
        .expect("mutation completion time");
    assert!(served < FAILOVER_BOUND, "mutations served after {served:?}");
    assert!(graceful_controller_failover.new_leader != graceful_controller_failover.old_leader);
    shutdown_cluster(mem::take(&mut graceful_controller_failover.cluster)).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/graceful_failover.feature",
    name = "Graceful resignation replaces the controller before the fetch timeout"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gracefully_stopped_controller_is_replaced_within_the_bound(
    graceful_controller_failover: GracefulControllerFailover,
) {
}

use {std::mem, tokio::sync::MutexGuard};
