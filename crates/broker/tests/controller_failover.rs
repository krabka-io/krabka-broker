//! Graceful failover of the active controller under production raft timings.
//!
//! A three-node combined cluster runs with the production controller timings:
//! the five-second `controller_election_timeout`, the default fetch-miss limit
//! and the default broker heartbeat cadence. The test stops the active
//! controller the way SIGTERM does, with `controlled_shutdown`, and measures
//! how long the two survivors take to elect a new leader and then to serve a
//! controller mutation through it.
//!
//! Kafka's `KafkaRaftClient` resigns on a graceful shutdown: the leader moves
//! to `Resigned` and sends `EndQuorumEpoch` to the voters, ordered by fetch
//! offset, so the first preferred successor elects at once instead of waiting
//! out its fetch timeout. A leader that stopped without resigning would leave
//! the survivors to notice it only when their five-second fetch timer expired,
//! and then to win an election.

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
};

mod support;

/// How long the survivors may take, from the start of the graceful stop, to
/// elect a new controller and serve a mutation through it.
///
/// It covers the leadership drain (a broker heartbeat and its 50 ms re-ask),
/// the resignation and one election, about half a second in all. It is below
/// one five-second `controller_election_timeout`, which is the least a stop
/// without a resignation takes: the survivors elect only once their fetch
/// timer expires.
const FAILOVER_BOUND: Duration = Duration::from_secs(4);

/// Give `config` the production controller and broker heartbeat timings.
fn production_timings(config: &mut BrokerConfig) {
    let defaults = BrokerConfig::default();
    config.controller_election_timeout = defaults.controller_election_timeout;
    config.controller_heartbeat_interval = defaults.controller_heartbeat_interval;
    config.controller_heartbeat_interval_explicit = defaults.controller_heartbeat_interval_explicit;
    config.controller_fetch_miss_limit = defaults.controller_fetch_miss_limit;
    config.heartbeat_interval = defaults.heartbeat_interval;
    config.heartbeat_timeout = defaults.heartbeat_timeout;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gracefully_stopped_controller_is_replaced_within_the_bound() {
    let _g = support::cluster_lock().lock().await;
    let mut cluster =
        start_n_node_customized_with_retry(3, |_, config| production_timings(config), "cluster")
            .await;
    wait_for_all_brokers_registered(&cluster, 3).await;

    let old = cluster[0].0.wait_until_controller_leader().await;
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
            tokio::time::sleep(Duration::from_millis(10)).await;
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
                    tokio::time::sleep(Duration::from_millis(50)).await;
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

    assert!(
        drained.is_ok(),
        "the leadership drain completes: {drained:?}"
    );
    assert!(
        served < FAILOVER_BOUND,
        "elected after {elected:?}, served after {served:?}"
    );
    shutdown_cluster(cluster).await;
}
