//! In-process integration tests for leader election, the KIP-101 leader
//! epoch, and ISR shrink and expand.
//!
//! These tests are gated off Windows, like the other multi-broker tests.
//! openraft and `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol under test.
#![cfg(test)]

use {
    crate::support::{
        NodeIndex, RoleNodeSetup,
        client::{BatchProduceSetup, connect_client},
        discovery::named_topic_metadata,
        durability::replicated_topic_fixture,
        produce::{ProduceAcknowledgements, ProduceTimeoutMillis},
        shutdown_cluster,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, NodeId, config::NodeRole},
    krabka_metadata::{LeaderRecoveryState, MetadataRecord, PartitionRecord, TopicConfigRecord},
    krabka_protocol::owned::metadata_response::MetadataResponse,
    krabka_raft::reconfig::ReconfigOutcome,
    std::{
        iter, mem,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    support::{
        cluster_lock,
        durability::{produce_acks, time_acks_after_isr_shrink},
    },
    tempfile::TempDir,
    tokio::{net::TcpListener, sync::MutexGuard, time},
};

mod support;

/// Waits until exactly one broker reports itself as the metadata controller
/// leader. It returns the 0-based cluster index of that broker.
async fn find_controller_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) -> usize {
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await.unwrap();
    }
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id() == Some(cfg.node_id) {
            return i;
        }
    }
    panic!("a leader was elected but no handle self-identifies as leader");
}

/// Waits until every survivor sees a controller leader that is not `victim`.
/// The victim may have been the raft leader. The survivors then need an
/// election before any of them can drive the partition failover.
async fn wait_for_controller_leader_other_than(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    victim: krabka_broker::NodeId,
) {
    for (h, _, _) in cluster {
        support::await_controller_replacement(
            h,
            victim,
            "no new controller leader within 30s after kill",
        )
        .await;
    }
}

/// The heartbeat session of the soak cluster and of Kafka's default
/// `broker.session.timeout.ms`.
const RESTART_SESSION_TIMEOUT: krabka_units::Time = krabka_units::secs(9);

/// How long after the kill the cluster has to move the partition and take an
/// `acks=all` write again: the session timeout, then one fencing.
const RESTART_FAILOVER_DEADLINE: Duration = Duration::from_secs(25);

/// Kafka's `LEADER_NOT_AVAILABLE`: the image names no leader with a known address.
const LEADER_NOT_AVAILABLE: i16 = 5;

/// One `acks=all` write of `value` to whichever broker `observer`'s image names
/// as the leader of `topic`-0, or the error code that write got.
async fn produce_to_current_leader(
    observer: &BrokerHandle,
    addresses: &[(krabka_broker::NodeId, SocketAddr)],
    topic: &str,
    value: &str,
) -> Result<i64, i16> {
    let leader = observer
        .partition_record_for_test(topic, 0)
        .map(|partition| partition.leader)
        .ok_or(LEADER_NOT_AVAILABLE)?;
    let (_, address) = addresses
        .iter()
        .find(|(node, _)| *node == leader)
        .ok_or(LEADER_NOT_AVAILABLE)?;
    produce_acks(&address.to_string(), &[value], BatchProduceSetup {
        topic,
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        timeout: ProduceTimeoutMillis(2_000),
        ..Default::default()
    })
    .await
}

/// Whether `image` has partition 0 of `topic` led by a broker other than
/// `victim`, at a leader epoch past the one `before` records.
fn leadership_moved_off(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    victim: krabka_broker::NodeId,
    before: &krabka_metadata::PartitionRecord,
) -> bool {
    image.partition(topic, 0).is_some_and(|partition| {
        partition.leader != victim
            && partition.leader != krabka_broker::NodeId(0)
            && partition.leader_epoch > before.leader_epoch
    })
}

/// Whether `image` has `victim` back in the ISR of partition 0 of `topic` and
/// unfenced.
fn rejoined_as_follower(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    victim: krabka_broker::NodeId,
) -> bool {
    image
        .partition(topic, 0)
        .is_some_and(|partition| partition.isr.contains(&victim))
        && image.broker(victim).is_some_and(|broker| !broker.fenced)
}

#[derive(Default)]
struct ElectionBehavior {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    bootstrap: String,
    topic: String,
    victim: Option<NodeId>,
    dead_config: Option<BrokerConfig>,
    directories: Vec<TempDir>,
    reborn: Option<BrokerHandle>,
    metadata: Option<MetadataResponse>,
    timed_append: Option<(i64, Duration)>,
    reconfiguration: Option<ReconfigOutcome>,
    occupied: Option<TcpListener>,
    pre_offsets: Vec<i64>,
    post_attempts: Vec<Result<i64, i16>>,
    observer_index: usize,
    addresses: Vec<(NodeId, SocketAddr)>,
    before_partition: Option<PartitionRecord>,
    killed_at: Option<Instant>,
    acked: Vec<String>,
    after_offset: Option<i64>,
}

#[rstest::fixture]
fn election_behavior() -> ElectionBehavior {
    // Election operations and all wire/log observations belong to one fresh topic.
    ElectionBehavior::default()
}

impl ElectionBehavior {
    async fn create_session_partition(&self, controller: usize, replicas: &[i32]) {
        let client = connect_client(self.cluster[controller].1.listen_addr.to_string(), None).await;
        let created = client
            .send(support::topics::create_topic_request(support::topic_on(
                &self.topic,
                &[replicas],
            )))
            .await
            .expect("CreateTopics");
        assert!(created.topics[0].error_code == 0, "{created:?}");
        client.close();
        let victim = self.victim.expect("victim leader");
        for (broker, _, _) in &self.cluster {
            broker
                .wait_for_image(|image| {
                    image.partition(&self.topic, 0).is_some_and(|partition| {
                        partition.leader == victim && partition.isr.len() == 3
                    })
                })
                .await;
        }
    }

    async fn await_session_leader_replacement(&self) {
        let victim = self.victim.expect("victim leader");
        let before = self.before_partition.as_ref().expect("pre-crash partition");
        let moved = time::timeout(
            RESTART_FAILOVER_DEADLINE,
            self.cluster[self.observer_index]
                .0
                .wait_for_image(|image| leadership_moved_off(image, &self.topic, victim, before)),
        )
        .await;
        assert!(
            moved.is_ok(),
            "leadership did not move off the restarted broker within {RESTART_FAILOVER_DEADLINE:?}; before the kill {before:?}, now {:?}, victim registration {:?}",
            self.cluster[self.observer_index]
                .0
                .partition_record_for_test(&self.topic, 0),
            self.cluster[self.observer_index]
                .0
                .controller_image_for_test()
                .broker(victim)
        );
    }

    fn current_surviving_leader(&self) -> &BrokerHandle {
        let leader = self.cluster[self.observer_index]
            .0
            .partition_record_for_test(&self.topic, 0)
            .expect("partition")
            .leader;
        self.cluster
            .iter()
            .find(|(_, config, _)| config.node_id == leader)
            .map(|(broker, _, _)| broker)
            .expect("survivor leader")
    }

    async fn shutdown(&mut self) {
        let Self {
            guard,
            cluster,
            reborn,
            directories,
            occupied,
            ..
        } = mem::take(self);
        drop(occupied);
        if let Some(broker) = reborn {
            broker.shutdown().await;
        }
        shutdown_cluster(cluster).await;
        drop(directories);
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "a registered three-broker RF3 topic named {topic} has elected leadership and ISR"
)]
async fn elected_replicated_topic(election_behavior: &mut ElectionBehavior, topic: String) {
    election_behavior.guard = Some(cluster_lock().lock().await);
    election_behavior.topic = topic;
    let (cluster, bootstrap) = replicated_topic_fixture()
        .topic(&election_behavior.topic)
        .call()
        .await;
    election_behavior.cluster = cluster;
    election_behavior.bootstrap = bootstrap;
}

#[rstest_bdd_macros::when("broker {node} shuts down")]
async fn stop_election_broker(election_behavior: &mut ElectionBehavior, node: u64) {
    let index = election_behavior
        .cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() == node)
        .expect("election broker");
    let (broker, config, directory) = election_behavior.cluster.remove(index);
    election_behavior.victim = Some(config.node_id);
    election_behavior.dead_config = Some(config);
    election_behavior.directories.push(directory);
    broker.shutdown().await;
}

#[rstest_bdd_macros::when(
    "the survivors elect a controller and a partition leader and Metadata reads that partition"
)]
async fn read_new_election_leader(election_behavior: &mut ElectionBehavior) {
    let victim = election_behavior.victim.expect("stopped broker");
    wait_for_controller_leader_other_than(&election_behavior.cluster, victim).await;
    election_behavior.cluster[0]
        .0
        .wait_until_partition_leader_changed(&election_behavior.topic, 0, victim)
        .await;
    let client = connect_client(election_behavior.cluster[0].1.listen_addr.to_string(), None).await;
    election_behavior.metadata = Some(
        client
            .send(named_topic_metadata(&election_behavior.topic))
            .await
            .expect("metadata"),
    );
    client.close();
}

#[rstest_bdd_macros::then(
    "the new partition leader is broker 2 or 3 and its leader epoch is greater than zero"
)]
fn observed_new_election_leader(election_behavior: &ElectionBehavior) {
    let topic = election_behavior
        .metadata
        .as_ref()
        .expect("metadata read")
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(election_behavior.topic.as_str()))
        .expect("topic");
    let partition = topic.partitions.first().expect("partition");
    assert!(
        partition.leader_id == 2 || partition.leader_id == 3,
        "unexpected new leader: {}",
        partition.leader_id
    );
    assert!(
        partition.leader_epoch > 0,
        "leader_epoch should bump after election"
    );
}

#[rstest_bdd_macros::when(
    "acks all appends a,b,c with a 15000-millisecond timeout while ISR shrinks"
)]
async fn append_across_isr_shrink(election_behavior: &mut ElectionBehavior) {
    election_behavior.timed_append = Some(
        time_acks_after_isr_shrink(
            &election_behavior.bootstrap,
            &election_behavior.topic,
            &["a", "b", "c"],
            ProduceTimeoutMillis(15_000),
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the append starts at offset 0 and completes in less than ten seconds")]
fn completed_isr_shrink_append(election_behavior: &ElectionBehavior) {
    let (offset, elapsed) = election_behavior.timed_append.expect("timed append");
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(10),
        "shrink should be quick on for_tests config; took {elapsed:?}"
    );
}

#[rstest_bdd_macros::given(
    "unclean leader election is enabled and the current leader is the only ISR member"
)]
async fn unclean_election_premise(election_behavior: &mut ElectionBehavior) {
    election_behavior.cluster[0]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: election_behavior.topic.clone(),
            overrides: iter::once(("unclean.leader.election.enable".into(), "true".into()))
                .collect(),
        }))
        .await
        .expect("enable unclean election");
    let mut partition = election_behavior.cluster[0]
        .0
        .partition_record_for_test(&election_behavior.topic, 0)
        .expect("partition");
    let victim = partition.leader;
    election_behavior.victim = Some(victim);
    partition.isr = vec![victim];
    election_behavior.cluster[0]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(partition))
        .await
        .expect("make leader only ISR member");
    election_behavior.cluster[0]
        .0
        .wait_for_image(|image| {
            image
                .partition(&election_behavior.topic, 0)
                .is_some_and(|partition| partition.isr == [victim])
        })
        .await;
}

#[rstest_bdd_macros::when("the sole ISR leader shuts down while a survivor observes Recovering")]
async fn stop_unclean_election_leader(election_behavior: &mut ElectionBehavior) {
    let victim = election_behavior.victim.expect("unclean leader");
    let index = election_behavior
        .cluster
        .iter()
        .position(|(_, config, _)| config.node_id == victim)
        .expect("leader broker");
    let (broker, config, directory) = election_behavior.cluster.remove(index);
    election_behavior.dead_config = Some(config);
    election_behavior.directories.push(directory);
    tokio::join!(
        election_behavior.cluster[0].0.wait_for_image(|image| image
            .leader_recovery_state(&election_behavior.topic, 0)
            == LeaderRecoveryState::Recovering),
        broker.shutdown()
    );
}

#[rstest_bdd_macros::then(
    "the survivor reports Recovered with a leader different from the stopped broker"
)]
async fn recovered_unclean_election(election_behavior: &ElectionBehavior) {
    election_behavior.cluster[0]
        .0
        .wait_for_image(|image| {
            image.leader_recovery_state(&election_behavior.topic, 0)
                == LeaderRecoveryState::Recovered
                && image
                    .partition(&election_behavior.topic, 0)
                    .is_some_and(|partition| Some(partition.leader) != election_behavior.victim)
        })
        .await;
}

#[rstest_bdd_macros::when("the stopped broker rejoins from its original log and addresses")]
async fn rejoin_unclean_election_broker(election_behavior: &mut ElectionBehavior) {
    election_behavior
        .dead_config
        .as_mut()
        .expect("stopped config")
        .bootstrap_mode = BootstrapMode::Rejoin;
    election_behavior.reborn = Some(
        support::start_reusing_addrs(
            election_behavior
                .dead_config
                .as_ref()
                .expect("stopped config"),
            "KIP-704 broker restart",
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the restarted broker also reports Recovered")]
async fn restarted_unclean_election_state(election_behavior: &ElectionBehavior) {
    election_behavior
        .reborn
        .as_ref()
        .expect("rejoined broker")
        .wait_for_image(|image| {
            image.leader_recovery_state(&election_behavior.topic, 0)
                == LeaderRecoveryState::Recovered
        })
        .await;
}

#[rstest_bdd_macros::when("the controller finalizes kraft.version 1")]
async fn enable_dynamic_election_membership(election_behavior: &mut ElectionBehavior) {
    let controller = find_controller_leader(&election_behavior.cluster).await;
    election_behavior.reconfiguration = Some(
        election_behavior.cluster[controller]
            .0
            .finalize_kraft_version_for_test(1)
            .await
            .expect("activate kraft.version 1"),
    );
}

#[rstest_bdd_macros::then("the version activation is committed")]
fn committed_dynamic_election_membership(election_behavior: &ElectionBehavior) {
    assert!(matches!(
        election_behavior
            .reconfiguration
            .as_ref()
            .expect("version activation"),
        ReconfigOutcome::Committed
    ));
}

#[rstest_bdd_macros::when(
    "the highest-numbered non-controller follower leaves the voter set, reserves a replacement client address and shuts down"
)]
async fn remove_election_follower(election_behavior: &mut ElectionBehavior) {
    let controller = find_controller_leader(&election_behavior.cluster).await;
    let index = (0..election_behavior.cluster.len())
        .rev()
        .find(|&index| index != controller)
        .expect("follower controller");
    election_behavior.cluster[controller]
        .0
        .change_membership(
            election_behavior
                .cluster
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, (_, config, _))| config.node_id)
                .collect(),
        )
        .await
        .expect("remove follower from voter set");
    let (broker, config, directory) = election_behavior.cluster.remove(index);
    election_behavior.victim = Some(config.node_id);
    election_behavior.dead_config = Some(config);
    election_behavior.directories.push(directory);
    election_behavior.occupied = Some(
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve occupied client port"),
    );
    let listen = election_behavior
        .occupied
        .as_ref()
        .expect("occupied port")
        .local_addr()
        .expect("occupied address");
    election_behavior
        .dead_config
        .as_mut()
        .expect("joiner config")
        .listen_addr = listen;
    election_behavior
        .dead_config
        .as_mut()
        .expect("joiner config")
        .advertised_listener = listen.to_string();
    broker.shutdown().await;
}

#[rstest_bdd_macros::then("the remaining partition ISR has two members")]
async fn shrunken_election_isr(election_behavior: &ElectionBehavior) {
    election_behavior.cluster[0]
        .0
        .wait_until_isr_len(&election_behavior.topic, 0, 2)
        .await;
}

#[rstest_bdd_macros::when(
    "the follower starts on an empty log through localhost KIP-853 auto-join, with the occupied client port released after one second"
)]
async fn join_election_follower(election_behavior: &mut ElectionBehavior) {
    election_behavior.directories.push(TempDir::new().unwrap());
    let controller = find_controller_leader(&election_behavior.cluster).await;
    let bootstrap_node = election_behavior.cluster[controller].1.node_id;
    let bootstrap = election_behavior.cluster[controller]
        .1
        .controller_listen_addr;
    let config = election_behavior
        .dead_config
        .as_mut()
        .expect("joiner config");
    config.log_dir = election_behavior
        .directories
        .last()
        .expect("empty joiner log")
        .path()
        .to_path_buf();
    config.bootstrap_mode = BootstrapMode::Join;
    config.controller_quorum_voters = vec![(bootstrap_node, bootstrap.to_string())];
    config.bootstrap_servers = vec![format!("localhost:{}", bootstrap.port())];
    config.auto_join = true;
    let occupied = election_behavior
        .occupied
        .take()
        .expect("occupied client port");
    let release = tokio::spawn(async move {
        time::sleep(Duration::from_secs(1)).await;
        drop(occupied);
    });
    election_behavior.reborn = Some(
        support::start_reusing_addrs(
            election_behavior
                .dead_config
                .as_ref()
                .expect("joiner config"),
            "reborn follower",
        )
        .await,
    );
    release.await.expect("release occupied port");
}

#[rstest_bdd_macros::then("the partition ISR expands back to three members")]
async fn expanded_election_isr(election_behavior: &ElectionBehavior) {
    election_behavior.cluster[0]
        .0
        .wait_until_isr_len(&election_behavior.topic, 0, 3)
        .await;
}

#[rstest_bdd_macros::when("acks one writes a,b,c,d,e before the leader is lost")]
async fn pre_election_records(election_behavior: &mut ElectionBehavior) {
    for value in ["a", "b", "c", "d", "e"] {
        election_behavior.pre_offsets.push(
            produce_acks(&election_behavior.bootstrap, &[value], BatchProduceSetup {
                topic: &election_behavior.topic,
                ..Default::default()
            })
            .await
            .expect("pre"),
        );
    }
}

#[rstest_bdd_macros::when("acks one attempts f,g,h,i,j through the first surviving broker")]
async fn post_election_records(election_behavior: &mut ElectionBehavior) {
    let bootstrap = election_behavior.cluster[0].1.listen_addr.to_string();
    for value in ["f", "g", "h", "i", "j"] {
        election_behavior.post_attempts.push(
            produce_acks(&bootstrap, &[value], BatchProduceSetup {
                topic: &election_behavior.topic,
                ..Default::default()
            })
            .await,
        );
    }
}

#[rstest_bdd_macros::then(
    "all five pre-loss writes were acknowledged and each post-loss attempt either succeeds or requests a leader refresh with code 6"
)]
fn served_election_attempts(election_behavior: &ElectionBehavior) {
    assert!(election_behavior.pre_offsets.len() == 5);
    assert!(election_behavior.post_attempts.len() == 5);
    assert!(
        election_behavior
            .post_attempts
            .iter()
            .all(|answer| matches!(answer, Ok(_) | Err(6))),
        "post-failover answers: {:?}",
        election_behavior.post_attempts
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "broker death elects new leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_death_elects_new_leader(election_behavior: ElectionBehavior) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "unclean failover recovers after a real broker restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_failover_recovers_after_a_real_broker_restart(
    election_behavior: ElectionBehavior,
) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "acks all completes after isr shrink"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_after_isr_shrink(election_behavior: ElectionBehavior) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "isr expand on catchup"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isr_expand_on_catchup(election_behavior: ElectionBehavior) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "produce during leader failover"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_during_leader_failover(election_behavior: ElectionBehavior) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::given(
    "three brokers use nine-second heartbeat sessions and an RF3 restart-fencing partition is led by a non-controller broker"
)]
async fn session_fenced_leader(election_behavior: &mut ElectionBehavior) {
    election_behavior.guard = Some(cluster_lock().lock().await);
    election_behavior.topic = "restart-fencing".into();
    election_behavior.cluster = support::start_n_node_with(3, |_, config| {
        config.heartbeat_timeout = RESTART_SESSION_TIMEOUT;
    })
    .await
    .expect("start the cluster");
    support::wait_for_all_brokers_registered(&election_behavior.cluster, 3).await;
    let controller = find_controller_leader(&election_behavior.cluster).await;
    let victim_index = (controller + 1) % election_behavior.cluster.len();
    let victim = election_behavior.cluster[victim_index].1.node_id;
    election_behavior.victim = Some(victim);
    election_behavior.observer_index = controller;
    election_behavior.addresses = election_behavior
        .cluster
        .iter()
        .map(|(_, config, _)| (config.node_id, config.listen_addr))
        .collect();
    let mut replicas = vec![i32::try_from(victim.0).expect("node id")];
    replicas.extend(
        election_behavior
            .cluster
            .iter()
            .map(|(_, config, _)| config.node_id)
            .filter(|node| *node != victim)
            .map(|node| i32::try_from(node.0).expect("node id")),
    );
    election_behavior
        .create_session_partition(controller, &replicas)
        .await;
    election_behavior.cluster[victim_index]
        .0
        .wait_until_local_partition_leader(&election_behavior.topic, 0, victim)
        .await;
}

#[rstest_bdd_macros::given(
    "node 1 alone votes as controller and leads RF3 controller-restart-fencing with broker-only replicas 2 and 3 and nine-second heartbeat sessions"
)]
async fn session_fenced_controller(election_behavior: &mut ElectionBehavior) {
    election_behavior.guard = Some(cluster_lock().lock().await);
    support::init_tracing();
    election_behavior.topic = "controller-restart-fencing".into();
    let (endpoints, mut data_listeners, mut controller_listeners) =
        support::single_controller_endpoints(3).await;
    let topology = endpoints.topology();
    let directories: Vec<TempDir> = iter::repeat_with(|| TempDir::new().expect("dir"))
        .take(3)
        .collect();
    let configs: Vec<BrokerConfig> = (0..3)
        .map(|index| {
            let mut config = topology.config(directories[index].path(), RoleNodeSetup {
                index: NodeIndex(index),
                mode: if index == 0 {
                    BootstrapMode::Bootstrap
                } else {
                    BootstrapMode::Join
                },
                ..Default::default()
            });
            if index == 0 {
                config.roles = vec![NodeRole::Broker, NodeRole::Controller];
            }
            config.heartbeat_timeout = RESTART_SESSION_TIMEOUT;
            config
        })
        .collect();
    let mut brokers = Vec::new();
    for config in &configs {
        brokers.push(
            Box::pin(support::start_held_node(
                config.clone(),
                &mut controller_listeners,
                &mut data_listeners,
                "start a node",
            ))
            .await,
        );
    }
    election_behavior.cluster = brokers
        .into_iter()
        .zip(configs)
        .zip(directories)
        .map(|((broker, config), directory)| (broker, config, directory))
        .collect();
    election_behavior.cluster[0]
        .0
        .wait_until_brokers_registered(3)
        .await;
    let victim = election_behavior.cluster[0].1.node_id;
    election_behavior.victim = Some(victim);
    election_behavior.observer_index = 1;
    election_behavior
        .create_session_partition(0, &[1, 2, 3])
        .await;
    election_behavior.before_partition = Some(
        election_behavior.cluster[1]
            .0
            .partition_record_for_test(&election_behavior.topic, 0)
            .expect("partition"),
    );
}

#[rstest_bdd_macros::when(
    "acks all commits a,b,c,d,e and the observer records the leader epoch before restart"
)]
async fn acknowledged_session_fencing_records(election_behavior: &mut ElectionBehavior) {
    for value in ["a", "b", "c", "d", "e"] {
        produce_to_current_leader(
            &election_behavior.cluster[election_behavior.observer_index].0,
            &election_behavior.addresses,
            &election_behavior.topic,
            value,
        )
        .await
        .expect("acks=all before the kill");
        election_behavior.acked.push(value.to_owned());
    }
    election_behavior.before_partition = Some(
        election_behavior.cluster[election_behavior.observer_index]
            .0
            .partition_record_for_test(&election_behavior.topic, 0)
            .expect("partition"),
    );
}

#[rstest_bdd_macros::when("the partition leader crashes inside its existing heartbeat session")]
async fn crash_session_fenced_leader(election_behavior: &mut ElectionBehavior) {
    let victim = election_behavior.victim.expect("victim leader");
    let index = election_behavior
        .cluster
        .iter()
        .position(|(_, config, _)| config.node_id == victim)
        .expect("victim broker");
    let (broker, mut config, directory) = election_behavior.cluster.remove(index);
    if election_behavior.observer_index > index {
        election_behavior.observer_index -= 1;
    }
    config.bootstrap_mode = BootstrapMode::Rejoin;
    election_behavior.dead_config = Some(config);
    election_behavior.directories.push(directory);
    election_behavior.killed_at = Some(Instant::now());
    broker.crash_for_test().await;
}

#[rstest_bdd_macros::when(
    "the leader restarts concurrently with failover and an acks all after record must commit within twenty-five seconds of the crash"
)]
async fn restart_and_write_session_fenced_leader(election_behavior: &mut ElectionBehavior) {
    let killed_at = election_behavior.killed_at.expect("crash instant");
    let failover = async {
        election_behavior.await_session_leader_replacement().await;
        let elected = killed_at.elapsed();
        let deadline = killed_at + RESTART_FAILOVER_DEADLINE;
        let offset = loop {
            match produce_to_current_leader(
                &election_behavior.cluster[election_behavior.observer_index].0,
                &election_behavior.addresses,
                &election_behavior.topic,
                "after",
            )
            .await
            {
                Ok(offset) => break offset,
                Err(code) => {
                    assert!(
                        Instant::now() < deadline,
                        "acks=all still failing with {code} {RESTART_FAILOVER_DEADLINE:?} after the kill; partition {:?}",
                        election_behavior.cluster[election_behavior.observer_index]
                            .0
                            .partition_record_for_test(&election_behavior.topic, 0)
                    );
                    time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        (elected, offset)
    };
    let (reborn, (elected, after_offset)) = tokio::join!(
        support::start_reusing_addrs(
            election_behavior
                .dead_config
                .as_ref()
                .expect("victim config"),
            "restart the killed leader"
        ),
        failover
    );
    election_behavior.reborn = Some(reborn);
    election_behavior.after_offset = Some(after_offset);
    election_behavior.acked.push("after".to_owned());
    tracing::info!(
        ?elected,
        after_offset,
        "leadership moved off the restarted broker"
    );
}

#[rstest_bdd_macros::then(
    "a surviving broker leads at a higher epoch and its local records are exactly a,b,c,d,e,after"
)]
fn durable_session_fencing_records(election_behavior: &ElectionBehavior) {
    let image = election_behavior.cluster[election_behavior.observer_index]
        .0
        .controller_image_for_test();
    assert!(leadership_moved_off(
        &image,
        &election_behavior.topic,
        election_behavior.victim.expect("victim leader"),
        election_behavior
            .before_partition
            .as_ref()
            .expect("pre-crash partition")
    ));
    let held: Vec<String> = election_behavior
        .current_surviving_leader()
        .local_batches_for_test(&election_behavior.topic, 0)
        .expect("leader log")
        .iter()
        .flat_map(|batch| &batch.records)
        .filter_map(|record| record.value.as_ref())
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .collect();
    assert!(
        held == election_behavior.acked,
        "every acknowledged record survives the failover"
    );
    assert!(election_behavior.after_offset.is_some());
}

#[rstest_bdd_macros::then(
    "within twenty-five seconds partition leadership moves off the restarted controller at a higher epoch while it remains controller leader"
)]
async fn fenced_controller_partition_leadership(election_behavior: &ElectionBehavior) {
    let victim = election_behavior.victim.expect("victim controller");
    election_behavior.await_session_leader_replacement().await;
    assert!(
        election_behavior
            .reborn
            .as_ref()
            .expect("restarted controller")
            .controller_leader_id()
            == Some(victim)
    );
}

#[rstest_bdd_macros::then("the restarted broker rejoins the ISR unfenced as a follower")]
async fn rejoined_session_fenced_broker(election_behavior: &ElectionBehavior) {
    let victim = election_behavior.victim.expect("victim broker");
    election_behavior.cluster[election_behavior.observer_index]
        .0
        .wait_for_image(|image| rejoined_as_follower(image, &election_behavior.topic, victim))
        .await;
}

#[rstest_bdd_macros::then(
    "the restarted controller rejoins the ISR unfenced as a follower within twenty-five seconds"
)]
async fn rejoined_session_fenced_controller(election_behavior: &ElectionBehavior) {
    let victim = election_behavior.victim.expect("victim controller");
    time::timeout(
        RESTART_FAILOVER_DEADLINE,
        election_behavior.cluster[election_behavior.observer_index]
            .0
            .wait_for_image(|image| rejoined_as_follower(image, &election_behavior.topic, victim)),
    )
    .await
    .expect("the restarted broker rejoins the ISR as a follower");
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "a leader restarted inside its session is fenced and loses leadership"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_restarted_inside_its_session_is_fenced_and_loses_leadership(
    election_behavior: ElectionBehavior,
) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/leader_election.feature",
    name = "a controller leader restarted inside its session loses its partition leadership"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_controller_leader_restarted_inside_its_session_loses_its_partition_leadership(
    election_behavior: ElectionBehavior,
) {
    election_behavior.shutdown().await;
    drop(election_behavior);
}
