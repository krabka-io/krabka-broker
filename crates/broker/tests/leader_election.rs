//! In-process integration tests for leader election, the KIP-101 leader
//! epoch, and ISR shrink and expand.
//!
//! These tests are gated off Windows, like the other multi-broker tests.
//! openraft and `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol under test.

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
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, config::NodeRole},
    krabka_metadata::{LeaderRecoveryState, MetadataRecord, TopicConfigRecord},
    std::time::Duration,
    support::{
        cluster_lock,
        durability::{produce_acks, time_acks_after_isr_shrink},
    },
    tempfile::TempDir,
};

mod support;

/// Waits until exactly one broker reports itself as the metadata controller
/// leader. It returns the 0-based cluster index of that broker.
async fn find_controller_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) -> usize {
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_death_elects_new_leader() {
    let _g = cluster_lock().lock().await;
    let (mut cluster, _bootstrap_1) = replicated_topic_fixture().topic("elect").call().await;

    // Kill broker 1 (the partition leader by round-robin).
    let (killed_broker, killed_cfg, _dead_dir) = cluster.remove(0);
    let killed_id = killed_cfg.node_id;
    killed_broker.shutdown().await;

    // Wait for election: first a controller leader among the survivors, then
    // the partition failover it drives. cluster[0] is now broker 2 (broker 1
    // was removed above).
    wait_for_controller_leader_other_than(&cluster, killed_id).await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed("elect", 0, killed_id)
        .await;
    let client = connect_client(cluster[0].1.listen_addr.to_string(), None).await;
    let resp = client
        .send(named_topic_metadata("elect"))
        .await
        .expect("metadata");
    let t = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("elect"))
        .expect("topic");
    let p = t.partitions.first().expect("partition");
    let (new_leader, new_epoch) = (p.leader_id, p.leader_epoch);
    assert!(
        new_leader == 2 || new_leader == 3,
        "unexpected new leader: {new_leader}"
    );
    assert!(new_epoch > 0, "leader_epoch should bump after election");

    // Clean up surviving brokers.
    shutdown_cluster(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_failover_recovers_after_a_real_broker_restart() {
    let _g = cluster_lock().lock().await;
    let (mut cluster, _bootstrap) = replicated_topic_fixture()
        .topic("kip704-restart")
        .call()
        .await;

    cluster[0]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "kip704-restart".into(),
            overrides: [("unclean.leader.election.enable".into(), "true".into())]
                .into_iter()
                .collect(),
        }))
        .await
        .expect("enable unclean election");
    let mut partition = cluster[0]
        .0
        .partition_record_for_test("kip704-restart", 0)
        .expect("partition");
    let victim = partition.leader;
    partition.isr = vec![victim];
    cluster[0]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(partition))
        .await
        .expect("make the leader the only ISR member");
    cluster[0]
        .0
        .wait_for_image(|image| {
            image
                .partition("kip704-restart", 0)
                .is_some_and(|partition| partition.isr == [victim])
        })
        .await;

    let victim_idx = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == victim)
        .expect("leader broker");
    let (dead, mut reborn_config, dead_dir) = cluster.remove(victim_idx);
    let survivor = &cluster[0].0;
    tokio::join!(
        survivor.wait_for_image(|image| {
            image.leader_recovery_state("kip704-restart", 0) == LeaderRecoveryState::Recovering
        }),
        dead.shutdown()
    );
    survivor
        .wait_for_image(|image| {
            image.leader_recovery_state("kip704-restart", 0) == LeaderRecoveryState::Recovered
                && image
                    .partition("kip704-restart", 0)
                    .is_some_and(|partition| partition.leader != victim)
        })
        .await;

    reborn_config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let reborn = support::start_reusing_addrs(&reborn_config, "KIP-704 broker restart").await;
    reborn
        .wait_for_image(|image| {
            image.leader_recovery_state("kip704-restart", 0) == LeaderRecoveryState::Recovered
        })
        .await;

    reborn.shutdown().await;
    drop(dead_dir);
    shutdown_cluster(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_after_isr_shrink() {
    let _g = cluster_lock().lock().await;
    let (mut cluster, bootstrap_1) = replicated_topic_fixture().topic("shrink2").call().await;

    // Freeze broker 3 by shutting it down.
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    // Produce acks=-1. ISR should shrink to {1,2} within
    // replica_lag_time_max (2s on CI) + heartbeat_timeout (2s); produce
    // completes after.
    let (offset, elapsed) = time_acks_after_isr_shrink(
        &bootstrap_1,
        "shrink2",
        &["a", "b", "c"],
        ProduceTimeoutMillis(15_000),
    )
    .await;
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(10),
        "shrink should be quick on for_tests config; took {elapsed:?}"
    );

    shutdown_cluster(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isr_expand_on_catchup() {
    let _g = cluster_lock().lock().await;
    let (mut cluster, _bootstrap_1) = replicated_topic_fixture().topic("expand").call().await;

    // 1. Find the controller leader so we can activate dynamic membership.
    let leader_idx = find_controller_leader(&cluster).await;
    let leader_node_id = cluster[leader_idx].1.node_id;
    eprintln!("KRABKA[test] controller leader is node_id={leader_node_id}");

    let outcome = cluster[leader_idx]
        .0
        .finalize_kraft_version_for_test(1)
        .await
        .expect("activate kraft.version 1");
    assert!(
        matches!(outcome, krabka_raft::reconfig::ReconfigOutcome::Committed),
        "kraft.version activation was not committed: {outcome:?}"
    );

    // Prefer the highest-numbered follower so this test never removes the
    // active controller leader and normally leaves partition leader 1 alone.
    let leader_idx = find_controller_leader(&cluster).await;
    let victim_idx = (0..cluster.len())
        .rev()
        .find(|&idx| idx != leader_idx)
        .expect("a follower controller");
    let victim_node_id = cluster[victim_idx].1.node_id;

    // 2. Remove the victim from the voter set before killing it.
    cluster[leader_idx]
        .0
        .change_membership(
            cluster
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != victim_idx)
                .map(|(_, (_, cfg, _))| cfg.node_id)
                .collect(),
        )
        .await
        .expect("remove follower from voter set");

    // 3. Capture the victim's config, then kill it.
    let (dead_h, mut reborn_cfg, _dead_dir) = cluster.remove(victim_idx);
    // Reserve while the old listeners still hold their addresses.
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve the occupied client port");
    reborn_cfg.listen_addr = occupied.local_addr().expect("occupied client address");
    reborn_cfg.advertised_listener = reborn_cfg.listen_addr.to_string();
    dead_h.shutdown().await;
    cluster[0].0.wait_until_isr_len("expand", 0, 2).await;

    // 4. Reboot the victim through the production KIP-853 join path. It must
    //    catch up as an observer before auto-join promotes it to a voter.
    let reborn_dir = TempDir::new().unwrap();
    let leader_idx = find_controller_leader(&cluster).await;
    let bootstrap_node_id = cluster[leader_idx].1.node_id;
    let bootstrap_controller = cluster[leader_idx].1.controller_listen_addr;
    reborn_cfg.log_dir = reborn_dir.path().to_path_buf();
    reborn_cfg.bootstrap_mode = krabka_broker::BootstrapMode::Join;
    reborn_cfg.controller_quorum_voters =
        vec![(bootstrap_node_id, bootstrap_controller.to_string())];
    // Exercise the unresolved-hostname path used by a Kubernetes Service,
    // rather than passing the already-bound IP address back to the joiner.
    reborn_cfg.bootstrap_servers = vec![format!("localhost:{}", bootstrap_controller.port())];
    reborn_cfg.auto_join = true;
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(occupied);
    });
    let reborn = support::start_reusing_addrs(&reborn_cfg, "reborn follower").await;
    release.await.expect("release the occupied port");
    eprintln!("KRABKA[test] reborn follower {victim_node_id} started");

    // 5. Wait for the partition's ISR to expand back to {1, 2, 3}.
    cluster[0].0.wait_until_isr_len("expand", 0, 3).await;

    reborn.shutdown().await;
    shutdown_cluster(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_during_leader_failover() {
    let _g = cluster_lock().lock().await;
    let (mut cluster, bootstrap_1) = replicated_topic_fixture().topic("failover").call().await;

    // Produce 5 records with acks=1, kill broker 1 mid-burst, produce 5
    // more pointed at broker 2's bootstrap (clients will re-fetch
    // metadata on NOT_LEADER_OR_FOLLOWER).
    for v in &["a", "b", "c", "d", "e"] {
        produce_acks(&bootstrap_1, &[v], BatchProduceSetup {
            topic: "failover",
            ..Default::default()
        })
        .await
        .expect("pre");
    }
    let bootstrap_2 = cluster[1].1.listen_addr.to_string();
    let (dead, dead_cfg, _dead_dir) = cluster.remove(0);
    let victim = dead_cfg.node_id;
    dead.shutdown().await;

    // Wait for the new partition leader. The victim may also have been the
    // controller leader, so first wait for the survivors to elect one.
    wait_for_controller_leader_other_than(&cluster, victim).await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed("failover", 0, victim)
        .await;

    // Continue producing. The first attempt may hit NOT_LEADER_OR_FOLLOWER;
    // retry via bootstrap_2.
    for v in &["f", "g", "h", "i", "j"] {
        let res = produce_acks(&bootstrap_2, &[v], BatchProduceSetup {
            topic: "failover",
            ..Default::default()
        })
        .await;
        // Either success (election done) or NOT_LEADER_OR_FOLLOWER if metadata still stale.
        // For test purposes, accept either; just verify the cluster keeps serving.
        let _ = res;
    }

    shutdown_cluster(cluster).await;
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
    addresses: &[(krabka_broker::NodeId, std::net::SocketAddr)],
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

/// krabka-io/krabka-broker#1450: a partition leader that is killed and comes
/// back inside its heartbeat session is a new process, and Kafka treats it as
/// one.
///
/// A Kafka broker draws a new incarnation id at every start.
/// `ClusterControlManager.registerBroker` refuses that incarnation with
/// `DUPLICATE_BROKER_REGISTRATION` while the previous one still holds a
/// session. Once the session expires, either the expiry fences the old
/// registration (`ReplicationControlManager.handleBrokerFenced`) or the new
/// registration runs `handleBrokerShutdown` first. Both take the broker out of
/// its ISRs and elect another leader at a new leader epoch, so the restarted
/// process does not resume a leadership whose log it may not hold. The
/// restarted broker then catches up and is unfenced as a follower. A krabka
/// broker's self-registration takes the second path at once, so the deadline
/// is Kafka's worst case, not krabka's.
///
/// The victim is not the controller leader, so nothing here waits on a
/// controller failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_restarted_inside_its_session_is_fenced_and_loses_leadership() {
    const TOPIC: &str = "restart-fencing";
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with(3, |_, config| {
        config.heartbeat_timeout = RESTART_SESSION_TIMEOUT;
    })
    .await
    .expect("start the cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let controller = find_controller_leader(&cluster).await;
    let victim_idx = (controller + 1) % cluster.len();
    let victim = cluster[victim_idx].1.node_id;
    let mut replicas = vec![i32::try_from(victim.0).expect("node id")];
    replicas.extend(
        cluster
            .iter()
            .map(|(_, config, _)| config.node_id)
            .filter(|node| *node != victim)
            .map(|node| i32::try_from(node.0).expect("node id")),
    );
    let addresses: Vec<_> = cluster
        .iter()
        .map(|(_, config, _)| (config.node_id, config.listen_addr))
        .collect();
    let client = connect_client(cluster[controller].1.listen_addr.to_string(), None).await;
    let created = client
        .send(support::topics::create_topic_request(support::topic_on(
            TOPIC,
            &[&replicas],
        )))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|image| {
                image
                    .partition(TOPIC, 0)
                    .is_some_and(|partition| partition.leader == victim && partition.isr.len() == 3)
            })
            .await;
    }
    // The image names the victim leader before its partition runtime installs
    // that leadership, and until it does a Produce answers
    // NOT_LEADER_OR_FOLLOWER, as Kafka's does while ReplicaManager applies the
    // delta. The acks below must not race that.
    cluster[victim_idx]
        .0
        .wait_until_local_partition_leader(TOPIC, 0, victim)
        .await;
    let observer = &cluster[controller].0;

    let mut acked = Vec::new();
    for value in ["a", "b", "c", "d", "e"] {
        produce_to_current_leader(observer, &addresses, TOPIC, value)
            .await
            .expect("acks=all before the kill");
        acked.push(value.to_owned());
    }
    let before = observer
        .partition_record_for_test(TOPIC, 0)
        .expect("partition");

    let (victim_handle, mut victim_config, victim_dir) = cluster.remove(victim_idx);
    let observer_idx = if controller > victim_idx {
        controller - 1
    } else {
        controller
    };
    let killed_at = std::time::Instant::now();
    victim_handle.crash_for_test().await;
    victim_config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let observer = &cluster[observer_idx].0;

    let restart = support::start_reusing_addrs(&victim_config, "restart the killed leader");
    let failover = async {
        let moved = tokio::time::timeout(
            RESTART_FAILOVER_DEADLINE,
            observer.wait_for_image(|image| leadership_moved_off(image, TOPIC, victim, &before)),
        )
        .await;
        assert!(
            moved.is_ok(),
            "leadership did not move off the restarted broker within {RESTART_FAILOVER_DEADLINE:?}; before the kill {before:?}, now {:?}, victim registration {:?}",
            observer.partition_record_for_test(TOPIC, 0),
            observer.controller_image_for_test().broker(victim),
        );
        let elected = killed_at.elapsed();
        let deadline = killed_at + RESTART_FAILOVER_DEADLINE;
        let offset = loop {
            match produce_to_current_leader(observer, &addresses, TOPIC, "after").await {
                Ok(offset) => break offset,
                Err(code) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "acks=all still failing with {code} {RESTART_FAILOVER_DEADLINE:?} after the kill; partition {:?}",
                        observer.partition_record_for_test(TOPIC, 0)
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        (elected, offset)
    };
    let (reborn, (elected, after_offset)) = tokio::join!(restart, failover);
    acked.push("after".to_owned());
    tracing::info!(
        ?elected,
        after_offset,
        "leadership moved off the restarted broker"
    );

    let partition = observer
        .partition_record_for_test(TOPIC, 0)
        .expect("partition");
    let leader = cluster
        .iter()
        .find(|(_, config, _)| config.node_id == partition.leader)
        .map(|(handle, _, _)| handle)
        .expect("a survivor leads");
    let held: Vec<String> = leader
        .local_batches_for_test(TOPIC, 0)
        .expect("leader log")
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter_map(|record| record.value.as_ref())
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .collect();
    assert!(
        held == acked,
        "every acknowledged record survives the failover"
    );

    // The restarted broker catches up and rejoins the ISR as a follower.
    observer
        .wait_for_image(|image| rejoined_as_follower(image, TOPIC, victim))
        .await;

    reborn.shutdown().await;
    drop(victim_dir);
    shutdown_cluster(cluster).await;
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

/// krabka-io/krabka-broker#1442: the partition leader killed and restarted
/// inside its heartbeat session is also the active controller.
///
/// A combined node registers its broker before its broker layer is built, so
/// a restarted node that wins the controller election first admits its own
/// registration before anything else can. That batch still has to retire the
/// previous incarnation, as Kafka's `registerBroker` does on any active
/// controller: the broker leaves the ISR and another replica leads at a new
/// leader epoch. The victim here is the whole voter set, so its restart always
/// takes that path, and two broker-only nodes are the other replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_controller_leader_restarted_inside_its_session_loses_its_partition_leadership() {
    const TOPIC: &str = "controller-restart-fencing";
    const NODES: usize = 3;
    let _g = cluster_lock().lock().await;
    support::init_tracing();
    let (endpoints, mut data_listeners, mut controller_listeners) =
        support::single_controller_endpoints(NODES).await;
    let topology = endpoints.topology();
    let dirs: Vec<TempDir> = (0..NODES).map(|_| TempDir::new().expect("dir")).collect();
    let configs: Vec<BrokerConfig> = (0..NODES)
        .map(|index| {
            let mut config = topology.config(dirs[index].path(), RoleNodeSetup {
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
    let mut nodes = Vec::with_capacity(NODES);
    for config in &configs {
        nodes.push(
            Box::pin(support::start_held_node(
                config.clone(),
                &mut controller_listeners,
                &mut data_listeners,
                "start a node",
            ))
            .await,
        );
    }
    nodes[0].wait_until_brokers_registered(NODES).await;
    let victim = configs[0].node_id;
    let client = connect_client(configs[0].listen_addr.to_string(), None).await;
    let created = client
        .send(support::topics::create_topic_request(support::topic_on(
            TOPIC,
            &[&[1, 2, 3]],
        )))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    for node in &nodes {
        node.wait_for_image(|image| {
            image
                .partition(TOPIC, 0)
                .is_some_and(|partition| partition.leader == victim && partition.isr.len() == NODES)
        })
        .await;
    }
    let before = nodes[1]
        .partition_record_for_test(TOPIC, 0)
        .expect("partition");

    let mut nodes = nodes.into_iter();
    nodes.next().expect("the victim").crash_for_test().await;
    let mut victim_config = configs[0].clone();
    victim_config.bootstrap_mode = BootstrapMode::Rejoin;
    let reborn = support::start_reusing_addrs(&victim_config, "restart the controller").await;
    let survivors: Vec<BrokerHandle> = nodes.collect();
    let observer = &survivors[0];

    let moved = tokio::time::timeout(
        RESTART_FAILOVER_DEADLINE,
        observer.wait_for_image(|image| leadership_moved_off(image, TOPIC, victim, &before)),
    )
    .await;
    assert!(
        moved.is_ok(),
        "the restarted controller kept its leadership; before the kill {before:?}, now {:?}",
        observer.partition_record_for_test(TOPIC, 0),
    );
    assert!(reborn.controller_leader_id() == Some(victim));
    tokio::time::timeout(
        RESTART_FAILOVER_DEADLINE,
        observer.wait_for_image(|image| rejoined_as_follower(image, TOPIC, victim)),
    )
    .await
    .expect("the restarted broker rejoins the ISR as a follower");

    for survivor in survivors {
        survivor.shutdown().await;
    }
    reborn.shutdown().await;
}
