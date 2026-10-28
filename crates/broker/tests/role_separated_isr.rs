//! ISR changes in a role-separated cluster: one controller-only node and two
//! broker-only nodes.
//!
//! A partition leader proposes each ISR change to the active controller in an
//! `AlterPartition`. Kafka sends it on the CONTROLLER listener of that
//! controller. A controller-only node has no broker registration. So a leader
//! that looks for the controller among the registered brokers never reaches
//! it, and the controller never commits the change.
//!
//! Only the leader can put a replica back into the ISR. A follower that
//! restarts and catches up is therefore the test: it rejoins the ISR only when
//! the `AlterPartition` of the leader reaches the controller.
#![cfg(test)]

use {
    crate::support::{
        NodeIndex, RoleNodeSetup, client::create_topic_for_broker, topics::configured_topic_request,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, config::NodeRole},
    tempfile::TempDir,
};

mod support;

/// A booted role-separated cluster: node 1 is the controller-only voter, and
/// nodes 2 and 3 are broker-only nodes.
struct RoleSeparated {
    controller: BrokerHandle,
    brokers: Vec<BrokerHandle>,
    /// The configs of the broker-only nodes, in the order of `brokers` at
    /// boot. A test restarts a node on the same ports and log dir with them.
    broker_configs: Vec<BrokerConfig>,
    // Dropping these removes the log dirs that the nodes still hold open.
    _dirs: Vec<TempDir>,
}

impl RoleSeparated {
    async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

/// Boots the controller-only node first and waits until it leads, then boots
/// the two broker-only nodes and waits until both are registered.
async fn start_role_separated() -> RoleSeparated {
    const NODES: usize = 3;
    let (endpoints, mut data_listeners, mut ctrl_listeners) =
        support::single_controller_endpoints(NODES).await;
    let topology = endpoints.topology();
    let mut dirs = Vec::with_capacity(NODES);

    let ctrl_dir = TempDir::new().unwrap();
    let ctrl_cfg = topology.config(ctrl_dir.path(), RoleNodeSetup {
        role: NodeRole::Controller,
        ..Default::default()
    });
    let controller = support::start_held_node(
        ctrl_cfg,
        &mut ctrl_listeners,
        &mut data_listeners,
        "controller-only start",
    )
    .await;
    dirs.push(ctrl_dir);
    controller.wait_until_controller_leader().await.unwrap();

    let mut brokers = Vec::with_capacity(NODES - 1);
    let mut broker_configs = Vec::with_capacity(NODES - 1);
    for index in 1..NODES {
        let dir = TempDir::new().unwrap();
        let cfg = topology.config(dir.path(), RoleNodeSetup {
            index: NodeIndex(index),
            mode: BootstrapMode::Join,
            ..Default::default()
        });
        broker_configs.push(cfg.clone());
        brokers.push(
            support::start_held_node(
                cfg,
                &mut ctrl_listeners,
                &mut data_listeners,
                "broker-only start",
            )
            .await,
        );
        dirs.push(dir);
    }
    controller.wait_until_brokers_registered(NODES - 1).await;
    RoleSeparated {
        controller,
        brokers,
        broker_configs,
        _dirs: dirs,
    }
}

/// The ISR of partition 0 of `topic` in the image of the controller, sorted.
fn sorted_isr(cluster: &RoleSeparated, topic: &str) -> Vec<u64> {
    let mut isr = cluster
        .controller
        .partition_isr_for_test(topic, 0)
        .expect("the partition is in the image");
    isr.sort_unstable();
    isr
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

const TOPIC: &str = "rolesep-isr";

#[derive(Default)]
struct RoleSeparatedIsr {
    cluster: Option<RoleSeparated>,
    leader: Option<u64>,
    restart: Option<BrokerConfig>,
}

#[fixture]
fn role_separated_isr() -> RoleSeparatedIsr {
    // Preserve the stopped follower's ports and directory for its real rejoin.
    RoleSeparatedIsr::default()
}

#[given("one controller-only voter manages two broker-only nodes")]
async fn controller_only_topology(role_separated_isr: &mut RoleSeparatedIsr) {
    support::init_tracing();
    role_separated_isr.cluster = Some(start_role_separated().await);
}

#[when("a two-replica rolesep-isr partition is created")]
async fn replicated_role_topic(role_separated_isr: &RoleSeparatedIsr) {
    let cluster = role_separated_isr
        .cluster
        .as_ref()
        .expect("role-separated cluster");
    create_topic_for_broker(
        &cluster.controller,
        configured_topic_request()
            .topic(TOPIC)
            .replication_factor(2)
            .call(),
    )
    .await;
}

#[then("the controller commits brokers 2 and 3 in the partition ISR")]
async fn full_role_isr(role_separated_isr: &RoleSeparatedIsr) {
    let cluster = role_separated_isr
        .cluster
        .as_ref()
        .expect("role-separated cluster");
    cluster.controller.wait_until_isr_len(TOPIC, 0, 2).await;
    assert!(sorted_isr(cluster, TOPIC) == vec![2, 3]);
}

#[when("the broker-only follower crashes without controlled shutdown")]
async fn crash_role_follower(role_separated_isr: &mut RoleSeparatedIsr) {
    let cluster = role_separated_isr
        .cluster
        .as_mut()
        .expect("role-separated cluster");
    let leader = cluster
        .controller
        .partition_record_for_test(TOPIC, 0)
        .expect("partition in image")
        .leader
        .0;
    let follower = cluster
        .brokers
        .iter()
        .position(|broker| broker.node_id() != leader)
        .expect("follower");
    role_separated_isr.restart = Some(cluster.broker_configs[follower].clone());
    role_separated_isr.leader = Some(leader);
    cluster.brokers.remove(follower).crash_for_test().await;
}

#[then("only the partition leader remains in the controller's ISR")]
async fn reduced_role_isr(role_separated_isr: &RoleSeparatedIsr) {
    let cluster = role_separated_isr
        .cluster
        .as_ref()
        .expect("role-separated cluster");
    cluster.controller.wait_until_isr_len(TOPIC, 0, 1).await;
    assert!(
        sorted_isr(cluster, TOPIC) == vec![role_separated_isr.leader.expect("partition leader")]
    );
}

#[when("the follower restarts using the same broker-only configuration")]
async fn restart_role_follower(role_separated_isr: &mut RoleSeparatedIsr) {
    let broker = support::start_reusing_addrs(
        role_separated_isr
            .restart
            .as_ref()
            .expect("follower configuration"),
        "restarted follower",
    )
    .await;
    role_separated_isr
        .cluster
        .as_mut()
        .expect("role-separated cluster")
        .brokers
        .push(broker);
}

#[scenario(
    path = "tests/features/migrated/tests/role_separated_isr.feature",
    name = "a restarted follower rejoins the isr when the controller is controller only"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_rejoins_the_isr_when_the_controller_is_controller_only(
    role_separated_isr: RoleSeparatedIsr,
) {
    role_separated_isr
        .cluster
        .take()
        .expect("role-separated cluster")
        .shutdown()
        .await;
    drop(role_separated_isr);
}
