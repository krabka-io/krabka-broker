//! The `__krabka_audit` topic in a cluster with an isolated controller: one
//! controller-only node that is the whole voter set, and broker-only nodes.
//!
//! The controller starts first and sees no registered broker, so it creates
//! nothing. Kafka never places a replica on a controller-only node. The first
//! broker-only node to start creates the topic on itself, and its audit writer
//! then records the broker start in that partition.
#![cfg(test)]

use {
    crate::support::controller_voters,
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, config::NodeRole, coordinator::AUDIT_TOPIC},
    krabka_raft::NodeId,
    tempfile::TempDir,
};

mod support;

use {
    krabka_broker::{BrokerConfig, BrokerHandle},
    tokio::net::TcpListener,
};
struct IsolatedAudit {
    controller_config: Option<BrokerConfig>,
    broker_config: Option<BrokerConfig>,
    clients: Vec<TcpListener>,
    controllers: Vec<TcpListener>,
    directories: Vec<TempDir>,
    controller: Option<BrokerHandle>,
    broker: Option<BrokerHandle>,
}
#[rstest::fixture]
fn isolated_audit() -> IsolatedAudit {
    // Start this scenario with fresh inputs and observations.

    IsolatedAudit {
        controller_config: None,
        broker_config: None,
        clients: Vec::new(),
        controllers: Vec::new(),
        directories: Vec::new(),
        controller: None,
        broker: None,
    }
}
#[rstest_bdd_macros::given(
    "a controller-only voter 1 and a joining broker-only node 2 with reserved listeners"
)]
async fn isolated_audit_topology(isolated_audit: &mut IsolatedAudit) {
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(2).await;
    let voters = vec![(1_u64, controller_addrs[0])];
    let controller_dir = TempDir::new().unwrap();
    let mut controller_config = support::broker_config()
        .log_dir(controller_dir.path())
        .client_addrs(&client_addrs)
        .controller_addrs(&controller_addrs)
        .voters(controller_voters(&voters))
        .call();
    controller_config.roles = vec![NodeRole::Controller];
    let broker_dir = TempDir::new().unwrap();
    let mut broker_config = support::broker_config()
        .log_dir(broker_dir.path())
        .index(1)
        .client_addrs(&client_addrs)
        .controller_addrs(&controller_addrs)
        .voters(controller_voters(&voters))
        .mode(BootstrapMode::Join)
        .call();
    broker_config.roles = vec![NodeRole::Broker];
    isolated_audit.controller_config = Some(controller_config);
    isolated_audit.broker_config = Some(broker_config);
    isolated_audit.clients = client_listeners;
    isolated_audit.controllers = controller_listeners;
    isolated_audit.directories = vec![controller_dir, broker_dir];
}
#[rstest_bdd_macros::when("the isolated controller starts and becomes quorum leader")]
async fn start_isolated_audit_controller(isolated_audit: &mut IsolatedAudit) {
    let controller = Broker::start_with_listeners(
        isolated_audit
            .controller_config
            .take()
            .expect("controller configured"),
        Some(isolated_audit.controllers.remove(0)),
        Some(isolated_audit.clients.remove(0)),
    )
    .await
    .expect("controller-only start");
    controller.wait_until_controller_leader().await.unwrap();
    isolated_audit.controller = Some(controller);
}
#[rstest_bdd_macros::then("the controller alone has not created an audit topic")]
fn controller_does_not_create_audit(isolated_audit: &IsolatedAudit) {
    let controller = isolated_audit
        .controller
        .as_ref()
        .expect("controller started");
    assert!(
        controller
            .controller_image_for_test()
            .topic(AUDIT_TOPIC)
            .is_none()
    );
}
#[rstest_bdd_macros::when("the broker-only node starts and joins the controller")]
async fn start_isolated_audit_broker(isolated_audit: &mut IsolatedAudit) {
    isolated_audit.broker = Some(
        Broker::start_with_listeners(
            isolated_audit
                .broker_config
                .take()
                .expect("broker configured"),
            Some(isolated_audit.controllers.remove(0)),
            Some(isolated_audit.clients.remove(0)),
        )
        .await
        .expect("broker-only start"),
    );
}
#[rstest_bdd_macros::then(
    "only broker 2 leads and replicates audit partition 0 and writes its startup record"
)]
async fn audit_belongs_to_broker(isolated_audit: &IsolatedAudit) {
    let controller = isolated_audit
        .controller
        .as_ref()
        .expect("controller started");
    let broker = isolated_audit.broker.as_ref().expect("broker started");
    controller
        .wait_until_partition_present(AUDIT_TOPIC, 0)
        .await;
    let placement: Vec<_> = controller
        .controller_image_for_test()
        .partitions_of(AUDIT_TOPIC)
        .map(|record| {
            (
                record.partition,
                record.leader,
                record.replicas.clone(),
                record.isr.clone(),
            )
        })
        .collect();
    broker
        .wait_until_local_log_end_offset(AUDIT_TOPIC, 0, 1)
        .await;
    assert!(placement == vec![(0, NodeId(2), vec![NodeId(2)], vec![NodeId(2)])]);
    assert!(controller.local_log_end_offset(AUDIT_TOPIC, 0).is_none());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit_isolated_controller.feature",
    name = "only a broker holds the audit partition under an isolated controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_broker_holds_the_audit_partition_under_an_isolated_controller(
    isolated_audit: IsolatedAudit,
) {
    isolated_audit
        .broker
        .expect("broker started")
        .shutdown()
        .await;
    isolated_audit
        .controller
        .expect("controller started")
        .shutdown()
        .await;
}
