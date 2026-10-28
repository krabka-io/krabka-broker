//! The two setup steps every admin test in this suite repeats: building a
//! `krabka-client-core` client against a started broker, and creating the topic
//! whose configuration, partitions, or records the test then drives.

use {
    crate::support::{
        client::connect_owned,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    std::net::SocketAddr,
};

pub async fn build_client(addr: SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "admin-handlers-test",
        "client build",
    )
    .await
}

pub async fn create_topic_helper(client: &krabka_client_core::Client, name: &str, partitions: i32) {
    let req = create_topic_request(
        creatable_topic()
            .name((name).into())
            .partitions(partitions)
            .call(),
    );
    let resp = client.send(req).await.expect("create_topics");
    let result = &resp.topics[0];
    assert!(
        result.error_code == 0,
        "create_topics failed: {:?}",
        result.error_message
    );
}

use {
    crate::support::{
        registered_cluster, shutdown_cluster, start_n_node_client, start_n_node_with_retry,
    },
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::Client,
    rstest::fixture,
    rstest_bdd_macros::given,
    tempfile::TempDir,
};

/// An admin client's live broker cluster, retained until the scenario closes it.
#[derive(Default)]
pub struct AdminCluster {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    client: Option<Client>,
}

impl AdminCluster {
    pub fn client(&self) -> &Client { self.client.as_ref().expect("an admin client was connected") }

    pub fn broker(&self) -> &BrokerHandle { &self.cluster[0].0 }

    pub fn cluster(&self) -> &[(BrokerHandle, BrokerConfig, TempDir)] { &self.cluster }

    pub async fn start_two(&mut self) {
        self.cluster = start_n_node_with_retry(2).await;
        self.client = Some(build_client(self.cluster[0].1.listen_addr).await);
    }

    pub async fn fenced_three_brokers(&mut self) -> NodeId {
        self.cluster = registered_cluster(3).await;
        let leader = self.cluster[0].0.controller_leader_id();
        let stopped_index = if leader == Some(NodeId(3)) { 1 } else { 2 };
        let (stopped, config, _directory) = self.cluster.remove(stopped_index);
        let fenced = config.node_id;
        stopped.shutdown().await;
        let broker = &self.cluster[0].0;
        broker
            .wait_for_image(|_| broker.fenced_broker_ids_for_test().contains(&fenced.0))
            .await;
        self.client = Some(build_client(self.cluster[0].1.listen_addr).await);
        fenced
    }

    pub async fn shutdown(self) { shutdown_cluster(self.cluster).await; }
}

#[fixture]
pub fn admin() -> AdminCluster {
    // Start this scenario with fresh inputs and observations.

    AdminCluster {
        cluster: Vec::new(),
        client: None,
    }
}

#[given("an admin client connected to one standalone broker")]
async fn standalone_admin(admin: &mut AdminCluster) {
    let (cluster, client) = start_n_node_client(1, "admin-handlers-test").await;
    admin.cluster = cluster;
    admin.client = Some(client);
}

#[given("an admin client connected to two combined brokers")]
async fn two_broker_admin(admin: &mut AdminCluster) { admin.start_two().await; }
