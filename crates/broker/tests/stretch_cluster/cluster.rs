//! The three-site cluster a site is *stopped* in: it boots two data sites and
//! a witness site, hands out handles and addresses, and takes a handle away
//! when its site is lost.
//!
//! The relayed variant that leaves every broker running and cuts the network
//! instead is `LinkedCluster`, in the `linked` module.

use std::time::Duration;

use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use tempfile::TempDir;

use crate::{
    NODE_A, NODE_B, NODE_C, TOPIC, WITNESS,
    produce::{client_at, create_topic},
    profile::{apply_stretch_config, wait_for_stretch_metadata},
    support,
    view::wait_for_leader_and_isr,
    within,
};

/// A running three-site cluster. Handles are taken out as sites are stopped,
/// so `shutdown` can still drain whatever is left.
pub struct Cluster {
    handles: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Boot the three-site cluster: two data sites, one witness site,
    /// `min.insync.replicas=2` (the only value a stretch profile accepts at
    /// rf=3 over three sites), and the witness role on the `site-c` node.
    ///
    /// Retries like `support::start_n_node_with_retry`, which cannot be reused
    /// here because it takes no per-broker customizer.
    pub async fn start() -> Self {
        let mut last_err = None;
        for attempt in 1..=3 {
            let started = support::start_n_node_with(3, apply_stretch_config).await;
            match started {
                Ok(cluster) => {
                    support::wait_for_all_brokers_registered(&cluster, 3).await;
                    for (handle, _, _) in &cluster {
                        wait_for_stretch_metadata(handle).await;
                    }
                    let (handles, configs, dirs) = cluster.into_iter().fold(
                        (Vec::new(), Vec::new(), Vec::new()),
                        |(mut handles, mut configs, mut dirs), (handle, config, dir)| {
                            handles.push(Some(handle));
                            configs.push(config);
                            dirs.push(dir);
                            (handles, configs, dirs)
                        },
                    );
                    return Self {
                        handles,
                        configs,
                        _dirs: dirs,
                    };
                }
                Err(error) => {
                    tracing::warn!(attempt, %error, "stretch cluster start failed; retrying");
                    last_err = Some(error);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        panic!("stretch cluster start failed after 3 attempts; last error: {last_err:?}");
    }

    pub fn handle(&self, index: usize) -> &BrokerHandle {
        self.handles[index]
            .as_ref()
            .unwrap_or_else(|| panic!("broker {index} is still running"))
    }

    pub fn addr(&self, index: usize) -> String {
        self.configs[index].listen_addr.to_string()
    }

    /// Lose a site: stop its broker and let it leave the cluster.
    pub async fn stop(&mut self, index: usize) {
        let handle = self.handles[index]
            .take()
            .unwrap_or_else(|| panic!("broker {index} was already stopped"));
        within("stopping a site", handle.shutdown()).await;
    }

    pub async fn shutdown(mut self) {
        for handle in self.handles.drain(..).flatten() {
            within("cluster shutdown", handle.shutdown()).await;
        }
    }
}

/// Bring the cluster up with a topic and every replica in the ISR.
pub async fn cluster_with_topic() -> (Cluster, WireUuid) {
    let cluster = Cluster::start().await;
    let client = client_at(&cluster.addr(NODE_A)).await;
    let topic_id = create_topic(&client).await;
    for index in [NODE_A, NODE_B, NODE_C] {
        within(
            "the partition reaches every node",
            cluster.handle(index).wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the initial three-replica ISR",
        1,
        &[1, 2, WITNESS],
    )
    .await;
    (cluster, topic_id)
}
