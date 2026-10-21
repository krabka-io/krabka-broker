//! A three-broker cluster whose data listeners each sit behind a [`Relay`].
//!
//! A replication barrier is a leader whose followers stay in its ISR but
//! fetch nothing more, so its high watermark stops below what it appends.
//! Cutting the relay in front of a broker builds one without stopping the
//! broker: its peers can no longer fetch from it, while its controller plane,
//! which no relay fronts, keeps its session and its leaderships. A suite then
//! checks what a coordinator answers for a write that cannot commit, and
//! what the next leader serves once the broker dies.

use {
    crate::support::{
        ClusterBootstrapSetup, NodeIndex, RoleTopology, await_broker_start, bind_and_hold_ports,
        broker_config, init_tracing, listener_pairs, relay::Relay, start_reusing_addrs,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId},
    std::{net::SocketAddr, time::Duration},
    tempfile::TempDir,
};

/// Three brokers whose data listeners each sit behind a [`Relay`]: every
/// broker advertises its relay, and binds its real listener behind it. A cut
/// relay takes that broker off the data plane as its peers see it, while it
/// runs and while its controller plane, which no relay fronts, keeps its
/// session and its leaderships.
pub struct RelayedCluster {
    brokers: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    relays: Vec<Relay>,
    _dirs: Vec<TempDir>,
}

impl RelayedCluster {
    /// Start the cluster, `configure` adjusting each broker's config, and
    /// retry a start that fails, up to three times.
    ///
    /// # Panics
    ///
    /// Panics if the third start fails too.
    pub async fn start(configure: impl Fn(&mut BrokerConfig)) -> Self {
        let mut last_error = None;
        for attempt in 1..=3 {
            match Self::try_start(&configure).await {
                Ok(cluster) => return cluster,
                Err(error) => {
                    tracing::warn!(attempt, %error, "relayed cluster start failed");
                    last_error = Some(error);
                    // intentional: a fresh start needs the failed one's ports
                    // released, which has no awaiter.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        panic!("relayed cluster start failed after 3 attempts: {last_error:?}");
    }

    async fn try_start(configure: &impl Fn(&mut BrokerConfig)) -> Result<Self, BrokerError> {
        init_tracing();
        let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
            bind_and_hold_ports(3).await;
        let mut relays = Vec::with_capacity(3);
        for address in &client_addrs {
            relays.push(Relay::start(*address).await);
        }
        let voters: Vec<_> = (0..3)
            .map(|index| (u64::try_from(index + 1).unwrap(), controller_addrs[index]))
            .collect();
        let topology = RoleTopology::new(&client_addrs, &controller_addrs, &voters);

        let mut starts = Vec::with_capacity(3);
        let mut metas = Vec::with_capacity(3);
        for (index, (data, controller)) in listener_pairs(client_listeners, controller_listeners) {
            let dir = TempDir::new().expect("tempdir");
            let node = topology.node_setup(ClusterBootstrapSetup {
                index: NodeIndex(index),
                ..Default::default()
            });
            let mut config = broker_config()
                .log_dir(dir.path())
                .index(index)
                .client_addrs(node.client_addrs)
                .controller_addrs(node.controller_addrs)
                .voters(node.voters)
                .mode(node.mode)
                .call()
                .with_internal_topics_for(3);
            config.advertised_listener = relays[index].addr().to_string();
            config.directory_id = uuid::Uuid::from_u128(u128::from(config.node_id.0));
            config.auto_join = false;
            config.bootstrap_servers = vec![];
            configure(&mut config);
            let spawned = config.clone();
            starts.push(tokio::spawn(async move {
                Broker::start_with_listeners(spawned, Some(controller), Some(data)).await
            }));
            metas.push((config, dir));
        }
        let mut brokers = Vec::with_capacity(3);
        let mut configs = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for (start, (config, dir)) in starts.into_iter().zip(metas) {
            let handle = await_broker_start(start).await?;
            brokers.push(Some(handle));
            configs.push(config);
            dirs.push(dir);
        }
        for handle in brokers.iter().flatten() {
            handle.wait_until_brokers_registered(3).await;
        }
        Ok(Self {
            brokers,
            configs,
            relays,
            _dirs: dirs,
        })
    }

    fn index_of(&self, node: u64) -> usize {
        self.configs
            .iter()
            .position(|config| config.node_id == NodeId(node))
            .expect("a cluster member")
    }

    /// The running broker `node`.
    ///
    /// # Panics
    ///
    /// Panics if `node` is not a member, or does not run.
    pub fn handle(&self, node: u64) -> &BrokerHandle {
        self.brokers[self.index_of(node)]
            .as_ref()
            .expect("the broker runs")
    }

    /// The real listen address of `node`, past its relay.
    pub fn address(&self, node: u64) -> SocketAddr { self.configs[self.index_of(node)].listen_addr }

    /// The relay in front of the data listener of `node`.
    pub fn relay(&self, node: u64) -> &Relay { &self.relays[self.index_of(node)] }

    /// Take the running broker `node` out of the cluster, for the caller to
    /// stop.
    ///
    /// # Panics
    ///
    /// Panics if `node` is not a member, or does not run.
    pub fn take(&mut self, node: u64) -> BrokerHandle {
        let index = self.index_of(node);
        self.brokers[index].take().expect("the broker runs")
    }

    /// Start `node` again on the addresses it vacated, from the logs it left.
    ///
    /// # Panics
    ///
    /// Panics if `node` still runs, or does not start.
    pub async fn restart(&mut self, node: u64) {
        let index = self.index_of(node);
        assert!(self.brokers[index].is_none(), "broker {node} still runs");
        let mut config = self.configs[index].clone();
        config.bootstrap_mode = BootstrapMode::Rejoin;
        let handle = start_reusing_addrs(&config, &format!("restart broker {node}")).await;
        handle.wait_until_brokers_registered(3).await;
        self.brokers[index] = Some(handle);
    }

    /// Stop every running broker, and then every relay.
    pub async fn shutdown(mut self) {
        for handle in self.brokers.drain(..).flatten() {
            handle.shutdown().await;
        }
        for relay in self.relays.drain(..) {
            relay.shutdown().await;
        }
    }
}
