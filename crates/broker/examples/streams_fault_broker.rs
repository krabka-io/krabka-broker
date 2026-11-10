//! Isolated Streams native-backend fault harness, never a production server.
//!
//! `cargo run -p krabka-broker --example streams_fault_broker -- DATA FLAGS LISTEN`
//! Add `rejoin` as the final argument to reopen an initialized DATA directory.
//! The controller reserves the port immediately after LISTEN.
//! This starts a real broker with a filesystem backend installed through the native
//! embedding SPI. FLAGS/{set,get,delete} contain `ok`, `transient` or `permanent`.
//! For isolated clusters, set `STREAMS_FIXTURE_NODE` and `STREAMS_FIXTURE_VOTERS`
//! (comma-separated ID@CONTROLLER), and share FLAGS across the nodes.
//! Missing flags mean healthy IO. The harness preserves DATA and FLAGS on exit.
//! See tools/kafka44/README.md for the released-codec fault fixture.
#[path = "fixtures/streams_assignors.rs"]
mod streams_assignors;

use {
    krabka_broker::{
        Broker, BrokerConfig, NodeId,
        api_catalog::UnstableApiVersions,
        coordinator::unified::streams::description::{
            BackendFuture, FileTopologyDescriptionBackend, StorageFailure, TopologyDescription,
            TopologyDescriptionBackend,
        },
    },
    krabka_raft::{BootstrapMode, UnstableFeatureVersions},
    std::{env, error::Error, path::PathBuf, sync::Arc},
    tokio::{fs, signal},
};

#[derive(Debug)]
struct FaultBackend {
    flags: PathBuf,
    storage: FileTopologyDescriptionBackend,
}

impl FaultBackend {
    async fn injected_failure(&self, operation: &str) -> Result<(), StorageFailure> {
        let mode = fs::read_to_string(self.flags.join(operation))
            .await
            .unwrap_or_else(|_| "ok".into());
        eprintln!("FAULT_SPI {operation} {}", mode.trim());
        match mode.trim() {
            "ok" => Ok(()),
            "permanent" => Err(StorageFailure::Permanent),
            _ => Err(StorageFailure::Transient),
        }
    }
}

impl TopologyDescriptionBackend for FaultBackend {
    fn set<'a>(
        &'a self,
        group: &'a str,
        epoch: i32,
        description: &'a TopologyDescription,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.injected_failure("set").await?;
            self.storage.set(group, epoch, description).await
        })
    }

    fn get<'a>(
        &'a self,
        group: &'a str,
        epoch: i32,
    ) -> BackendFuture<'a, Option<TopologyDescription>> {
        Box::pin(async move {
            self.injected_failure("get").await?;
            self.storage.get(group, epoch).await
        })
    }

    fn delete<'a>(&'a self, group: &'a str) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.injected_failure("delete").await?;
            self.storage.delete(group).await
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().collect();
    let (data, flags, listen, rejoin) = match args.as_slice() {
        [_, data, flags, listen] => (data, flags, listen, false),
        [_, data, flags, listen, mode] if mode == "rejoin" => (data, flags, listen, true),
        _ => return Err("expected DATA FLAGS LISTEN [rejoin] arguments".into()),
    };
    let flags = PathBuf::from(flags);
    fs::create_dir_all(&flags).await?;
    let mut config = BrokerConfig::for_tests(PathBuf::from(data));
    config.listen_addr = listen.parse()?;
    config.advertised_listener.clone_from(listen);
    let mut controller_addr = config.listen_addr;
    controller_addr.set_port(
        controller_addr
            .port()
            .checked_add(1)
            .ok_or("LISTEN port has no successor")?,
    );
    config.controller_listen_addr = controller_addr;
    config
        .controller_quorum_voters
        .first_mut()
        .ok_or("test configuration has no controller voter")?
        .1 = controller_addr.to_string();
    if let Ok(node) = env::var("STREAMS_FIXTURE_NODE") {
        let node: u64 = node.parse()?;
        config.node_id = NodeId(node);
        config.broker_id = i32::try_from(node)?;
        config.directory_id = uuid::Uuid::from_u128(u128::from(node));
        config.auto_join = false;
        config.bootstrap_servers.clear();
        config.controller_quorum_voters = env::var("STREAMS_FIXTURE_VOTERS")?
            .split(',')
            .map(|entry| {
                let (id, address) = entry.split_once('@').ok_or("expected ID@ADDRESS voter")?;
                Ok((NodeId(id.parse()?), address.to_owned()))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        let nodes = config.controller_quorum_voters.len();
        config = config.with_internal_topics_for(nodes);
    }
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = UnstableFeatureVersions::Enabled;
    config.audit_enabled = false;
    if rejoin {
        config.bootstrap_mode = BootstrapMode::Rejoin;
    }
    config.streams_group.topology_description_backend = Some(Arc::new(FaultBackend {
        storage: FileTopologyDescriptionBackend::new(flags.join("store")).await?,
        flags,
    }));
    if env::var_os("STREAMS_FIXTURE_ASSIGNORS").is_some() {
        streams_assignors::register(&mut config.streams_group)?;
    }
    // Isolated ACL/callback qualification only: privileged plaintext control
    // listener plus one synthetic SASL/PLAIN actor on LISTEN+2.
    if env::var_os("STREAMS_FIXTURE_ACL").is_some() {
        use {
            krabka_broker::{authorizer::SimpleAclAuthorizer, config::ListenerSpec},
            krabka_security::{ListenerProtocol, SaslMechanism},
        };
        let mut sasl_addr = config.listen_addr;
        sasl_addr.set_port(sasl_addr.port().checked_add(2).ok_or("no SASL port")?);
        config.listeners = vec![
            ListenerSpec {
                name: "PLAINTEXT".into(),
                bind_addr: config.listen_addr,
                advertised: listen.clone(),
                protocol: ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            },
            ListenerSpec {
                name: "SASL_PLAINTEXT".into(),
                bind_addr: sasl_addr,
                advertised: sasl_addr.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            },
        ];
        config.inter_broker_listener_name = "PLAINTEXT".into();
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        config
            .plain_credentials
            .insert("actor".into(), "qualification-fixture-password".into());
        config.super_users.insert("ANONYMOUS".into());
        config.authorizer = Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
    }
    let broker = Broker::start(config).await?;
    println!("READY {}", broker.listen_addr());
    signal::ctrl_c().await?;
    broker.shutdown().await;
    Ok(())
}
