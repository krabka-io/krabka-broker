//! `InterBrokerClient` wired into the replicator and the heartbeat.
//!
//! A two-broker cluster whose inter-broker listener is `SASL_PLAINTEXT`
//! authenticates its outbound fetch and heartbeat traffic and replicates
//! records end-to-end.
//!
//! Gated to non-Windows (openraft `debug_assert!` race on the hosted Windows
//! runner -- the same gate as `tests/replication.rs`).

use {
    crate::{
        harness::admin_plain_password,
        support::{
            self, ClusterNodeSetup, NodeIndex, broker_config,
            client::connect_client,
            controller_voters, init_tracing_with, listeners,
            produce::{batch_request, replicated_topic_produce_for},
            records::{batch_from_records, value_record},
            shutdown_cluster,
        },
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::InterBrokerCredentials},
    krabka_protocol::{
        owned::{create_topics_request::CreateTopicsRequest, produce_response::ProduceResponse},
        primitives::uuid::Uuid,
        records::RecordBatch,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    rustls::crypto::aws_lc_rs,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::Path,
    },
    tempfile::TempDir,
    tokio::net::TcpListener,
};

/// Ephemeral loopback ports and the open listeners that hold them.
struct ReservedListeners {
    addrs: Vec<SocketAddr>,
    listeners: Vec<TcpListener>,
}

/// Reserve `n` ephemeral loopback ports and keep their listeners open.
async fn reserve_listeners(n: usize) -> ReservedListeners {
    let mut addrs = Vec::with_capacity(n);
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap());
        listeners.push(l);
    }
    ReservedListeners { addrs, listeners }
}

/// Build a SASL-enabled broker config with two listeners.
///
/// The first listener is a PLAINTEXT data-plane listener on
/// `listen_addr`. The test clients use it, because they do not speak SASL
/// yet. The second listener is a `SASL_PLAINTEXT` inter-broker listener.
/// The replicator and the heartbeat use it against the peer broker.
const DEFAULT_SASL_ENDPOINTS: [SocketAddr; 1] =
    [SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)];

#[derive(krabka_macros::FieldDefaults)]
struct SaslNodeSetup<'a> {
    cluster: ClusterNodeSetup<'a>,
    #[default(&DEFAULT_SASL_ENDPOINTS)]
    sasl_addrs: &'a [SocketAddr],
}

#[bon::builder]
fn sasl_two_listener_config(
    log_dir: &Path,
    #[builder(default)] cluster: ClusterNodeSetup<'_>,
    #[builder(default = & DEFAULT_SASL_ENDPOINTS)] sasl_addrs: &'_ [SocketAddr],
) -> BrokerConfig {
    let setup = SaslNodeSetup {
        cluster,
        sasl_addrs,
    };
    let listen = setup.cluster.client_addrs[setup.cluster.index.0];
    let sasl = setup.sasl_addrs[setup.cluster.index.0];
    let mut cfg = broker_config()
        .log_dir(log_dir)
        .index(setup.cluster.index.0)
        .client_addrs(setup.cluster.client_addrs)
        .controller_addrs(setup.cluster.controller_addrs)
        .voters(setup.cluster.voters)
        .mode(setup.cluster.mode)
        .call();
    cfg.listeners = vec![
        listeners::listener("PLAINTEXT", listen, ListenerProtocol::Plaintext),
        listeners::listener("SASL_PLAINTEXT", sasl, ListenerProtocol::SaslPlaintext),
    ];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_owned();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_owned(), admin_plain_password());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "broker".to_owned(),
        password: admin_plain_password(),
    });
    cfg
}

/// Start a 2-broker cluster whose inter-broker listener is
/// `SASL_PLAINTEXT`.
///
/// This helper is a copy of `support::start_n_node`, but it uses the
/// two-listener config above. It returns `(handle, config, tempdir)`
/// triples in broker id order.
async fn start_two_node_sasl() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let _ = aws_lc_rs::default_provider().install_default();
    init_tracing_with("warn");

    let ReservedListeners {
        addrs: plaintext_addrs,
        listeners: plaintext_listeners,
    } = reserve_listeners(2).await;
    let ReservedListeners {
        addrs: sasl_addrs,
        listeners: sasl_listeners,
    } = reserve_listeners(2).await;
    let ReservedListeners {
        addrs: controller_addrs,
        listeners: controller_listeners,
    } = reserve_listeners(2).await;
    let voters: Vec<(u64, SocketAddr)> = (0..2_u64)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    let dir0 = TempDir::new().unwrap();
    let cfg0 = sasl_two_listener_config()
        .log_dir(dir0.path())
        .cluster(ClusterNodeSetup {
            client_addrs: &plaintext_addrs,
            controller_addrs: &controller_addrs,
            voters: controller_voters(&voters),
            ..Default::default()
        })
        .sasl_addrs(&sasl_addrs)
        .call();
    let dir1 = TempDir::new().unwrap();
    let cfg1 = sasl_two_listener_config()
        .log_dir(dir1.path())
        .cluster(ClusterNodeSetup {
            index: NodeIndex(1),
            client_addrs: &plaintext_addrs,
            controller_addrs: &controller_addrs,
            voters: controller_voters(&voters),
            ..Default::default()
        })
        .sasl_addrs(&sasl_addrs)
        .call();
    // KIP-595 static-quorum bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the SASL controller
    // wire — no add_learner / change_membership (KIP-853 dynamic voter reconfiguration). Start
    // them concurrently: `Broker::start` blocks until a leader is committed,
    // which needs a voter majority up, so a sequential `start().await` on
    // broker0 alone would deadlock.
    let mut listeners = plaintext_listeners
        .into_iter()
        .zip(sasl_listeners)
        .zip(controller_listeners);
    let (data0, controller0) = listeners.next().unwrap();
    let (data1, controller1) = listeners.next().unwrap();
    let cfg0_for_spawn = cfg0.clone();
    let cfg1_for_spawn = cfg1.clone();
    let join0 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg0_for_spawn, Some(controller0), <[_; 2]>::from(data0)).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_for_spawn, Some(controller1), <[_; 2]>::from(data1)).await
    });
    let broker0 = join0.await.expect("join0 spawn").expect("broker0 start");
    let broker1 = join1.await.expect("join1 spawn").expect("broker1 start");
    vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)]
}

/// Start two brokers with a `SASL_PLAINTEXT` inter-broker listener,
/// create a topic with rf=2, produce, and check that the follower
/// converges.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/two_broker_sasl.feature",
    name = "two broker sasl plaintext replication"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_broker_sasl_plaintext_replication(replication: SaslReplication) {
    shutdown_cluster(replication.cluster).await;
}

struct SaslReplication {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    topic_id: Option<Uuid>,
    produced: Option<ProduceResponse>,
}

#[fixture]
fn replication() -> SaslReplication {
    // Start this scenario with fresh inputs and observations.

    SaslReplication {
        cluster: Vec::new(),
        topic_id: None,
        produced: None,
    }
}

#[given("two registered brokers authenticate inter-broker fetch and heartbeat with PLAIN")]
async fn sasl_replication_cluster(replication: &mut SaslReplication) {
    replication.cluster = start_two_node_sasl().await;
    for (broker, _, _) in &replication.cluster {
        broker.wait_until_brokers_registered(2).await;
    }
}

#[given("topic sasl-repl has replicas 1 and 2 with broker 1 as leader")]
async fn replicated_sasl_topic(replication: &mut SaslReplication) {
    let admin = connect_client(replication.cluster[0].1.listen_addr.to_string(), None).await;
    let response = admin
        .send(CreateTopicsRequest {
            topics: vec![support::topic_on("sasl-repl", &[&[1, 2]])],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0);
    replication.topic_id = Some(response.topics[0].topic_id);
    for (broker, _, _) in &replication.cluster {
        broker.wait_until_partition_present("sasl-repl", 0).await;
    }
}

#[when("10 records are produced to broker 1 over the data-plane listener")]
async fn produce_replicated_sasl_records(replication: &mut SaslReplication) {
    let producer = connect_client(replication.cluster[0].1.listen_addr.to_string(), None).await;
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 9,
        ..batch_from_records(
            (0..10)
                .map(|index| value_record(index, Some(bytes::Bytes::from(format!("v{index}")))))
                .collect(),
        )
    };
    replication.produced = Some(
        producer
            .send(batch_request(
                batch,
                replicated_topic_produce_for(
                    "sasl-repl",
                    replication.topic_id.expect("the replicated topic id").0,
                ),
            ))
            .await
            .unwrap(),
    );
}

#[then("the replicated produce is acknowledged without error")]
fn replicated_sasl_produce_accepted(replication: &SaslReplication) {
    let response = replication
        .produced
        .as_ref()
        .expect("records were produced");
    assert!(response.responses[0].partition_responses[0].error_code == 0);
}

#[then("both authenticated replicas reach log end offset 10")]
async fn authenticated_replication_caught_up(replication: &SaslReplication) {
    for (broker, _, _) in &replication.cluster {
        broker
            .wait_until_local_log_end_offset("sasl-repl", 0, 10)
            .await;
    }
}
