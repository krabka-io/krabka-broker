//! `InterBrokerClient` wired into the replicator and the heartbeat.
//!
//! A two-broker cluster whose inter-broker listener is `SASL_PLAINTEXT`
//! authenticates its outbound fetch and heartbeat traffic and replicates
//! records end-to-end.
//!
//! Gated to non-Windows (openraft `debug_assert!` race on the hosted Windows
//! runner -- the same gate as `tests/replication.rs`).

use std::net::SocketAddr;

use assert2::assert;
use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle,
    config::{InterBrokerCredentials, ListenerSpec},
};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::harness::admin_plain_password;

/// Reserve `n` ephemeral loopback ports and keep their listeners open.
async fn reserve_listeners(n: usize) -> (Vec<SocketAddr>, Vec<tokio::net::TcpListener>) {
    let mut addrs = Vec::with_capacity(n);
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap());
        listeners.push(l);
    }
    (addrs, listeners)
}

/// Build a SASL-enabled broker config with two listeners.
///
/// The first listener is a PLAINTEXT data-plane listener on
/// `listen_addr`. The test clients use it, because they do not speak SASL
/// yet. The second listener is a `SASL_PLAINTEXT` inter-broker listener.
/// The replicator and the heartbeat use it against the peer broker.
fn sasl_two_listener_config(
    i: usize,
    plaintext_addrs: &[SocketAddr],
    sasl_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
) -> BrokerConfig {
    let listen = plaintext_addrs[i];
    let sasl = sasl_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = krabka_broker::NodeId(u64::try_from(i + 1).unwrap());
    cfg.controller_listen_addr = controller_addrs[i];
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg.bootstrap_mode = mode;
    cfg.listeners = vec![
        ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: listen,
            advertised: listen.to_string(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
        ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: sasl,
            advertised: sasl.to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
    ];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), admin_plain_password());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "broker".to_string(),
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
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let (plaintext_addrs, plaintext_listeners) = reserve_listeners(2).await;
    let (sasl_addrs, sasl_listeners) = reserve_listeners(2).await;
    let (controller_addrs, controller_listeners) = reserve_listeners(2).await;
    let voters: Vec<(u64, SocketAddr)> = (0..2_u64)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    let dir0 = TempDir::new().unwrap();
    let cfg0 = sasl_two_listener_config(
        0,
        &plaintext_addrs,
        &sasl_addrs,
        &controller_addrs,
        &voters,
        dir0.path(),
        BootstrapMode::Bootstrap,
    );
    let dir1 = TempDir::new().unwrap();
    let cfg1 = sasl_two_listener_config(
        1,
        &plaintext_addrs,
        &sasl_addrs,
        &controller_addrs,
        &voters,
        dir1.path(),
        BootstrapMode::Bootstrap,
    );
    // KIP-595 Slice 3c static bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the SASL controller
    // wire — no add_learner / change_membership (KIP-853, Slice 5). Start
    // them concurrently: `Broker::start` blocks until a leader is committed,
    // which needs a voter majority up, so a sequential `start().await` on
    // broker0 alone would deadlock.
    let mut listeners = plaintext_listeners
        .into_iter()
        .zip(sasl_listeners)
        .zip(controller_listeners);
    let ((plaintext0, sasl0), controller0) = listeners.next().unwrap();
    let ((plaintext1, sasl1), controller1) = listeners.next().unwrap();
    let cfg0_for_spawn = cfg0.clone();
    let cfg1_for_spawn = cfg1.clone();
    let join0 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg0_for_spawn, Some(controller0), [plaintext0, sasl0]).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_for_spawn, Some(controller1), [plaintext1, sasl1]).await
    });
    let broker0 = join0.await.expect("join0 spawn").expect("broker0 start");
    let broker1 = join1.await.expect("join1 spawn").expect("broker1 start");
    vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)]
}

/// Start two brokers with a `SASL_PLAINTEXT` inter-broker listener,
/// create a topic with rf=2, produce, and check that the follower
/// converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_broker_sasl_plaintext_replication() {
    let cluster = start_two_node_sasl().await;

    // Wait for both brokers to register in each other's image.
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(2).await;
    }

    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "sasl-repl".into(),
                num_partitions: 1,
                replication_factor: 2,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);
    let topic_id = resp.topics[0].topic_id;

    // Wait for the topic to propagate to every broker's image.
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("sasl-repl", 0).await;
    }

    // Produce 10 records to the leader.
    let producer = Client::builder()
        .bootstrap(leader_addr)
        .build()
        .await
        .unwrap();
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 9,
        records: (0..10)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let prod = producer
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "sasl-repl".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(prod.responses[0].partition_responses[0].error_code == 0);

    // Wait until every broker's local log reaches >= 10. The SASL
    // inter-broker handshake on each follower-fetch round trip is the
    // critical path here — a misconfigured replicator would never
    // commit a record and this awaiter would time out.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("sasl-repl", 0, 10).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
