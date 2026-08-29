//! Boot of the three in-process brokers this suite needs, and the two waits
//! that gate on the cluster reaching a usable state.
//!
//! The scenario needs a 3-voter static quorum with a *shared* `Local` remote
//! tier and a topic-backed `RlmmKind`, all bootstrapping into broker 1's own
//! loopback listener. That boot is long enough, and specific enough, to sit
//! apart from the test that drives it. The waits belong beside it because both
//! observe the same three handles: broker registration on every node, and the
//! `tiered_storage_rlmm_topic_backed` gauge flipping on every node.

use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, RemoteStorageBackend,
    RlmmKind,
};
use tempfile::TempDir;

use crate::support;

/// Boots three in-process brokers with a shared Local remote tier and a
/// topic-backed RLMM.
///
/// The test needs a 3-voter quorum, so that the surviving 2 out of 3 can commit
/// the partition-leader-election record after broker 1 shuts down.
///
/// Returns `(broker1, broker2, broker3, dirs[], shared_remote_dir)`. All
/// brokers share the remote dir, so they write to and read from the same object
/// store.
pub(crate) async fn start_three_tiered_brokers() -> (
    BrokerHandle,
    BrokerHandle,
    BrokerHandle,
    Vec<TempDir>,
    TempDir,
) {
    support::init_tracing();

    // Pre-bind concrete client + controller ports for all 3 brokers.
    // Concrete ports are required: the advertised_listener is registered into
    // the controller image before the listener binds (a `:0` would register
    // port 0 and break inter-broker replication); controller ports go into the
    // static voter set so peers can dial each other.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(3).await;

    let log_dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("log dir")).collect();
    // **Shared** remote dir: all brokers point at the same Local object store.
    let remote_dir = TempDir::new().expect("shared remote dir");

    // 3-voter static voter set.
    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Build a config for broker `i` (1-indexed broker_id/node_id).
    let mut broker_configs: Vec<BrokerConfig> = (0..3)
        .map(|i| {
            let mut cfg = BrokerConfig::for_tests(log_dirs[i].path().to_path_buf());
            cfg.broker_id = i32::try_from(i + 1).unwrap();
            cfg.node_id = krabka_broker::NodeId(u64::try_from(i + 1).unwrap());
            cfg.directory_id = uuid::Uuid::from_u128(u128::try_from(i + 1).unwrap());
            cfg.listen_addr = client_addrs[i];
            cfg.advertised_listener = format!("127.0.0.1:{}", client_addrs[i].port());
            cfg.controller_listen_addr = controller_addrs[i];
            cfg.controller_quorum_voters = voters
                .iter()
                .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
                .collect();
            cfg.bootstrap_mode = BootstrapMode::Bootstrap;
            cfg.auto_join = false;
            cfg.bootstrap_servers = vec![];
            cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
                dir: remote_dir.path().to_path_buf(),
            });
            cfg.remote_log_manager_interval = krabka_units::secs(1);
            // RLMM: all 3 brokers bootstrap into broker 1's loopback.
            // num_partitions=1 keeps all metadata on a single partition.
            // replication=3 prevents the topic from being created before all
            // brokers are registered; sorted placement makes broker 1 leader.
            // Broker 2's RLMM consumer reads CopySegment events from broker 1
            // before broker 1 dies; the cached metadata is then used for remote
            // reads from the survivor.
            cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
                bootstrap: format!("127.0.0.1:{}", client_addrs[0].port()),
                num_partitions: 1,
                replication: 3,
                snapshot_interval: krabka_units::hours(1),
                snapshot_dir: std::path::PathBuf::new(), // derived from log_dir
                security: None,
                ..KafkaRlmmConfig::default()
            });
            cfg
        })
        .collect();

    // Static cold-boot: all 3 start concurrently (sequential would deadlock —
    // a leader needs a majority of the static voter set up).
    let (config0, config1, config2) = (
        broker_configs.remove(0),
        broker_configs.remove(0),
        broker_configs.remove(0),
    );
    let mut client_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (client0, controller0) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client1, controller1) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client2, controller2) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let j0 = tokio::spawn(async move {
        Broker::start_with_listeners(config0, Some(controller0), Some(client0)).await
    });
    let j1 = tokio::spawn(async move {
        Broker::start_with_listeners(config1, Some(controller1), Some(client1)).await
    });
    let j2 = tokio::spawn(async move {
        Broker::start_with_listeners(config2, Some(controller2), Some(client2)).await
    });
    let b1 = j0.await.expect("b1 spawn join").expect("b1 start");
    let b2 = j1.await.expect("b2 spawn join").expect("b2 start");
    let b3 = j2.await.expect("b3 spawn join").expect("b3 start");

    (b1, b2, b3, log_dirs, remote_dir)
}

/// Waits until all three brokers see each other registered, that is, until
/// `broker_count` >= 3.
pub(crate) async fn await_all_brokers_registered(
    b1: &BrokerHandle,
    b2: &BrokerHandle,
    b3: &BrokerHandle,
) {
    // Each broker's own metadata image must show all 3 brokers registered.
    b1.wait_until_brokers_registered(3).await;
    b2.wait_until_brokers_registered(3).await;
    b3.wait_until_brokers_registered(3).await;
}

/// Waits until the topic-backed RLMM is active on all three brokers.
pub(crate) async fn await_all_rlmm_active(b1: &BrokerHandle, b2: &BrokerHandle, b3: &BrokerHandle) {
    // Topic-backed RLMM going live flips the tiered_storage_rlmm_topic_backed
    // gauge to 1 on each broker (the same signal rlmm_topic_backed_active_for_test
    // reads directly).
    b1.wait_for_metrics("b1 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b2.wait_for_metrics("b2 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b3.wait_for_metrics("b3 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
}
