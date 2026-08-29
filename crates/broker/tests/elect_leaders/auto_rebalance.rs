//! The background leader-rebalance ticker: with
//! `auto_leader_rebalance_enable` on, the broker submits the preferred
//! election itself and no operator request is needed.
//!
//! The cluster is brought up by hand rather than through
//! `support::start_n_node`, because the rebalance settings have to be applied
//! to each `BrokerConfig` before the broker starts.

use krabka_broker::Broker;

use crate::{
    cluster_lock, support,
    wait::{wait_isr_contains, wait_partition_exists, wait_partition_leader},
    wire::create_topic_plaintext,
};

/// A 3-broker PLAINTEXT cluster with automatic leader rebalance on.
///
/// The cluster sets `auto_leader_rebalance_enable = true`,
/// `leader_imbalance_check_interval = krabka_units::secs(1)`, and `leader_imbalance_per_broker = krabka_units::percent(0)`.
///
/// Scenario:
/// 1. Create an rf=2 topic over the wire. With 3 registered brokers,
///    round-robin assigns `replicas = [1, 2]`, so broker 1 is the preferred
///    leader.
/// 2. Shut broker 1 down. Broker 2 becomes partition leader.
/// 3. Revive broker 1 with Rejoin. It catches up into the ISR.
/// 4. The background rebalance ticker runs with interval=1s and threshold=0%.
///    It fires within about 2 ticks and submits `ElectLeaders Preferred`
///    internally.
/// 5. Within 15s, broker 1 must be the partition leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_rebalance_restores_preferred_leader() {
    let _g = cluster_lock().lock().await;

    support::init_tracing();

    // ── Phase 1: start a 3-broker cluster with rebalance enabled. ─────────
    // We can't pass rebalance config overrides through `start_n_node`, so we
    // replicate its static multi-voter bring-up here and apply the rebalance
    // fields after building each BrokerConfig. All three brokers boot in
    // `Bootstrap` mode with the same static voter set (KIP-595 Slice 3c);
    // KIP-853 auto-join is Slice 5.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(3).await;
    let voters: Vec<(u64, std::net::SocketAddr)> = (0u64..3)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    let dir0 = tempfile::TempDir::new().unwrap();
    let dir1 = tempfile::TempDir::new().unwrap();
    let dir2 = tempfile::TempDir::new().unwrap();

    let mut cfg0 = support::broker_config(
        0,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir0.path(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    cfg0.features.auto_leader_rebalance_enable = true;
    cfg0.leader_imbalance_check_interval = krabka_units::secs(1);
    cfg0.leader_imbalance_per_broker = krabka_units::percent(0);

    let mut cfg1 = support::broker_config(
        1,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir1.path(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    cfg1.features.auto_leader_rebalance_enable = true;
    cfg1.leader_imbalance_check_interval = krabka_units::secs(1);
    cfg1.leader_imbalance_per_broker = krabka_units::percent(0);

    let mut cfg2 = support::broker_config(
        2,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir2.path(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    cfg2.features.auto_leader_rebalance_enable = true;
    cfg2.leader_imbalance_check_interval = krabka_units::secs(1);
    cfg2.leader_imbalance_per_broker = krabka_units::percent(0);

    // Start all three statically; they elect among themselves over the wire.
    let mut client_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (client0, controller0) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client1, controller1) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client2, controller2) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let cfg1_clone = cfg1.clone();
    let cfg2_clone = cfg2.clone();
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_clone, Some(controller1), Some(client1)).await
    });
    let join2 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg2_clone, Some(controller2), Some(client2)).await
    });
    let h0 = Broker::start_with_listeners(cfg0.clone(), Some(controller0), Some(client0))
        .await
        .expect("broker 1 start");
    let h1 = join1.await.expect("spawn join1").expect("broker 2 start");
    let h2 = join2.await.expect("spawn join2").expect("broker 3 start");

    // Wait for all 3 brokers to see each other registered.
    h0.wait_until_brokers_registered(3).await;
    h1.wait_until_brokers_registered(3).await;
    h2.wait_until_brokers_registered(3).await;

    let addr = h0.listen_addr();
    let topic = "foo-rebalance";

    // ── Phase 2: create rf=2 topic via PLAINTEXT wire. ────────────────────
    // With 3 registered brokers sorted [1, 2, 3] and rf=2, the round-robin
    // assignment for partition 0 is replicas=[1, 2]. Broker 1 is preferred.
    create_topic_plaintext(addr, topic, 1, 2).await;

    wait_partition_exists(&h0, topic, 0).await;
    wait_partition_exists(&h1, topic, 0).await;
    // Wait for broker 1 to be the initial leader (as preferred replica).
    wait_partition_leader(&h0, topic, 0, 1).await;
    eprintln!("initial partition leader is broker 1 (preferred)");

    // ── Phase 3: kill broker 1 (preferred leader). ────────────────────────
    h0.shutdown().await;
    eprintln!("broker 1 shut down; waiting for failover");

    // Wait for broker 2 or 3 to report a new leader (not broker 1).
    h1.wait_until_partition_leader_changed(topic, 0, krabka_broker::NodeId(1))
        .await;
    eprintln!(
        "new leader after broker 1 death: {:?}",
        h1.partition_leader_for_test(topic, 0)
    );

    // ── Phase 4: revive broker 1 (Rejoin). ───────────────────────────────
    let mut rejoin_cfg = cfg0.clone();
    rejoin_cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let h0_new = Broker::start(rejoin_cfg).await.expect("rejoin broker 1");
    eprintln!("broker 1 rejoined; waiting for ISR expansion");

    // Wait for broker 1 to be back in the ISR (visible from broker 2's image).
    wait_isr_contains(&h1, topic, 0, 1).await;
    eprintln!("broker 1 back in ISR; waiting for auto-rebalance tick to fire");

    // ── Phase 5: wait for auto-rebalance to restore broker 1 as leader. ──
    // The ticker fires every 1s with threshold=0%; observe the committed
    // metadata image on a surviving broker reflect broker 1 as leader again.
    wait_partition_leader(&h1, topic, 0, 1).await;
    eprintln!("auto-rebalance restored preferred leader (broker 1)");

    // Clean up.
    h0_new.shutdown().await;
    h1.shutdown().await;
    h2.shutdown().await;
    drop(dir0);
    drop(dir1);
    drop(dir2);
}
