//! Static-voter bootstrap of an `n`-broker cluster.
//!
//! `start_n_node_with` is the one helper that boots a whole cluster: it
//! reserves and holds a client and a controller port per broker, builds each
//! node's `BrokerConfig` for the shared static voter set, starts every broker
//! concurrently, and waits for a leader. It is long enough, and self-contained
//! enough, to sit in its own file, with the per-broker config builder it is the
//! only caller of.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use tempfile::TempDir;

/// Build a `BrokerConfig` for broker `i` (0-indexed) in a static `n`-voter
/// cluster. Every broker boots in `Bootstrap` mode with the *same* configured
/// `controller_quorum_voters` set, so each node seeds the full voter set and
/// elects among the configured peers over the real KIP-595 wire. There is no
/// auto-join, because KIP-853 dynamic reconfig is Slice 5.
fn static_voter_broker_config(
    i: usize,
    own_client_addr: SocketAddr,
    own_controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = NodeId(u64::try_from(i + 1).unwrap());
    // Bind a concrete (pre-bound) client port. The broker self-registers its
    // `advertised_listener` host:port into the controller image *before* it
    // binds its listeners and rewrites a `:0` advertised port to the real one
    // — so a `:0` here would register port 0 and break the inter-broker
    // heartbeat / replication dial. Give it a real port up front.
    cfg.listen_addr = own_client_addr;
    cfg.advertised_listener = own_client_addr.to_string();
    // The controller listener must bind the *same* concrete port that this
    // node advertises in the shared voter set, or its peers can't dial it.
    cfg.controller_listen_addr = own_controller_addr;
    cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id.0));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (NodeId(*id), a.to_string()))
        .collect();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg
}

/// Like [`start_n_node`], but it invokes `customize(i, &mut cfg)` on each
/// broker's `BrokerConfig` before start. A test can then add per-broker
/// overrides such as `rack` or `replica_selector` and still keep the race-free
/// held-listener bootstrap. There is no `bind_and_drop_ports` TOCTOU window in
/// which a concurrently running test can steal a just-released port
/// (`AddrInUse`).
pub async fn start_n_node_with(
    n: u64,
    mut customize: impl FnMut(usize, &mut BrokerConfig),
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    super::init_tracing();

    let n_usize = usize::try_from(n).unwrap();

    // Reserve concrete client + controller ports for every broker by binding
    // ephemeral loopback listeners and *holding them live* until each broker
    // adopts its pair via `Broker::start_with_listeners`. The ports must be
    // concrete up front: each controller addr goes into the shared static
    // voter set so peers can dial it, and each broker self-registers its
    // advertised client `host:port` into the controller image *before* it
    // binds its data-plane listener — a `:0` there would register port 0 and
    // break the inter-broker heartbeat / replication dial.
    //
    // Unlike the bind-and-drop trick (`bind_and_drop_ports`), these sockets are
    // never dropped before the broker re-binds them, so there is no TOCTOU
    // window for a concurrently-running test to steal a just-released port —
    // the `AddrInUse` flake under parallel `cargo test` / `cargo llvm-cov`.
    let mut client_listeners = Vec::with_capacity(n_usize);
    let mut controller_listeners = Vec::with_capacity(n_usize);
    for _ in 0..n_usize {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let client_addrs: Vec<SocketAddr> = client_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;
    let controller_addrs: Vec<SocketAddr> = controller_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;

    // The shared static voter set every node is configured with.
    let voters: Vec<(u64, SocketAddr)> = (0..n_usize)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Start all n brokers in Bootstrap mode with the same voter set,
    // *concurrently*. `Broker::start*` blocks until the cold-boot controller
    // sees a committed leader (step 2: it waits on `watch_leader` before
    // submitting its self-registration), and a leader can only be elected once
    // a majority of the static voter set is up and dialable. So a sequential
    // `start().await` on the first broker would deadlock — it can never elect
    // alone. Spawn every broker's `start` and join them.
    let mut starts = Vec::with_capacity(n_usize);
    let mut metas: Vec<(BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (i, (data_listener, controller_listener)) in client_listeners
        .into_iter()
        .zip(controller_listeners)
        .enumerate()
    {
        let dir = TempDir::new().unwrap();
        let mut cfg = static_voter_broker_config(
            i,
            client_addrs[i],
            controller_addrs[i],
            &voters,
            dir.path(),
        );
        customize(i, &mut cfg);
        let cfg_for_spawn = cfg.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(
                cfg_for_spawn,
                Some(controller_listener),
                Some(data_listener),
            )
            .await
        }));
        metas.push((cfg, dir));
    }

    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (handle, (cfg, dir)) in starts.into_iter().zip(metas) {
        let broker = handle
            .await
            .map_err(|e| BrokerError::Startup(format!("broker start task panicked: {e}")))??;
        out.push((broker, cfg, dir));
    }

    // Wait (event-driven, bounded) for the static set to elect a leader. We await
    // the first broker's controller leader watch channel rather than the panicking
    // `wait_until_controller_leader()` helper, because a timeout here must return
    // `Err` so `start_n_node_with_retry` can retry (a panic would not be retried).
    let mut leader_rx = out[0].0.watch_leader_for_test();
    let elected = tokio::time::timeout(
        Duration::from_secs(30),
        leader_rx.wait_for(|l| matches!(l, Some(id) if *id != 0)),
    )
    .await;
    let timed_out = match &elected {
        Err(_elapsed) => true,      // tokio::time::timeout fired
        Ok(Err(_recv_err)) => true, // watch channel closed unexpectedly
        Ok(Ok(_)) => false,
    };
    if timed_out {
        let counts: Vec<usize> = out
            .iter()
            .map(|(h, _, _)| h.voter_count_for_test())
            .collect();
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n_usize} voters within 30s \
             (voter counts={counts:?})"
        )));
    }
    assert!(
        out.iter()
            .any(|(h, _, _)| h.voter_count_for_test() >= n_usize),
        "leader elected but voter set not committed to {n_usize}"
    );

    Ok(out)
}
