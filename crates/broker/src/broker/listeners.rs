//! Data-plane listener binding and the resumption of interrupted KIP-113
//! log-dir moves. Both run at the point where the broker is ready to accept
//! traffic, and both need the resolved listener set, so they share a module.

use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use tokio::{
    net::{TcpListener, TcpSocket},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{Broker, accept::accept_loop},
    config::BrokerConfig,
    error::BrokerError,
    log_dir,
    partition_registry::PartitionRegistry,
};

/// Binds a listening socket with `SO_REUSEADDR`, matching Kafka's own
/// `SocketServer` (`socket.setReuseAddress(true)` before `bind`). Without it,
/// rebinding the same port right after a previous listener on it closes can
/// fail with `EADDRINUSE` while the OS still holds sockets that used that
/// port in `TIME_WAIT` -- exactly the case a broker restart on its
/// previously-bound port hits.
pub(super) fn bind_reuseaddr(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(1024)
}

pub(super) struct ListenerStartup {
    pub(super) bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>,
    pub(super) listen_addr: SocketAddr,
    pub(super) future_logs:
        Arc<DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>>,
}

/// Binds the default data-plane listener before the broker registers itself,
/// when the config asks for an OS-assigned port and the caller supplied no
/// data-plane listener of its own.
///
/// `register_broker` reads `advertised_listener` to build the
/// `BrokerRegistrationRecord` this node publishes to the controller, and it
/// runs inside `start_metadata_phase`, before [`bind_listeners_and_recover_moves`]
/// binds the data plane. A `:0` config would therefore publish port 0 to every
/// other broker's `Metadata` and `ListGroups` routing. This binds the listener
/// now and writes the port it got into `listen_addr` and `advertised_listener`
/// first, exactly as `bind_ephemeral_controller_listener` does for the
/// controller listener. The bound socket is kept in `data_plane_listeners`, so
/// `bind_listeners_and_recover_moves` adopts it later instead of binding
/// again, and no other process can take the port in between.
///
/// A caller-supplied data-plane listener, a concrete port, and a
/// `config.listeners` (KIP-113 multi-listener) setup all keep their config as
/// it is.
pub(super) fn bind_ephemeral_data_plane_listener(
    config: &mut BrokerConfig,
    data_plane_listeners: &mut Vec<TcpListener>,
) -> Result<(), BrokerError> {
    if !data_plane_listeners.is_empty()
        || !config.listeners.is_empty()
        || config.listen_addr.port() != 0
    {
        return Ok(());
    }
    let listener = bind_reuseaddr(config.listen_addr)?;
    let bound = listener.local_addr()?;
    config.listen_addr = bound;
    if let Some((host, _)) = config.advertised_listener.rsplit_once(':') {
        config.advertised_listener = format!("{host}:{}", bound.port());
    }
    data_plane_listeners.push(listener);
    Ok(())
}

pub(super) async fn bind_listeners_and_recover_moves(
    config: &mut BrokerConfig,
    mut supplied_listeners: Vec<TcpListener>,
    partitions: &Arc<PartitionRegistry>,
    throttle_state: &Arc<crate::throttle::ThrottleState>,
) -> Result<ListenerStartup, BrokerError> {
    let listener_specs = config.effective_listeners();
    let mut bound = Vec::with_capacity(listener_specs.len());
    for spec in listener_specs {
        let listener = if let Some(index) = supplied_listeners.iter().position(|listener| {
            listener
                .local_addr()
                .is_ok_and(|addr| addr == spec.bind_addr)
        }) {
            supplied_listeners.swap_remove(index)
        } else {
            bind_reuseaddr(spec.bind_addr)?
        };
        let address = listener.local_addr()?;
        bound.push((spec, listener, address));
    }
    let listen_addr = bound
        .iter()
        .find(|(spec, _, _)| spec.name == config.inter_broker_listener_name)
        .map_or(bound[0].2, |(_, _, address)| *address);
    if config.advertised_listener.ends_with(":0")
        && let Some((host, _)) = config.advertised_listener.rsplit_once(':')
    {
        config.advertised_listener = format!("{host}:{}", listen_addr.port());
    }
    let future_logs = Arc::new(DashMap::new());
    for log_dir in config.all_log_dirs() {
        for (topic, partition_id) in log_dir::scan_future(&log_dir).unwrap_or_default() {
            let partition = PartitionIndex(partition_id);
            if !partitions.contains(&topic, partition) {
                let path = log_dir::future_partition_dir(&log_dir, &topic, partition_id);
                if let Err(error) = std::fs::remove_dir_all(&path) {
                    tracing::warn!(path = %path.display(), %error, "failed to remove stranded future log");
                }
                continue;
            }
            if let Err(error) = crate::future_log::resume_move(
                partitions,
                &future_logs,
                &log_dir,
                &config.log_config,
                &topic,
                partition,
                crate::future_log::MovePolicy {
                    retry_backoff: config.future_log_move_retry_backoff,
                    read_chunk: config.future_log_move_read_chunk,
                    throttle: throttle_state.alter_log_dirs.clone(),
                },
            ) {
                tracing::warn!(%topic, partition = partition_id, ?error,
                    "failed to resume interrupted log-dir move");
            }
        }
    }
    Ok(ListenerStartup {
        bound,
        listen_addr,
        future_logs,
    })
}

pub(super) fn spawn_listener_tasks(
    broker: &Arc<Broker>,
    bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>,
) -> (CancellationToken, Vec<JoinHandle<()>>) {
    let shutdown = CancellationToken::new();
    let tasks = bound
        .into_iter()
        .map(|(spec, listener, _)| {
            tokio::spawn(accept_loop(
                Arc::clone(broker),
                listener,
                spec,
                shutdown.clone(),
            ))
        })
        .collect();
    (shutdown, tasks)
}
