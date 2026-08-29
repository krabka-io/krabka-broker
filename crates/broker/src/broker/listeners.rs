//! Data-plane listener binding and the resumption of interrupted KIP-113
//! log-dir moves. Both run at the point where the broker is ready to accept
//! traffic, and both need the resolved listener set, so they share a module.

use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{Broker, accept::accept_loop},
    config::BrokerConfig,
    error::BrokerError,
    log_dir,
    partition_registry::PartitionRegistry,
};

pub(super) struct ListenerStartup {
    pub(super) bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)>,
    pub(super) listen_addr: SocketAddr,
    pub(super) future_logs:
        Arc<DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>>,
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
            TcpListener::bind(spec.bind_addr).await?
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
