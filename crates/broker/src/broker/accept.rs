//! The per-listener accept loop and the socket tuning, connection-limit, and
//! KIP-612 accept-rate checks it applies to every inbound connection. It is a
//! module of its own because it is the broker's only network ingress point.

use std::sync::Arc;

use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::broker::Broker;

/// KIP-612 quota key for accept-rate throttling; matches the
/// `connection_creation_rate` config name Kafka's `AlterClientQuotas` uses
/// for `ip` entities.
const CONNECTION_CREATION_RATE_QUOTA_KEY: &str = "connection_creation_rate";

fn connection_creation_delay(rate: f64, maximum: Time) -> Time {
    let delay_micros = crate::quota::positive_f64_to_u64((1.0_f64 / rate) * 1_000_000.0);
    Time::from_micros(i64::try_from(delay_micros).unwrap_or(i64::MAX)).min(maximum)
}

async fn shutdown_connection_tasks(connections: &mut JoinSet<()>) {
    connections.shutdown().await;
}

pub(super) async fn accept_loop(
    broker: Arc<Broker>,
    listener: TcpListener,
    spec: crate::config::ListenerSpec,
    shutdown: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!(name = %spec.name, "listener shutting down");
                break;
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result
                    && error.is_panic()
                {
                    tracing::warn!(?error, name = %spec.name, "connection task panicked");
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, name = %spec.name, "accepted connection");
                        let peer_ip = peer.ip();
                        tune_accepted_socket(
                            &stream,
                            broker.config.socket_send_buffer,
                            broker.config.socket_receive_buffer,
                        );

                        // `max.connections` / `max.connections.per.ip` caps.
                        // Reserve a slot before doing any work; on rejection
                        // close the socket immediately (Kafka silently drops
                        // connections past either ceiling). The returned guard
                        // is moved into the spawned task so both counters are
                        // released however the connection ends.
                        let conn_guard = match broker.connections.try_acquire(peer_ip) {
                            Ok(guard) => guard,
                            Err(limit) => {
                                let reason = match limit {
                                    crate::broker::connection_limiter::ConnectionLimit::Global => {
                                        crate::metrics::ConnectionCloseReason::MaxConnections
                                    }
                                    crate::broker::connection_limiter::ConnectionLimit::PerIp => {
                                        crate::metrics::ConnectionCloseReason::MaxConnectionsPerIp
                                    }
                                };
                                broker.metrics.record_connection_close(reason);
                                tracing::debug!(
                                    %peer,
                                    name = %spec.name,
                                    ?limit,
                                    "connection limit reached; closing connection"
                                );
                                drop(stream);
                                continue;
                            }
                        };

                        // KIP-612 connection_creation_rate enforcement. Applies
                        // to both IPv4 and IPv6 peers — the quota is keyed by the
                        // peer IP's string form for either family.
                        let image = broker.controller.current_image();
                        if let Some((entity_key, rate)) =
                            crate::quota::lookup_ip_quota_with_key(
                                &image,
                                peer_ip,
                                CONNECTION_CREATION_RATE_QUOTA_KEY,
                            )
                            && rate > 0.0
                        {
                            let initial_rate = crate::quota::positive_f64_to_u64(rate).max(1);
                            let bucket = broker.quota_buckets.get_or_create(
                                CONNECTION_CREATION_RATE_QUOTA_KEY,
                                &entity_key,
                                "",
                                "",
                                initial_rate,
                            );
                            if bucket.try_consume(1) == 0 {
                                let delay = connection_creation_delay(
                                    rate,
                                    broker.config.connection_creation_throttle_max,
                                );
                                broker.metrics.observe_quota_throttle(
                                    crate::metrics::QuotaType::ConnectionCreation,
                                    delay.secs_f64(),
                                );
                                tokio::time::sleep(delay.to_std()).await;
                            }
                        }
                        let b = broker.clone();
                        let s = spec.clone();
                        connections.spawn(async move {
                            // Hold the connection guard for the lifetime of the
                            // connection; dropping it releases the global +
                            // per-IP slots.
                            let _conn_guard = conn_guard;
                            // `Box::pin` the per-connection handler: the request
                            // dispatch state machine (68 API handlers, each now
                            // carrying a tracing span) is a legitimately large
                            // future that trips `clippy::large_futures` once held
                            // inline in this spawned task. Boxing moves it to the
                            // heap (one alloc per long-lived connection — free).
                            Box::pin(crate::network::dispatch::serve_connection_on_listener(
                                b, stream, s,
                            ))
                            .await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, name = %spec.name, "accept failed");
                    }
                }
            }
        }
    }
    shutdown_connection_tasks(&mut connections).await;
}

/// Tune an accepted broker connection before serving it.
///
/// - `TCP_NODELAY`: disable Nagle so that delayed ACKs do not stall the
///   request/response ping-pong by up to ~40 ms. Apache Kafka sets this on its
///   broker sockets. Without it, small-request latency and the header+records
///   write coalescing (once fetch uses `sendfile`) both get worse.
/// - `SO_SNDBUF`/`SO_RCVBUF`: apply the configured, independently tunable
///   buffers so large fetches and produces retain enough in-flight headroom.
///
/// All failures are non-fatal and logged at debug level. A connection that the
/// broker cannot tune still serves correctly, but less efficiently.
fn tune_accepted_socket(
    stream: &tokio::net::TcpStream,
    send_buffer: ByteSize,
    receive_buffer: ByteSize,
) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(error = %e, "TCP_NODELAY set failed on accepted socket");
    }
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_send_buffer_size(send_buffer.bytes_usize()) {
        tracing::debug!(error = %e, "SO_SNDBUF set failed on accepted socket");
    }
    if let Err(e) = sock.set_recv_buffer_size(receive_buffer.bytes_usize()) {
        tracing::debug!(error = %e, "SO_RCVBUF set failed on accepted socket");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;
    use krabka_units::{kibibytes, millis};

    use super::*;

    #[tokio::test]
    async fn accepted_socket_tuning_sets_nodelay_and_large_buffers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let client_task = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (server, _) = listener.accept().await.expect("accept loopback client");
        let client = client_task
            .await
            .expect("connect task")
            .expect("connect loopback client");

        let sock = socket2::SockRef::from(&server);
        server.set_nodelay(false).expect("clear TCP_NODELAY");
        sock.set_send_buffer_size(4096).expect("shrink send buffer");
        sock.set_recv_buffer_size(8192).expect("shrink recv buffer");
        let send_before = sock.send_buffer_size().expect("read baseline send buffer");
        let recv_before = sock.recv_buffer_size().expect("read baseline recv buffer");

        tune_accepted_socket(&server, kibibytes(64), kibibytes(128));

        assert!(server.nodelay().expect("read TCP_NODELAY"));
        // Kernels clamp and may double requested sizes, so compare the distinct
        // configured buffers instead of asserting host-dependent exact values.
        let send_after = sock.send_buffer_size().expect("read send buffer");
        let recv_after = sock.recv_buffer_size().expect("read recv buffer");
        assert!(send_after > send_before);
        assert!(recv_after > recv_before);
        assert!(recv_after > send_after);
        drop(client);
    }

    #[test]
    fn connection_creation_delay_honors_nondefault_cap() {
        assert!(connection_creation_delay(0.1, millis(17)) == millis(17));
    }

    #[tokio::test]
    async fn shutdown_connection_tasks_aborts_and_awaits_every_task() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut connections = JoinSet::new();
        for _ in 0..2 {
            let task_started = Arc::clone(&started);
            let task_dropped = Arc::clone(&dropped);
            connections.spawn(async move {
                let _drop_counter = DropCounter(task_dropped);
                task_started.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            });
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection tasks start");

        shutdown_connection_tasks(&mut connections).await;

        assert!(connections.is_empty());
        assert!(dropped.load(Ordering::SeqCst) == 2);
    }
}
