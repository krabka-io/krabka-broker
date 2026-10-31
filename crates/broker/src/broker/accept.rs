//! The per-listener accept loop and the socket tuning, connection-limit, and
//! KIP-612 accept-rate checks it applies to every inbound connection. It is a
//! module of its own because it is the broker's only network ingress point.

use {
    crate::{
        broker::Broker,
        config::ListenerSpec,
        metrics::{ConnectionCloseReason, QuotaType},
        network::{client::tune_socket, dispatch},
        quota::{lookup_ip_quota_with_key, positive_f64_to_u64},
    },
    krabka_raft::{AcceptingListener, ConnectionGuard, ConnectionLimit, ConnectionQuotas},
    krabka_units::{ByteSize, Time, convert::TimeExt as _},
    std::{
        net::{IpAddr, SocketAddr},
        ops::ControlFlow,
        sync::Arc,
        time::Instant,
    },
    tokio::{
        net::{TcpListener, TcpStream},
        task::JoinSet,
        time,
    },
    tokio_util::sync::CancellationToken,
};

/// KIP-612 quota key for accept-rate throttling; matches the
/// `connection_creation_rate` config name Kafka's `AlterClientQuotas` uses
/// for `ip` entities.
const CONNECTION_CREATION_RATE_QUOTA_KEY: &str = "connection_creation_rate";

fn connection_creation_delay(rate: f64, maximum: Time) -> Time {
    let delay_micros = positive_f64_to_u64((1.0_f64 / rate) * 1_000_000.0);
    Time::from_micros(i64::try_from(delay_micros).unwrap_or(i64::MAX)).min(maximum)
}

/// The throttle for a new connection from `peer_ip`, or `None` when the
/// connection may be served.
///
/// A connection is throttled when the peer's `ip` entity has a positive
/// `connection_creation_rate` and its token bucket is empty. The bucket spends
/// no token on a throttled connection, as Kafka's
/// `recordIpConnectionMaybeThrottle` un-records it.
fn connection_creation_throttle(broker: &Broker, peer_ip: IpAddr) -> Option<Time> {
    let image = broker.controller.current_image();
    let (entity_key, rate) = lookup_ip_quota_with_key(
        &image,
        broker.quota_buckets.ip_names(),
        peer_ip,
        CONNECTION_CREATION_RATE_QUOTA_KEY,
    )?;
    if rate <= 0.0 {
        return None;
    }
    let bucket =
        broker
            .quota_buckets
            .get_or_create(CONNECTION_CREATION_RATE_QUOTA_KEY, &entity_key, rate);
    (bucket.try_consume(1) == 0)
        .then(|| connection_creation_delay(rate, broker.config.connection_creation_throttle_max))
}

/// What the accept loop does with a connection it accepted.
enum Admission {
    /// Serve the connection. The guard holds its connection slots and must
    /// live as long as the connection does.
    Serve(ConnectionGuard),
    /// Over the peer's KIP-612 `connection_creation_rate`: hold the socket
    /// for the throttle time, then close it without serving it.
    Throttled(Time),
    /// Over a connection cap: close the socket at once.
    Rejected,
}

/// Decide whether the connection `peer` opened on `accepting` is served.
///
/// KIP-612 `connection_creation_rate` is checked before the connection takes
/// a slot, as Kafka's `ConnectionQuotas.inc` records the ip rate before it
/// counts the connection. A connection over the rate is never served: the
/// broker holds the socket for the throttle time and then closes it, so the
/// client backs off and reconnects later. The caller runs the hold as a task
/// of its own, so the accept loop keeps accepting other connections
/// meanwhile, as Kafka's acceptor does with `throttledSockets`.
///
/// Then come the connection caps: this listener's, the broker's and the peer
/// address's. A slot is reserved before doing any work; on rejection the
/// caller closes the socket at once. Kafka closes a connection over the
/// per-IP cap and waits for a slot under the other two.
fn admit(
    broker: &Broker,
    spec: &ListenerSpec,
    accepting: AcceptingListener<'_>,
    quotas: &ConnectionQuotas,
    peer: SocketAddr,
) -> Admission {
    let peer_ip = peer.ip();
    if let Some(delay) = connection_creation_throttle(broker, peer_ip) {
        // Kafka un-records a throttled connection from the broker-wide and
        // listener rates.
        broker.connections.refund_creation(accepting, quotas);
        broker
            .metrics
            .observe_quota_throttle(QuotaType::ConnectionCreation, delay.secs_f64());
        tracing::debug!(
            %peer,
            name = %spec.name,
            delay_ms = delay.millis_i64(),
            "connection_creation_rate exceeded; closing connection after the throttle"
        );
        return Admission::Throttled(delay);
    }
    match broker.connections.try_acquire_within(peer_ip, accepting, quotas) {
        Ok(guard) => Admission::Serve(guard),
        Err(limit) => {
            broker.metrics.record_connection_close(close_reason(limit));
            tracing::debug!(
                %peer,
                name = %spec.name,
                ?limit,
                "connection limit reached; closing connection"
            );
            Admission::Rejected
        }
    }
}

/// Hold a throttled socket for `delay`, then close it without reading a
/// request. Kafka's acceptor keeps the socket in `throttledSockets` until
/// the throttle ends, so the client does not reconnect at once.
async fn close_after(stream: TcpStream, delay: Time) {
    time::sleep(delay.to_std()).await;
    drop(stream);
}

/// The close reason a connection rejected at `limit` is counted under: the
/// per-IP cap has its own, the broker-wide and listener caps share one.
fn close_reason(limit: ConnectionLimit) -> ConnectionCloseReason {
    match limit {
        ConnectionLimit::Global | ConnectionLimit::Listener => ConnectionCloseReason::MaxConnections,
        ConnectionLimit::PerIp => ConnectionCloseReason::MaxConnectionsPerIp,
    }
}

async fn shutdown_connection_tasks(connections: &mut JoinSet<()>) { connections.shutdown().await; }

/// Admit and serve one accepted connection, or throttle or reject it.
///
/// `Break` means `shutdown` fired while the accept waited out a
/// `max.connection.creation.rate` throttle, so the accept loop must stop.
async fn on_accept(
    broker: &Arc<Broker>,
    spec: &ListenerSpec,
    shutdown: &CancellationToken,
    connections: &mut JoinSet<()>,
    (stream, peer): (TcpStream, SocketAddr),
) -> ControlFlow<()> {
    tracing::debug!(%peer, name = %spec.name, "accepted connection");
    // Kafka's `ConnectionQuotas.protectedListener` is evaluated at each
    // accept, so a listener added or removed while the broker runs protects or
    // unprotects the inter-broker listener from the next connection on.
    let accepting = AcceptingListener {
        name: &spec.name,
        protected: broker
            .data_plane
            .is_protected(&spec.name, &broker.config.inter_broker_listener_name),
    };
    tune_accepted_socket(
        &stream,
        broker.config.socket_send_buffer,
        broker.config.socket_receive_buffer,
    );

    // The connection quotas are resolved per accept, so a dynamic change
    // applies to the next connection without a restart. Over
    // `max.connection.creation.rate`, broker-wide or for this listener, the
    // acceptor waits the throttle out and then takes the connection, as
    // Kafka's `ConnectionQuotas.waitForConnectionSlot` does.
    let quotas = broker
        .connections
        .quotas(&broker.controller.current_image(), broker.config.node_id, &spec.name);
    let delay = broker.connections.creation_delay(
        accepting,
        &quotas,
        Instant::now(),
        broker.config.connection_creation_throttle_max.to_std(),
    );
    if !delay.is_zero() {
        tokio::select! {
            () = time::sleep(delay) => {}
            () = shutdown.cancelled() => return ControlFlow::Break(()),
        }
    }

    match admit(broker, spec, accepting, &quotas, peer) {
        Admission::Serve(conn_guard) => {
            let b = Arc::clone(broker);
            let s = spec.clone();
            connections.spawn(async move {
                // Hold the connection guard for the lifetime of the
                // connection; dropping it releases the global + per-IP slots.
                let _conn_guard = conn_guard;
                // `Box::pin` the per-connection handler: the request dispatch
                // state machine (68 API handlers, each now carrying a tracing
                // span) is a legitimately large future that trips
                // `clippy::large_futures` once held inline in this spawned
                // task. Boxing moves it to the heap (one alloc per long-lived
                // connection — free).
                Box::pin(dispatch::serve_connection_on_listener(b, stream, s)).await;
            });
        }
        Admission::Throttled(delay) => {
            connections.spawn(close_after(stream, delay));
        }
        Admission::Rejected => drop(stream),
    }
    ControlFlow::Continue(())
}

/// Accept connections on `listener` until `shutdown`, then close the
/// listener and every connection it accepted.
pub(super) async fn accept_loop(
    broker: Arc<Broker>,
    listener: TcpListener,
    spec: ListenerSpec,
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
                    Ok(accepted) => {
                        if on_accept(&broker, &spec, &shutdown, &mut connections, accepted)
                            .await
                            .is_break()
                        {
                            break;
                        }
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
fn tune_accepted_socket(stream: &TcpStream, send_buffer: ByteSize, receive_buffer: ByteSize) {
    tune_socket(stream, send_buffer, receive_buffer, [
        |e| tracing::debug!(error = %e, "TCP_NODELAY set failed on accepted socket"),
        |e| tracing::debug!(error = %e, "SO_SNDBUF set failed on accepted socket"),
        |e| tracing::debug!(error = %e, "SO_RCVBUF set failed on accepted socket"),
    ]);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            network::test_support::{check_socket_tuning, tcp_pair},
            test_support::wait_tasks_started,
        },
        assert2::assert,
        krabka_units::{kibibytes, millis},
        std::{
            future,
            sync::atomic::{AtomicUsize, Ordering},
        },
    };

    #[tokio::test]
    async fn accepted_socket_tuning_sets_nodelay_and_large_buffers() {
        let (server, client) = tcp_pair().await;
        check_socket_tuning(&server, |socket| {
            tune_accepted_socket(socket, kibibytes(64), kibibytes(128));
        });
        drop(client);
    }

    #[test]
    fn connection_creation_delay_honors_nondefault_cap() {
        assert!(connection_creation_delay(0.1, millis(17)) == millis(17));
    }

    #[test]
    fn close_reason_separates_the_per_ip_cap_from_the_others() {
        let cases = [
            (ConnectionLimit::Global, ConnectionCloseReason::MaxConnections),
            (ConnectionLimit::Listener, ConnectionCloseReason::MaxConnections),
            (ConnectionLimit::PerIp, ConnectionCloseReason::MaxConnectionsPerIp),
        ];
        for (limit, expected) in cases {
            assert!(close_reason(limit) == expected, "{limit:?}");
        }
    }

    #[tokio::test]
    async fn shutdown_connection_tasks_aborts_and_awaits_every_task() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
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
                future::pending::<()>().await;
            });
        }
        wait_tasks_started(
            || started.load(Ordering::SeqCst) == 2,
            "connection tasks start",
        )
        .await;

        shutdown_connection_tasks(&mut connections).await;

        assert!(connections.is_empty());
        assert!(dropped.load(Ordering::SeqCst) == 2);
    }
}
