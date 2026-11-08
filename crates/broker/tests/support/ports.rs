//! Reserving ephemeral loopback ports for a multi-broker test cluster.
//!
//! [`hold_port`] holds one port. The other two helpers hand back one client
//! address and one controller address per broker. All of them keep each port
//! held until the caller lets it go, so nothing can take a port between the
//! reservation and the listener that binds it.
//! [`bind_and_hold_ports`] returns listeners for a caller that hands them to
//! `Broker::start_with_listeners`. [`reserve_ports`] returns non-listening
//! reservations for a caller whose listener binds the address itself: a
//! `Broker::start`, or a JVM container published with `docker run -p`.

use {
    super::reserved_port::reserve_port,
    std::net::{Ipv4Addr, SocketAddr},
    tokio::net::{TcpListener, TcpSocket},
};

/// A loopback port held by a [`reserve_port`] reservation until this is
/// dropped.
///
/// For a port that must be known before its listener starts -- one written into
/// a voter set, a container's `-p`, or a config file -- and that is bound later
/// by something with `SO_REUSEADDR`: a Tokio listener, the JDK's, or the proxy
/// that `docker run -p` starts. Keep it for as long as the port may be bound
/// again, such as across a broker restart, since the port is free the moment
/// this is dropped.
pub struct HeldPort {
    /// The socket that holds the port; never read.
    _reservation: TcpSocket,
    port: u16,
}

impl HeldPort {
    /// The held port number.
    #[must_use]
    pub fn port(&self) -> u16 { self.port }
}

/// Hold a fresh ephemeral loopback port. See [`HeldPort`].
#[must_use]
pub fn hold_port() -> HeldPort {
    let (reservation, address) = reserve_port((Ipv4Addr::LOCALHOST, 0));
    HeldPort {
        _reservation: reservation,
        port: address.port(),
    }
}

/// `n` client and `n` controller loopback ports, held by [`reserve_port`]
/// reservations until this is dropped.
///
/// Keep it until every listener that binds these addresses has bound them. A
/// listener with `SO_REUSEADDR` can bind a reserved port, whether on the
/// loopback address or on `0.0.0.0`; a socket without it cannot. Tokio and
/// mio set it on every listener, the JDK sets it on Kafka's, and the proxy
/// that `docker run -p` starts sets it too.
pub struct ReservedPorts {
    /// The client address of each broker, by index.
    pub clients: Vec<SocketAddr>,
    /// The controller address of each broker, index-aligned with `clients`.
    pub controllers: Vec<SocketAddr>,
    /// The sockets that hold the ports; never read.
    _reservations: Vec<TcpSocket>,
}

/// Reserve `n` pairs of ephemeral loopback ports, one client port and one
/// controller port per broker. The ports stay held, and refuse connections,
/// until the returned [`ReservedPorts`] is dropped.
///
/// It replaces binding a listener on `127.0.0.1:0`, reading its port and
/// dropping it: a port dropped that way is free again at once, and under
/// parallel test binaries another `bind(0)` can be handed it before the broker
/// binds it, which fails the broker's start with `AddrInUse`.
pub fn reserve_ports(n: usize) -> ReservedPorts {
    let mut reservations = Vec::with_capacity(2 * n);
    let mut clients = Vec::with_capacity(n);
    let mut controllers = Vec::with_capacity(n);
    for _ in 0..n {
        for addresses in [&mut clients, &mut controllers] {
            let (socket, address) = reserve_port((Ipv4Addr::LOCALHOST, 0));
            reservations.push(socket);
            addresses.push(address);
        }
    }
    ReservedPorts {
        clients,
        controllers,
        _reservations: reservations,
    }
}

/// Binds `n` pairs of
/// ephemeral loopback listeners, one client and one controller per broker, and
/// returns their concrete addrs **alongside the still-open listeners**,
/// index-aligned.
///
/// Hand `client_listeners[i]` and `controller_listeners[i]` to
/// [`krabka_broker::Broker::start_with_listeners`] or
/// `start_with_controller_listener`, so the OS port is never released before
/// the broker adopts it.
///
/// The returned `SocketAddr`s are the listeners' real `local_addr()`s, so the
/// caller builds its static voter set and advertised addresses from them as it
/// would from [`reserve_ports`]. Use [`reserve_ports`] instead when something
/// other than `start_with_listeners` binds the address.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub async fn bind_and_hold_ports(
    n: usize,
) -> (
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    Vec<TcpListener>,
    Vec<TcpListener>,
) {
    let mut client_listeners = Vec::with_capacity(n);
    let mut controller_listeners = Vec::with_capacity(n);
    for _ in 0..n {
        client_listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        controller_listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let client_addrs = client_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let controller_addrs = controller_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    (
        client_addrs,
        controller_addrs,
        client_listeners,
        controller_listeners,
    )
}
