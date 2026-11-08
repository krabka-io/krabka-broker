//! Holding a loopback port for a listener that a broker binds later.

use {
    std::net::{Ipv4Addr, SocketAddr},
    tokio::net::TcpSocket,
};

/// A loopback port held for a listener that the broker binds later, and its
/// address. Keep the socket until the broker no longer needs the port.
///
/// A port bound and dropped at once is free again at once: on a busy host
/// any `bind("127.0.0.1:0")` -- another scenario's broker in the same binary,
/// or another test binary's -- can be handed it before the broker binds it.
/// The broker's bind then fails with `AddrInUse`, or the port answers as the
/// other socket's listener.
///
/// A socket that is bound but not listening keeps the port out of both
/// `bind(0)` and `connect()`'s ephemeral pick, and refuses connections as a
/// closed port does. Under `SO_REUSEADDR`, which mio also sets on the
/// listener the broker binds, it lets that one listener bind the port.
///
/// Creating and binding the socket needs no Tokio runtime, so a synchronous
/// test that spawns the broker binary can hold one too.
pub fn reserve_port() -> (TcpSocket, SocketAddr) {
    let socket = TcpSocket::new_v4().expect("reservation socket");
    socket
        .set_reuseaddr(true)
        .expect("SO_REUSEADDR on the reservation socket");
    socket
        .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("reserve a loopback port");
    let address = socket.local_addr().expect("reserved address");
    (socket, address)
}
