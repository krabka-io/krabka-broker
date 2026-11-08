//! Holding a loopback port for a listener that a broker binds later.

use {std::net::SocketAddr, tokio::net::TcpSocket};

/// Holds the port of `addr` for a listener that the broker binds later, and
/// returns the reservation and its address. Port 0 reserves a fresh ephemeral
/// port; any other port takes that one back, such as a port a stopped broker
/// has just let go of. Keep the socket until the broker no longer needs the
/// port.
///
/// A port bound and dropped at once, or given back by a broker that stopped,
/// is free again at once: on a busy host any `bind("127.0.0.1:0")` -- another
/// scenario's broker in the same binary, or another test binary's -- can be
/// handed it before the broker binds it. The broker's bind then fails with
/// `AddrInUse`, or the port answers as the other socket's listener.
///
/// A socket that is bound but not listening keeps the port out of both
/// `bind(0)` and `connect()`'s ephemeral pick, and refuses connections as a
/// closed port does. Under `SO_REUSEADDR`, which mio also sets on the
/// listener the broker binds, it lets that one listener bind the port, and it
/// shares the port with connections a stopped broker closed that are still in
/// `TIME_WAIT`.
///
/// Creating and binding the socket needs no Tokio runtime, so a synchronous
/// test that spawns the broker binary can hold one too.
///
/// # Panics
/// Panics if the socket cannot be created or cannot bind `addr`.
pub fn reserve_port(addr: impl Into<SocketAddr>) -> (TcpSocket, SocketAddr) {
    let addr = addr.into();
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .expect("reservation socket");
    socket
        .set_reuseaddr(true)
        .expect("SO_REUSEADDR on the reservation socket");
    socket
        .bind(addr)
        .unwrap_or_else(|e| panic!("reserve port {addr}: {e:?}"));
    let address = socket.local_addr().expect("reserved address");
    (socket, address)
}
