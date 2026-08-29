//! Reserving ephemeral loopback ports for a multi-broker test cluster.
//!
//! Both helpers hand back one client address and one controller address per
//! broker. [`bind_and_hold_ports`] also returns the listeners it bound, so
//! nothing can take a port between the reservation and the broker that adopts
//! it; [`bind_and_drop_ports`] is the older form, for callers that let
//! `Broker::start` re-bind the address itself.

use std::net::SocketAddr;

/// Reserve `n` pairs of ephemeral loopback ports, one client port and one
/// controller port per broker, with the bind-and-drop trick. Bind a
/// `TcpListener` on `127.0.0.1:0`, read its assigned port, then drop the
/// listener. The OS does not immediately reuse the port for another bind, so
/// the caller can pass it to `Broker::start` and the broker re-binds it on the
/// same address.
///
/// This avoids the Linux `TIME_WAIT` problem that fixed ports hit when many
/// tests in the same binary boot 3-broker clusters back-to-back.
pub async fn bind_and_drop_ports(n: usize) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let mut client_addrs = Vec::with_capacity(n);
    let mut controller_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }
    (client_addrs, controller_addrs)
}

/// Race-free replacement for [`bind_and_drop_ports`]. It binds `n` pairs of
/// ephemeral loopback listeners, one client and one controller per broker, and
/// returns their concrete addrs **alongside the still-open listeners**,
/// index-aligned.
///
/// Hand `client_listeners[i]` and `controller_listeners[i]` to
/// [`krabka_broker::Broker::start_with_listeners`] or
/// `start_with_controller_listener`, so the OS port is never released before
/// the broker adopts it. That closes the [`bind_and_drop_ports`] TOCTOU window
/// in which a concurrently-running test binary steals the freed port
/// (`AddrInUse`) under parallel `cargo nextest`.
///
/// The returned `SocketAddr`s are the listeners' real `local_addr()`s, so the
/// caller builds its static voter set and advertised addresses from them
/// exactly as with [`bind_and_drop_ports`]. The only call-site change is to
/// pass the matching listener into `start_with_listeners` instead of letting
/// `Broker::start` re-bind the address.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub async fn bind_and_hold_ports(
    n: usize,
) -> (
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    Vec<tokio::net::TcpListener>,
    Vec<tokio::net::TcpListener>,
) {
    let mut client_listeners = Vec::with_capacity(n);
    let mut controller_listeners = Vec::with_capacity(n);
    for _ in 0..n {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
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
