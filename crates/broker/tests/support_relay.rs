//! Coverage for the `support::relay` fault injector.
//!
//! `tests/support/` is helper code compiled into *every* integration-test
//! binary in this crate, so it cannot carry `#[test]` functions of its own —
//! they would run once per binary. The relay's own tests therefore live here, in
//! a dedicated suite whose upstream is a plain echo server rather than a broker:
//! what is under test is the forwarding and the cut, not anything Kafka.
//!
//! Every wait is bounded with `tokio::time::timeout`. A relay bug is usually a
//! read that never completes, and an unbounded wait would turn that into CI's
//! 600s kill, reported as TIMEOUT with no cause; a bounded one fails in seconds
//! and says which step hung.
#![cfg(test)]

use {
    assert2::{assert, check},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{collections::BTreeMap, future::Future, io, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt, copy},
        net::{TcpListener, TcpStream},
        time,
    },
};

mod support;

use support::relay::{Relay, SiteLink};

// Generous enough that a loaded runner does not fail a healthy relay, short
// enough that a broken one reports in seconds.
const TIMEOUT: Duration = Duration::from_secs(5);

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    time::timeout(TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {TIMEOUT:?}"))
}

// Bind a loopback echo server and return its address. Its tasks end when the
// test's runtime is dropped, which is the end of the test.
async fn start_echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = listener.local_addr().expect("echo local addr");
    tokio::spawn(async move {
        while let Ok((mut stream, _peer)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                let _ = copy(&mut read, &mut write).await;
            });
        }
    });
    addr
}

async fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> io::Result<Vec<u8>> {
    stream.write_all(payload).await?;
    let mut echoed = vec![0u8; payload.len()];
    // `read_exact` reports a torn-down link as `UnexpectedEof` rather than a
    // short read, so both halves of "the link is gone" land in the `Err` arm.
    stream.read_exact(&mut echoed).await?;
    Ok(echoed)
}

// Connect through `addr` and echo `payload` back, as one fallible step.
//
// A cut relay may refuse at either point — the connect can fail outright, or it
// can complete and the first read then end at once, which is what the
// accept-and-drop cut produces. The caller cares only that the link does not
// carry data, so both failures are collapsed into one `Err`.
async fn probe(addr: SocketAddr, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    round_trip(&mut stream, payload).await
}

#[derive(Default)]
struct RelayScenario {
    upstream: Option<SocketAddr>,
    address: Option<SocketAddr>,
    relay: Option<Arc<Relay>>,
    open: Option<TcpStream>,
    reply: Option<io::Result<Vec<u8>>>,
    site: Option<SiteLink>,
    controller: Option<SocketAddr>,
    data: Option<SocketAddr>,
    site_replies: BTreeMap<&'static str, io::Result<Vec<u8>>>,
}

impl RelayScenario {
    fn relay(&self) -> &Relay { self.relay.as_ref().expect("the relay is running") }

    fn address(&self) -> SocketAddr { self.address.expect("the relay bound its address") }

    async fn shutdown(&mut self) {
        if let Some(relay) = self.relay.take() {
            Arc::into_inner(relay)
                .expect("sole owner of the relay")
                .shutdown()
                .await;
        }
        if let Some(site) = self.site.take() {
            site.shutdown().await;
        }
    }
}

#[fixture]
fn relay_case() -> RelayScenario {
    // Each link generation and captured reply belongs to one fault scenario.
    RelayScenario {
        relay: None,
        ..Default::default()
    }
}

#[given("a loopback echo server is reachable through a relay")]
async fn echo_relay(relay_case: &mut RelayScenario) {
    let upstream = start_echo().await;
    let relay = Arc::new(Relay::start(upstream).await);
    relay_case.upstream = Some(upstream);
    relay_case.address = Some(relay.addr());
    relay_case.relay = Some(relay);
}

#[when("a fresh connection sends {payload} through the relay")]
async fn fresh_relay_probe(relay_case: &mut RelayScenario, payload: String) {
    relay_case.reply = Some(
        within(
            "fresh connection round trip",
            probe(relay_case.address(), payload.as_bytes()),
        )
        .await,
    );
}

#[when("a connection opens and sends {payload} through the relay")]
async fn open_relay_connection(relay_case: &mut RelayScenario, payload: String) {
    let mut stream = within("connect", TcpStream::connect(relay_case.address()))
        .await
        .expect("connect through a healthy relay");
    relay_case.reply = Some(
        within(
            "open connection round trip",
            round_trip(&mut stream, payload.as_bytes()),
        )
        .await,
    );
    relay_case.open = Some(stream);
}

#[when("the already open connection sends {payload}")]
async fn existing_relay_probe(relay_case: &mut RelayScenario, payload: String) {
    relay_case.reply = Some(
        within(
            "existing connection round trip",
            round_trip(
                relay_case.open.as_mut().expect("a connection was opened"),
                payload.as_bytes(),
            ),
        )
        .await,
    );
}

#[then("the relay echoes exactly {payload}")]
fn relay_echoed(relay_case: &RelayScenario, payload: String) {
    check!(
        relay_case
            .reply
            .as_ref()
            .expect("a round trip was attempted")
            .as_ref()
            .expect("the relay carries the connection")
            .as_slice()
            == payload.as_bytes()
    );
}

#[then("the relay refuses to carry the connection")]
fn relay_does_not_carry_data(relay_case: &RelayScenario) {
    assert!(let Err(_) = relay_case.reply.as_ref().expect("a round trip was attempted"), "a cut or stopped link must not carry data");
}

#[then("the relay binds a separate port from its upstream")]
fn separate_relay_port(relay_case: &RelayScenario) {
    check!(
        Some(relay_case.address()) != relay_case.upstream,
        "the relay binds a port of its own"
    );
}

#[when("the relay link is cut")]
fn cut_relay_link(relay_case: &RelayScenario) { relay_case.relay().cut(); }

#[when("the relay link heals")]
fn heal_relay_link(relay_case: &RelayScenario) { relay_case.relay().heal(); }

#[then("the healed relay retains its original listener address")]
fn relay_listener_unchanged(relay_case: &RelayScenario) {
    check!(relay_case.relay().addr() == relay_case.address());
}

#[when("the relay shuts down while its connection remains open")]
async fn shut_down_live_relay(relay_case: &mut RelayScenario) {
    let relay = Arc::into_inner(relay_case.relay.take().expect("the relay is running"))
        .expect("sole owner of the relay");
    within("shutdown", relay.shutdown()).await;
}

#[when("another task cuts the shared relay")]
async fn shared_relay_cut(relay_case: &RelayScenario) {
    let other = Arc::clone(
        relay_case
            .relay
            .as_ref()
            .expect("the shared relay is running"),
    );
    tokio::spawn(async move { other.cut() })
        .await
        .expect("cut from another task");
}

#[when("another task heals the shared relay")]
async fn shared_relay_heal(relay_case: &RelayScenario) {
    let other = Arc::clone(
        relay_case
            .relay
            .as_ref()
            .expect("the shared relay is running"),
    );
    tokio::spawn(async move { other.heal() })
        .await
        .expect("heal from another task");
}

#[given("a site link connects separate controller and client echo servers")]
async fn two_listener_site(relay_case: &mut RelayScenario) {
    let controller = start_echo().await;
    let client = start_echo().await;
    let site = SiteLink::start(controller, client).await;
    relay_case.controller = Some(site.controller_addr());
    relay_case.data = Some(site.client_addr());
    relay_case.site = Some(site);
}

#[when("the controller sends ctrl and the client sends data through the site")]
async fn site_healthy_probes(relay_case: &mut RelayScenario) {
    relay_case.site_replies.insert(
        "controller",
        within(
            "controller round trip",
            probe(
                relay_case.controller.expect("a controller listener exists"),
                b"ctrl",
            ),
        )
        .await,
    );
    relay_case.site_replies.insert(
        "client",
        within(
            "client round trip",
            probe(relay_case.data.expect("a client listener exists"), b"data"),
        )
        .await,
    );
}

#[then("the distinct site listeners echo ctrl and data respectively")]
fn site_echoed_on_both_planes(relay_case: &RelayScenario) {
    check!(relay_case.controller != relay_case.data);
    check!(
        relay_case.site_replies["controller"]
            .as_ref()
            .expect("controller round trip")
            .as_slice()
            == b"ctrl"
    );
    check!(
        relay_case.site_replies["client"]
            .as_ref()
            .expect("client round trip")
            .as_slice()
            == b"data"
    );
}

#[when("the whole site link is cut")]
fn cut_site(relay_case: &RelayScenario) {
    relay_case.site.as_ref().expect("the site is running").cut();
}

#[when("both site listeners receive an attempted x round trip")]
async fn site_cut_probes(relay_case: &mut RelayScenario) {
    relay_case.site_replies.insert(
        "controller",
        within(
            "controller while cut",
            probe(
                relay_case.controller.expect("a controller listener exists"),
                b"x",
            ),
        )
        .await,
    );
    relay_case.site_replies.insert(
        "client",
        within(
            "client while cut",
            probe(relay_case.data.expect("a client listener exists"), b"x"),
        )
        .await,
    );
}

#[then("neither site listener carries the attempted round trip")]
fn site_refuses_both_planes(relay_case: &RelayScenario) {
    assert!(let Err(_) = &relay_case.site_replies["controller"]);
    assert!(let Err(_) = &relay_case.site_replies["client"]);
}

#[when("the whole site link heals")]
fn heal_site(relay_case: &RelayScenario) {
    relay_case
        .site
        .as_ref()
        .expect("the site is running")
        .heal();
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "relay forwards bytes to the upstream"
)]
#[tokio::test]
async fn relay_forwards_bytes_to_the_upstream(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "cut tears down an open connection and stops new ones"
)]
#[tokio::test]
async fn cut_tears_down_an_open_connection_and_stops_new_ones(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "heal restores forwarding without a restart"
)]
#[tokio::test]
async fn heal_restores_forwarding_without_a_restart(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "shutdown ends the relay with a connection still open"
)]
#[tokio::test]
async fn shutdown_ends_the_relay_with_a_connection_still_open(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "cut and heal work through a shared relay"
)]
#[tokio::test]
async fn cut_and_heal_work_through_a_shared_relay(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}

#[scenario(
    path = "tests/features/migrated/tests/support_relay.feature",
    name = "site link cuts and heals both listeners together"
)]
#[tokio::test]
async fn site_link_cuts_and_heals_both_listeners_together(relay_case: RelayScenario) {
    relay_case.shutdown().await;
    drop(relay_case);
}
