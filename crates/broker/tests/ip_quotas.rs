//! Broker-side integration tests for KIP-612 IP quotas.
//!
//! Tests:
//! 1. `ip_quota_alter_then_describe_round_trip`. Over SASL/PLAIN, it alters
//!    (ip=127.0.0.1) `connection_creation_rate=2.0`, describes it, and
//!    asserts.
//! 2. `connection_creation_rate_closes_the_throttled_connection`. Over
//!    PLAINTEXT with rate=1, a second connection from the same ip closes
//!    without a response, and a connection from another ip is served at once.
//! 3. `unthrottled_ip_unaffected`. Over PLAINTEXT with no quota, it opens 5
//!    connections and asserts a wall time <500ms.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `ip_quotas/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "ip_quotas/cluster.rs"]
mod cluster;
#[path = "ip_quotas/quota_admin.rs"]
mod quota_admin;
#[path = "ip_quotas/wire.rs"]
mod wire;

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest};
use tokio::net::TcpStream;

use crate::{
    cluster::{
        start_single_broker_plaintext, start_single_broker_plaintext_with_conn_caps,
        start_single_broker_sasl_plaintext_with_users,
    },
    quota_admin::{drive_alter_client_quotas_sasl, drive_describe_client_quotas_sasl},
    wire::round_trip,
};

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: `AlterClientQuotas` sets (ip=127.0.0.1)
/// `connection_creation_rate=2.0`. The value must then appear in the metadata
/// image and in `DescribeClientQuotas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ip_quota_alter_then_describe_round_trip() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("ip".into(), Some("127.0.0.1".into()))],
            vec![("connection_creation_rate".into(), 2.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter should succeed");

    // Wait until the quota is visible in the image.
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("connection_creation_rate"))
                == Some(&2.0)
        })
        .await;

    let desc = drive_describe_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![("ip".into(), /*ANY*/ 2, None)],
        false,
    )
    .await;
    assert!(desc.len() == 1);
    assert!(
        desc[0]
            .1
            .iter()
            .find(|(k, _)| k == "connection_creation_rate")
            .map(|(_, v)| *v)
            == Some(2.0)
    );
}

/// Test 2: sets rate=1 connection per second for `127.0.0.1` through
/// `submit_metadata_record_for_test`, because a PLAINTEXT cluster has no SASL
/// admin path. Kafka's acceptor (`SocketServer.Acceptor.accept`) never serves
/// a connection over the ip rate: it holds the socket in `throttledSockets`
/// for the throttle time and then closes it, and it keeps accepting other
/// connections meanwhile (#760).
///
/// | connection | expected |
/// |---|---|
/// | first from `127.0.0.1` | served, `ApiVersions` answers |
/// | second from `127.0.0.1`, inside the window | closed without a response |
/// | from `127.0.0.2`, while the second is held | served with no added delay |
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_closes_the_throttled_connection() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    let rec = krabka_metadata::MetadataRecord::V1ClientQuota(krabka_metadata::ClientQuotaRecord {
        entity: vec![krabka_metadata::QuotaEntity {
            entity_type: "ip".into(),
            entity_name: Some("127.0.0.1".into()),
        }],
        config_key: "connection_creation_rate".into(),
        config_value: Some(1.0),
    });
    handle
        .submit_metadata_record_for_test(rec)
        .await
        .expect("seed quota");
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|m| m.get("connection_creation_rate"))
                .is_some()
        })
        .await;

    let api_versions = || {
        let mut body = BytesMut::new();
        ApiVersionsRequest::default()
            .encode(&mut body, 0)
            .expect("encode ApiVersions");
        body
    };
    let connect_from = |source: [u8; 4]| async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket
            .bind(std::net::SocketAddr::from((source, 0)))
            .expect("bind source address");
        socket.connect(addr).await.expect("connect")
    };

    // Kafka's rate sensor starts empty. The broker's own connections may have
    // spent this bucket's token, so wait out one refill first.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    let mut first = connect_from([127, 0, 0, 1]).await;
    let served = round_trip(&mut first, 18, 0, 1, false, &api_versions()).await;
    assert!(served.is_ok(), "first connection is served: {served:?}");

    let mut throttled = connect_from([127, 0, 0, 1]).await;
    let body = api_versions();
    let throttled_task =
        tokio::spawn(async move { round_trip(&mut throttled, 18, 0, 2, false, &body).await });
    // Let the accept loop take the throttled connection before the next one.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    let mut other = connect_from([127, 0, 0, 2]).await;
    let other_served = round_trip(&mut other, 18, 0, 3, false, &api_versions()).await;
    let other_elapsed = started.elapsed();
    assert!(
        other_served.is_ok(),
        "another ip is served: {other_served:?}"
    );
    assert!(
        other_elapsed < std::time::Duration::from_millis(500),
        "another ip waits for no throttle, took {other_elapsed:?}"
    );

    let refused = tokio::time::timeout(std::time::Duration::from_secs(5), throttled_task)
        .await
        .expect("the throttled connection closes after the throttle")
        .expect("round-trip task");
    // The broker closes the socket with the request unread, so the client sees
    // an end of stream or, when the kernel answers the unread bytes with a
    // reset, a reset.
    assert!(
        refused.as_ref().is_err_and(|error| matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
        )),
        "the throttled connection closes without a response: {refused:?}"
    );
    drop((first, other));
}

/// Test 3: no `connection_creation_rate` quota is configured. The test opens 5
/// connections and asserts a wall time < 500ms, the unthrottled baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_ip_unaffected() {
    let (_handle, _dir, addr) = start_single_broker_plaintext().await;
    // No connection_creation_rate quota configured.

    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "expected fast unthrottled connect, got {elapsed:?}"
    );
}

/// M-2: a per-IP connection cap refuses connections past the limit, and
/// `ConnectionGuard::drop` frees the slot once an existing connection
/// closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_connections_per_ip_refuses_excess_and_frees_on_close() {
    let (_handle, _dir, addr) = start_single_broker_plaintext_with_conn_caps(usize::MAX, 1).await;

    let av_body = {
        let mut b = BytesMut::new();
        ApiVersionsRequest::default()
            .encode(&mut b, 0)
            .expect("encode ApiVersions");
        b.to_vec()
    };

    // Connection 1: within the per-IP cap (0 -> 1). A successful round-trip
    // proves the broker accepted it; keep the stream open to hold the slot.
    let mut c1 = TcpStream::connect(addr).await.expect("connect c1");
    round_trip(&mut c1, 18, 0, 1, false, &av_body)
        .await
        .expect("c1 ApiVersions succeeds (within cap)");

    // Connection 2 from the same IP exceeds the per-IP cap. The broker accepts
    // the socket then immediately drops it (no handler spawned), so the
    // request round-trip fails (peer closed the connection).
    let mut c2 = TcpStream::connect(addr).await.expect("tcp connect c2");
    let c2_result = round_trip(&mut c2, 18, 0, 1, false, &av_body).await;
    assert!(
        c2_result.is_err(),
        "c2 must be refused while c1 holds the only per-IP slot, got {c2_result:?}"
    );

    // Closing c1 frees the slot. The decrement happens when the c1 handler task
    // observes the close, so retry briefly until a fresh connection succeeds.
    drop(c1);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let mut c3 = TcpStream::connect(addr).await.expect("connect c3");
        if round_trip(&mut c3, 18, 0, 1, false, &av_body).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "per-IP slot was not freed after c1 closed"
        );
        // intentional: the per-IP ConnectionGuard decrement is coordinator-local
        // (not in the metadata image and has no metric); each iteration re-drives
        // the real connect+round-trip under test, so keep the bounded retry poll.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
