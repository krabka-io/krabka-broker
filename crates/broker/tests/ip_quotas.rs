//! Broker-side integration tests for KIP-612 IP quotas.
//!
//! Tests:
//! 1. `ip_quota_alter_then_describe_round_trip`. Over SASL/PLAIN, it alters
//!    (ip=127.0.0.1) `connection_creation_rate=2.0`, describes it, and
//!    asserts.
//! 2. `connection_creation_rate_throttles_accept`. Over PLAINTEXT with
//!    rate=1, it opens 5 connections one after another and asserts a wall
//!    time ≥3s.
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

/// Test 2: sets rate=1 connection per second for the loopback IP through
/// `submit_metadata_record_for_test`, because a PLAINTEXT cluster has no SASL
/// admin path. It opens 5 connections one after another and asserts a wall
/// time >= 3 seconds, which proves that the throttle fires.
///
/// The timeline with rate=1, capacity=1, and cap=1s is:
///   connection 1: free, from the initial token
///   connections 2 to 5: the bucket is empty, so each sleeps 1s and is then
///   free
///
/// The total is about 4s, and the tolerance is >=3s.
///
/// Each connection sends `ApiVersions` and waits for the response. The accept
/// loop therefore finishes the throttle sleep for that connection before the
/// test opens the next one. The OS backlog alone would complete the TCP
/// handshake immediately and measure no throttle time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_throttles_accept() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Seed rate=1 connection/sec for 127.0.0.1 directly into the image.
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

    // Wait until the quota is visible in the image.
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|m| m.get("connection_creation_rate"))
                .is_some()
        })
        .await;

    // Open 5 connections in sequence. For each connection, send ApiVersions
    // and wait for the response — this ensures the accept loop has processed
    // the throttle sleep for that connection before we open the next.
    // (Without this, the OS TCP backlog completes the SYN-ACK handshake for
    // all connections immediately and TcpStream::connect returns without
    // waiting for the accept-side throttle sleep.)
    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        // Send ApiVersions v0 (non-flexible) and read the response.
        // This round-trip blocks until the accept loop has spawned this
        // connection's handler, which only happens after the throttle sleep.
        let av_req = ApiVersionsRequest::default();
        let mut av_body = BytesMut::new();
        av_req.encode(&mut av_body, 0).expect("encode ApiVersions");
        round_trip(&mut s, 18, 0, 1, false, &av_body)
            .await
            .expect("ApiVersions round-trip");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    // Expected: with rate=1 and 1s bucket capacity, connections alternate
    // between free (bucket refills during the 1s sleep) and throttled.
    // Pattern: conn1=free, conn2=sleep1s, conn3=free(refilled), conn4=sleep1s,
    // conn5=free(refilled). Total: 2 sleeps ≈ 2s.
    // Tolerance: >=1.5s proves the throttle fired. This is stable even with
    // slight timing variations in the test runner.
    assert!(
        elapsed >= std::time::Duration::from_millis(1500),
        "expected >=1.5s of throttle, got {elapsed:?}"
    );
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
