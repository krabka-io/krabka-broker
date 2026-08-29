//! Behaviour tests for the refresh loop: the immediate first fetch, HTTPS with
//! a custom trust bundle, the on-demand signal and its rate limit, and the two
//! shared timestamps a validator reads.

use assert2::{assert, check};
use krabka_units::{millis, minutes};
use qubit_clock::{
    MockWaiterKind,
    sleep::{MockSleeper, SystemSleeper},
};

use super::*;
use crate::oauth_jwks::test_support::{
    JWKS_BODY, await_until, make_signal_refresher, serve_jwks, serve_jwks_counting,
    serve_jwks_https, test_refresher,
};

#[tokio::test]
async fn refresher_populates_handle_then_stops_on_shutdown() {
    let (addr, srv_shutdown) = serve_jwks(JWKS_BODY).await;
    let handle = JwksHandle::default();
    assert!(handle.load().is_empty());
    let shutdown = CancellationToken::new();
    let refresher = test_refresher(
        format!("http://{addr}/jwks"),
        handle.clone(),
        millis(50),
        shutdown.clone(),
        None,
        Arc::new(SystemSleeper::new()),
    );
    let task = tokio::spawn(refresher.run());

    // Poll until the immediate first fetch lands.
    await_until("first JWKS fetch populates handle", || {
        !handle.load().is_empty()
    })
    .await;
    assert!(handle.load().len() == 1);

    shutdown.cancel();
    task.await.unwrap();
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_fetches_jwks_over_https_with_custom_trust() {
    let (addr, srv_shutdown, ca_path) = serve_jwks_https(JWKS_BODY).await;
    let handle = JwksHandle::default();
    let shutdown = CancellationToken::new();
    let refresher = test_refresher(
        format!("https://127.0.0.1:{}/jwks", addr.port()),
        handle.clone(),
        millis(50),
        shutdown.clone(),
        Some(ca_path),
        Arc::new(SystemSleeper::new()),
    );
    let task = tokio::spawn(refresher.run());
    await_until("first HTTPS JWKS fetch populates handle", || {
        !handle.load().is_empty()
    })
    .await;
    assert!(handle.load().len() == 1);
    shutdown.cancel();
    task.await.unwrap();
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert() {
    // Server presents cert A; trust bundle is an unrelated cert B. Every
    // refresh fails TLS verification, so the handle must never populate.
    //
    // Driven on a mock timeline instead of a wall-clock sleep: the first
    // fetch fires immediately (t=0 tick), then the loop parks on the
    // refresh-interval sleep. Advancing the timeline fires each subsequent
    // fetch deterministically — exact and instant, with no flaky timing.
    let (addr, srv_shutdown, _server_cert_path) = serve_jwks_https(JWKS_BODY).await;

    let dir = tempfile::tempdir().unwrap();
    let params = rcgen::CertificateParams::new(vec!["unrelated.example".to_string()]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = params.self_signed(&key).unwrap();
    let bogus_ca = dir.path().join("bogus-ca.pem");
    std::fs::write(&bogus_ca, cert.pem()).unwrap();

    let handle = JwksHandle::default();
    let shutdown = CancellationToken::new();
    let interval = millis(50);
    let sleeper = MockSleeper::new();
    let timeline = sleeper.timeline();
    let refresher = test_refresher(
        format!("https://127.0.0.1:{}/jwks", addr.port()),
        handle.clone(),
        interval,
        shutdown.clone(),
        Some(bogus_ca),
        Arc::new(sleeper),
    );
    let task = tokio::spawn(refresher.run());

    // Drive several refresh intervals. Before each advance, block (bounded
    // real time, hang-guard only) until the loop has parked on the interval
    // sleep — this confirms the prior fetch attempt completed — then assert
    // the failing fetch left the handle empty. `wait_for_blocked_waiters`
    // runs on a blocking thread so it never stalls the current-thread
    // runtime that must drive the refresher's HTTPS attempt.
    for _ in 0..3 {
        let tl = timeline.clone();
        let parked = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "refresher should park on the interval sleep between fetches",
        );
        assert!(
            handle.load().is_empty(),
            "fetch should fail verification and leave handle empty",
        );
        timeline.advance(interval.to_std());
    }

    shutdown.cancel();
    task.await.unwrap();
    srv_shutdown.cancel();
}

// ---- On-demand refresh + cache-expiry timestamp -------------

#[tokio::test]
async fn refresher_signal_triggers_on_demand_refresh_when_pause_elapsed() {
    let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (refresher, signal_tx, _last_successful, last_on_demand, shutdown, handle) =
        make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    let task = tokio::spawn(refresher.run());

    // Drive at least one signal refresh.
    signal_tx.send(()).await.unwrap();
    // Poll until the on-demand timestamp moves and the handle has been
    // populated by the on-demand fetch.
    await_until(
        "on-demand fetch advances timestamp and populates handle",
        || last_on_demand.load(Ordering::Relaxed) > 0 && !handle.load().is_empty(),
    )
    .await;
    check!(
        last_on_demand.load(Ordering::Relaxed) > 0,
        "on-demand timestamp should have advanced past sentinel 0",
    );
    check!(
        count.load(Ordering::Relaxed) >= 1,
        "server should have served the on-demand request"
    );
    check!(
        handle.load().len() == 1,
        "refresher must store the fetched key set"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_signal_dropped_when_within_min_pause_window() {
    let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    // 60s pause — second signal MUST be rate-limited.
    let (refresher, signal_tx, _last_successful, last_on_demand, shutdown, _handle) =
        make_signal_refresher(endpoint, minutes(1));
    let task = tokio::spawn(refresher.run());

    // First signal: fires. Wait on the HTTP counter (the strict
    // happens-after of refresh_and_swap) rather than the timestamp
    // store, which the select! arm performs BEFORE the HTTP call —
    // otherwise CI races between timestamp-set and request-arrival.
    signal_tx.send(()).await.unwrap();
    for _ in 0..100 {
        if count.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let first_ts = last_on_demand.load(Ordering::Relaxed);
    assert!(first_ts > 0, "first signal must have fired a refresh");
    let count_after_first = count.load(Ordering::Relaxed);
    assert!(count_after_first >= 1);

    // Second signal within the 60s pause: dropped.
    signal_tx.send(()).await.unwrap();
    // Yield the runtime a few times to let the select! arm process.
    for _ in 0..10 {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let actual = (
        last_on_demand.load(Ordering::Relaxed),
        count.load(Ordering::Relaxed),
    );
    assert!(
        actual == (first_ts, count_after_first),
        "second signal within min_pause must change neither timestamp nor HTTP request count"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_successful_refresh_updates_last_successful_fetch_timestamp() {
    let (addr, srv_shutdown, _count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (refresher, signal_tx, last_successful, _last_on_demand, shutdown, _handle) =
        make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    let task = tokio::spawn(refresher.run());

    assert!(last_successful.load(Ordering::Relaxed) == 0);
    signal_tx.send(()).await.unwrap();
    await_until(
        "successful fetch advances last_successful timestamp",
        || last_successful.load(Ordering::Relaxed) > 0,
    )
    .await;
    assert!(
        last_successful.load(Ordering::Relaxed) > 0,
        "last_successful_fetch_ms must advance after a successful fetch",
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_failed_refresh_does_not_advance_last_successful_fetch() {
    // Endpoint that always returns 500 ⇒ fetch fails, timestamp must stay
    // at the sentinel 0.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_shutdown = CancellationToken::new();
    let srv_token = srv_shutdown.clone();
    let app = axum::Router::new().route(
        "/jwks",
        axum::routing::get(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_token.cancelled().await })
            .await
            .unwrap();
    });

    let endpoint = format!("http://{addr}/jwks");
    let (refresher, signal_tx, last_successful, last_on_demand, shutdown, _handle) =
        make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    let task = tokio::spawn(refresher.run());

    signal_tx.send(()).await.unwrap();
    // Wait for the failed refresh attempt to complete & log; the on-demand
    // rate-limit timestamp advances even when the fetch itself fails.
    await_until(
        "failed on-demand fetch still advances rate-limit timestamp",
        || last_on_demand.load(Ordering::Relaxed) > 0,
    )
    .await;
    // On-demand timestamp advances regardless (rate-limit accounting);
    // success timestamp must stay at 0.
    assert!(
        last_on_demand.load(Ordering::Relaxed) > 0,
        "on-demand rate-limit timestamp updates even when the fetch itself fails",
    );
    assert!(
        last_successful.load(Ordering::Relaxed) == 0,
        "failed fetch must leave last_successful_fetch_ms at sentinel 0"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_passes_ignore_key_use_through_to_jwks_parser() {
    // JWKS body has an RSA key marked `use=enc`. With ignore_key_use=true
    // the refresher should still install it; with false it would be filtered
    // out (yielding an empty key set).
    const ENC_KEY_BODY: &str =
        r#"{"keys":[{"kty":"RSA","kid":"enc-kid","use":"enc","n":"AQAB","e":"AQAB"}]}"#;
    let (addr, srv_shutdown, _count) = serve_jwks_counting(ENC_KEY_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (mut refresher, signal_tx, _last_successful, _last_on_demand, shutdown, handle) =
        make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    refresher.ignore_key_use = true;
    let task = tokio::spawn(refresher.run());

    signal_tx.send(()).await.unwrap();
    await_until("on-demand fetch installs the use=enc key", || {
        !handle.load().is_empty()
    })
    .await;
    assert!(
        handle.load().len() == 1,
        "ignore_key_use=true must keep the use=enc key in the installed set"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}
