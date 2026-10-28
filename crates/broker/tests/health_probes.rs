#![cfg(test)]

//! The `/healthz` and `/readyz` probes across a broker's startup.
//!
//! The interesting claim is the transition: `/readyz` must answer 503 while
//! the broker is coming up and 200 once it is serving, and `/healthz` must
//! answer 200 throughout, because a liveness probe that failed mid-recovery
//! would have the kubelet kill the node and restart the recovery.
//!
//! Nothing here waits out a window. The probes are served over the same
//! `HealthState` the broker is handed, so the test holds the broker in the
//! not-ready state simply by not having started it yet, and the 200 is
//! asserted against a handle that has already come back from
//! `Broker::start_with_health`.

mod support;

use {
    crate::support::{client::http_get, listeners},
    assert2::check,
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle,
        config::DEFAULT_READINESS_MAX_METADATA_LAG,
        health::{HealthState, router},
    },
    krabka_security::ListenerProtocol,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{net::SocketAddr, path::Path, time::Duration},
    tempfile::{TempDir, tempdir},
    tokio::{net::TcpListener, time},
};

/// The status line and body of one HTTP response.
struct ProbeResponse {
    status: String,
    body: String,
}

/// One HTTP/1.1 GET, returned as its status line and body.
async fn get(addr: SocketAddr, path: &str) -> ProbeResponse {
    let raw = http_get(addr, path, true).await;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    let status = head.lines().next().unwrap_or_default().to_owned();
    ProbeResponse {
        status,
        body: body.to_owned(),
    }
}

/// Serves the probe routes over `state` on an ephemeral port, the way the
/// broker binary does before it calls `Broker::start`.
async fn serve_probes(state: HealthState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    bound
}

fn loopback_config(log_dir: &Path) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![listeners::loopback_listener(
        "PLAINTEXT",
        ListenerProtocol::Plaintext,
    )];
    cfg.inter_broker_listener_name = "PLAINTEXT".into();
    cfg
}

/// Each scenario owns its probe server and, when started, its broker.
struct ProbeScenario {
    state: Option<HealthState>,
    address: Option<SocketAddr>,
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    readiness: Vec<ProbeResponse>,
    liveness: Option<ProbeResponse>,
}

#[fixture]
fn probes() -> ProbeScenario {
    // Start this scenario with fresh inputs and observations.

    ProbeScenario {
        state: None,
        address: None,
        directory: None,
        broker: None,
        readiness: Vec::new(),
        liveness: None,
    }
}

#[given("a broker whose log directories are still recovering")]
async fn recovering_broker(probes: &mut ProbeScenario) {
    let state = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
    probes.address = Some(serve_probes(state.clone()).await);
    probes.state = Some(state);
    probes.directory = Some(tempdir().unwrap());
}

#[when("the broker finishes starting")]
async fn start_broker(probes: &mut ProbeScenario) {
    let state = probes.state.as_ref().expect("probe state exists").clone();
    let directory = probes.directory.as_ref().expect("log directory exists");
    probes.broker = Some(
        Broker::start_with_health(loopback_config(directory.path()), state)
            .await
            .unwrap(),
    );
}

#[given("a running broker with health probes")]
async fn running_broker(probes: &mut ProbeScenario) {
    recovering_broker(probes).await;
    start_broker(probes).await;
}

#[when("readiness is requested")]
async fn request_readiness(probes: &mut ProbeScenario) {
    let address = probes.address.expect("probe server exists");
    probes.readiness = vec![get(address, "/readyz").await];
}

#[when("liveness and readiness are requested")]
async fn request_both(probes: &mut ProbeScenario) {
    request_readiness(probes).await;
    probes.liveness = Some(get(probes.address.expect("probe server exists"), "/healthz").await);
}

#[when("readiness is sampled three times")]
async fn sample_readiness(probes: &mut ProbeScenario) {
    let address = probes.address.expect("probe server exists");
    probes.readiness.clear();
    for _ in 0..3 {
        probes.readiness.push(get(address, "/readyz").await);
        time::sleep(Duration::from_millis(50)).await;
    }
}

#[when("log directory recovery completes")]
fn finish_recovery(probes: &ProbeScenario) {
    probes
        .state
        .as_ref()
        .expect("probe state exists")
        .mark_log_dir_recovery_complete();
}

#[when("the listeners have bound")]
fn bind_listeners(probes: &ProbeScenario) {
    probes
        .state
        .as_ref()
        .expect("probe state exists")
        .mark_listeners_bound();
}

#[when("the broker begins shutting down")]
fn begin_shutdown(probes: &ProbeScenario) {
    probes
        .state
        .as_ref()
        .expect("probe state exists")
        .mark_shutting_down();
}

#[then("readiness is blocked by {condition}")]
fn blocked_readiness(probes: &ProbeScenario, condition: String) {
    let [ProbeResponse { status, body }] = probes.readiness.as_slice() else {
        panic!("one readiness response was requested")
    };
    check!(status.contains("503 Service Unavailable"), "{status}");
    check!(
        body.starts_with(&format!("not ready: {condition}: ")),
        "{body}"
    );
}

#[then("every readiness response is ready")]
fn ready_responses(probes: &ProbeScenario) {
    check!(!probes.readiness.is_empty());
    for ProbeResponse { status, body } in &probes.readiness {
        check!(status.contains("200 OK"), "{status}");
        check!(body == "ready\n");
    }
}

#[then("liveness remains healthy")]
fn healthy_liveness(probes: &ProbeScenario) {
    let ProbeResponse { status, body } = probes.liveness.as_ref().expect("liveness was requested");
    check!(status.contains("200 OK"), "{status}");
    check!(body == "ok\n");
}

async fn stop_broker(mut probes: ProbeScenario) {
    if let Some(broker) = probes.broker.take() {
        broker.shutdown().await;
    }
}

#[scenario(
    path = "tests/features/migrated/tests/health_probes.feature",
    name = "Liveness stays healthy while readiness changes at startup"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_is_503_during_startup_and_200_once_the_broker_is_up(probes: ProbeScenario) {
    stop_broker(probes).await;
}

#[scenario(
    path = "tests/features/migrated/tests/health_probes.feature",
    name = "Readiness names the first unfinished startup condition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_names_each_condition_as_startup_advances(probes: ProbeScenario) {
    stop_broker(probes).await;
}

#[scenario(
    path = "tests/features/migrated/tests/health_probes.feature",
    name = "A running broker stays ready across repeated probes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_stays_200_while_the_broker_runs(probes: ProbeScenario) {
    stop_broker(probes).await;
}

#[scenario(
    path = "tests/features/migrated/tests/health_probes.feature",
    name = "A ready broker leaves readiness when shutdown begins"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_is_503_when_shutting_down(probes: ProbeScenario) { stop_broker(probes).await; }

#[scenario(
    path = "tests/features/migrated/tests/health_probes.feature",
    name = "Shutdown takes precedence over unfinished startup"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutting_down_outranks_every_other_not_ready_condition(probes: ProbeScenario) {
    stop_broker(probes).await;
}
