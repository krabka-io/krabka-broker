//! The readiness rule and the two routes, driven with a stub metadata
//! authority so no quorum has to exist.

use assert2::assert;
use axum::{body::Body, http::Request};
use tower::ServiceExt as _;

use super::*;

struct StubProgress {
    node: i64,
    quorum: i64,
}

impl MetadataProgress for StubProgress {
    fn node_metadata_offset(&self) -> i64 {
        self.node
    }
    fn quorum_committed_offset(&self) -> i64 {
        self.quorum
    }
}

/// How far a case has taken the broker through startup.
#[derive(Clone, Copy)]
enum Phase {
    Nothing,
    LogDirsRecovered,
    ListenersBound,
    /// Quorum reached, with this node's offset and the quorum's committed
    /// offset.
    QuorumReached(i64, i64),
}

fn state_at(phase: Phase, max_lag: u64) -> HealthState {
    let state = HealthState::new(max_lag);
    if matches!(phase, Phase::Nothing) {
        return state;
    }
    state.mark_log_dir_recovery_complete();
    if matches!(phase, Phase::LogDirsRecovered) {
        return state;
    }
    state.mark_listeners_bound();
    if let Phase::QuorumReached(node, quorum) = phase {
        state.install_metadata_progress(Arc::new(StubProgress { node, quorum }));
    }
    state
}

async fn get(state: HealthState, path: &str) -> (StatusCode, String) {
    let resp = router(state)
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[test]
fn readiness_names_the_first_unmet_condition() {
    let cases = [
        (Phase::Nothing, Err(NotReady::LogDirRecovery)),
        (Phase::LogDirsRecovered, Err(NotReady::ListenersBound)),
        (
            Phase::ListenersBound,
            Err(NotReady::MetadataQuorumUnreached),
        ),
        // Exactly at the bound is still ready; one past it is not.
        (Phase::QuorumReached(900, 1000), Ok(())),
        (
            Phase::QuorumReached(899, 1000),
            Err(NotReady::MetadataLag {
                lag: 101,
                max_lag: 100,
            }),
        ),
        // A node that has applied past the last offset it heard the quorum
        // commit is caught up, not 100 records ahead of ready.
        (Phase::QuorumReached(1001, 1000), Ok(())),
        // Both at the pre-first-record sentinel: a freshly bootstrapped
        // cluster is caught up, not unknown.
        (Phase::QuorumReached(-1, -1), Ok(())),
    ];
    for (phase, expected) in cases {
        assert!(state_at(phase, 100).readiness() == expected);
    }
}

#[tokio::test]
async fn healthz_is_200_at_every_startup_phase() {
    for phase in [
        Phase::Nothing,
        Phase::LogDirsRecovered,
        Phase::ListenersBound,
        Phase::QuorumReached(0, 1_000_000),
    ] {
        let (status, body) = get(state_at(phase, 100), "/healthz").await;
        assert!(status == StatusCode::OK);
        assert!(body == "ok\n");
    }
}

#[tokio::test]
async fn readyz_body_names_the_failing_condition() {
    let cases = [
        (Phase::Nothing, "log_dir_recovery"),
        (Phase::LogDirsRecovered, "listeners_bound"),
        (Phase::ListenersBound, "metadata_quorum_unreached"),
        (Phase::QuorumReached(0, 1000), "metadata_lag"),
    ];
    for (phase, condition) in cases {
        let (status, body) = get(state_at(phase, 100), "/readyz").await;
        assert!(status == StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            body.starts_with(&format!("not ready: {condition}: ")),
            "{body}"
        );
    }
}

#[tokio::test]
async fn readyz_is_200_once_every_condition_holds() {
    let (status, body) = get(state_at(Phase::QuorumReached(1000, 1000), 100), "/readyz").await;
    assert!(status == StatusCode::OK);
    assert!(body == "ready\n");
}

#[test]
fn metadata_progress_is_installed_once() {
    let state = HealthState::new(100);
    state.mark_log_dir_recovery_complete();
    state.mark_listeners_bound();
    state.install_metadata_progress(Arc::new(StubProgress {
        node: 0,
        quorum: 1000,
    }));
    // A second install must not replace the first: the authority this node
    // joined is the one its readiness is measured against, and a later one
    // that reported no lag would mask a node that is genuinely behind.
    state.install_metadata_progress(Arc::new(StubProgress {
        node: 1000,
        quorum: 1000,
    }));
    assert!(
        state.readiness()
            == Err(NotReady::MetadataLag {
                lag: 1000,
                max_lag: 100,
            })
    );
}
