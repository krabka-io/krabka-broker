//! Authenticated, loopback-only pprof administration listener.

#[cfg(not(target_family = "wasm"))]
use {
    crate::http_server::spawn,
    axum::{
        Router,
        extract::{Request, State},
        http::{StatusCode, header::AUTHORIZATION},
        middleware::{self, Next},
        response::{IntoResponse, Response},
    },
    std::{
        net::SocketAddr, path::Path,
        sync::Arc,
        time::{Duration, Instant},
    },
    subtle::ConstantTimeEq as _,
    tokio::sync::{Mutex, Semaphore},
    tokio_util::sync::CancellationToken,
};

#[cfg(not(target_family = "wasm"))]
#[derive(Clone)]
struct Protection {
    token: Arc<[u8]>,
    active: Arc<Semaphore>,
    last_started: Arc<Mutex<Option<Instant>>>,
}

#[cfg(not(target_family = "wasm"))]
fn router(
    token: Vec<u8>,
    profiling: krabka_telemetry::profiling::ProfilingConfig,
) -> Result<Router, krabka_telemetry::profiling::ProfilingError> {
    let protection = Protection {
        token: token.into(),
        active: Arc::new(Semaphore::new(1)),
        last_started: Arc::new(Mutex::new(None)),
    };
    Ok(
        krabka_telemetry::profiling::pprof_router_with_config(profiling)?.layer(
            middleware::from_fn_with_state(protection, authorize_and_limit),
        ),
    )
}

#[cfg(not(target_family = "wasm"))]
async fn authorize_and_limit(
    State(state): State<Protection>,
    request: Request,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "));
    let authorized = supplied.is_some_and(|supplied| {
        supplied.len() == state.token.len() && bool::from(supplied.ct_eq(&state.token))
    });
    if !authorized {
        tracing::warn!(path = %request.uri().path(), "profiling request denied");
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(permit) = state.active.clone().try_acquire_owned() else {
        tracing::warn!(path = %request.uri().path(), "concurrent profiling request rejected");
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let mut last_started = state.last_started.lock().await;
    if last_started.is_some_and(|last| last.elapsed() < Duration::from_secs(1)) {
        tracing::warn!(path = %request.uri().path(), "repeated profiling request rejected");
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    *last_started = Some(Instant::now());
    drop(last_started);
    tracing::info!(path = %request.uri().path(), "authorized profiling request started");
    let response = next.run(request).await;
    drop(permit);
    response
}

/// Bind the explicitly enabled profiling listener and serve until shutdown.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn run(
    addr: SocketAddr,
    token_file: &Path,
    profiling: krabka_telemetry::profiling::ProfilingConfig,
    shutdown: CancellationToken,
) -> Result<SocketAddr, krabka_telemetry::profiling::ProfilingError> {
    let token = std::fs::read(token_file)?;
    let token = token.strip_suffix(b"\n").unwrap_or(&token).to_vec();
    if token.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "profiling authentication token file is empty",
        )
        .into());
    }
    let app = router(token, profiling)?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "authenticated profiling server listening");
    spawn(
        listener,
        app,
        shutdown,
        |error| tracing::warn!(%error, "profiling server error"),
    );
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        axum::{body::Body, http::Request},
        tower::ServiceExt as _,
    };

    #[tokio::test]
    async fn profiling_rejects_unauthenticated_requests() {
        let response = router(b"secret".to_vec(), Default::default())
            .unwrap()
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/profile")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status() == StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_bearer_token_reaches_protected_router() {
        let app = router(b"secret".to_vec(), Default::default()).unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/not-a-profile")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status() == StatusCode::NOT_FOUND);

        let repeated = app
            .oneshot(
                Request::builder()
                    .uri("/not-a-profile")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(repeated.status() == StatusCode::TOO_MANY_REQUESTS);
    }
}
