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
        net::SocketAddr,
        path::Path,
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
    Ok(protect(
        krabka_telemetry::profiling::pprof_router_with_config(profiling)?,
        token,
    ))
}

/// Wraps `inner` so that every request needs the bearer `token`, at most one
/// request runs at a time, and a new one starts at most once a second.
#[cfg(not(target_family = "wasm"))]
fn protect(inner: Router, token: Vec<u8>) -> Router {
    let protection = Protection {
        token: token.into(),
        active: Arc::new(Semaphore::new(1)),
        last_started: Arc::new(Mutex::new(None)),
    };
    inner.layer(middleware::from_fn_with_state(
        protection,
        authorize_and_limit,
    ))
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

/// `BrokerConfig::validate` refuses `profiling_listen_addr` on this platform,
/// so this is unreachable from a validated config.
#[cfg(target_family = "wasm")]
pub(crate) fn run(
    addr: std::net::SocketAddr,
    _token_file: &std::path::Path,
    _profiling: krabka_telemetry::profiling::ProfilingConfig,
    _shutdown: tokio_util::sync::CancellationToken,
) -> std::future::Ready<Result<std::net::SocketAddr, krabka_telemetry::profiling::ProfilingError>> {
    std::future::ready(Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "cannot serve profiling on {addr}: the profiling server is unavailable on this \
             platform"
        ),
    )
    .into()))
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use {
        super::*,
        assert2::assert,
        axum::{body::Body, http::Request, routing::get},
        tokio::{
            io::{AsyncReadExt as _, AsyncWriteExt as _},
            sync::Notify,
        },
        tower::ServiceExt as _,
    };

    fn request(uri: &str, authorization: Option<&str>) -> Request<Body> {
        let builder = Request::builder().uri(uri);
        match authorization {
            Some(value) => builder.header(AUTHORIZATION, value),
            None => builder,
        }
        .body(Body::empty())
        .unwrap()
    }

    #[tokio::test]
    async fn profiling_rejects_requests_without_the_right_bearer_token() {
        let cases = [
            ("no header", None),
            ("not a bearer scheme", Some("Basic secret")),
            ("a shorter token", Some("Bearer secre")),
            ("a longer token", Some("Bearer secrets")),
            ("a different token", Some("Bearer secreT")),
        ];
        for (name, authorization) in cases {
            let response = router(
                b"secret".to_vec(),
                krabka_telemetry::profiling::ProfilingConfig::default(),
            )
            .unwrap()
            .oneshot(request("/debug/pprof/profile", authorization))
            .await
            .unwrap();
            assert!(response.status() == StatusCode::UNAUTHORIZED, "{name}");
        }
    }

    #[tokio::test]
    async fn valid_bearer_token_reaches_protected_router() {
        let app = router(
            b"secret".to_vec(),
            krabka_telemetry::profiling::ProfilingConfig::default(),
        )
        .unwrap();
        let response = app
            .clone()
            .oneshot(request("/not-a-profile", Some("Bearer secret")))
            .await
            .unwrap();
        assert!(response.status() == StatusCode::NOT_FOUND);

        let repeated = app
            .oneshot(request("/not-a-profile", Some("Bearer secret")))
            .await
            .unwrap();
        assert!(repeated.status() == StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn a_second_request_is_rejected_while_one_is_running() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let inner = {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            Router::new().route(
                "/slow",
                get(move || async move {
                    entered.notify_one();
                    release.notified().await;
                    "done"
                }),
            )
        };
        let app = protect(inner, b"secret".to_vec());
        let first = tokio::spawn(app.clone().oneshot(request("/slow", Some("Bearer secret"))));
        entered.notified().await;

        let concurrent = app
            .oneshot(request("/slow", Some("Bearer secret")))
            .await
            .unwrap();
        assert!(concurrent.status() == StatusCode::TOO_MANY_REQUESTS);

        release.notify_one();
        assert!(first.await.unwrap().unwrap().status() == StatusCode::OK);
    }

    #[test]
    fn an_invalid_profiling_policy_is_refused() {
        let profiling = krabka_telemetry::profiling::ProfilingConfig {
            profiling_cpu_default_duration: krabka_units::secs(61),
            ..krabka_telemetry::profiling::ProfilingConfig::default()
        };
        assert!(let Err(krabka_telemetry::profiling::ProfilingError::Config(_)) =
            router(b"secret".to_vec(), profiling));
    }

    async fn status_line(addr: SocketAddr, authorization: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!(
                    "GET /not-a-profile HTTP/1.1\r\nHost: localhost\r\n{authorization}\
                     Connection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response.lines().next().unwrap_or_default().to_owned()
    }

    #[tokio::test]
    async fn run_serves_with_the_token_from_the_file_without_its_newline() {
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "secret\n").unwrap();
        let shutdown = CancellationToken::new();
        let bound = run(
            "127.0.0.1:0".parse().unwrap(),
            &token_file,
            krabka_telemetry::profiling::ProfilingConfig::default(),
            shutdown.clone(),
        )
        .await
        .unwrap();

        assert!(status_line(bound, "").await == "HTTP/1.1 401 Unauthorized");
        assert!(
            status_line(bound, "Authorization: Bearer secret\r\n").await
                == "HTTP/1.1 404 Not Found"
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn run_refuses_a_missing_or_empty_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n").unwrap();
        let cases = [
            (
                "missing",
                dir.path().join("missing"),
                std::io::ErrorKind::NotFound,
            ),
            ("empty", empty, std::io::ErrorKind::InvalidInput),
        ];
        for (name, token_file, kind) in cases {
            let error = run(
                "127.0.0.1:0".parse().unwrap(),
                &token_file,
                krabka_telemetry::profiling::ProfilingConfig::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&error, krabka_telemetry::profiling::ProfilingError::Io(error)
                    if error.kind() == kind),
                "{name}"
            );
        }
    }
}
