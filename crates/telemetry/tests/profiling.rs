#![cfg(test)]

#[cfg(unix)]
mod unix_only {
    use {
        axum::{
            Router,
            body::{self, Body},
            http::{Request, StatusCode},
            routing::get,
        },
        flate2::read::GzDecoder,
        krabka_telemetry::profiling,
        std::{env, io::Read as _, net::SocketAddr, time::Duration},
        tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            time,
        },
        tower::ServiceExt,
    }; // for `oneshot`

    #[tokio::test]
    async fn cpu_profile_endpoint_returns_pprof_bytes() {
        let app = profiling::pprof_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/profile?seconds=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(resp.status() == StatusCode::OK);
        let body = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        // A pprof CPU profile is a non-empty gzip/protobuf blob.
        assert2::assert!(!body.is_empty());
        assert2::assert!(body.starts_with(&[0x1f, 0x8b]));

        let mut decoder = GzDecoder::new(&body[..]);
        let mut raw = Vec::new();
        decoder
            .read_to_end(&mut raw)
            .expect("profile body should be valid gzip");
        assert2::assert!(!raw.is_empty());
    }

    async fn unused_loopback_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    async fn http_get(addr: SocketAddr, path: &str) -> String {
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

        for _ in 0..50 {
            match TcpStream::connect(addr).await {
                Ok(mut stream) => {
                    stream.write_all(request.as_bytes()).await.unwrap();
                    let mut response = Vec::new();
                    stream.read_to_end(&mut response).await.unwrap();
                    return String::from_utf8_lossy(&response).into_owned();
                }
                Err(_) => time::sleep(Duration::from_millis(20)).await,
            }
        }

        panic!("admin server at {addr} did not accept a connection");
    }

    #[tokio::test]
    async fn serve_admin_binds_and_serves_extra_route() {
        let addr = unused_loopback_addr().await;
        profiling::serve_admin(
            addr,
            Router::new().route("/ready", get(|| async { "ready" })),
        )
        .await
        .unwrap();

        let response = http_get(addr, "/ready").await;
        assert2::assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert2::assert!(response.contains("\r\n\r\nready"));
    }

    #[tokio::test]
    async fn serve_admin_from_env_uses_default_addr() {
        if env::var_os("KRABKA_ADMIN_LISTEN_ADDR").is_some() {
            return;
        }

        let addr = unused_loopback_addr().await;
        profiling::serve_admin_from_env(&addr.to_string())
            .await
            .unwrap();

        let response = http_get(addr, "/__missing").await;
        assert2::assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[tokio::test]
    async fn serve_admin_from_env_with_uses_default_addr_and_extra_routes() {
        if env::var_os("KRABKA_ADMIN_LISTEN_ADDR").is_some() {
            return;
        }

        let addr = unused_loopback_addr().await;
        profiling::serve_admin_from_env_with(
            &addr.to_string(),
            Router::new().route("/ready-env", get(|| async { "ready-env" })),
        )
        .await
        .unwrap();

        let response = http_get(addr, "/ready-env").await;
        assert2::assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert2::assert!(response.contains("\r\n\r\nready-env"));
    }
}
