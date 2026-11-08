//! Shared HTTP retry policy and signed S3 bucket requests for control-plane checks.

use {
    crate::{ObjectStoreError, S3Config},
    object_store::{
        RetryConfig,
        aws::{AwsAuthorizer, AwsCredential},
        client::HttpRequestBody,
    },
    quick_xml::de,
    serde::de::DeserializeOwned,
    std::fmt,
    tokio::time,
};

/// The path-style URL of `cfg`'s bucket, on its custom endpoint if it has one.
pub fn s3_bucket_url(cfg: &S3Config) -> Result<reqwest::Url, ObjectStoreError> {
    let endpoint = cfg.endpoint.as_ref().map_or_else(
        || format!("https://s3.{}.amazonaws.com/{}", cfg.region, cfg.bucket),
        |endpoint| format!("{}/{}", endpoint.trim_end_matches('/'), cfg.bucket),
    );
    reqwest::Url::parse(&endpoint)
        .map_err(|error| ObjectStoreError::InvalidConfig(format!("S3 endpoint: {error}")))
}

/// The HTTP client for direct S3 bucket requests.
///
/// A custom endpoint bypasses the system proxy, the way a local or in-cluster
/// S3 implementation needs.
pub fn s3_http_client(cfg: &S3Config) -> Result<reqwest::Client, ObjectStoreError> {
    let mut client = reqwest::Client::builder();
    client = client
        .timeout(cfg.request_timeout)
        .connect_timeout(cfg.connect_timeout);
    if cfg.endpoint.is_some() {
        client = client.no_proxy();
    }
    client.build().map_err(backend)
}

/// An [`ObjectStoreError::Backend`] carrying `error`'s message.
pub fn backend(error: impl fmt::Display) -> ObjectStoreError {
    ObjectStoreError::Backend(error.to_string())
}

/// Sends `request` and returns the response body.
///
/// A non-success status is an error naming `operation`, the status and the
/// body. Transient transport failures, 408, 429, and server errors retry
/// within `retry`'s attempt and wall-clock bounds. When those bounds run out,
/// the error is [`ObjectStoreError::Unavailable`]; any other failure is
/// [`ObjectStoreError::Backend`].
pub async fn read_ok_body(
    request: reqwest::RequestBuilder,
    operation: &str,
    retry: &RetryConfig,
) -> Result<bytes::Bytes, ObjectStoreError> {
    let attempts = async move {
        let mut retries = 0;
        let mut backoff = retry.backoff.init_backoff;
        loop {
            let attempt = request.try_clone().ok_or_else(|| {
                backend(format!("{operation}: control-plane GET cannot be cloned"))
            })?;
            let (message, retryable) = match attempt.send().await {
                Ok(response) => {
                    let status = response.status();
                    let retryable_status = status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::REQUEST_TIMEOUT;
                    match response.bytes().await {
                        Ok(body) if status.is_success() => return Ok(body),
                        Ok(body) => (
                            format!(
                                "{operation} returned {status}: {}",
                                String::from_utf8_lossy(&body)
                            ),
                            retryable_status,
                        ),
                        Err(error) => (
                            format!(
                                "{operation} returned {status}, but its body could not be read: {error}"
                            ),
                            status.is_success() || retryable_status,
                        ),
                    }
                }
                Err(error) => (
                    error.to_string(),
                    !error.is_builder() && !error.is_redirect(),
                ),
            };
            if !retryable {
                return Err(ObjectStoreError::Backend(message));
            }
            if retries >= retry.max_retries {
                return Err(ObjectStoreError::Unavailable(message));
            }
            retries += 1;
            time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(retry.backoff.max_backoff);
        }
    };
    time::timeout(retry.retry_timeout, attempts)
        .await
        .map_err(|_| {
            ObjectStoreError::Unavailable(format!("{operation}: retry deadline exceeded"))
        })?
}

/// Sends a SigV4-signed `GET` for `url` and decodes the XML response body.
///
/// `operation` names the S3 API call in the errors.
pub async fn signed_s3_get_xml<T: DeserializeOwned>(
    client: &reqwest::Client,
    credential: &AwsCredential,
    cfg: &S3Config,
    url: reqwest::Url,
    operation: &str,
) -> Result<T, ObjectStoreError> {
    let mut signed = http::Request::get(url.as_str())
        .body(HttpRequestBody::empty())
        .map_err(backend)?;
    AwsAuthorizer::new(credential, "s3", &cfg.region)
        .try_authorize(&mut signed, None)
        .map_err(backend)?;
    let body = read_ok_body(
        client.get(url).headers(signed.into_parts().0.headers),
        operation,
        &RetryConfig {
            max_retries: cfg.max_retries,
            retry_timeout: cfg.retry_timeout,
            ..RetryConfig::default()
        },
    )
    .await?;
    de::from_reader(body.as_ref())
        .map_err(|error| ObjectStoreError::Backend(format!("{operation}: {error}")))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{
            HttpListener, HttpPage, HttpServer, accept_http_request, http_listener, read_request,
            respond, s3_config, serve_http_pages,
        },
        assert2::check,
        std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering::SeqCst},
        },
        tokio::io::AsyncWriteExt as _,
    };

    #[tokio::test]
    async fn a_request_that_never_reaches_the_store_can_be_retried() {
        let HttpListener { listener, endpoint } = http_listener().await;
        drop(listener);

        let client = s3_http_client(&s3_config("bucket", endpoint.clone())).unwrap();
        let error = read_ok_body(client.get(endpoint), "ListMultipartUploads", &RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
        .await
        .unwrap_err();

        assert2::assert!(let ObjectStoreError::Unavailable(message) = error);
        check!(message.contains("error sending request"), "{message}");
    }

    /// How a control-plane request ended.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Answered,
        Unavailable,
        Refused,
        Other,
    }

    impl Outcome {
        fn of(result: &Result<bytes::Bytes, ObjectStoreError>) -> Self {
            match result {
                Ok(_) => Self::Answered,
                Err(ObjectStoreError::Unavailable(_)) => Self::Unavailable,
                Err(ObjectStoreError::Backend(_)) => Self::Refused,
                Err(_) => Self::Other,
            }
        }
    }

    #[yare::parameterized(
        no_retries = { 0, vec!["503 Service Unavailable"], Outcome::Unavailable },
        retry_503 = { 1, vec!["503 Service Unavailable", "200 OK"], Outcome::Answered },
        no_retry_forbidden = { 2, vec!["403 Forbidden"], Outcome::Refused },
        retry_rate_limit = { 1, vec!["429 Too Many Requests", "200 OK"], Outcome::Answered },
    )]
    #[test_macro(tokio::test)]
    async fn control_plane_retries_transient_statuses_only_within_the_attempt_budget(
        max_retries: usize,
        statuses: Vec<&str>,
        want: Outcome,
    ) {
        let pages = statuses
            .into_iter()
            .map(|status| HttpPage::new("GET / HTTP/1.1", status, "result"))
            .collect();
        let HttpServer { endpoint, server } = serve_http_pages(pages, false).await;
        let result = read_ok_body(
            reqwest::Client::new().get(endpoint),
            "test control-plane",
            &RetryConfig {
                max_retries,
                ..RetryConfig::default()
            },
        )
        .await;
        check!(Outcome::of(&result) == want);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_persistently_failing_control_plane_stops_at_the_retry_count() {
        let HttpListener { listener, endpoint } = http_listener().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                observed.fetch_add(1, SeqCst);
                respond(&mut socket, "503 Service Unavailable", "retry").await;
            }
        });
        let result = read_ok_body(
            reqwest::Client::new().get(endpoint),
            "failing control-plane",
            &RetryConfig {
                max_retries: 1,
                ..RetryConfig::default()
            },
        )
        .await;
        server.abort();
        check!(Outcome::of(&result) == Outcome::Unavailable);
        check!(attempts.load(SeqCst) == 2);
    }

    #[tokio::test]
    async fn a_truncated_permission_error_preserves_status_and_does_not_retry() {
        let HttpListener { listener, endpoint } = http_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_http_request(listener).await;
            socket
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\ncontent-length: 100\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let error = read_ok_body(
            reqwest::Client::new().get(endpoint),
            "permission check",
            &RetryConfig {
                max_retries: 2,
                ..RetryConfig::default()
            },
        )
        .await
        .unwrap_err();
        server.await.unwrap();
        assert2::assert!(let ObjectStoreError::Backend(message) = error);
        check!(message.contains("permission check returned 403 Forbidden"));
    }
}
