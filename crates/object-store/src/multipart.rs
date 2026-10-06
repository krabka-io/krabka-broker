//! S3's incomplete-multipart listing, which `object_store` 0.13 does not expose,
//! and the signed bucket `GET` it shares with the WORM check.

use object_store::{
    aws::{AwsAuthorizer, AwsCredential},
    client::HttpRequestBody,
};
use serde::{Deserialize, de::DeserializeOwned};

use crate::{ObjectStoreError, S3Config, build::build_s3_store};

/// One upload that S3 has initiated but not completed or aborted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteMultipartUpload {
    /// Object key the upload would create.
    pub key: String,
    /// Backend upload identifier.
    pub upload_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListResponse {
    #[serde(default, rename = "Upload")]
    uploads: Vec<IncompleteUpload>,
    #[serde(default)]
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct IncompleteUpload {
    key: String,
    upload_id: String,
}

/// Lists every incomplete S3 multipart upload under `prefix`.
///
/// `object_store` exposes multipart creation and abort but not S3's
/// `ListMultipartUploads` operation, so the verifier makes this one signed S3
/// request directly with the same credential provider.
///
/// # Errors
///
/// Returns an error when the S3 configuration, credentials, request, or XML
/// response is invalid.
pub async fn list_s3_multipart_uploads(
    cfg: &S3Config,
    prefix: Option<&str>,
) -> Result<Vec<IncompleteMultipartUpload>, ObjectStoreError> {
    let store = build_s3_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let client = s3_http_client(cfg)?;
    let bucket_url = s3_bucket_url(cfg)?;
    let mut key_marker = None;
    let mut upload_id_marker = None;
    let mut found = Vec::new();

    loop {
        let mut url = bucket_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("uploads", "");
            if let Some(prefix) = prefix {
                query.append_pair("prefix", prefix);
            }
            if let Some(marker) = key_marker.as_deref() {
                query.append_pair("key-marker", marker);
            }
            if let Some(marker) = upload_id_marker.as_deref() {
                query.append_pair("upload-id-marker", marker);
            }
        }
        let page: ListResponse =
            signed_s3_get_xml(&client, &credential, cfg, url, "ListMultipartUploads").await?;
        found.extend(
            page.uploads
                .into_iter()
                .map(|upload| IncompleteMultipartUpload {
                    key: upload.key,
                    upload_id: upload.upload_id,
                }),
        );
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        upload_id_marker = page.next_upload_id_marker;
        if key_marker.is_none() && upload_id_marker.is_none() {
            return Err(ObjectStoreError::Backend(
                "truncated ListMultipartUploads response has no continuation marker".into(),
            ));
        }
    }
    Ok(found)
}

/// The path-style URL of `cfg`'s bucket, on its custom endpoint if it has one.
pub(crate) fn s3_bucket_url(cfg: &S3Config) -> Result<reqwest::Url, ObjectStoreError> {
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
pub(crate) fn s3_http_client(cfg: &S3Config) -> Result<reqwest::Client, ObjectStoreError> {
    let mut client = reqwest::Client::builder();
    if cfg.endpoint.is_some() {
        client = client.no_proxy();
    }
    client.build().map_err(backend)
}

/// An [`ObjectStoreError::Backend`] carrying `error`'s message.
pub(crate) fn backend(error: impl std::fmt::Display) -> ObjectStoreError {
    ObjectStoreError::Backend(error.to_string())
}

/// Sends `request` and returns the response body.
///
/// A non-success status is an error naming `operation`, the status and the
/// body.
pub(crate) async fn read_ok_body(
    request: reqwest::RequestBuilder,
    operation: &str,
) -> Result<bytes::Bytes, ObjectStoreError> {
    let response = request.send().await.map_err(backend)?;
    let status = response.status();
    let body = response.bytes().await.map_err(backend)?;
    if !status.is_success() {
        return Err(ObjectStoreError::Backend(format!(
            "{operation} returned {status}: {}",
            String::from_utf8_lossy(&body)
        )));
    }
    Ok(body)
}

/// Sends a SigV4-signed `GET` for `url` and decodes the XML response body.
///
/// `operation` names the S3 API call in the errors.
pub(crate) async fn signed_s3_get_xml<T: DeserializeOwned>(
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
    )
    .await?;
    quick_xml::de::from_reader(body.as_ref())
        .map_err(|error| ObjectStoreError::Backend(format!("{operation}: {error}")))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    async fn serve_pages(
        pages: Vec<(&'static str, &'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (expected_request, status, body) in pages {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    let read = socket.read(&mut chunk).await.unwrap();
                    request.extend_from_slice(&chunk[..read]);
                }
                let request = String::from_utf8(request).unwrap();
                check!(request.starts_with(expected_request));
                check!(
                    request
                        .to_ascii_lowercase()
                        .contains("authorization: aws4-hmac-sha256")
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        (endpoint, server)
    }

    fn config(endpoint: String) -> S3Config {
        S3Config {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
            allow_http: true,
            ..Default::default()
        }
    }

    #[test]
    fn parses_incomplete_uploads_and_pagination() {
        let page: ListResponse = quick_xml::de::from_str(
            r"<ListMultipartUploadsResult>
                <IsTruncated>true</IsTruncated>
                <NextKeyMarker>worm/b</NextKeyMarker>
                <NextUploadIdMarker>next</NextUploadIdMarker>
                <Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload>
                <Upload><Key>worm/b</Key><UploadId>two</UploadId></Upload>
            </ListMultipartUploadsResult>",
        )
        .unwrap();

        check!(page.is_truncated);
        check!(page.next_key_marker.as_deref() == Some("worm/b"));
        check!(page.next_upload_id_marker.as_deref() == Some("next"));
        check!(page.uploads.len() == 2);
        check!(page.uploads[0].key == "worm/a");
        check!(page.uploads[0].upload_id == "one");
    }

    #[tokio::test]
    async fn lists_incomplete_uploads_under_the_prefix() {
        let body = "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload></ListMultipartUploadsResult>";
        let (endpoint, server) = serve_pages(vec![(
            "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
            "200 OK",
            body,
        )])
        .await;

        let uploads = list_s3_multipart_uploads(&config(endpoint), Some("worm/"))
            .await
            .unwrap();
        server.await.unwrap();

        check!(
            uploads
                == vec![IncompleteMultipartUpload {
                    key: "worm/a".into(),
                    upload_id: "one".into(),
                }]
        );
    }

    #[tokio::test]
    async fn follows_multipart_pagination_markers() {
        let first = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>worm/a</NextKeyMarker><NextUploadIdMarker>one</NextUploadIdMarker><Upload><Key>worm/a</Key><UploadId>one</UploadId></Upload></ListMultipartUploadsResult>";
        let second = "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>worm/b</Key><UploadId>two</UploadId></Upload></ListMultipartUploadsResult>";
        let (endpoint, server) = serve_pages(vec![
            (
                "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
                "200 OK",
                first,
            ),
            (
                "GET /bucket?uploads=&prefix=worm%2F&key-marker=worm%2Fa&upload-id-marker=one HTTP/1.1",
                "200 OK",
                second,
            ),
        ])
        .await;

        let uploads = list_s3_multipart_uploads(&config(endpoint), Some("worm/"))
            .await
            .unwrap();
        server.await.unwrap();

        check!(uploads.len() == 2);
        check!(uploads[1].key == "worm/b");
        check!(uploads[1].upload_id == "two");
    }

    #[tokio::test]
    async fn rejects_truncated_page_without_markers() {
        let body = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>";
        let (endpoint, server) =
            serve_pages(vec![("GET /bucket?uploads= HTTP/1.1", "200 OK", body)]).await;

        let error = list_s3_multipart_uploads(&config(endpoint), None)
            .await
            .unwrap_err();
        server.await.unwrap();

        check!(error.to_string().contains("no continuation marker"));
    }

    #[tokio::test]
    async fn reports_unsuccessful_multipart_listing() {
        let (endpoint, server) = serve_pages(vec![(
            "GET /bucket?uploads= HTTP/1.1",
            "403 Forbidden",
            "denied",
        )])
        .await;

        let error = list_s3_multipart_uploads(&config(endpoint), None)
            .await
            .unwrap_err();
        server.await.unwrap();

        check!(error.to_string().contains("403 Forbidden: denied"));
    }

    #[tokio::test]
    async fn a_request_that_never_reaches_the_store_is_a_backend_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let client = s3_http_client(&config(endpoint.clone())).unwrap();
        let error = read_ok_body(client.get(endpoint), "ListMultipartUploads")
            .await
            .unwrap_err();

        assert2::assert!(let ObjectStoreError::Backend(message) = error);
        check!(message.contains("error sending request"), "{message}");
    }
}
