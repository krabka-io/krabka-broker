//! S3's incomplete-multipart listing, which `object_store` 0.13 does not expose.

use {
    crate::{
        ObjectStoreError, S3Config,
        build::build_s3_store,
        control_plane::{s3_bucket_url, s3_http_client, signed_s3_get_xml},
    },
    serde::Deserialize,
};

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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{HttpPage, HttpServer, s3_config, serve_http_pages},
        assert2::check,
    };

    async fn with_listing<T>(
        pages: Vec<HttpPage>,
        prefix: Option<&str>,
        extract: impl FnOnce(Result<Vec<IncompleteMultipartUpload>, ObjectStoreError>) -> T,
    ) -> T {
        let HttpServer { endpoint, server } = serve_http_pages(pages, true).await;
        let result = extract(list_s3_multipart_uploads(&config(endpoint), prefix).await);
        server.await.unwrap();
        result
    }

    async fn check_listing_error(pages: Vec<HttpPage>, reason: &str) {
        let error = with_listing(pages, None, Result::unwrap_err).await;
        check!(error.to_string().contains(reason));
    }

    fn config(endpoint: String) -> S3Config { s3_config("bucket", endpoint) }

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
        let uploads = with_listing(
            vec![HttpPage::new(
                "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
                "200 OK",
                body,
            )],
            Some("worm/"),
            Result::unwrap,
        )
        .await;

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
        let uploads = with_listing(vec![
            HttpPage::new(
                "GET /bucket?uploads=&prefix=worm%2F HTTP/1.1",
                "200 OK",
                first,
            ),
            HttpPage::new(
                "GET /bucket?uploads=&prefix=worm%2F&key-marker=worm%2Fa&upload-id-marker=one HTTP/1.1",
                "200 OK",
                second,
            ),
        ], Some("worm/"), Result::unwrap).await;

        check!(uploads.len() == 2);
        check!(uploads[1].key == "worm/b");
        check!(uploads[1].upload_id == "two");
    }

    #[tokio::test]
    async fn rejects_truncated_page_without_markers() {
        let body = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>";
        check_listing_error(
            vec![HttpPage::new(
                "GET /bucket?uploads= HTTP/1.1",
                "200 OK",
                body,
            )],
            "no continuation marker",
        )
        .await;
    }

    #[tokio::test]
    async fn reports_unsuccessful_multipart_listing() {
        check_listing_error(
            vec![HttpPage::new(
                "GET /bucket?uploads= HTTP/1.1",
                "403 Forbidden",
                "denied",
            )],
            "403 Forbidden: denied",
        )
        .await;
    }
}
