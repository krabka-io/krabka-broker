//! S3 control-plane checks required before enabling a WORM writer.

use object_store::{aws::AwsAuthorizer, client::HttpRequestBody};
use serde::Deserialize;

use crate::{ObjectStoreError, S3Config, build::build_s3_store};

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VersioningConfiguration {
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectLockConfiguration {
    object_lock_enabled: Option<String>,
    rule: Option<ObjectLockRule>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectLockRule {
    default_retention: Option<DefaultRetention>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DefaultRetention {
    mode: Option<String>,
    days: Option<u64>,
    years: Option<u64>,
}

/// Confirms that an S3 bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Names the failed control-plane request or the concrete missing policy.
pub async fn verify_s3_worm_bucket(cfg: &S3Config) -> Result<(), ObjectStoreError> {
    let versioning: VersioningConfiguration = get_bucket_xml(cfg, "versioning").await?;
    if versioning.status.as_deref() != Some("Enabled") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires versioning Enabled, found {}",
            cfg.bucket,
            versioning.status.as_deref().unwrap_or("unset")
        )));
    }

    let lock: ObjectLockConfiguration = get_bucket_xml(cfg, "object-lock").await?;
    if lock.object_lock_enabled.as_deref() != Some("Enabled") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires Object Lock Enabled",
            cfg.bucket
        )));
    }
    let retention = lock
        .rule
        .and_then(|rule| rule.default_retention)
        .ok_or_else(|| {
            ObjectStoreError::InvalidConfig(format!(
                "WORM bucket `{}` has no Object Lock default retention",
                cfg.bucket
            ))
        })?;
    if retention.mode.as_deref() != Some("COMPLIANCE") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` default retention must use COMPLIANCE mode, found {}",
            cfg.bucket,
            retention.mode.as_deref().unwrap_or("unset")
        )));
    }
    if retention.days.unwrap_or(0) == 0 && retention.years.unwrap_or(0) == 0 {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` default retention has no positive Days or Years",
            cfg.bucket
        )));
    }
    Ok(())
}

async fn get_bucket_xml<T: for<'de> Deserialize<'de>>(
    cfg: &S3Config,
    api: &str,
) -> Result<T, ObjectStoreError> {
    let operation = match api {
        "versioning" => "GetBucketVersioning",
        "object-lock" => "GetObjectLockConfiguration",
        _ => "GetBucketConfiguration",
    };
    let store = build_s3_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let endpoint = cfg.endpoint.as_ref().map_or_else(
        || format!("https://s3.{}.amazonaws.com/{}", cfg.region, cfg.bucket),
        |endpoint| format!("{}/{}", endpoint.trim_end_matches('/'), cfg.bucket),
    );
    let mut url = reqwest::Url::parse(&endpoint)
        .map_err(|error| ObjectStoreError::InvalidConfig(format!("S3 endpoint: {error}")))?;
    url.query_pairs_mut().append_pair(api, "");
    let mut signed = http::Request::get(url.as_str())
        .body(HttpRequestBody::empty())
        .map_err(|error| ObjectStoreError::Backend(error.to_string()))?;
    AwsAuthorizer::new(&credential, "s3", &cfg.region).authorize(&mut signed, None);
    let mut client = reqwest::Client::builder();
    if cfg.endpoint.is_some() {
        client = client.no_proxy();
    }
    let response = client
        .build()
        .map_err(|error| ObjectStoreError::Backend(error.to_string()))?
        .get(url)
        .headers(signed.into_parts().0.headers)
        .send()
        .await
        .map_err(|error| ObjectStoreError::Backend(error.to_string()))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| ObjectStoreError::Backend(error.to_string()))?;
    if !status.is_success() {
        return Err(ObjectStoreError::Backend(format!(
            "{operation} returned {status}: {}",
            String::from_utf8_lossy(&body)
        )));
    }
    quick_xml::de::from_reader(body.as_ref())
        .map_err(|error| ObjectStoreError::Backend(format!("{operation}: {error}")))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[tokio::test]
    async fn requires_versioning_and_positive_compliance_retention() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (query, body) in [
                (
                    "versioning",
                    "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
                ),
                (
                    "object-lock",
                    "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>",
                ),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    let read = socket.read(&mut chunk).await.unwrap();
                    request.extend_from_slice(&chunk[..read]);
                }
                let request = String::from_utf8(request).unwrap();
                check!(request.starts_with(&format!("GET /bucket?{query}= HTTP/1.1")));
                check!(
                    request
                        .to_ascii_lowercase()
                        .contains("authorization: aws4-hmac-sha256")
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let cfg = S3Config {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
            allow_http: true,
            ..Default::default()
        };

        verify_s3_worm_bucket(&cfg).await.unwrap();
        server.await.unwrap();
    }
}
