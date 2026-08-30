//! S3 control-plane checks required before enabling a WORM writer.

use object_store::{aws::AwsAuthorizer, client::HttpRequestBody};
use serde::Deserialize;

use crate::{
    GcsConfig, ObjectStoreError, S3Config,
    build::{build_gcs_store, build_s3_store},
};

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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsBucketPolicy {
    versioning: Option<GcsVersioning>,
    retention_policy: Option<GcsRetentionPolicy>,
}

#[derive(Deserialize)]
struct GcsVersioning {
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsRetentionPolicy {
    retention_period: Option<String>,
    is_locked: Option<bool>,
}

/// Confirms that an S3 bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Names the failed control-plane request or the concrete missing policy.
pub async fn verify_s3_worm_bucket(cfg: &S3Config) -> Result<(), ObjectStoreError> {
    let versioning: VersioningConfiguration = get_bucket_xml(cfg, "versioning").await?;
    let lock: ObjectLockConfiguration = get_bucket_xml(cfg, "object-lock").await?;
    validate_s3_policy(cfg, &versioning, lock)
}

fn validate_s3_policy(
    cfg: &S3Config,
    versioning: &VersioningConfiguration,
    lock: ObjectLockConfiguration,
) -> Result<(), ObjectStoreError> {
    if versioning.status.as_deref() != Some("Enabled") {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires versioning Enabled, found {}",
            cfg.bucket,
            versioning.status.as_deref().unwrap_or("unset")
        )));
    }

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

/// Confirms that a GCS bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Names the failed control-plane request or the concrete missing policy.
pub async fn verify_gcs_worm_bucket(cfg: &GcsConfig) -> Result<(), ObjectStoreError> {
    let store = build_gcs_store(cfg)?;
    let credential = store.credentials().get_credential().await?;
    let endpoint = cfg
        .endpoint
        .as_deref()
        .unwrap_or("https://storage.googleapis.com");
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|error| ObjectStoreError::InvalidConfig(format!("GCS endpoint: {error}")))?;
    url.path_segments_mut()
        .map_err(|()| ObjectStoreError::InvalidConfig("GCS endpoint cannot be a base URL".into()))?
        .pop_if_empty()
        .extend(["storage", "v1", "b", &cfg.bucket]);
    url.query_pairs_mut()
        .append_pair("fields", "versioning,retentionPolicy");
    let mut request = reqwest::Client::new().get(url);
    if !credential.bearer.is_empty() {
        request = request.bearer_auth(&credential.bearer);
    }
    let response = request
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
            "GCS Buckets.get returned {status}: {}",
            String::from_utf8_lossy(&body)
        )));
    }
    let policy: GcsBucketPolicy = serde_json::from_slice(&body)
        .map_err(|error| ObjectStoreError::Backend(format!("GCS Buckets.get: {error}")))?;
    validate_gcs_policy(cfg, policy)
}

fn validate_gcs_policy(cfg: &GcsConfig, policy: GcsBucketPolicy) -> Result<(), ObjectStoreError> {
    if policy.versioning.and_then(|v| v.enabled) != Some(true) {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires GCS versioning enabled",
            cfg.bucket
        )));
    }
    let retention = policy.retention_policy.ok_or_else(|| {
        ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` has no GCS retention policy",
            cfg.bucket
        ))
    })?;
    if retention.is_locked != Some(true) {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` requires a locked GCS retention policy",
            cfg.bucket
        )));
    }
    let seconds = retention
        .retention_period
        .as_deref()
        .and_then(|period| period.parse::<u64>().ok())
        .unwrap_or(0);
    if seconds == 0 {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "WORM bucket `{}` GCS retention policy has no positive retention period",
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
    let response = reqwest::Client::new()
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

    const GCS_TEST_KEY: &str = r#"{
        "private_key":"unused",
        "private_key_id":"unused",
        "client_email":"unused",
        "disable_oauth":true
    }"#;

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

    #[test]
    fn s3_policy_rejects_each_missing_protection() {
        let cfg = S3Config {
            bucket: "bucket".into(),
            ..Default::default()
        };
        let versioning = |status: Option<&str>| VersioningConfiguration {
            status: status.map(str::to_string),
        };
        let lock = |enabled: Option<&str>, mode: Option<&str>, days| ObjectLockConfiguration {
            object_lock_enabled: enabled.map(str::to_string),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: mode.map(str::to_string),
                    days,
                    years: None,
                }),
            }),
        };

        for (versioning, lock, reason) in [
            (
                versioning(Some("Suspended")),
                lock(Some("Enabled"), Some("COMPLIANCE"), Some(1)),
                "versioning Enabled",
            ),
            (
                versioning(Some("Enabled")),
                lock(None, Some("COMPLIANCE"), Some(1)),
                "Object Lock Enabled",
            ),
            (
                versioning(Some("Enabled")),
                ObjectLockConfiguration {
                    object_lock_enabled: Some("Enabled".into()),
                    rule: None,
                },
                "no Object Lock default retention",
            ),
            (
                versioning(Some("Enabled")),
                lock(Some("Enabled"), Some("GOVERNANCE"), Some(1)),
                "COMPLIANCE",
            ),
            (
                versioning(Some("Enabled")),
                lock(Some("Enabled"), Some("COMPLIANCE"), Some(0)),
                "no positive Days or Years",
            ),
        ] {
            let error = validate_s3_policy(&cfg, &versioning, lock).unwrap_err();
            check!(error.to_string().contains(reason), "{reason}: {error}");
        }
    }

    #[test]
    fn gcs_policy_rejects_each_missing_protection() {
        let cfg = GcsConfig {
            bucket: "bucket".into(),
            ..Default::default()
        };
        for (json, reason) in [
            (
                r#"{"versioning":{"enabled":false},"retentionPolicy":{"retentionPeriod":"1","isLocked":true}}"#,
                "versioning enabled",
            ),
            (
                r#"{"versioning":{"enabled":true}}"#,
                "no GCS retention policy",
            ),
            (
                r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"1","isLocked":false}}"#,
                "locked GCS retention policy",
            ),
            (
                r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"0","isLocked":true}}"#,
                "no positive retention period",
            ),
        ] {
            let policy = serde_json::from_str(json).unwrap();
            let error = validate_gcs_policy(&cfg, policy).unwrap_err();
            check!(error.to_string().contains(reason), "{reason}: {error}");
        }
    }

    #[tokio::test]
    async fn confirms_gcs_versioning_and_locked_retention() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                let read = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            check!(request.starts_with("GET /storage/v1/b/bucket?fields="));
            let body = r#"{"versioning":{"enabled":true},"retentionPolicy":{"retentionPeriod":"86400","isLocked":true}}"#;
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
        });
        let cfg = GcsConfig {
            bucket: "bucket".into(),
            endpoint: Some(endpoint),
            service_account_key: Some(GCS_TEST_KEY.into()),
            allow_http: true,
            ..Default::default()
        };

        verify_gcs_worm_bucket(&cfg).await.unwrap();
        server.await.unwrap();
    }
}
