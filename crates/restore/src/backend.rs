//! The archive handle: the `--archive-*` flags turned into an object store.
//!
//! The broker holds the same backend mapping behind its own TOML config, but a
//! recovery tool must run when the broker does not, so this builds straight on
//! the object-store layer instead.

use std::sync::Arc;

use crabka_object_store::{
    GcsConfig, ObjectOps, ObjectStoreClient, ObjectStoreConfig, S3Config, build_object_store,
};
use object_store::path::Path;

use crate::{args::RestoreArgs, error::RestoreError};

/// Region used when `--archive-s3-region` is absent. AWS requires a region,
/// and `MinIO` and R2 accept this one as a placeholder.
const DEFAULT_S3_REGION: &str = "us-east-1";

/// A read handle on the archive, with the operator's key prefix applied.
///
/// The client is fully async. `S3RemoteStorage` would also serve the reads,
/// but it uses `block_in_place`, which fails without a current multi-thread
/// Tokio runtime handle.
#[derive(Clone)]
pub struct ArchiveStore {
    client: ObjectStoreClient,
    prefix: Option<String>,
}

impl ArchiveStore {
    /// The object operations to read the archive with.
    #[must_use]
    pub fn ops(&self) -> &dyn ObjectOps {
        &self.client
    }

    /// The key prefix every archive key carries, absent when the archive is
    /// the whole bucket.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// The absolute object key for a path relative to the archive root.
    #[must_use]
    pub fn key(&self, relative: &str) -> Path {
        match &self.prefix {
            Some(prefix) => Path::from(format!("{prefix}/{relative}")),
            None => Path::from(relative),
        }
    }

    /// The prefix to list the archive root under.
    #[must_use]
    pub fn root(&self) -> Option<Path> {
        self.prefix.as_deref().map(Path::from)
    }
}

/// Build the archive handle the `--archive-*` flags describe.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the backend builder rejects the
/// bucket, region, endpoint, or credentials, and
/// [`RestoreError::InvalidArgument`] when no backend was selected.
pub fn open_archive(args: &RestoreArgs) -> Result<ArchiveStore, RestoreError> {
    let config = object_store_config(args)?;
    let store: Arc<dyn object_store::ObjectStore> = build_object_store(&config)?;
    Ok(ArchiveStore {
        client: ObjectStoreClient::new(store),
        prefix: normalize_prefix(args.archive.prefix.as_deref()),
    })
}

/// Map the `--archive-*` flags onto an object-store configuration.
///
/// The prefix is not set here. It stays on [`ArchiveStore`], so the same
/// handle can address a key inside the archive and list the archive root.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when no backend was selected.
/// The argument parser makes exactly one of them required, so this reports a
/// caller that built [`RestoreArgs`] by hand.
pub fn object_store_config(args: &RestoreArgs) -> Result<ObjectStoreConfig, RestoreError> {
    let archive = &args.archive;
    if let Some(root) = &archive.local {
        return Ok(ObjectStoreConfig::Local { root: root.clone() });
    }
    if let Some(bucket) = &archive.s3_bucket {
        return Ok(ObjectStoreConfig::S3(S3Config {
            bucket: bucket.clone(),
            prefix: None,
            region: archive
                .s3_region
                .clone()
                .unwrap_or_else(|| DEFAULT_S3_REGION.to_owned()),
            endpoint: archive.s3_endpoint.clone(),
            access_key_id: archive.s3_access_key_id.clone(),
            secret_access_key: archive.s3_secret_access_key.clone(),
            allow_http: archive.s3_allow_http,
            ..S3Config::default()
        }));
    }
    if let Some(bucket) = &archive.gcs_bucket {
        return Ok(ObjectStoreConfig::Gcs(GcsConfig {
            bucket: bucket.clone(),
            prefix: None,
            service_account_path: archive.gcs_service_account_path.clone(),
            endpoint: archive.gcs_endpoint.clone(),
            allow_http: archive.gcs_allow_http,
            ..GcsConfig::default()
        }));
    }
    Err(RestoreError::InvalidArgument(
        "no archive source: pass one of --archive-local, --archive-s3-bucket, \
         or --archive-gcs-bucket"
            .to_owned(),
    ))
}

/// Trim the separators an operator copies out of a console URL, so
/// `/tier/`, `tier`, and `tier/` all address the same archive root.
fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    let trimmed = prefix?.trim().trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser as _;

    use super::*;

    fn args_from(extra: &[&str]) -> RestoreArgs {
        let mut argv = vec!["krabka-restore", "--log-dir", "/target"];
        argv.extend_from_slice(extra);
        crate::Cli::parse_from(argv).args
    }

    #[test]
    fn a_local_archive_maps_to_the_local_backend() {
        let config =
            object_store_config(&args_from(&["--archive-local", "/archive"])).expect("config");
        check!(
            matches!(config, ObjectStoreConfig::Local { root } if root == std::path::Path::new("/archive"))
        );
    }

    #[test]
    fn s3_flags_map_onto_the_s3_config() {
        let config = object_store_config(&args_from(&[
            "--archive-s3-bucket",
            "backups",
            "--archive-s3-region",
            "eu-west-1",
            "--archive-s3-endpoint",
            "http://minio:9000",
            "--archive-s3-access-key-id",
            "key",
            "--archive-s3-secret-access-key",
            "secret",
            "--archive-s3-allow-http",
        ]))
        .expect("config");
        let ObjectStoreConfig::S3(s3) = config else {
            panic!("expected an S3 config");
        };
        check!(s3.bucket == "backups");
        check!(s3.region == "eu-west-1");
        check!(s3.endpoint.as_deref() == Some("http://minio:9000"));
        check!(s3.access_key_id.as_deref() == Some("key"));
        check!(s3.secret_access_key.as_deref() == Some("secret"));
        check!(s3.allow_http);
        // The prefix belongs to the handle, not to the store config.
        check!(s3.prefix.is_none());
    }

    #[test]
    fn an_absent_s3_region_falls_back_to_the_placeholder() {
        let config =
            object_store_config(&args_from(&["--archive-s3-bucket", "backups"])).expect("config");
        let ObjectStoreConfig::S3(s3) = config else {
            panic!("expected an S3 config");
        };
        check!(s3.region == DEFAULT_S3_REGION);
        check!(!s3.allow_http);
    }

    #[test]
    fn gcs_flags_map_onto_the_gcs_config() {
        let config = object_store_config(&args_from(&[
            "--archive-gcs-bucket",
            "backups",
            "--archive-gcs-service-account-path",
            "/etc/sa.json",
            "--archive-gcs-endpoint",
            "http://fake-gcs:4443",
            "--archive-gcs-allow-http",
        ]))
        .expect("config");
        let ObjectStoreConfig::Gcs(gcs) = config else {
            panic!("expected a GCS config");
        };
        check!(
            gcs == GcsConfig {
                bucket: "backups".to_owned(),
                service_account_path: Some("/etc/sa.json".to_owned()),
                endpoint: Some("http://fake-gcs:4443".to_owned()),
                allow_http: true,
                ..GcsConfig::default()
            }
        );
    }

    #[test]
    fn a_hand_built_args_without_a_backend_is_rejected() {
        let mut args = args_from(&["--archive-local", "/archive"]);
        args.archive.local = None;
        check!(matches!(
            object_store_config(&args),
            Err(RestoreError::InvalidArgument(_))
        ));
    }

    #[test]
    fn prefixes_normalize_to_one_spelling() {
        for spelling in ["tier", "/tier", "tier/", "/tier/", " /tier/ "] {
            check!(
                normalize_prefix(Some(spelling)) == Some("tier".to_owned()),
                "{spelling:?}"
            );
        }
        for empty in [None, Some(""), Some("/"), Some("   "), Some("///")] {
            check!(normalize_prefix(empty).is_none(), "{empty:?}");
        }
    }

    #[test]
    fn keys_are_built_under_the_prefix() {
        // `LocalFileSystem` canonicalizes its root, so the archive must exist.
        let archive = tempfile::tempdir().expect("temp dir");
        let store = open_archive(&args_from(&[
            "--archive-local",
            &archive.path().display().to_string(),
            "--archive-prefix",
            "/tier/",
        ]))
        .expect("store");
        check!(store.prefix() == Some("tier"));
        check!(store.key("orders-0-abc/000.log") == Path::from("tier/orders-0-abc/000.log"));
        check!(store.root() == Some(Path::from("tier")));
    }

    #[test]
    fn keys_are_bare_without_a_prefix() {
        let archive = tempfile::tempdir().expect("temp dir");
        let store = open_archive(&args_from(&[
            "--archive-local",
            &archive.path().display().to_string(),
        ]))
        .expect("store");
        check!(store.prefix().is_none());
        check!(store.key("orders-0-abc/000.log") == Path::from("orders-0-abc/000.log"));
        check!(store.root().is_none());
    }
}
