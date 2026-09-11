//! The archive the capture is written to: the `--archive-*` flags turned into
//! an object store, plus the three operations this tool needs from it.
//!
//! The flags carry the names `krabka restore` gives them, because the capture
//! is written into the archive the restore reads and an operator must not have
//! to learn two spellings for one bucket.

use std::{collections::BTreeSet, sync::Arc};

use clap::{ArgGroup, Args};
use krabka_object_store::{
    GcsConfig, ObjectOps, ObjectStoreClient, ObjectStoreConfig, PutRequest, S3Config,
    build_object_store,
};
use object_store::path::Path;

use crate::error::BackupError;

/// Region used when `--archive-s3-region` is absent. AWS requires a region,
/// and `MinIO` and R2 accept this one as a placeholder.
const DEFAULT_S3_REGION: &str = "us-east-1";

/// Where the archive is.
///
/// Exactly one backend is selected. A sub-flag of a backend that was not
/// selected is rejected by [`ArchiveArgs::validate`] rather than ignored,
/// which is how `krabka restore` treats the same flag set.
#[derive(Args, Debug, Default)]
#[command(next_help_heading = "Archive")]
#[command(group(
    ArgGroup::new("archive_backend")
        .required(true)
        .args(["local", "s3_bucket", "gcs_bucket"]),
))]
pub struct ArchiveArgs {
    /// Use a local directory tree as the archive.
    #[arg(long = "archive-local", value_name = "DIR")]
    pub local: Option<std::path::PathBuf>,

    /// Use this S3 or S3-compatible bucket.
    #[arg(long = "archive-s3-bucket", value_name = "BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 region. Defaults to `us-east-1`, which `MinIO` and R2 accept as a
    /// placeholder.
    #[arg(long = "archive-s3-region", value_name = "REGION")]
    pub s3_region: Option<String>,

    /// S3 endpoint URL, for a non-AWS S3-compatible store.
    #[arg(long = "archive-s3-endpoint", value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// S3 access key id. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-access-key-id", value_name = "ID")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret access key. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-secret-access-key", value_name = "SECRET")]
    pub s3_secret_access_key: Option<String>,

    /// Allow plaintext HTTP to the S3 endpoint.
    #[arg(long = "archive-s3-allow-http")]
    pub s3_allow_http: bool,

    /// Use this Google Cloud Storage bucket.
    #[arg(long = "archive-gcs-bucket", value_name = "BUCKET")]
    pub gcs_bucket: Option<String>,

    /// Path to a GCS service-account JSON key. Without it Workload Identity or
    /// application default credentials apply.
    #[arg(long = "archive-gcs-service-account-path", value_name = "PATH")]
    pub gcs_service_account_path: Option<String>,

    /// GCS API base URL, for an emulator.
    #[arg(long = "archive-gcs-endpoint", value_name = "URL")]
    pub gcs_endpoint: Option<String>,

    /// Allow plaintext HTTP to the GCS endpoint.
    #[arg(long = "archive-gcs-allow-http")]
    pub gcs_allow_http: bool,

    /// Key prefix inside the archive, for a bucket that holds more than the
    /// tiered-storage tree. It applies to every backend.
    #[arg(long = "archive-prefix", value_name = "PREFIX")]
    pub prefix: Option<String>,
}

impl ArchiveArgs {
    /// Reject a sub-flag whose backend was not selected.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::InvalidArgument`] naming both the flag that was
    /// given and the backend flag it needs.
    pub fn validate(&self) -> Result<(), BackupError> {
        let s3 = self.s3_bucket.is_some();
        let gcs = self.gcs_bucket.is_some();
        let orphans: [(&str, bool, &str); 8] = [
            ("--archive-s3-region", self.s3_region.is_some(), "s3"),
            ("--archive-s3-endpoint", self.s3_endpoint.is_some(), "s3"),
            (
                "--archive-s3-access-key-id",
                self.s3_access_key_id.is_some(),
                "s3",
            ),
            (
                "--archive-s3-secret-access-key",
                self.s3_secret_access_key.is_some(),
                "s3",
            ),
            ("--archive-s3-allow-http", self.s3_allow_http, "s3"),
            (
                "--archive-gcs-service-account-path",
                self.gcs_service_account_path.is_some(),
                "gcs",
            ),
            ("--archive-gcs-endpoint", self.gcs_endpoint.is_some(), "gcs"),
            ("--archive-gcs-allow-http", self.gcs_allow_http, "gcs"),
        ];
        for (flag, given, backend) in orphans {
            let selected = if backend == "s3" { s3 } else { gcs };
            if given && !selected {
                return Err(BackupError::InvalidArgument(format!(
                    "{flag} needs --archive-{backend}-bucket"
                )));
            }
        }
        Ok(())
    }

    /// Map the flags onto an object-store configuration.
    fn config(&self) -> Result<ObjectStoreConfig, BackupError> {
        if let Some(root) = &self.local {
            return Ok(ObjectStoreConfig::Local { root: root.clone() });
        }
        if let Some(bucket) = &self.s3_bucket {
            return Ok(ObjectStoreConfig::S3(S3Config {
                bucket: bucket.clone(),
                prefix: None,
                region: self
                    .s3_region
                    .clone()
                    .unwrap_or_else(|| DEFAULT_S3_REGION.to_owned()),
                endpoint: self.s3_endpoint.clone(),
                access_key_id: self.s3_access_key_id.clone(),
                secret_access_key: self.s3_secret_access_key.clone(),
                allow_http: self.s3_allow_http,
                ..S3Config::default()
            }));
        }
        if let Some(bucket) = &self.gcs_bucket {
            return Ok(ObjectStoreConfig::Gcs(GcsConfig {
                bucket: bucket.clone(),
                prefix: None,
                service_account_path: self.gcs_service_account_path.clone(),
                endpoint: self.gcs_endpoint.clone(),
                allow_http: self.gcs_allow_http,
                ..GcsConfig::default()
            }));
        }
        Err(BackupError::InvalidArgument(
            "no archive backend selected: pass --archive-local, --archive-s3-bucket or \
             --archive-gcs-bucket"
                .to_owned(),
        ))
    }

    /// Build the handle these flags describe.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::InvalidArgument`] when the flags contradict each
    /// other, and [`BackupError::ObjectStore`] when the backend builder rejects
    /// the bucket, region, endpoint, or credentials.
    pub fn open(&self) -> Result<Archive, BackupError> {
        self.validate()?;
        let store: Arc<dyn object_store::ObjectStore> = build_object_store(&self.config()?)?;
        Ok(Archive {
            client: ObjectStoreClient::new(store),
            prefix: normalize_prefix(self.prefix.as_deref()),
        })
    }
}

/// A read and write handle on the archive, with the operator's key prefix
/// applied.
pub struct Archive {
    client: ObjectStoreClient,
    prefix: Option<String>,
}

impl Archive {
    /// The absolute object key for a path relative to the archive root.
    #[must_use]
    pub fn key(&self, relative: &str) -> Path {
        match &self.prefix {
            Some(prefix) => Path::from(format!("{prefix}/{relative}")),
            None => Path::from(relative),
        }
    }

    /// Write one object, overwriting whatever was there.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the write fails.
    pub async fn put(&self, relative: &str, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.client
            .put(&self.key(relative), bytes.into(), PutRequest::default())
            .await?;
        Ok(())
    }

    /// Read one object whole.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the object is absent or the
    /// read fails.
    pub async fn get(&self, relative: &str) -> Result<Vec<u8>, BackupError> {
        Ok(self.client.get(&self.key(relative)).await?.to_vec())
    }

    /// Read an exact object key without applying the backup-output prefix.
    ///
    /// # Errors
    /// Returns [`BackupError::ObjectStore`] when the object cannot be read.
    pub async fn get_absolute(&self, key: &str) -> Result<Vec<u8>, BackupError> {
        Ok(self.client.get(&Path::from(key)).await?.to_vec())
    }

    /// The distinct directory names one level below `relative`.
    ///
    /// Object stores have no directories, so this lists the keys under the
    /// prefix and keeps the segment that follows it. An empty listing is an
    /// empty set rather than an error: a bucket that has never been captured
    /// into is not a failure, and the caller says what that means.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the listing fails.
    pub async fn child_directories(&self, relative: &str) -> Result<BTreeSet<String>, BackupError> {
        let prefix = self.key(relative);
        let listing = self.client.list(Some(prefix.clone())).await?;
        let under = format!("{prefix}/");
        Ok(listing
            .into_iter()
            .filter_map(|meta| {
                let key = meta.location.as_ref().to_owned();
                let rest = key.strip_prefix(&under)?;
                let (head, _) = rest.split_once('/')?;
                (!head.is_empty()).then(|| head.to_owned())
            })
            .collect())
    }
}

/// Strip the leading and trailing slashes an operator may have typed, and map
/// an empty prefix onto no prefix at all.
fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    let trimmed = prefix?.trim_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        ArchiveArgs, BTreeSet, DEFAULT_S3_REGION, GcsConfig, ObjectStoreConfig, normalize_prefix,
    };

    fn local_args(root: &std::path::Path) -> ArchiveArgs {
        ArchiveArgs {
            local: Some(root.to_path_buf()),
            ..ArchiveArgs::default()
        }
    }

    #[test]
    fn a_prefix_is_normalized_to_its_bare_middle() {
        let cases = [
            (None, None),
            (Some(""), None),
            (Some("/"), None),
            (Some("tier"), Some("tier".to_owned())),
            (Some("/tier/"), Some("tier".to_owned())),
            (Some("prod/tier"), Some("prod/tier".to_owned())),
        ];
        for (input, expected) in cases {
            check!(normalize_prefix(input) == expected, "for {input:?}");
        }
    }

    #[test]
    fn a_key_carries_the_prefix_when_there_is_one() {
        let root = tempfile::tempdir().expect("archive root");
        let archive = ArchiveArgs {
            prefix: Some("prod/tier".to_owned()),
            ..local_args(root.path())
        }
        .open()
        .expect("open a local archive");
        check!(
            archive.key("restore-inputs/1/manifest.json").as_ref()
                == "prod/tier/restore-inputs/1/manifest.json"
        );

        let bare = local_args(root.path())
            .open()
            .expect("open a local archive");
        check!(
            bare.key("restore-inputs/1/manifest.json").as_ref() == "restore-inputs/1/manifest.json"
        );
    }

    #[test]
    fn a_sub_flag_without_its_backend_names_both_flags() {
        let root = tempfile::tempdir().expect("archive root");
        let args = ArchiveArgs {
            s3_region: Some("eu-west-1".to_owned()),
            ..local_args(root.path())
        };
        let message = args
            .validate()
            .expect_err("a sub-flag without its backend is rejected")
            .to_string();
        check!(message.contains("--archive-s3-region"), "got: {message}");
        check!(message.contains("--archive-s3-bucket"), "got: {message}");
    }

    #[test]
    fn the_s3_flags_map_onto_the_s3_config_with_a_placeholder_region() {
        let args = ArchiveArgs {
            s3_bucket: Some("krabka-tier".to_owned()),
            s3_endpoint: Some("http://minio:9000".to_owned()),
            s3_access_key_id: Some("key".to_owned()),
            s3_secret_access_key: Some("secret".to_owned()),
            s3_allow_http: true,
            ..ArchiveArgs::default()
        };
        let ObjectStoreConfig::S3(s3) = args.config().expect("an S3 config") else {
            panic!("the s3 bucket selects the S3 backend")
        };
        // `S3Config` carries no `PartialEq`, so the fields the mapping sets
        // are checked one by one.
        check!(s3.bucket == "krabka-tier");
        check!(s3.prefix == None);
        check!(s3.region == DEFAULT_S3_REGION);
        check!(s3.endpoint == Some("http://minio:9000".to_owned()));
        check!(s3.access_key_id == Some("key".to_owned()));
        check!(s3.secret_access_key == Some("secret".to_owned()));
        check!(s3.allow_http);

        // The region is the operator's when they gave one, and the prefix
        // stays on the handle rather than on the store config.
        let ObjectStoreConfig::S3(s3) = ArchiveArgs {
            s3_region: Some("eu-west-1".to_owned()),
            prefix: Some("prod/".to_owned()),
            ..ArchiveArgs {
                s3_bucket: Some("krabka-tier".to_owned()),
                ..ArchiveArgs::default()
            }
        }
        .config()
        .expect("an S3 config") else {
            panic!("the s3 bucket selects the S3 backend")
        };
        check!(s3.region == "eu-west-1");
        check!(s3.prefix == None);
    }

    #[test]
    fn the_gcs_flags_map_onto_the_gcs_config() {
        let args = ArchiveArgs {
            gcs_bucket: Some("krabka-tier".to_owned()),
            gcs_service_account_path: Some("/etc/sa.json".to_owned()),
            gcs_endpoint: Some("http://fake-gcs:4443".to_owned()),
            gcs_allow_http: true,
            ..ArchiveArgs::default()
        };
        let ObjectStoreConfig::Gcs(gcs) = args.config().expect("a GCS config") else {
            panic!("the gcs bucket selects the GCS backend")
        };
        check!(
            gcs == GcsConfig {
                bucket: "krabka-tier".to_owned(),
                prefix: None,
                service_account_path: Some("/etc/sa.json".to_owned()),
                endpoint: Some("http://fake-gcs:4443".to_owned()),
                allow_http: true,
                ..GcsConfig::default()
            }
        );
    }

    #[test]
    fn a_flag_set_that_selects_no_backend_at_all_names_the_three_that_would() {
        let Err(error) = ArchiveArgs::default().open() else {
            panic!("no backend is not a default")
        };
        let message = error.to_string();
        for flag in [
            "--archive-local",
            "--archive-s3-bucket",
            "--archive-gcs-bucket",
        ] {
            check!(message.contains(flag), "for {flag}, got: {message}");
        }
    }

    #[test]
    fn every_sub_flag_needs_the_backend_it_belongs_to() {
        let root = tempfile::tempdir().expect("archive root");
        let cases: [(&str, &str, ArchiveArgs); 8] = [
            (
                "--archive-s3-region",
                "--archive-s3-bucket",
                ArchiveArgs {
                    s3_region: Some("eu-west-1".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-s3-endpoint",
                "--archive-s3-bucket",
                ArchiveArgs {
                    s3_endpoint: Some("http://minio:9000".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-s3-access-key-id",
                "--archive-s3-bucket",
                ArchiveArgs {
                    s3_access_key_id: Some("key".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-s3-secret-access-key",
                "--archive-s3-bucket",
                ArchiveArgs {
                    s3_secret_access_key: Some("secret".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-s3-allow-http",
                "--archive-s3-bucket",
                ArchiveArgs {
                    s3_allow_http: true,
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-gcs-service-account-path",
                "--archive-gcs-bucket",
                ArchiveArgs {
                    gcs_service_account_path: Some("/etc/sa.json".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-gcs-endpoint",
                "--archive-gcs-bucket",
                ArchiveArgs {
                    gcs_endpoint: Some("http://fake-gcs:4443".to_owned()),
                    ..local_args(root.path())
                },
            ),
            (
                "--archive-gcs-allow-http",
                "--archive-gcs-bucket",
                ArchiveArgs {
                    gcs_allow_http: true,
                    ..local_args(root.path())
                },
            ),
        ];
        for (flag, backend, args) in cases {
            let message = args
                .validate()
                .expect_err("a sub-flag without its backend is rejected")
                .to_string();
            check!(message.contains(flag), "for {flag}, got: {message}");
            check!(message.contains(backend), "for {flag}, got: {message}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_archive_lists_the_directories_one_level_below_a_prefix() {
        let root = tempfile::tempdir().expect("archive root");
        let archive = local_args(root.path()).open().expect("open the archive");
        check!(
            archive
                .child_directories("restore-inputs")
                .await
                .expect("list an archive that has never been captured into")
                .is_empty()
        );

        for key in [
            "restore-inputs/0000000000000001/manifest.json",
            "restore-inputs/0000000000000001/rlmm-snapshot",
            "restore-inputs/0000000000000002/manifest.json",
            // A key directly under the root names no capture directory.
            "restore-inputs/stray",
        ] {
            archive
                .put(key, b"bytes".to_vec())
                .await
                .expect("write an object");
        }

        check!(
            archive
                .child_directories("restore-inputs")
                .await
                .expect("list the captures")
                == BTreeSet::from(["0000000000000001".to_owned(), "0000000000000002".to_owned()])
        );
        check!(
            archive
                .get("restore-inputs/stray")
                .await
                .expect("read an object back")
                == b"bytes".to_vec()
        );
    }

    #[test]
    fn a_sub_flag_of_the_selected_backend_is_accepted() {
        let args = ArchiveArgs {
            s3_bucket: Some("krabka-tier".to_owned()),
            s3_region: Some("eu-west-1".to_owned()),
            local: None,
            ..ArchiveArgs::default()
        };
        check!(args.validate().is_ok());
    }
}
