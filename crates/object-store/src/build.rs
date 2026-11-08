//! Construction of an `object_store::ObjectStore` handle from a config.

#[cfg(target_family = "wasm")]
use crate::unavailable::unavailable;
use {
    super::{error::ObjectStoreError, object_store_api as object_store},
    crate::config::ObjectStoreConfig,
    object_store::{ObjectStore, memory::InMemory},
    std::sync::Arc,
};
#[cfg(not(target_family = "wasm"))]
use {
    crate::config::{GcsConfig, S3Config},
    object_store::{
        ClientOptions, RetryConfig,
        aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum, S3ConditionalPut},
        gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
        local::LocalFileSystem,
    },
    std::{env, time::Duration},
};

/// The retry budget `cfg` asks for.
///
/// The backoff curve is `object_store`'s: what an operator turns is how many
/// attempts a request gets and how long the whole sequence may take, not the
/// shape of the sleeps between them.
#[cfg(not(target_family = "wasm"))]
fn retry_config(max_retries: usize, retry_timeout: Duration) -> RetryConfig {
    RetryConfig {
        backoff: object_store::BackoffConfig::default(),
        max_retries,
        retry_timeout,
    }
}

/// The HTTP-client bounds `cfg` asks for, on top of `base`.
///
/// Both timeouts are always set rather than left to the crate's defaults: a
/// store that stalls has to hit a bound the operator chose, and a value that
/// only exists inside a dependency is not one they can turn.
#[cfg(not(target_family = "wasm"))]
fn client_options(
    base: ClientOptions,
    allow_http: bool,
    request_timeout: Duration,
    connect_timeout: Duration,
) -> ClientOptions {
    base.with_allow_http(allow_http)
        .with_timeout(request_timeout)
        .with_connect_timeout(connect_timeout)
}

/// Build an `object_store` handle for `cfg`.
///
/// The builder wiring covers the credential chains, the endpoints, and
/// `allow_http`. It is identical to the wiring in the per-crate constructors.
///
/// # Errors
///
/// Returns [`ObjectStoreError::InvalidConfig`] if the backend builder rejects
/// the combination of bucket, region, endpoint, and credentials. On
/// `wasm32-wasip1`, also for every backend other than
/// [`ObjectStoreConfig::InMemory`].
pub fn build_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    match cfg {
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::S3(s3) => build_s3(s3),
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::Gcs(gcs) => Ok(Arc::new(build_gcs_store(gcs)?)),
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::Local { root } => {
            let store = LocalFileSystem::new_with_prefix(root)
                .map_err(|e| ObjectStoreError::InvalidConfig(format!("local: {e}")))?;
            Ok(Arc::new(store))
        }
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::S3(_) => Err(unavailable("S3")),
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::Gcs(_) => Err(unavailable("GCS")),
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::Local { .. } => Err(unavailable("local filesystem")),
        ObjectStoreConfig::InMemory => Ok(Arc::new(InMemory::new())),
    }
}

/// Refuse an empty bucket name. The backend builders take one as a bucket that
/// is named, so without this check the store would build and fail only on its
/// first request.
#[cfg(not(target_family = "wasm"))]
fn require_bucket(backend: &str, bucket: &str) -> Result<(), ObjectStoreError> {
    if bucket.is_empty() {
        return Err(ObjectStoreError::InvalidConfig(format!(
            "{backend} builder: the bucket name is empty"
        )));
    }
    Ok(())
}

/// The settings an S3 store is built from: the `AWS_*` environment, with the
/// explicit [`S3Config`] applied on top of it.
///
/// The environment is read the way `object_store`'s
/// `AmazonS3Builder::from_env` reads it: each variable whose name starts with
/// `AWS_` and names a builder setting, such as `AWS_REGION`,
/// `AWS_ENDPOINT_URL_S3`, `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`, or
/// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`. A variable fills only a setting
/// the config leaves unset:
///
/// - A non-empty `region` wins over `AWS_REGION`, which wins over
///   `AWS_DEFAULT_REGION`. With none of them the region is `us-east-1`.
/// - An `endpoint` wins over `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` and
///   `AWS_ENDPOINT`, all of which are then ignored.
/// - Either half of a static key in the config ignores `AWS_ACCESS_KEY_ID`,
///   `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, so a half-given key is
///   refused rather than completed from the environment.
/// - The bucket, `allow_http`, conditional put and the checksum are always the
///   config's, and the retry and timeout bounds always come from it.
///
/// [`S3Settings::new`] takes the environment as an argument, so a test can
/// resolve a config against a map without touching the process environment.
#[cfg(not(target_family = "wasm"))]
pub struct S3Settings {
    builder: AmazonS3Builder,
}

#[cfg(not(target_family = "wasm"))]
impl S3Settings {
    /// `cfg` over this process's environment.
    #[must_use]
    pub fn from_process_env(cfg: &S3Config) -> Self {
        Self::new(
            cfg,
            env::vars_os().filter_map(|(name, value)| {
                Some((name.into_string().ok()?, value.into_string().ok()?))
            }),
        )
    }

    /// `cfg` over the environment variables in `env`.
    #[must_use]
    pub fn new<N, V>(cfg: &S3Config, env: impl IntoIterator<Item = (N, V)>) -> Self
    where
        N: AsRef<str>,
        V: Into<String>,
    {
        let mut settings: Vec<(AmazonS3ConfigKey, String)> = env
            .into_iter()
            .filter_map(|(name, value)| {
                let key = environment_key(name.as_ref())?;
                (!config_owns(cfg, key)).then(|| (key, value.into()))
            })
            .collect();
        // Both variables set the one region. Applying `AWS_DEFAULT_REGION`
        // first lets `AWS_REGION` win, as it does in the AWS SDKs, whatever
        // order the environment lists them in.
        settings.sort_by_key(|(key, _)| *key != AmazonS3ConfigKey::DefaultRegion);
        let builder = settings
            .into_iter()
            .fold(AmazonS3Builder::new(), |builder, (key, value)| {
                builder.with_config(key, value)
            });
        Self {
            builder: apply_config(builder, cfg),
        }
    }

    /// The resolved value of the builder setting `name` names, spelled the
    /// way `object_store` spells it, for example `aws_region` or
    /// `aws_web_identity_token_file`. `None` when nothing set it or when
    /// `object_store` has no such setting.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<String> {
        let key = name.parse::<AmazonS3ConfigKey>().ok()?;
        self.builder.get_config_value(&key)
    }

    /// The region the store signs for. `us-east-1` when neither the config nor
    /// the environment names one, as in `object_store`.
    #[must_use]
    pub fn region(&self) -> String {
        self.builder
            .get_config_value(&AmazonS3ConfigKey::Region)
            .unwrap_or_else(|| DEFAULT_REGION.to_owned())
    }

    /// The endpoint the store sends requests to, `None` for AWS's own.
    /// `AWS_ENDPOINT_URL_S3` wins over the generic endpoint, as it does in
    /// `object_store`.
    #[must_use]
    pub fn endpoint(&self) -> Option<String> {
        self.builder
            .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
            .or_else(|| self.builder.get_config_value(&AmazonS3ConfigKey::Endpoint))
    }

    /// Build the store.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError::InvalidConfig`] if the builder rejects the
    /// resolved settings, for example a static key without its secret.
    pub fn build(self) -> Result<AmazonS3, ObjectStoreError> {
        self.builder
            .build()
            .map_err(|e| ObjectStoreError::InvalidConfig(format!("S3 builder: {e}")))
    }
}

/// The region `object_store` signs for when nothing names one.
#[cfg(not(target_family = "wasm"))]
const DEFAULT_REGION: &str = "us-east-1";

/// The builder setting the environment variable `name` sets, if any.
#[cfg(not(target_family = "wasm"))]
fn environment_key(name: &str) -> Option<AmazonS3ConfigKey> {
    if !name.starts_with("AWS_") {
        return None;
    }
    name.to_ascii_lowercase().parse().ok()
}

/// Whether `cfg` decides `key` on its own, so the environment may not fill it.
#[cfg(not(target_family = "wasm"))]
fn config_owns(cfg: &S3Config, key: AmazonS3ConfigKey) -> bool {
    match key {
        AmazonS3ConfigKey::Bucket
        | AmazonS3ConfigKey::ConditionalPut
        | AmazonS3ConfigKey::Checksum
        | AmazonS3ConfigKey::Client(_) => true,
        AmazonS3ConfigKey::Endpoint | AmazonS3ConfigKey::S3Endpoint => cfg.endpoint.is_some(),
        AmazonS3ConfigKey::AccessKeyId
        | AmazonS3ConfigKey::SecretAccessKey
        | AmazonS3ConfigKey::Token => {
            cfg.access_key_id.is_some() || cfg.secret_access_key.is_some()
        }
        _ => false,
    }
}

/// `builder` with every setting `cfg` makes applied on top of it.
#[cfg(not(target_family = "wasm"))]
fn apply_config(builder: AmazonS3Builder, cfg: &S3Config) -> AmazonS3Builder {
    let conditional_put = if cfg.conditional_put {
        S3ConditionalPut::ETagMatch
    } else {
        S3ConditionalPut::Disabled
    };
    let mut builder = krabka_macros::connection_policy!(
        builder
            .with_bucket_name(&cfg.bucket)
            .with_allow_http(cfg.allow_http)
            .with_conditional_put(conditional_put),
        cfg
    );
    if !cfg.region.is_empty() {
        builder = builder.with_region(&cfg.region);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    // Each half goes to the builder on its own, so it refuses a key id without
    // its secret, and a secret without its key id, rather than quietly falling
    // back to another credential source.
    if let Some(key_id) = &cfg.access_key_id {
        builder = builder.with_access_key_id(key_id);
    }
    if let Some(secret) = &cfg.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if cfg.checksum_sha256 {
        builder = builder.with_checksum_algorithm(Checksum::SHA256);
    }
    builder
}

/// Build the S3 store for `cfg` over this process's environment.
///
/// # Errors
///
/// Returns [`ObjectStoreError::InvalidConfig`] for an empty bucket name or
/// settings the builder rejects.
#[cfg(not(target_family = "wasm"))]
pub fn build_s3_store(cfg: &S3Config) -> Result<AmazonS3, ObjectStoreError> {
    Ok(build_s3_store_resolved(cfg)?.0)
}

/// [`build_s3_store`], with `cfg`'s region and endpoint replaced by the ones
/// the store resolved, for a request the store does not sign itself.
#[cfg(not(target_family = "wasm"))]
pub fn build_s3_store_resolved(cfg: &S3Config) -> Result<(AmazonS3, S3Config), ObjectStoreError> {
    require_bucket("S3", &cfg.bucket)?;
    let settings = S3Settings::from_process_env(cfg);
    let resolved = S3Config {
        region: settings.region(),
        endpoint: settings.endpoint(),
        ..cfg.clone()
    };
    Ok((settings.build()?, resolved))
}

#[cfg(not(target_family = "wasm"))]
fn build_s3(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    Ok(Arc::new(build_s3_store(cfg)?))
}

#[cfg(not(target_family = "wasm"))]
pub fn build_gcs_store(cfg: &GcsConfig) -> Result<GoogleCloudStorage, ObjectStoreError> {
    require_bucket("GCS", &cfg.bucket)?;
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&cfg.bucket);
    if let Some(path) = &cfg.service_account_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = &cfg.service_account_key {
        builder = builder.with_service_account_key(key);
    }
    if let Some(adc) = &cfg.application_credentials_path {
        builder = builder.with_application_credentials(adc);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    builder = krabka_macros::connection_policy!(builder, cfg);
    let store = builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("GCS builder: {e}")))?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        crate::{
            config::{GcsConfig, ObjectStoreConfig, S3Config},
            test_support::{HttpListener, http_listener, round_trip, s3_config},
        },
        assert2::{assert, check},
        object_store::{ObjectStoreExt as _, path::Path},
        std::{
            future,
            sync::atomic::{AtomicUsize, Ordering},
            time::{Duration, Instant},
        },
    };

    /// The essential S3 config with `region` and nothing else.
    fn s3_in(region: &str) -> S3Config {
        S3Config {
            bucket: "b".into(),
            region: region.into(),
            ..Default::default()
        }
    }

    /// `cfg` over an environment that sets each `(name, value)`.
    fn settings(cfg: &S3Config, env: &[(&str, &str)]) -> S3Settings {
        S3Settings::new(cfg, env.iter().copied())
    }

    /// The settings the config decides on its own: the environment never
    /// changes them, whatever the config says.
    #[yare::parameterized(
        bucket = { "AWS_BUCKET", "other", "aws_bucket", "b" },
        allow_http = { "AWS_ALLOW_HTTP", "true", "allow_http", "false" },
        conditional_put = { "AWS_CONDITIONAL_PUT", "disabled", "aws_conditional_put", "etag" },
        checksum = { "AWS_CHECKSUM_ALGORITHM", "sha256", "aws_checksum_algorithm", "sha256" },
    )]
    fn the_config_owns_a_setting_outright(variable: &str, value: &str, key: &str, want: &str) {
        let got = settings(&s3_in("us-east-1"), &[(variable, value)]).value(key);
        check!(got.as_deref() == Some(want));
    }

    /// Turning conditional put off reaches the builder. `object_store`
    /// defaults it on, so leaving the setting alone would keep it on.
    #[test]
    fn conditional_put_off_disables_it() {
        let cfg = S3Config {
            conditional_put: false,
            ..s3_in("us-east-1")
        };
        check!(settings(&cfg, &[]).value("aws_conditional_put").as_deref() == Some("disabled"));
    }

    /// A checksum the config turns off is not turned back on by
    /// `AWS_CHECKSUM_ALGORITHM`.
    #[test]
    fn checksum_off_stays_off() {
        let cfg = S3Config {
            checksum_sha256: false,
            ..s3_in("us-east-1")
        };
        let env = [("AWS_CHECKSUM_ALGORITHM", "sha256")];
        check!(settings(&cfg, &env).value("aws_checksum_algorithm") == None);
    }

    /// Only `AWS_*` names count, as in `object_store`'s `from_env`.
    #[test]
    fn a_variable_without_the_aws_prefix_is_ignored() {
        check!(settings(&s3_in(""), &[("REGION", "eu-west-1")]).region() == "us-east-1");
    }

    /// `AWS_ENDPOINT_URL_S3` wins over the generic endpoint variable, as it
    /// does when `object_store` builds the store.
    #[test]
    fn the_s3_endpoint_variable_wins_over_the_generic_one() {
        let env = [
            ("AWS_ENDPOINT_URL_S3", "https://s3.example.test"),
            ("AWS_ENDPOINT_URL", "https://generic.example.test"),
        ];
        check!(
            settings(&s3_in("us-east-1"), &env).endpoint().as_deref()
                == Some("https://s3.example.test")
        );
    }

    /// A static key in the config with a stray session token in the
    /// environment would sign with a token the key does not own.
    #[test]
    fn a_configured_static_key_builds_without_the_environment_token() {
        let cfg = S3Config {
            access_key_id: Some("id".into()),
            secret_access_key: Some("secret".into()),
            ..s3_in("us-east-1")
        };
        let env = [
            ("AWS_ACCESS_KEY_ID", "env-id"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
            ("AWS_SESSION_TOKEN", "env-token"),
        ];
        let resolved = settings(&cfg, &env);
        check!(resolved.value("aws_access_key_id").as_deref() == Some("id"));
        check!(resolved.value("aws_secret_access_key").as_deref() == Some("secret"));
        check!(resolved.value("aws_session_token") == None);
    }

    /// Settings the environment does not name keep the builder's own
    /// defaults, and a name that is no setting reads back as `None`.
    #[test]
    fn an_unknown_setting_name_reads_back_as_none() {
        check!(settings(&s3_in("us-east-1"), &[]).value("not_a_setting") == None);
    }

    #[test]
    fn inmemory_builds() {
        assert!(build_object_store(&ObjectStoreConfig::InMemory).is_ok());
    }

    #[tokio::test]
    async fn inmemory_round_trips() {
        let store = build_object_store(&ObjectStoreConfig::InMemory).unwrap();
        let path = Path::from("t/x");
        let got = round_trip(&*store, &path, b"hi").await;
        assert!(&got[..] == b"hi");
    }

    #[test]
    fn local_builds_against_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ObjectStoreConfig::Local {
            root: dir.path().to_path_buf(),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn s3_builds_with_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio:9000".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// The integrity knobs are opt-out. Turning both off must still yield a
    /// usable builder: the flags select request headers, not a different
    /// backend.
    #[test]
    fn s3_builds_with_integrity_knobs_off() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            conditional_put: false,
            checksum_sha256: false,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// The default config turns both knobs on, so the common path must build
    /// with conditional put and SHA-256 checksums wired in.
    #[test]
    fn s3_builds_with_integrity_knobs_on() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from crates/remote-storage/src/gcs.rs tests: with every credential
    // field None, the builder selects Workload Identity / ADC and constructs.
    #[test]
    fn gcs_workload_identity_builds() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from gcs.rs tests: a custom endpoint + allow_http builds.
    #[test]
    fn gcs_honors_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            endpoint: Some("http://fake-gcs:4443".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// A store that accepts the connection and then answers nothing must not
    /// hang the caller: `request_timeout` is the bound, and it is the operator
    /// who set it.
    #[tokio::test]
    async fn s3_request_timeout_bounds_a_stalled_store() {
        let HttpListener { listener, endpoint } = http_listener().await;
        // Hold the accepted sockets open for the length of the test: dropping
        // them would answer the request with a reset, which is not a stall.
        let server = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _held = socket;
                    future::pending::<()>().await;
                });
            }
        });

        let store = build_object_store(&ObjectStoreConfig::S3(S3Config {
            max_retries: 0,
            request_timeout: Duration::from_millis(200),
            ..s3_config("b", endpoint)
        }))
        .unwrap();

        let started = Instant::now();
        let result = store.get(&Path::from("stalled")).await;
        let elapsed = started.elapsed();
        server.abort();

        assert!(result.is_err());
        check!(
            elapsed < Duration::from_secs(5),
            "a 200ms request timeout let a stalled store block for {elapsed:?}"
        );
    }

    /// `max_retries` is the attempt budget, not a suggestion: a store that
    /// answers `503 SlowDown` to everything must be given exactly
    /// `max_retries + 1` attempts before the error reaches the caller.
    #[tokio::test]
    async fn s3_max_retries_bounds_the_attempts_against_a_throttling_store() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let HttpListener { listener, endpoint } = http_listener().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&chunk[..read]),
                    }
                }
                if request.is_empty() {
                    continue;
                }
                counter.fetch_add(1, Ordering::SeqCst);
                let body = "<Error><Code>SlowDown</Code><Message>Please reduce your \
                            request rate.</Message></Error>";
                let response = format!(
                    "HTTP/1.1 503 Slow Down\r\nContent-Length: {}\r\nConnection: \
                     close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });

        let store = build_object_store(&ObjectStoreConfig::S3(S3Config {
            max_retries: 2,
            retry_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(5),
            ..s3_config("b", endpoint)
        }))
        .unwrap();

        let result = store.get(&Path::from("throttled")).await;
        server.abort();

        assert!(result.is_err());
        check!(attempts.load(Ordering::SeqCst) == 3);
    }
}
