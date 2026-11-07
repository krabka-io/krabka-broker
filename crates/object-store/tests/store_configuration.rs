//! Scenarios for `tests/features/store_configuration`: which backend
//! configurations build an archive store, which are refused, what an operator
//! gets by default, and that a printed configuration hides its credentials.
#![cfg(test)]

use {
    krabka_bdd::Given,
    krabka_object_store::{
        GcsConfig, ObjectStoreConfig, ObjectStoreError, S3Config, build_object_store,
    },
    std::fs,
    tempfile::TempDir,
};

const CONFIGURED: &str = "Given an operator has configured the <backend> backend";
const BUILD: &str = "When the broker attempts to build the archive store";
const PRINT: &str = "When the broker prints the backend configuration";
const RESOLVE: &str = "When the broker resolves the backend configuration";

/// The credential value every credential setting carries, so a `Then` can
/// look for it in printed output.
const CREDENTIAL: &str = "operator-credential-7f3a";

/// A service-account key in the shape GCS emulators accept: it names the
/// credential but asks for no token, so a store built from it never signs.
fn emulator_service_account_key() -> String {
    format!(
        r#"{{"private_key":"{CREDENTIAL}","private_key_id":"{CREDENTIAL}","client_email":"archiver@example.com","disable_oauth":true}}"#
    )
}

/// The backend an operator selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Backend {
    InMemory,
    #[step_param("local-filesystem", "local")]
    LocalFilesystem,
    #[step_param("s3", "S3")]
    S3,
    #[step_param("gcs", "GCS")]
    Gcs,
}

impl Backend {
    /// The backend name a refusal of this backend's configuration carries.
    fn refusal_prefix(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::LocalFilesystem => "local:",
            Self::S3 => "S3 builder:",
            Self::Gcs => "GCS builder:",
        }
    }
}

/// One setting an operator adds on top of a backend's essential settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Setting {
    #[step_param("its essential settings")]
    Essential,
    #[step_param("an empty bucket name")]
    EmptyBucketName,
    #[step_param("an access key id")]
    AccessKeyId,
    #[step_param("a secret access key", "its secret access key")]
    SecretAccessKey,
    #[step_param("a service-account key file")]
    ServiceAccountKeyFile,
    #[step_param("an inline service-account key")]
    InlineServiceAccountKey,
    #[step_param("an application credentials file")]
    ApplicationCredentialsFile,
}

/// An S3 connection setting whose default an operator relies on.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum ConnectionSetting {
    #[step_param("multipart threshold in bytes")]
    MultipartThreshold,
    #[step_param("multipart part size in bytes")]
    MultipartPartSize,
    #[step_param("retry count")]
    RetryCount,
    #[step_param("retry timeout in seconds")]
    RetryTimeout,
    #[step_param("request timeout in seconds")]
    RequestTimeout,
    #[step_param("connect timeout in seconds")]
    ConnectTimeout,
    #[step_param("conditional put")]
    ConditionalPut,
    #[step_param("SHA-256 checksum")]
    Sha256Checksum,
}

fn switch(on: bool) -> String { if on { "on" } else { "off" }.to_owned() }

impl ConnectionSetting {
    /// The value of this setting in `s3`, in the words of the feature.
    fn value_in(self, s3: &S3Config) -> String {
        match self {
            Self::MultipartThreshold => s3.multipart_threshold.to_string(),
            Self::MultipartPartSize => s3.multipart_chunk_size.to_string(),
            Self::RetryCount => s3.max_retries.to_string(),
            Self::RetryTimeout => s3.retry_timeout.as_secs().to_string(),
            Self::RequestTimeout => s3.request_timeout.as_secs().to_string(),
            Self::ConnectTimeout => s3.connect_timeout.as_secs().to_string(),
            Self::ConditionalPut => switch(s3.conditional_put),
            Self::Sha256Checksum => switch(s3.checksum_sha256),
        }
    }
}

/// The backend configuration an operator writes, and the directories it
/// points at.
struct OperatorConfiguration {
    backend: Backend,
    config: ObjectStoreConfig,
    /// The directory a local-filesystem backend is rooted at, held so it
    /// outlives the scenario.
    root: TempDir,
}

impl OperatorConfiguration {
    /// The `backend` configured with only the settings it cannot do without.
    fn essential(backend: Backend) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = match backend {
            Backend::InMemory => ObjectStoreConfig::InMemory,
            Backend::LocalFilesystem => ObjectStoreConfig::Local {
                root: root.path().to_path_buf(),
            },
            Backend::S3 => ObjectStoreConfig::S3(S3Config {
                bucket: "krabka-archive".to_owned(),
                region: "us-east-1".to_owned(),
                ..S3Config::default()
            }),
            Backend::Gcs => ObjectStoreConfig::Gcs(GcsConfig {
                bucket: "krabka-archive".to_owned(),
                ..GcsConfig::default()
            }),
        };
        Self {
            backend,
            config,
            root,
        }
    }

    /// A local-filesystem backend rooted at a directory that was never made.
    fn missing_local_root() -> Self {
        let mut configuration = Self::essential(Backend::LocalFilesystem);
        configuration.config = ObjectStoreConfig::Local {
            root: configuration.root.path().join("never-created"),
        };
        configuration
    }

    fn apply(&mut self, setting: Setting) {
        let credential = Some(CREDENTIAL.to_owned());
        match (&mut self.config, setting) {
            (_, Setting::Essential) => {}
            (ObjectStoreConfig::S3(s3), Setting::EmptyBucketName) => s3.bucket = String::new(),
            (ObjectStoreConfig::Gcs(gcs), Setting::EmptyBucketName) => gcs.bucket = String::new(),
            (ObjectStoreConfig::S3(s3), Setting::AccessKeyId) => s3.access_key_id = credential,
            (ObjectStoreConfig::S3(s3), Setting::SecretAccessKey) => {
                s3.secret_access_key = credential;
            }
            (ObjectStoreConfig::Gcs(gcs), Setting::ServiceAccountKeyFile) => {
                let file = self.root.path().join(format!("{CREDENTIAL}.json"));
                fs::write(&file, emulator_service_account_key()).unwrap();
                gcs.service_account_path = Some(file.display().to_string());
            }
            (ObjectStoreConfig::Gcs(gcs), Setting::InlineServiceAccountKey) => {
                gcs.service_account_key = Some(emulator_service_account_key());
            }
            (ObjectStoreConfig::Gcs(gcs), Setting::ApplicationCredentialsFile) => {
                gcs.application_credentials_path = credential;
            }
            (_, setting) => panic!("the {} backend has no {setting} setting", self.backend),
        }
    }
}

type Build = Result<(), ObjectStoreError>;

struct StoreConfigurationScenario {
    configuration: Given<OperatorConfiguration>,
    build: Given<Build>,
    printed: Given<String>,
    resolved: Given<S3Config>,
}

#[rstest::fixture]
fn store_configuration_scenario() -> StoreConfigurationScenario {
    StoreConfigurationScenario {
        configuration: Given::new("configuration", CONFIGURED),
        build: Given::new("build", BUILD),
        printed: Given::new("printed", PRINT),
        resolved: Given::new("resolved", RESOLVE),
    }
}

#[rstest_bdd_macros::given("an operator has configured the {backend} backend with {setting}")]
fn configured_with(
    store_configuration_scenario: &mut StoreConfigurationScenario,
    backend: Backend,
    setting: Setting,
) {
    let mut configuration = OperatorConfiguration::essential(backend);
    configuration.apply(setting);
    store_configuration_scenario
        .configuration
        .set(configuration);
}

#[rstest_bdd_macros::given(
    "an operator has configured the local-filesystem backend at a directory that does not exist"
)]
fn configured_missing_local_root(store_configuration_scenario: &mut StoreConfigurationScenario) {
    store_configuration_scenario
        .configuration
        .set(OperatorConfiguration::missing_local_root());
}

#[rstest_bdd_macros::given("the operator has also configured {setting}")]
fn also_configured(
    store_configuration_scenario: &mut StoreConfigurationScenario,
    setting: Setting,
) {
    store_configuration_scenario
        .configuration
        .get_mut()
        .apply(setting);
}

#[rstest_bdd_macros::when("the broker attempts to build the archive store")]
fn attempt_build(store_configuration_scenario: &mut StoreConfigurationScenario) {
    let build = build_object_store(&store_configuration_scenario.configuration.get().config)
        .map(|_store| ());
    store_configuration_scenario.build.set(build);
}

#[rstest_bdd_macros::when("the broker prints the backend configuration")]
fn print_configuration(store_configuration_scenario: &mut StoreConfigurationScenario) {
    let printed = format!(
        "{:?}",
        store_configuration_scenario.configuration.get().config
    );
    store_configuration_scenario.printed.set(printed);
}

#[rstest_bdd_macros::when("the broker resolves the backend configuration")]
fn resolve_configuration(store_configuration_scenario: &mut StoreConfigurationScenario) {
    let configuration = store_configuration_scenario.configuration.get();
    let ObjectStoreConfig::S3(s3) = &configuration.config else {
        panic!("the {} backend has no S3 settings", configuration.backend);
    };
    let resolved = s3.clone();
    store_configuration_scenario.resolved.set(resolved);
}

#[rstest_bdd_macros::then("the archive store is built")]
fn built(store_configuration_scenario: &StoreConfigurationScenario) {
    assert2::assert!(let Ok(()) = store_configuration_scenario.build.get());
}

#[rstest_bdd_macros::then("the build is refused as an invalid {backend} configuration")]
fn refused_invalid(store_configuration_scenario: &StoreConfigurationScenario, backend: Backend) {
    let build = store_configuration_scenario.build.get();
    assert2::assert!(let Err(ObjectStoreError::InvalidConfig(reason)) = build);
    assert2::assert!(reason.starts_with(backend.refusal_prefix()));
}

#[rstest_bdd_macros::then("the printed configuration hides the credential")]
fn hides_credential(store_configuration_scenario: &StoreConfigurationScenario) {
    let printed = store_configuration_scenario.printed.get();
    assert2::assert!(!printed.contains(CREDENTIAL));
    assert2::assert!(printed.contains("Some(\"***\")"));
}

#[rstest_bdd_macros::then("the default {setting} is {value}")]
fn setting_is(
    store_configuration_scenario: &StoreConfigurationScenario,
    setting: ConnectionSetting,
    value: String,
) {
    let resolved = store_configuration_scenario.resolved.get();
    assert2::assert!(setting.value_in(resolved) == value);
}

// `@inprogress` scenarios document known defects. `krabka_bdd::scenarios!`
// leaves them out of its step check by default but still runs them, so
// `tags` leaves them out of the run too.
krabka_bdd::scenarios!(
    "tests/features/store_configuration",
    tags = "not @formulated and not @inprogress",
    fixtures = [store_configuration_scenario: StoreConfigurationScenario]
);
