//! The three differential cases.

use {
    crate::{
        divergence::DivergenceReport,
        oracle::{KrabkaBroker, ORACLE_IMAGE, OracleBroker, krabka_api_versions, start_krabka},
        parse::{advertised, parse_single_broker},
        support::{JvmListeners, api_versions::supported_client_apis, manifest_dir},
    },
    assert2::assert,
    krabka_protocol::owned::api_versions_response::ApiVersion,
    std::{collections::BTreeSet, env, path::PathBuf},
};

/// Where the checked-in report lives.
fn expectation_path() -> PathBuf {
    manifest_dir()
        .join("tests")
        .join("fixtures")
        .join("api_versions")
        .join("divergence.json")
}

/// Whether this run should rewrite the expectation instead of comparing to it.
///
/// This suite is the file's only writer, so regenerating is an explicit opt-in:
/// `KRABKA_UPDATE_API_VERSIONS_EXPECTATION=1 cargo test -p krabka-broker
/// --test api_versions_differential -- --ignored`. A Bazel sandbox cannot write
/// to the source tree, which is the other reason this is off by default.
fn regenerating() -> bool {
    env::var("KRABKA_UPDATE_API_VERSIONS_EXPECTATION").is_ok_and(|v| v == "1")
}

/// What the client listener advertises, in the order the JVM tool prints,
/// which is ascending API key.
///
/// `start_krabka` binds one `PLAINTEXT` listener and configures no
/// client-metrics receiver, so this is the pair the broker answers that
/// listener with: the KIP-714 keys are withheld the way a stock Kafka broker
/// withholds them, and so is every key in
/// `api_catalog::INTER_BROKER_ONLY_APIS`.
fn catalog_sorted_by_key() -> Vec<ApiVersion> { supported_client_apis() }

/// The API keys one advertised table names.
fn key_set(apis: &[ApiVersion]) -> BTreeSet<i16> { apis.iter().map(|api| api.api_key).collect() }

struct ClientCatalog {
    listeners: Option<JvmListeners>,
    broker: Option<KrabkaBroker>,
    oracle: Option<OracleBroker>,
    krabka: Option<Vec<ApiVersion>>,
    kafka: Option<Vec<ApiVersion>>,
    observed: Option<DivergenceReport>,
    expected: Option<DivergenceReport>,
}
impl ClientCatalog {
    async fn shutdown(self) {
        let KrabkaBroker {
            handle,
            dir: _directory,
        } = self.broker.expect("Krabka started");
        handle.shutdown().await;
    }
}
#[rstest::fixture]
fn catalog() -> ClientCatalog {
    // Start this scenario with fresh inputs and observations.

    ClientCatalog {
        listeners: None,
        broker: None,
        oracle: None,
        krabka: None,
        kafka: None,
        observed: None,
        expected: None,
    }
}
#[rstest_bdd_macros::given("a PLAINTEXT Krabka client listener without a metrics receiver")]
async fn krabka_catalog_source(catalog: &mut ClientCatalog) {
    let listeners = JvmListeners::allocate();
    catalog.broker = Some(start_krabka(&listeners).await);
    catalog.listeners = Some(listeners);
}
#[rstest_bdd_macros::given("a stock Kafka broker from the pinned oracle image")]
fn kafka_catalog_source(catalog: &mut ClientCatalog) {
    catalog.oracle = Some(OracleBroker::start());
}
#[rstest_bdd_macros::when("the JVM API versions tool reads the Krabka client listener")]
fn read_krabka_catalog(catalog: &mut ClientCatalog) {
    let output = krabka_api_versions(catalog.listeners.as_ref().expect("Krabka listener"));
    eprintln!("KRABKA[test] krabka api-versions:\n{output}");
    catalog.krabka = Some(advertised(
        &parse_single_broker(&output).unwrap_or_else(|error| panic!("{error}\n{output}")),
    ));
}
#[rstest_bdd_macros::when("the same JVM API versions tool reads the stock Kafka broker")]
fn read_kafka_catalog(catalog: &mut ClientCatalog) {
    let output = catalog
        .oracle
        .as_ref()
        .expect("Kafka oracle")
        .api_versions();
    eprintln!("KRABKA[test] {ORACLE_IMAGE} api-versions:\n{output}");
    catalog.kafka = Some(advertised(
        &parse_single_broker(&output).unwrap_or_else(|error| panic!("{error}\n{output}")),
    ));
}
#[rstest_bdd_macros::then("the complete advertised Krabka table equals the client API catalog")]
fn client_catalog_matches(catalog: &ClientCatalog) {
    assert!(catalog.krabka.as_ref().expect("Krabka queried") == &catalog_sorted_by_key());
}
#[rstest_bdd_macros::when(
    "the advertised tables are compared with the reviewed divergence expectation"
)]
fn compare_advertised_catalogs(catalog: &mut ClientCatalog) {
    let observed = DivergenceReport::build(
        ORACLE_IMAGE,
        catalog.krabka.as_ref().expect("Krabka queried"),
        catalog.kafka.as_ref().expect("Kafka queried"),
    );
    let path = expectation_path();
    if regenerating() {
        observed.store(&path);
    }
    catalog.expected = Some(DivergenceReport::load(&path));
    catalog.observed = Some(observed);
}
#[rstest_bdd_macros::then(
    "every advertised API has a canonical name and the full divergence equals the reviewed report"
)]
fn reviewed_catalog_divergence(catalog: &ClientCatalog) {
    let observed = catalog.observed.as_ref().expect("tables compared");
    let expected = catalog
        .expected
        .as_ref()
        .expect("reviewed expectation loaded");
    let unnamed: Vec<i16> = observed
        .apis
        .iter()
        .filter(|row| row.name == "Unknown")
        .map(|row| row.api_key)
        .collect();
    assert!(
        unnamed.is_empty(),
        "api keys with no protocol name: {unnamed:?}"
    );
    let path = expectation_path();
    assert!(
        observed == expected,
        "the advertised-version divergence moved for {}; rerun with KRABKA_UPDATE_API_VERSIONS_EXPECTATION=1 and review {}",
        moved_rows(observed, expected),
        path.display()
    );
}
#[rstest_bdd_macros::then("both client listeners advertise exactly the same API key set")]
fn client_key_sets_match(catalog: &ClientCatalog) {
    let ours = key_set(catalog.krabka.as_ref().expect("Krabka queried"));
    let theirs = key_set(catalog.kafka.as_ref().expect("Kafka queried"));
    let krabka_only: Vec<i16> = ours.difference(&theirs).copied().collect();
    let kafka_only: Vec<i16> = theirs.difference(&ours).copied().collect();
    assert!(
        ours == theirs,
        "krabka advertises {krabka_only:?} that {ORACLE_IMAGE} does not, and misses {kafka_only:?} that it does"
    );
}
/// The API keys whose recorded row changed, for the failure message. The
/// assertion above compares the reports whole, and whole is 75 rows: naming the
/// handful that moved is what a reader needs before reading that dump.
fn moved_rows(observed: &DivergenceReport, expected: &DivergenceReport) -> String {
    let keys = |report: &DivergenceReport| -> BTreeSet<i16> {
        report.apis.iter().map(|row| row.api_key).collect()
    };
    let mut moved: Vec<i16> = keys(observed)
        .symmetric_difference(&keys(expected))
        .copied()
        .collect();
    moved.extend(observed.apis.iter().filter_map(|row| {
        let was = expected
            .apis
            .iter()
            .find(|old| old.api_key == row.api_key)?;
        (was != row).then_some(row.api_key)
    }));
    moved.sort_unstable();
    format!("api keys {moved:?}")
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/api_versions_differential/cases.feature",
    name = "krabka advertises exactly the api catalog"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn krabka_advertises_exactly_the_api_catalog(catalog: ClientCatalog) {
    catalog.shutdown().await;
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/api_versions_differential/cases.feature",
    name = "divergence from real kafka matches the expectation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn divergence_from_real_kafka_matches_the_expectation(catalog: ClientCatalog) {
    catalog.shutdown().await;
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/api_versions_differential/cases.feature",
    name = "the client listener prints the same api key set as kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_listener_prints_the_same_api_key_set_as_kafka(catalog: ClientCatalog) {
    catalog.shutdown().await;
}
