//! The checked-in record of how krabka's `ApiVersions` table differs from a
//! pinned real-Kafka broker's.
//!
//! [`DivergenceReport`] is the outer join of the two advertised tables on API
//! key. It is written to `tests/fixtures/api_versions/divergence.json` and read
//! back by the differential suite, so a range krabka starts or stops
//! advertising, or a range that moves away from Kafka's, arrives in a diff
//! rather than passing unnoticed. `aspect generate-kip-matrix` reads the same
//! file for the version columns of `docs/KIP_MATRIX.md`.

use {
    krabka_broker::telemetry,
    krabka_protocol::owned::api_versions_response::ApiVersion,
    serde::{Deserialize, Serialize},
    std::{collections::BTreeSet, fs, path::Path},
};

/// An inclusive advertised version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    pub(crate) min: i16,
    pub(crate) max: i16,
}

impl From<&ApiVersion> for VersionRange {
    fn from(api: &ApiVersion) -> Self {
        Self {
            min: api.min_version,
            max: api.max_version,
        }
    }
}

/// How krabka's row for one API key stands against the Kafka oracle's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Both advertise the key over the same range.
    Same,
    /// Both advertise the key, over different ranges.
    RangeDiffers,
    /// krabka advertises the key and the oracle does not. Both tables are read
    /// on a client listener with no client-metrics receiver, and krabka scopes
    /// its own response to that pair, so this verdict now means krabka serves
    /// an API the pinned Kafka release has not shipped -- not that krabka
    /// advertises a control-plane key on a listener Kafka would not.
    KrabkaOnly,
    /// The oracle advertises the key and krabka does not.
    KafkaOnly,
}

/// One API key's row of the join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRow {
    pub(crate) api_key: i16,
    /// The canonical Kafka request name, from krabka-protocol's generated
    /// `ApiKey` registry.
    pub(crate) name: String,
    pub(crate) krabka: Option<VersionRange>,
    pub(crate) kafka: Option<VersionRange>,
    pub(crate) verdict: Verdict,
    /// Why this row's divergence is the one this repository means to have.
    ///
    /// Filled from [`RANGE_DIVERGENCE_INTENTS`] for every
    /// [`Verdict::RangeDiffers`] row, and `None` everywhere else. A range that
    /// starts differing without a sentence written for it panics in
    /// [`DivergenceReport::build`] rather than landing in the checked-in file
    /// unexplained.
    pub(crate) intent: Option<String>,
}

/// Why krabka advertises a different range from the oracle, one entry per
/// [`Verdict::RangeDiffers`] row, keyed by `api_key`.
///
/// The table is exhaustive by construction: [`DivergenceReport::build`] panics
/// on a `RangeDiffers` row with no entry here, so a range that moves away from
/// Kafka's fails `divergence_from_real_kafka_matches_the_expectation` until
/// someone decides what the divergence is for. `aspect generate-kip-matrix`
/// renders the sentence beside the two version columns in `docs/KIP_MATRIX.md`.
///
/// It is empty (#784): with `unstable.api.versions.enable` and
/// `legacy_request_versions_enable` at their defaults, which is how the suite
/// starts krabka, every listener advertises exactly Kafka 4.3.1's table. The
/// Kafka trunk versions krabka implements (`ApiVersions` v5, `TxnOffsetCommit`
/// v6, the streams v1 pair, api keys 93 and 94) and the pre-4.0 versions it
/// still decodes (`Fetch` v0-v3, `ListOffsets` v0) are opt-ins, recorded in
/// `api_catalog`'s notes and `docs/KIP_MATRIX.md` instead.
const RANGE_DIVERGENCE_INTENTS: &[RecordedIntent<'static>] = &[];

/// Keys whose advertised ranges match the oracle's while the versions krabka
/// serves do not, one entry each, with why krabka means it.
///
/// `ApiVersions` reports what a broker advertises, not what it accepts, so the
/// join alone cannot see such a divergence. [`DivergenceReport::build`] labels
/// these rows [`Verdict::RangeDiffers`] anyway, keeping both advertised ranges
/// in the version columns, so `docs/KIP_MATRIX.md` states the divergence
/// instead of calling it a match. The build panics if one of these keys stops
/// advertising the same range, since the sentence would then be describing a
/// different divergence.
///
/// It is empty (#784): `Produce` v0-v2, advertised by both brokers, is refused
/// by krabka too unless `legacy_request_versions_enable` is set.
const SERVED_RANGE_DIVERGENCES: &[RecordedIntent<'static>] = &[];

/// Why krabka means one API key to diverge from the oracle.
struct RecordedIntent<'a> {
    api_key: i16,
    intent: &'a str,
}

/// The two recorded-intent tables one join reads.
#[derive(Clone, Copy)]
struct Intents<'a> {
    range: &'a [RecordedIntent<'a>],
    served: &'a [RecordedIntent<'a>],
}

/// The tables this repository records.
const RECORDED: Intents<'static> = Intents {
    range: RANGE_DIVERGENCE_INTENTS,
    served: SERVED_RANGE_DIVERGENCES,
};

/// The recorded intent for one row of the join.
///
/// Only a [`Verdict::RangeDiffers`] row carries one: a matching range needs no
/// explanation, and a one-sided row is explained once, in prose, by the
/// oracle's listener and configuration rather than per API key.
///
/// # Panics
///
/// Panics when a range differs and `intents` has no entry for the key. That
/// is the point of the table: a new divergence stops the differential suite
/// until someone writes down whether it is meant.
fn range_divergence_intent(intents: Intents<'_>, api_key: i16, verdict: Verdict) -> Option<String> {
    if verdict != Verdict::RangeDiffers {
        return None;
    }
    let Some(RecordedIntent { intent, .. }) = intents
        .range
        .iter()
        .chain(intents.served)
        .find(|recorded| recorded.api_key == api_key)
    else {
        panic!(
            "api_key {api_key} now advertises a different range from the \
             oracle and no intent is recorded for it. Add an entry to \
             `RANGE_DIVERGENCE_INTENTS` saying whether krabka means to \
             diverge here -- and, if it does not, change \
             `api_catalog::supported_apis` instead."
        )
    };
    Some((*intent).to_owned())
}

/// Both advertised tables, joined and sorted by API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergenceReport {
    /// The image the `kafka` column was read from, tag included.
    pub(crate) oracle_image: String,
    pub(crate) apis: Vec<ApiRow>,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DivergenceSetup<'a> {
    #[default(RECORDED)]
    intents: Intents<'a>,
    #[default("mirror.gcr.io/apache/kafka:4.3.1")]
    oracle_image: &'a str,
}

#[bon::bon]
impl DivergenceReport {
    /// Join krabka's advertised table against the oracle's.
    pub(crate) fn build(oracle_image: &str, krabka: &[ApiVersion], kafka: &[ApiVersion]) -> Self {
        Self::build_with()
            .krabka(krabka)
            .kafka(kafka)
            .oracle_image(oracle_image)
            .call()
    }

    /// [`Self::build`] against `intents` rather than the recorded tables.
    #[builder]
    fn build_with(
        krabka: &[ApiVersion],
        kafka: &[ApiVersion],
        #[builder(default = RECORDED)] intents: Intents<'_>,
        #[builder(default = "mirror.gcr.io/apache/kafka:4.3.1")] oracle_image: &'_ str,
    ) -> Self {
        let setup = DivergenceSetup {
            intents,
            oracle_image,
        };
        let DivergenceSetup {
            intents,
            oracle_image,
        } = setup;
        let mut keys: Vec<i16> = krabka
            .iter()
            .chain(kafka)
            .map(|api| api.api_key)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        keys.sort_unstable();

        let find = |table: &[ApiVersion], key: i16| -> Option<VersionRange> {
            table
                .iter()
                .find(|api| api.api_key == key)
                .map(VersionRange::from)
        };

        let apis = keys
            .into_iter()
            .map(|api_key| {
                let krabka = find(krabka, api_key);
                let kafka = find(kafka, api_key);
                let served_divergence = intents
                    .served
                    .iter()
                    .any(|recorded| recorded.api_key == api_key);
                let verdict = match (krabka, kafka) {
                    (Some(ours), Some(theirs)) if ours == theirs && served_divergence => {
                        Verdict::RangeDiffers
                    }
                    (Some(_), Some(_)) if served_divergence => panic!(
                        "api_key {api_key} is recorded in `SERVED_RANGE_DIVERGENCES` \
                         as advertising the oracle's range, and no longer does; \
                         rewrite or move its entry"
                    ),
                    (Some(ours), Some(theirs)) if ours == theirs => Verdict::Same,
                    (Some(_), Some(_)) => Verdict::RangeDiffers,
                    (Some(_), None) => Verdict::KrabkaOnly,
                    // A key reaches the join only from one of the two tables,
                    // so the empty case cannot arise.
                    (None, _) => Verdict::KafkaOnly,
                };
                ApiRow {
                    api_key,
                    name: telemetry::api_name(api_key).to_owned(),
                    krabka,
                    kafka,
                    verdict,
                    intent: range_divergence_intent(intents, api_key, verdict),
                }
            })
            .collect();

        Self {
            oracle_image: oracle_image.to_owned(),
            apis,
        }
    }

    /// Read the checked-in report.
    ///
    /// # Panics
    ///
    /// Panics when the file is missing or does not deserialize, both of which
    /// mean the expectation has to be regenerated rather than compared.
    pub(crate) fn load(path: &Path) -> Self {
        let body =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
    }

    /// Overwrite the checked-in report with this one.
    ///
    /// # Panics
    ///
    /// Panics when the file cannot be written.
    pub(crate) fn store(&self, path: &Path) {
        let mut body = serde_json::to_string_pretty(self).expect("serialize divergence report");
        body.push('\n');
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("create {}: {e}", parent.display()));
        }
        fs::write(path, &body).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        eprintln!(
            "KRABKA[test] rewrote {} ({} bytes)",
            path.display(),
            body.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        std::{
            collections::BTreeSet,
            fs,
            panic::{AssertUnwindSafe, catch_unwind},
            path::PathBuf,
        },
        tempfile::TempDir,
    };

    fn api(api_key: i16, min_version: i16, max_version: i16) -> ApiVersion {
        ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        }
    }

    fn range(min: i16, max: i16) -> VersionRange { VersionRange { min, max } }

    /// Intent tables of the shape the recorded ones once had, so the join's
    /// labelling is exercised whatever the recorded tables hold.
    const SAMPLE: Intents<'static> = Intents {
        range: &[RecordedIntent {
            api_key: 1,
            intent: "Fetch serves older versions.",
        }],
        served: &[RecordedIntent {
            api_key: 0,
            intent: "Produce serves older versions.",
        }],
    };

    struct ReportScenario {
        oracle: String,
        krabka: Vec<ApiVersion>,
        kafka: Vec<ApiVersion>,
        intents: Option<Intents<'static>>,
        listed_intents: Vec<(i16, String)>,
        outcome: Option<Result<DivergenceReport, String>>,
        directory: Option<TempDir>,
        checked_in: Option<String>,
        stored: Option<String>,
        reloaded: Option<DivergenceReport>,
        rebuilt: Option<DivergenceReport>,
    }

    impl ReportScenario {
        fn report(&self) -> &DivergenceReport {
            self.outcome
                .as_ref()
                .expect("a report was constructed")
                .as_ref()
                .expect("a valid report")
        }
    }

    #[fixture]
    fn report() -> ReportScenario {
        // Start this scenario with fresh inputs and observations.

        ReportScenario {
            oracle: String::new(),
            krabka: Vec::new(),
            kafka: Vec::new(),
            intents: None,
            listed_intents: Vec::new(),
            outcome: None,
            directory: None,
            checked_in: None,
            stored: None,
            reloaded: None,
            rebuilt: None,
        }
    }

    #[given("oracle image {image} and sample Fetch-range and Produce-served-range explanations")]
    fn sample_intents(report: &mut ReportScenario, image: String) {
        report.oracle = image;
        report.intents = Some(SAMPLE);
    }

    #[given("the checked-in API divergence explanations")]
    fn recorded_intents(report: &mut ReportScenario) {
        report.oracle = "oracle:1.2.3".into();
        report.intents = Some(RECORDED);
    }

    #[derive(DataTableRow)]
    struct AdvertisedRange {
        key: i16,
        minimum: i16,
        maximum: i16,
    }

    #[given("krabka advertises:")]
    fn krabka_ranges(report: &mut ReportScenario, #[datatable] rows: Rows<AdvertisedRange>) {
        report.krabka = rows
            .into_iter()
            .map(|row| api(row.key, row.minimum, row.maximum))
            .collect();
    }

    #[given("Kafka advertises:")]
    fn kafka_ranges(report: &mut ReportScenario, #[datatable] rows: Rows<AdvertisedRange>) {
        report.kafka = rows
            .into_iter()
            .map(|row| api(row.key, row.minimum, row.maximum))
            .collect();
    }

    #[when("the advertised tables are joined into a divergence report")]
    fn join_tables(report: &mut ReportScenario) {
        let intents = report.intents.expect("a divergence explanation table");
        report.outcome = Some(
            catch_unwind(AssertUnwindSafe(|| {
                DivergenceReport::build_with()
                    .krabka(&report.krabka)
                    .kafka(&report.kafka)
                    .intents(intents)
                    .oracle_image(&report.oracle)
                    .call()
            }))
            .map_err(|payload| {
                payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| {
                        payload
                            .downcast_ref::<&str>()
                            .map(|message| (*message).to_owned())
                    })
                    .expect("a string panic from divergence report construction")
            }),
        );
    }

    #[when("all configured divergence explanations are enumerated")]
    fn enumerate_intents(report: &mut ReportScenario) {
        let intents = report.intents.expect("the checked-in explanation tables");
        report.listed_intents = intents
            .range
            .iter()
            .chain(intents.served)
            .map(|recorded| (recorded.api_key, recorded.intent.to_owned()))
            .collect();
    }

    #[then("every explanation has a unique API key and a nonempty reason")]
    fn valid_intents(report: &ReportScenario) {
        let unique: BTreeSet<i16> = report.listed_intents.iter().map(|(key, _)| *key).collect();
        assert!(unique.len() == report.listed_intents.len());
        assert!(
            report
                .listed_intents
                .iter()
                .all(|(_, intent)| !intent.trim().is_empty())
        );
    }

    #[derive(DataTableRow)]
    struct ExpectedApi {
        key: i16,
        name: String,
        krabka_minimum: String,
        krabka_maximum: String,
        kafka_minimum: String,
        kafka_maximum: String,
        verdict: String,
        reason: String,
    }

    fn expected_range(minimum: &str, maximum: &str) -> Option<VersionRange> {
        if minimum == "absent" && maximum == "absent" {
            None
        } else {
            Some(range(
                minimum.parse().expect("a minimum API version"),
                maximum.parse().expect("a maximum API version"),
            ))
        }
    }

    #[then("the report has exactly these API rows in key order:")]
    fn report_rows(report: &ReportScenario, #[datatable] rows: Rows<ExpectedApi>) {
        let apis = rows
            .into_iter()
            .map(|row| ApiRow {
                api_key: row.key,
                name: row.name,
                krabka: expected_range(&row.krabka_minimum, &row.krabka_maximum),
                kafka: expected_range(&row.kafka_minimum, &row.kafka_maximum),
                verdict: match row.verdict.as_str() {
                    "same" => Verdict::Same,
                    "range-differs" => Verdict::RangeDiffers,
                    "krabka-only" => Verdict::KrabkaOnly,
                    "kafka-only" => Verdict::KafkaOnly,
                    other => panic!("unknown divergence verdict: {other}"),
                },
                intent: (row.reason != "none").then_some(row.reason),
            })
            .collect();
        let expected = DivergenceReport {
            oracle_image: report.oracle.clone(),
            apis,
        };
        assert!(report.report() == &expected);
    }

    #[then("the report is rejected because {message}")]
    fn report_rejected(report: &ReportScenario, message: String) {
        let outcome = report
            .outcome
            .as_ref()
            .expect("report construction was attempted");
        let Err(actual) = outcome else {
            panic!("report construction should fail because {message}: {outcome:?}")
        };
        assert!(
            actual.contains(&message),
            "expected report failure {message:?}, got {actual:?}"
        );
    }

    #[given("the checked-in divergence report loaded from a temporary file")]
    fn checked_in_report(report: &mut ReportScenario) {
        const CHECKED_IN: &str = include_str!("../fixtures/api_versions/divergence.json");
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("decoded.json");
        fs::write(&path, CHECKED_IN).expect("write checked-in");
        report.outcome = Some(Ok(DivergenceReport::load(&path)));
        report.checked_in = Some(CHECKED_IN.to_owned());
        report.directory = Some(directory);
    }

    #[when("the public report builder rejoins the recorded advertised ranges")]
    fn rebuild_recorded_report(report: &mut ReportScenario) {
        let source = report.report();
        let ours: Vec<_> = source
            .apis
            .iter()
            .filter_map(|row| {
                row.krabka
                    .map(|range| api(row.api_key, range.min, range.max))
            })
            .collect();
        let theirs: Vec<_> = source
            .apis
            .iter()
            .filter_map(|row| {
                row.kafka
                    .map(|range| api(row.api_key, range.min, range.max))
            })
            .collect();
        report.rebuilt = Some(DivergenceReport::build(
            &source.oracle_image,
            &ours,
            &theirs,
        ));
    }

    #[then("the rebuilt report equals the complete reviewed report")]
    fn rebuilt_recorded_report(report: &ReportScenario) {
        assert!(report.rebuilt.as_ref().expect("public builder invoked") == report.report());
    }

    #[when("the report is stored in a fresh nested JSON path and reloaded")]
    fn store_report(report: &mut ReportScenario) {
        let directory = tempfile::tempdir().expect("tempdir");
        let path: PathBuf = directory.path().join("nested").join("divergence.json");
        report.report().store(&path);
        report.stored = Some(fs::read_to_string(&path).expect("read rewritten"));
        report.reloaded = Some(DivergenceReport::load(&path));
        report.directory = Some(directory);
    }

    #[then("the stored bytes exactly match the checked-in report")]
    fn canonical_report(report: &ReportScenario) {
        let stored = report.stored.as_ref().expect("the report was stored");
        let checked_in = report
            .checked_in
            .as_ref()
            .expect("the checked-in report was loaded");
        assert!(stored == checked_in);
    }

    #[then("the reloaded report equals the original report")]
    fn round_trip(report: &ReportScenario) {
        assert!(report.reloaded.as_ref() == Some(report.report()));
    }

    scenarios!("tests/features/migrated/tests/api_versions_differential/divergence.feature", fixtures = [report: ReportScenario]);
}
