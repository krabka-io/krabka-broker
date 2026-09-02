//! The checked-in record of how krabka's `ApiVersions` table differs from a
//! pinned real-Kafka broker's.
//!
//! [`DivergenceReport`] is the outer join of the two advertised tables on API
//! key. It is written to `tests/fixtures/api_versions/divergence.json` and read
//! back by the differential suite, so a range krabka starts or stops
//! advertising, or a range that moves away from Kafka's, arrives in a diff
//! rather than passing unnoticed. `tools/generate-kip-matrix.py` reads the same
//! file for the version columns of `docs/KIP_MATRIX.md`.

use std::path::Path;

use krabka_protocol::owned::api_versions_response::ApiVersion;
use serde::{Deserialize, Serialize};

/// An inclusive advertised version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VersionRange {
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
pub(crate) enum Verdict {
    /// Both advertise the key over the same range.
    Same,
    /// Both advertise the key, over different ranges.
    RangeDiffers,
    /// krabka advertises the key and the oracle does not -- a statement about
    /// the listener and configuration the oracle was read on, which `oracle`
    /// spells out, rather than about Kafka's API set.
    KrabkaOnly,
    /// The oracle advertises the key and krabka does not.
    KafkaOnly,
}

/// One API key's row of the join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ApiRow {
    pub(crate) api_key: i16,
    /// The canonical Kafka request name, from krabka-protocol's generated
    /// `ApiKey` registry.
    pub(crate) name: String,
    pub(crate) krabka: Option<VersionRange>,
    pub(crate) kafka: Option<VersionRange>,
    pub(crate) verdict: Verdict,
}

/// Both advertised tables, joined and sorted by API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DivergenceReport {
    /// The image the `kafka` column was read from, tag included.
    pub(crate) oracle_image: String,
    pub(crate) apis: Vec<ApiRow>,
}

impl DivergenceReport {
    /// Join krabka's advertised table against the oracle's.
    pub(crate) fn build(oracle_image: &str, krabka: &[ApiVersion], kafka: &[ApiVersion]) -> Self {
        let mut keys: Vec<i16> = krabka
            .iter()
            .chain(kafka)
            .map(|api| api.api_key)
            .collect::<std::collections::BTreeSet<_>>()
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
                let verdict = match (krabka, kafka) {
                    (Some(ours), Some(theirs)) if ours == theirs => Verdict::Same,
                    (Some(_), Some(_)) => Verdict::RangeDiffers,
                    (Some(_), None) => Verdict::KrabkaOnly,
                    // A key reaches the join only from one of the two tables,
                    // so the empty case cannot arise.
                    (None, _) => Verdict::KafkaOnly,
                };
                ApiRow {
                    api_key,
                    name: krabka_broker::telemetry::api_name(api_key).to_owned(),
                    krabka,
                    kafka,
                    verdict,
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
        let body = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
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
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("create {}: {e}", parent.display()));
        }
        std::fs::write(path, &body).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        eprintln!(
            "KRABKA[test] rewrote {} ({} bytes)",
            path.display(),
            body.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn api(api_key: i16, min_version: i16, max_version: i16) -> ApiVersion {
        ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        }
    }

    fn range(min: i16, max: i16) -> VersionRange {
        VersionRange { min, max }
    }

    #[test]
    fn join_covers_both_tables_and_labels_each_key() {
        let krabka = vec![api(0, 3, 12), api(1, 4, 17), api(80, 0, 1)];
        let kafka = vec![api(0, 3, 12), api(1, 4, 18), api(88, 0, 0)];
        assert!(
            DivergenceReport::build("oracle:1.2.3", &krabka, &kafka)
                == DivergenceReport {
                    oracle_image: "oracle:1.2.3".to_owned(),
                    apis: vec![
                        ApiRow {
                            api_key: 0,
                            name: "Produce".to_owned(),
                            krabka: Some(range(3, 12)),
                            kafka: Some(range(3, 12)),
                            verdict: Verdict::Same,
                        },
                        ApiRow {
                            api_key: 1,
                            name: "Fetch".to_owned(),
                            krabka: Some(range(4, 17)),
                            kafka: Some(range(4, 18)),
                            verdict: Verdict::RangeDiffers,
                        },
                        ApiRow {
                            api_key: 80,
                            name: "AddRaftVoter".to_owned(),
                            krabka: Some(range(0, 1)),
                            kafka: None,
                            verdict: Verdict::KrabkaOnly,
                        },
                        ApiRow {
                            api_key: 88,
                            name: "StreamsGroupHeartbeat".to_owned(),
                            krabka: None,
                            kafka: Some(range(0, 0)),
                            verdict: Verdict::KafkaOnly,
                        },
                    ],
                }
        );
    }

    #[test]
    fn report_round_trips_through_json() {
        let report = DivergenceReport::build("oracle:1.2.3", &[api(18, 0, 4)], &[api(18, 0, 5)]);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("divergence.json");
        report.store(&path);
        assert!(DivergenceReport::load(&path) == report);
    }
}
