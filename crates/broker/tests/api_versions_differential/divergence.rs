//! The checked-in record of how krabka's `ApiVersions` table differs from a
//! pinned real-Kafka broker's.
//!
//! [`DivergenceReport`] is the outer join of the two advertised tables on API
//! key. It is written to `tests/fixtures/api_versions/divergence.json` and read
//! back by the differential suite, so a range krabka starts or stops
//! advertising, or a range that moves away from Kafka's, arrives in a diff
//! rather than passing unnoticed. `aspect generate-kip-matrix` reads the same
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
const RANGE_DIVERGENCE_INTENTS: &[(i16, &str)] = &[
    (
        1, // Fetch
        "Intended. krabka still serves the pre-v4 Fetch request shapes that \
         Kafka 4.x dropped: `api_catalog::client_facing_apis` takes the key's \
         `min_version` from `krabka_protocol::kafka_3_6_2::owned::fetch_request`, \
         and `handlers::fetch::encode_fetch_response` answers v0-v3 from the \
         same `kafka_3_6_2` flavor, which `throttle_audit`'s split probe also \
         covers. The wider range costs a client nothing -- version negotiation \
         picks the highest both sides know -- and it keeps a long-lived \
         pre-4.0 consumer working against krabka.",
    ),
    (
        2, // ListOffsets
        "Intended, and the same story as Fetch: krabka advertises ListOffsets \
         from v0, which Kafka 4.x no longer does. `client_facing_apis` sets \
         that `min_version` to 0 explicitly and `handlers::list_offsets` \
         routes v0 to its own hand-rolled `v0` module, because the generated \
         schema starts at v1. A modern client negotiates v11 either way.",
    ),
    (
        18, // ApiVersions
        "Intended: krabka advertises KIP-1242 ApiVersions v5, the routing \
         identity and the REBOOTSTRAP_REQUIRED answer, which no released \
         Kafka broker serves yet. The capability is real -- \
         `handlers::api_versions` implements the v5 checks from \
         `ROUTING_IDENTITY_MIN_VERSION` up and `api_versions/tests.rs` drives \
         every one of them -- so clamping `max_version` to the oracle's v4 \
         would make the broker deny a feature it has. The risk taken \
         deliberately: a client built against Kafka trunk schemas can \
         negotiate a version no released broker has validated, so a v5 shape \
         change before Kafka ships it lands here as a wire break.",
    ),
    (
        22, // InitProducerId
        "Intended: krabka advertises InitProducerId v6, the KIP-939 \
         two-phase-commit request shape with `enable2Pc` and \
         `keepPreparedTxn`. `handlers::init_producer_id` implements both \
         fields with Kafka's own gates -- a cluster without \
         `transaction.two.phase.commit.enable` gets \
         TRANSACTIONAL_ID_AUTHORIZATION_FAILED, a principal without the \
         TWO_PHASE_COMMIT ACL the same, and a transaction version below 2 \
         gets UNSUPPORTED_VERSION -- and `transactions_2pc.rs` drives them. \
         Kafka's oracle stops at v5 because it clamps the advertised maximum \
         to the finalized `transaction.version` feature, which this stock \
         broker leaves below 2; krabka advertises unconditionally and refuses \
         at call time instead. Same risk as ApiVersions v5: a client can pick \
         v6 against a cluster whose transaction version cannot serve it, and \
         learns that from the error code rather than from negotiation.",
    ),
];

/// The recorded intent for one row of the join.
///
/// Only a [`Verdict::RangeDiffers`] row carries one: a matching range needs no
/// explanation, and a one-sided row is explained once, in prose, by the
/// oracle's listener and configuration rather than per API key.
///
/// # Panics
///
/// Panics when a range differs and [`RANGE_DIVERGENCE_INTENTS`] has no entry
/// for the key. That is the point of the table: a new divergence stops the
/// differential suite until someone writes down whether it is meant.
fn range_divergence_intent(api_key: i16, verdict: Verdict) -> Option<String> {
    if verdict != Verdict::RangeDiffers {
        return None;
    }
    let intent = RANGE_DIVERGENCE_INTENTS
        .iter()
        .find(|(key, _)| *key == api_key)
        .map(|(_, intent)| (*intent).to_owned())
        .unwrap_or_else(|| {
            panic!(
                "api_key {api_key} now advertises a different range from the \
                 oracle and no intent is recorded for it. Add an entry to \
                 `RANGE_DIVERGENCE_INTENTS` saying whether krabka means to \
                 diverge here -- and, if it does not, change \
                 `api_catalog::supported_apis` instead."
            )
        });
    Some(intent)
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
                    intent: range_divergence_intent(api_key, verdict),
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

    /// The recorded intent for `api_key`, as `build` writes it.
    fn intent(api_key: i16) -> Option<String> {
        range_divergence_intent(api_key, Verdict::RangeDiffers)
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
                            intent: None,
                        },
                        ApiRow {
                            api_key: 1,
                            name: "Fetch".to_owned(),
                            krabka: Some(range(4, 17)),
                            kafka: Some(range(4, 18)),
                            verdict: Verdict::RangeDiffers,
                            intent: intent(1),
                        },
                        ApiRow {
                            api_key: 80,
                            name: "AddRaftVoter".to_owned(),
                            krabka: Some(range(0, 1)),
                            kafka: None,
                            verdict: Verdict::KrabkaOnly,
                            intent: None,
                        },
                        ApiRow {
                            api_key: 88,
                            name: "StreamsGroupHeartbeat".to_owned(),
                            krabka: None,
                            kafka: Some(range(0, 0)),
                            verdict: Verdict::KafkaOnly,
                            intent: None,
                        },
                    ],
                }
        );
    }

    /// Every recorded intent is reachable, so a key that stops diverging
    /// leaves no stale sentence behind.
    #[test]
    fn every_recorded_intent_is_non_empty_and_keyed_once() {
        let mut keys: Vec<i16> = RANGE_DIVERGENCE_INTENTS.iter().map(|(key, _)| *key).collect();
        let unique: std::collections::BTreeSet<i16> = keys.iter().copied().collect();
        keys.sort_unstable();
        assert!(keys == unique.into_iter().collect::<Vec<i16>>());
        assert!(
            RANGE_DIVERGENCE_INTENTS
                .iter()
                .all(|(_, intent)| !intent.trim().is_empty())
        );
    }

    /// A range that starts differing with nothing written for it fails the
    /// differential suite rather than landing in the checked-in file.
    #[test]
    #[should_panic(expected = "no intent is recorded")]
    fn an_unrecorded_range_divergence_panics() {
        let _ = DivergenceReport::build("oracle:1.2.3", &[api(3, 0, 13)], &[api(3, 0, 12)]);
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
