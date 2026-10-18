//! The lifecycle contract: the release a cluster starts on, the transitions it
//! goes through under load, and whether it may be rolled back to that release.
//!
//! A `lifecycle` contract is a soak contract whose fault plan holds lifecycle
//! transitions as well as faults: `roll` restarts one broker from another
//! release's image, `certificate_rotation` swaps one broker's TLS keystore and
//! truststore, and `reassignment` moves one replica of every partition of a
//! path onto another broker. The executor measures each one's disruption as it
//! measures a fault's, and after each one writes a [`Checkpoint`]: the image
//! every broker runs, the finalized feature levels, whether a rollback to the
//! source release is still supported, and a reconciliation of the records,
//! the committed offsets and the topic configuration as they stand.
//!
//! The source release is declared by digest, with the feature ranges it
//! supports. The target is the candidate the run is given, so it is not
//! declared. [`rollback`] derives whether a cluster may roll back to the
//! source from the levels it has finalized, by the rule of
//! `docs/persisted_formats.md`: a 1.x broker writes a new format only once its
//! feature level is finalized, so a cluster that finalized nothing above what
//! the source supports holds nothing the source cannot read.

use {
    super::{
        contract::{Contract, FaultKind, Isolation, Semantics},
        evidence::{Evidence, ReaderLedger},
        ledger::SeqSet,
        reconcile::{Finding, FindingKind},
        schedule::ScheduledFault,
    },
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fmt},
};

/// Which release a `roll` restarts a broker from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The declared source release: a rollback.
    Source,
    /// The candidate under test: an upgrade.
    Target,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Source => "source",
            Side::Target => "target",
        })
    }
}

/// The levels of one feature that a release supports, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelRange {
    pub min: i16,
    pub max: i16,
}

impl fmt::Display for LevelRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.min, self.max)
    }
}

/// A published release, by image digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    /// The release tag, for example `v1.0.5`.
    pub version: String,
    /// The image, `<reference>@sha256:<digest>`. A tag alone is refused: it
    /// can move after the contract is written.
    pub image: String,
    /// The feature ranges the release advertises in `ApiVersions`. A feature
    /// it does not name supports level 0 only, as Kafka's
    /// `QuorumFeatures.DISABLED` does. The executor reads the running source's
    /// ranges back and the judge compares them with these.
    pub supports: BTreeMap<String, LevelRange>,
}

/// The `lifecycle` section of a contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lifecycle {
    /// The release the cluster is formatted and started on.
    pub source: Release,
}

/// Whether a cluster may roll back to the source release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rollback", content = "reasons", rename_all = "snake_case")]
pub enum Rollback {
    /// Every finalized level is one the source supports.
    Supported,
    /// The finalized levels the source does not support. A source broker
    /// refuses to start over them.
    Unsupported(Vec<String>),
}

/// Whether a cluster that finalized `finalized` may roll back to `source`.
///
/// It may when the source supports every finalized level. A feature the
/// source does not name supports level 0 only, so finalizing such a feature
/// above 0 also rules a rollback out. A feature that is not finalized is at
/// level 0, which every release supports.
#[must_use]
pub fn rollback(source: &Release, finalized: &BTreeMap<String, i16>) -> Rollback {
    let reasons: Vec<String> = finalized
        .iter()
        .filter_map(|(feature, &level)| {
            let range = source
                .supports
                .get(feature)
                .copied()
                .unwrap_or(LevelRange { min: 0, max: 0 });
            (!(range.min..=range.max).contains(&level)).then(|| {
                format!(
                    "{feature} is finalized at {level}, but {} supports {range}",
                    source.version
                )
            })
        })
        .collect();
    if reasons.is_empty() {
        Rollback::Supported
    } else {
        Rollback::Unsupported(reasons)
    }
}

/// The cluster as it stood after one lifecycle transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The transition, `<plan>#<n>`.
    pub fault: String,
    pub unix_ms: i64,
    /// The image every broker runs, by node id, as `docker inspect` reports it.
    pub images: BTreeMap<i32, String>,
    /// The finalized feature levels, as `ApiVersions` reports them.
    pub finalized: BTreeMap<String, i16>,
    /// [`rollback`] over `finalized`.
    pub rollback: Rollback,
    /// For each path, what every record acknowledged before the read-back
    /// was checked against: the producer's acknowledged records then, and what
    /// a `read_uncommitted` reader found from the earliest offset.
    pub records: Vec<CheckpointRead>,
    /// Topic and broker configuration that differs from what the run set.
    #[serde(default)]
    pub config_mismatches: Vec<String>,
}

/// One path's records at a checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRead {
    pub path: String,
    pub semantics: Semantics,
    #[serde(default)]
    pub key_space: Option<u64>,
    /// The producer's acknowledged records when the read-back started.
    pub acknowledged: SeqSet,
    pub reader: ReaderLedger,
}

/// The acknowledged records of `read` that its reader did not find.
///
/// A compacted path is held to the latest acknowledged value of each key, which
/// compaction must keep; an older value of a key may already be gone.
#[must_use]
pub fn checkpoint_findings(read: &CheckpointRead) -> Vec<Finding> {
    let (kind, missing, detail) =
        if let (Semantics::Compacted, Some(keys)) = (read.semantics, read.key_space) {
            (
                FindingKind::CompactionRegression,
                regressed_keys(&read.acknowledged, keys, &read.reader.latest_by_key),
                "the latest acknowledged value of a key was not read back",
            )
        } else {
            (
                FindingKind::Lost,
                read.acknowledged.difference(&read.reader.seen),
                "acknowledged before the checkpoint and not read back",
            )
        };
    if missing.is_empty() {
        return Vec::new();
    }
    vec![Finding {
        kind,
        path: read.path.clone(),
        subject: read.reader.producer.clone(),
        isolation: Some(Isolation::ReadUncommitted),
        seqs: missing,
        detail: detail.to_owned(),
    }]
}

/// The latest acknowledged record of each of `keys` keys whose value the
/// reader's `latest_by_key` does not reach.
fn regressed_keys(acknowledged: &SeqSet, keys: u64, latest_by_key: &BTreeMap<u64, u64>) -> SeqSet {
    let mut latest: BTreeMap<u64, u64> = BTreeMap::new();
    for seq in acknowledged.iter() {
        latest.insert(seq % keys.max(1), seq);
    }
    let mut regressed = SeqSet::new();
    for (key, seq) in latest {
        if latest_by_key.get(&key).is_none_or(|read| *read < seq) {
            regressed.insert(seq);
        }
    }
    regressed
}

/// Whether `kind` is a lifecycle transition, which leaves a checkpoint.
#[must_use]
pub fn is_transition(kind: &FaultKind) -> bool {
    matches!(
        kind,
        FaultKind::Roll { .. } | FaultKind::CertificateRotation | FaultKind::Reassignment { .. }
    )
}

/// Every lifecycle failure of `evidence`: the checks a lifecycle contract adds
/// to the soak verdict, one line each.
///
/// - The source must advertise the feature ranges the contract declares.
/// - Every transition must leave a checkpoint, and every checkpoint must
///   reconcile its records and its configuration.
/// - After a `roll`, the rolled broker must run that side's image.
/// - A `roll` to the source must not follow a checkpoint that ruled a
///   rollback out.
#[must_use]
pub fn judge_lifecycle(
    contract: &Contract,
    schedule: &[ScheduledFault],
    evidence: &Evidence,
) -> Vec<String> {
    let Some(lifecycle) = &contract.lifecycle else {
        return Vec::new();
    };
    let mut failures = Vec::new();
    if evidence.source_supports != lifecycle.source.supports {
        failures.push(format!(
            "{} advertised {:?}, but the contract declares {:?}",
            lifecycle.source.version, evidence.source_supports, lifecycle.source.supports
        ));
    }
    let mut rollback_ruled_out: Option<String> = None;
    for fault in schedule.iter().filter(|fault| is_transition(&fault.kind)) {
        if let FaultKind::Roll { to: Side::Source } = fault.kind
            && let Some(reason) = &rollback_ruled_out
        {
            failures.push(format!(
                "{} rolled back to {} after a checkpoint ruled it out: {reason}",
                fault.id, lifecycle.source.version
            ));
        }
        let Some(checkpoint) = evidence
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.fault == fault.id)
        else {
            failures.push(format!("{}: no checkpoint after the transition", fault.id));
            continue;
        };
        for read in &checkpoint.records {
            failures.extend(
                checkpoint_findings(read)
                    .into_iter()
                    .map(|finding| format!("{}: {finding}", fault.id)),
            );
        }
        failures.extend(
            checkpoint
                .config_mismatches
                .iter()
                .map(|mismatch| format!("{}: configuration {mismatch}", fault.id)),
        );
        if let FaultKind::Roll { to } = fault.kind {
            let expected = evidence.releases.get(&to.to_string());
            let node = evidence
                .faults
                .iter()
                .find(|record| record.id == fault.id)
                .and_then(|record| record.node);
            let running = node.and_then(|node| checkpoint.images.get(&node));
            if expected.is_none() || running != expected {
                failures.push(format!(
                    "{}: node {node:?} runs {running:?} after a roll to the {to}, whose image is {expected:?}",
                    fault.id
                ));
            }
        }
        rollback_ruled_out = match &checkpoint.rollback {
            Rollback::Supported => None,
            Rollback::Unsupported(reasons) => Some(reasons.join("; ")),
        };
    }
    failures
}

#[cfg(test)]
mod tests {
    use {
        super::{
            CheckpointRead, LevelRange, Release, Rollback, checkpoint_findings, judge_lifecycle,
            rollback,
        },
        crate::soak::{
            contract::{Isolation, Semantics, fixture},
            evidence::{Evidence, ReaderLedger},
            ledger::SeqSet,
            reconcile::FindingKind,
            schedule::expand,
            synthetic::clean_run,
        },
        assert2::check,
        std::collections::BTreeMap,
    };

    fn source() -> Release {
        Release {
            version: "v1.0.5".to_owned(),
            image: "ghcr.io/krabka-io/krabka-broker@sha256:00".to_owned(),
            supports: BTreeMap::from([
                ("metadata.version".to_owned(), LevelRange {
                    min: 7,
                    max: 30,
                }),
                ("krabka.version".to_owned(), LevelRange { min: 0, max: 1 }),
            ]),
        }
    }

    fn levels(levels: &[(&str, i16)]) -> BTreeMap<String, i16> {
        levels
            .iter()
            .map(|(feature, level)| ((*feature).to_owned(), *level))
            .collect()
    }

    #[yare::parameterized(
        nothing_finalized = { &[], None },
        the_source_levels = { &[("metadata.version", 30), ("krabka.version", 1)], None },
        level_zero_of_an_unknown_feature = { &[("group.version", 0)], None },
        a_newer_metadata_version = {
            &[("metadata.version", 31)],
            Some("metadata.version is finalized at 31, but v1.0.5 supports 7-30")
        },
        the_first_new_krabka_format = {
            &[("krabka.version", 2)],
            Some("krabka.version is finalized at 2, but v1.0.5 supports 0-1")
        },
        a_feature_the_source_does_not_know = {
            &[("share.version", 1)],
            Some("share.version is finalized at 1, but v1.0.5 supports 0-0")
        },
    )]
    fn a_rollback_is_supported_only_below_every_level_the_source_supports(
        finalized: &[(&str, i16)],
        refused: Option<&str>,
    ) {
        let expected = refused.map_or(Rollback::Supported, |reason| {
            Rollback::Unsupported(vec![reason.to_owned()])
        });
        check!(rollback(&source(), &levels(finalized)) == expected);
    }

    fn read(semantics: Semantics, acknowledged: SeqSet, seen: SeqSet) -> CheckpointRead {
        let key_space = (semantics == Semantics::Compacted).then_some(4);
        let latest_by_key = key_space.map_or_else(BTreeMap::new, |keys| {
            let mut latest = BTreeMap::new();
            for seq in seen.iter() {
                latest.insert(seq % keys, seq);
            }
            latest
        });
        CheckpointRead {
            path: "p".to_owned(),
            semantics,
            key_space,
            acknowledged,
            reader: ReaderLedger {
                path: "p".to_owned(),
                isolation: Isolation::ReadUncommitted,
                producer: "p-0".to_owned(),
                seen,
                duplicates: Vec::new(),
                latest_by_key,
            },
        }
    }

    /// A clean synthetic run of the lifecycle fixture, judged, after `edit`.
    fn lifecycle_failures(edit: fn(&mut Evidence)) -> Vec<String> {
        let contract = fixture::lifecycle();
        let mut evidence = clean_run(&contract);
        edit(&mut evidence);
        judge_lifecycle(&contract, &expand(&contract).unwrap(), &evidence)
    }

    #[yare::parameterized(
        clean = { |_| {}, None },
        source_ranges_differ = {
            |e: &mut Evidence| e.source_supports.clear(),
            Some("v1.0.5 advertised {}")
        },
        no_checkpoint = {
            |e: &mut Evidence| { e.checkpoints.remove(1); },
            Some("rotate#0: no checkpoint")
        },
        wrong_image_after_a_roll = {
            |e: &mut Evidence| { e.checkpoints[0].images.insert(1, "synthetic-source".into()); },
            Some("upgrade#0: node Some(1) runs Some(\"synthetic-source\") after a roll to the target")
        },
        rollback_after_a_newer_level = {
            |e: &mut Evidence| e.checkpoints[2].rollback = Rollback::Unsupported(vec!["krabka.version is finalized at 2".into()]),
            Some("rollback#0 rolled back to v1.0.5 after a checkpoint ruled it out: krabka.version is finalized at 2")
        },
        config_drift = {
            |e: &mut Evidence| e.checkpoints[1].config_mismatches.push("classic-idempotent min.insync.replicas is 1, not 2".into()),
            Some("rotate#0: configuration classic-idempotent min.insync.replicas is 1, not 2")
        },
    )]
    fn a_lifecycle_run_fails_on_each_broken_promise(
        edit: fn(&mut Evidence),
        failure: Option<&str>,
    ) {
        let failures = lifecycle_failures(edit);
        check!(
            failures.len() == usize::from(failure.is_some()),
            "{failures:#?}"
        );
        check!(
            failure.is_none_or(|failure| failures.iter().any(|found| found.starts_with(failure))),
            "{failures:#?}"
        );
    }

    #[yare::parameterized(
        all_read = { Semantics::Idempotent, SeqSet::range(0, 10), SeqSet::range(0, 12), None },
        one_lost = {
            Semantics::Transactional,
            SeqSet::range(0, 10),
            SeqSet::range(0, 9),
            Some((FindingKind::Lost, vec![9]))
        },
        compacted_older_values_gone = {
            Semantics::Compacted, SeqSet::range(0, 8), SeqSet::range(4, 8), None
        },
        compacted_latest_value_gone = {
            Semantics::Compacted,
            SeqSet::range(0, 8),
            SeqSet::range(3, 7),
            Some((FindingKind::CompactionRegression, vec![7]))
        },
    )]
    fn a_checkpoint_holds_every_acknowledged_record_its_semantics_keep(
        semantics: Semantics,
        acknowledged: SeqSet,
        seen: SeqSet,
        expected: Option<(FindingKind, Vec<u64>)>,
    ) {
        let found: Vec<(FindingKind, Vec<u64>)> =
            checkpoint_findings(&read(semantics, acknowledged, seen))
                .into_iter()
                .map(|finding| (finding.kind, finding.seqs.iter().collect()))
                .collect();
        check!(found == expected.into_iter().collect::<Vec<_>>());
    }
}
