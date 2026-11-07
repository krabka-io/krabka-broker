//! The criterion lane's comparison of repeated reference and candidate
//! [`BenchmarkSummary`] files.

use {
    crate::{
        BenchmarkSummary, ParseBenchesError,
        contract::MINIMUM_ROUNDS,
        executables::BenchExecutable,
        stats::{calibrated_tolerance, median, relative_mad},
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs::{self, File},
        path::Path,
    },
};

/// How many times the larger side's relative median absolute deviation the
/// criterion lane allows. The budget lane reads its multiplier from the
/// contract instead.
const CRITERION_SPREAD_MULTIPLIER: f64 = 3.0;

/// One benchmark's variance-calibrated reference/candidate decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    /// Median reference time in nanoseconds.
    pub reference_median_ns: f64,
    /// Median candidate time in nanoseconds.
    pub candidate_median_ns: f64,
    /// Candidate/reference ratio.
    pub ratio: f64,
    /// Allowed relative slowdown, derived from repeated-run MAD with a floor.
    pub tolerance: f64,
    /// Whether this benchmark stayed within its tolerance, or its candidate
    /// passes overlapped the reference passes.
    pub passed: bool,
}

/// Machine-readable verdict over repeated same-host benchmark runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkVerdict {
    /// Exact commits represented by the reference samples.
    pub reference_commits: Vec<String>,
    /// Exact commits represented by the candidate samples.
    pub candidate_commits: Vec<String>,
    /// The executables every reference pass ran.
    pub reference_executables: Vec<BenchExecutable>,
    /// The executables every candidate pass ran.
    pub candidate_executables: Vec<BenchExecutable>,
    /// Per-benchmark decisions.
    pub benchmarks: BTreeMap<String, BenchmarkComparison>,
    /// True only when every benchmark passed.
    pub passed: bool,
}

/// The executables every summary of one side ran.
fn side_executables(
    side: &'static str,
    summaries: &[BenchmarkSummary],
) -> Result<Vec<BenchExecutable>, ParseBenchesError> {
    let Some(first) = summaries.first().map(|summary| &summary.executables) else {
        return Err(ParseBenchesError::TooFewSamples { side, count: 0 });
    };
    if first.is_empty() {
        return Err(ParseBenchesError::MissingExecutableIdentity(side));
    }
    if summaries
        .iter()
        .any(|summary| summary.executables != *first)
    {
        return Err(ParseBenchesError::ExecutableDrift(side));
    }
    Ok(first.clone())
}

/// Compare at least three repeated reference and candidate summaries.
///
/// Every summary must name the executables its pass ran. Each side's passes
/// must all have run the same executables, and no executable path may appear
/// on both sides: a shared path means the two revisions were measured with
/// the same binary.
///
/// The tolerance is three times the larger side's median absolute deviation,
/// with `minimum_tolerance` as a floor. A benchmark fails only when its median
/// ratio exceeds that tolerance and every candidate pass is also slower than
/// every reference pass: with a handful of passes per side, a median shift
/// inside the two sides' overlapping ranges is run-to-run noise, not a
/// regression. Bilateral all-zero samples are tied
/// below bencher's integer-nanosecond resolution. Other missing, non-finite, or
/// non-positive samples are errors and therefore cannot silently pass.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] for too few samples, mismatched benchmark
/// sets, missing, drifting or shared executables, or invalid measurements.
pub fn compare_summaries(
    reference: &[BenchmarkSummary],
    candidate: &[BenchmarkSummary],
    minimum_tolerance: f64,
) -> Result<BenchmarkVerdict, ParseBenchesError> {
    if !minimum_tolerance.is_finite() || minimum_tolerance < 0.0 {
        return Err(ParseBenchesError::InvalidTolerance);
    }
    for (side, summaries) in [("reference", reference), ("candidate", candidate)] {
        if summaries.len() < MINIMUM_ROUNDS {
            return Err(ParseBenchesError::TooFewSamples {
                side,
                count: summaries.len(),
            });
        }
    }
    let reference_names: Vec<_> = reference
        .first()
        .ok_or(ParseBenchesError::TooFewSamples {
            side: "reference",
            count: 0,
        })?
        .benchmarks
        .keys()
        .collect();
    if reference_names.is_empty() {
        return Err(ParseBenchesError::EmptyBenchmarkSet);
    }
    if reference.iter().any(|summary| {
        summary
            .benchmarks
            .keys()
            .ne(reference_names.iter().copied())
    }) || candidate.iter().any(|summary| {
        summary
            .benchmarks
            .keys()
            .ne(reference_names.iter().copied())
    }) {
        return Err(ParseBenchesError::BenchmarkSetMismatch);
    }
    let reference_executables = side_executables("reference", reference)?;
    let candidate_executables = side_executables("candidate", candidate)?;
    if let Some(shared) = reference_executables.iter().find(|reference| {
        candidate_executables
            .iter()
            .any(|candidate| candidate.path == reference.path)
    }) {
        return Err(ParseBenchesError::SharedExecutable(shared.path.clone()));
    }

    let mut benchmarks = BTreeMap::new();
    for name in reference_names {
        // Every summary holds exactly the reference names, checked above.
        let side_values = |summaries: &[BenchmarkSummary]| {
            summaries
                .iter()
                .map(|summary| {
                    summary
                        .benchmarks
                        .get(name)
                        .map(|metric| metric.ns_per_iter)
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(ParseBenchesError::BenchmarkSetMismatch)
        };
        let reference_values = side_values(reference)?;
        let candidate_values = side_values(candidate)?;
        let mut values = reference_values.iter().chain(&candidate_values);
        if values.clone().all(|value| *value == 0.0) {
            benchmarks.insert(name.clone(), BenchmarkComparison {
                reference_median_ns: 0.0,
                candidate_median_ns: 0.0,
                ratio: 1.0,
                tolerance: minimum_tolerance,
                passed: true,
            });
            continue;
        }
        if values.any(|value| !value.is_finite() || *value <= 0.0) {
            return Err(ParseBenchesError::InvalidSample(name.clone()));
        }
        let reference_median_ns = median(reference_values.clone());
        let candidate_median_ns = median(candidate_values.clone());
        let tolerance = calibrated_tolerance(
            minimum_tolerance,
            CRITERION_SPREAD_MULTIPLIER,
            relative_mad(&reference_values, reference_median_ns),
            relative_mad(&candidate_values, candidate_median_ns),
        );
        let ratio = candidate_median_ns / reference_median_ns;
        let slowest_reference = reference_values.iter().copied().fold(f64::MIN, f64::max);
        let fastest_candidate = candidate_values.iter().copied().fold(f64::MAX, f64::min);
        benchmarks.insert(name.clone(), BenchmarkComparison {
            reference_median_ns,
            candidate_median_ns,
            ratio,
            tolerance,
            passed: ratio <= 1.0 + tolerance || fastest_candidate <= slowest_reference,
        });
    }
    let passed = benchmarks.values().all(|benchmark| benchmark.passed);
    Ok(BenchmarkVerdict {
        reference_commits: reference
            .iter()
            .map(|summary| summary.commit.clone())
            .collect(),
        candidate_commits: candidate
            .iter()
            .map(|summary| summary.commit.clone())
            .collect(),
        reference_executables,
        candidate_executables,
        benchmarks,
        passed,
    })
}

/// Read every JSON summary in `directory`, sorted by filename.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] when the directory is missing, contains fewer
/// than three summaries, or a summary is unreadable or invalid JSON.
pub fn read_summaries(
    directory: &Path,
    side: &'static str,
) -> Result<Vec<BenchmarkSummary>, ParseBenchesError> {
    if !directory.is_dir() {
        return Err(ParseBenchesError::DirectoryNotFound(
            directory.to_path_buf(),
        ));
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();
    if paths.len() < MINIMUM_ROUNDS {
        return Err(ParseBenchesError::TooFewSamples {
            side,
            count: paths.len(),
        });
    }
    paths
        .into_iter()
        .map(|path| Ok(serde_json::from_reader(File::open(path)?)?))
        .collect()
}
