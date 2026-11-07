//! Criterion's bencher output format: one line per benchmark, read into
//! [`BenchmarkMetric`] values.

use {
    crate::{BenchmarkMetric, ParseBenchesError},
    regex::Regex,
    std::{
        collections::BTreeMap,
        fs::{self, File},
        io::{BufRead, BufReader},
        path::Path,
        sync::LazyLock,
    },
};

static BENCHER_REGEX: LazyLock<Result<Regex, regex::Error>> = LazyLock::new(|| {
    Regex::new(
        r"test\s+([\w\/\-\.]+)\s+\.\.\.\s+bench:\s+([\d,]+)\s+ns\/iter\s+\(\+\/-\s+([\d,]+)\)",
    )
});

/// A benchmark measurement together with the name of the benchmark it
/// belongs to, as read from one line of bencher output.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedMetric {
    /// Canonical benchmark name.
    pub name: String,
    /// The measurement of that benchmark.
    pub metric: BenchmarkMetric,
}

/// Parses a single line of Criterion bencher output.
///
/// Matches patterns of the shape:
/// `test log/append/100rec_1024B ... bench: 95,979 ns/iter (+/- 1,377,729)`
///
/// # Errors
///
/// Returns [`ParseBenchesError::InvalidNumber`] if matched numeric fields cannot
/// be parsed into floats.
pub fn parse_bencher_line(line: &str) -> Result<Option<NamedMetric>, ParseBenchesError> {
    let regex = BENCHER_REGEX
        .as_ref()
        .map_err(|error| ParseBenchesError::Pattern(error.clone()))?;
    let Some(caps) = regex.captures(line) else {
        return Ok(None);
    };

    let name = caps[1].to_string();
    let ns_raw = caps[2].replace(',', "");
    let variance_raw = caps[3].replace(',', "");

    let ns_per_iter: f64 = ns_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[2].to_string()))?;
    let variance_ns: f64 = variance_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[3].to_string()))?;

    Ok(Some(NamedMetric {
        name,
        metric: BenchmarkMetric {
            ns_per_iter,
            variance_ns,
        },
    }))
}

/// Parses all benchmark metrics from a `.txt` file into the target map.
///
/// # Errors
///
/// Returns an error if the file cannot be read, numeric parsing fails, or a
/// duplicate benchmark name is encountered.
pub fn parse_bencher_file(
    path: &Path,
    benchmarks: &mut BTreeMap<String, BenchmarkMetric>,
) -> Result<(), ParseBenchesError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if let Some(NamedMetric { name, metric }) = parse_bencher_line(&line)?
            && benchmarks.insert(name.clone(), metric).is_some()
        {
            return Err(ParseBenchesError::DuplicateBenchmark {
                name,
                file: path.to_path_buf(),
            });
        }
    }

    Ok(())
}

/// Scans the given directory for all `*.txt` files and parses their benchmark metrics.
///
/// # Errors
///
/// Returns an error if:
/// - The directory does not exist ([`ParseBenchesError::DirectoryNotFound`]).
/// - No `*.txt` files are found ([`ParseBenchesError::NoTxtFiles`]).
/// - No metrics were parsed from any found files ([`ParseBenchesError::NoMetricsParsed`]).
/// - Duplicate benchmark metrics are detected across files.
pub fn parse_benchmark_dir(
    results_dir: &Path,
) -> Result<BTreeMap<String, BenchmarkMetric>, ParseBenchesError> {
    if !results_dir.exists() || !results_dir.is_dir() {
        return Err(ParseBenchesError::DirectoryNotFound(
            results_dir.to_path_buf(),
        ));
    }

    let mut txt_files = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("txt") {
            txt_files.push(path);
        }
    }

    if txt_files.is_empty() {
        return Err(ParseBenchesError::NoTxtFiles(results_dir.to_path_buf()));
    }

    txt_files.sort();

    let mut benchmarks = BTreeMap::new();
    for file_path in &txt_files {
        parse_bencher_file(file_path, &mut benchmarks)?;
    }

    if benchmarks.is_empty() {
        return Err(ParseBenchesError::NoMetricsParsed(
            results_dir.to_path_buf(),
        ));
    }

    Ok(benchmarks)
}
