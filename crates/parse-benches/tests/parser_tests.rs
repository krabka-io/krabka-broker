use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};

use assert2::assert;
use krabka_parse_benches::{
    Args, BenchmarkSummary, ParseBenchesError, format_rfc3339_utc, parse_bencher_line,
    parse_benchmark_dir, resolve_commit_sha, run_from_args,
};
use tempfile::tempdir;

// Representative 29-metric log engine output fixture matching Criterion bencher format (space-padded, no commas)
const SAMPLE_CRITERION_BENCHER_OUTPUT: &str = r"
running 29 tests
test log/append/1rec_64B ... bench:        2225 ns/iter (+/- 8)
test log/append/10rec_64B ... bench:        5845 ns/iter (+/- 151)
test log/append/100rec_256B ... bench:       31200 ns/iter (+/- 412)
test log/append/100rec_1024B ... bench:       97083 ns/iter (+/- 1369686)
test log/append/500rec_256B ... bench:      120400 ns/iter (+/- 2300)
test log/append_large_message/owned_1rec_100KiB ... bench:      310000 ns/iter (+/- 4500)
test log/append_large_message/verbatim_1rec_100KiB ... bench:      180000 ns/iter (+/- 1200)
test log/append_large_message/verbatim_1rec_512KiB ... bench:      890000 ns/iter (+/- 9800)
test log/append_handoff/direct_mutex_verbatim_1rec_100KiB ... bench:      190000 ns/iter (+/- 2100)
test log/append_handoff/spawn_blocking_mutex_verbatim_1rec_100KiB ... bench:      420000 ns/iter (+/- 5000)
test log/append_handoff/block_in_place_mutex_verbatim_1rec_100KiB ... bench:      250000 ns/iter (+/- 3200)
test log/read/from_start_1MiB ... bench:     1500000 ns/iter (+/- 12000)
test log/read/from_start_unbounded ... bench:     3686508 ns/iter (+/- 11500)
test log/read/from_middle_1MiB ... bench:     1510000 ns/iter (+/- 13200)
test log/read/from_end_minus_100_1MiB ... bench:       17726 ns/iter (+/- 140)
test log/read/past_end_returns_empty ... bench:          22 ns/iter (+/- 0)
test log/open/50_appends_validate_on_open ... bench:      532433 ns/iter (+/- 1100)
test log/open/50_appends_no_validate ... bench:       45000 ns/iter (+/- 900)
test log/open/200_appends_validate_on_open ... bench:      320000 ns/iter (+/- 4200)
test log/open/200_appends_no_validate ... bench:      160000 ns/iter (+/- 2100)
test log/open/500_appends_validate_on_open ... bench:      780000 ns/iter (+/- 8900)
test log/open/500_appends_no_validate ... bench:     4880352 ns/iter (+/- 4500)
test log/truncate/truncate_recent_offset ... bench:       25000 ns/iter (+/- 500)
test log/accessors/log_end_offset ... bench:          15 ns/iter (+/- 1)
test log/accessors/log_start_offset ... bench:          14 ns/iter (+/- 1)
test log/accessors/lso ... bench:          16 ns/iter (+/- 2)
test log/file_write_shapes/seek_end_writev_100KiB ... bench:      210000 ns/iter (+/- 3400)
test log/file_write_shapes/writev_at_current_cursor_100KiB ... bench:      205000 ns/iter (+/- 3100)
test log/file_write_shapes/write_all_at_twice_100KiB ... bench:      230000 ns/iter (+/- 3900)

test result: ok. 29 passed; 0 failed; 0 ignored; 29 measured; 0 filtered out
";

const COMMA_GROUPED_BENCHER_OUTPUT: &str =
    "test log/append/100rec_1024B ... bench: 95,979 ns/iter (+/- 1,377,729)";

#[test]
fn parses_real_criterion_output_correctly() {
    let line = "test log/append/1rec_64B ... bench:        2225 ns/iter (+/- 8)";
    let result = parse_bencher_line(line).unwrap();

    assert!(result.is_some());
    let (name, metric) = result.unwrap();
    assert!(name == "log/append/1rec_64B");
    assert!((metric.ns_per_iter - 2225.0).abs() < f64::EPSILON);
    assert!((metric.variance_ns - 8.0).abs() < f64::EPSILON);
}

#[test]
fn parses_comma_grouped_bencher_line_correctly() {
    let result = parse_bencher_line(COMMA_GROUPED_BENCHER_OUTPUT).unwrap();

    assert!(result.is_some());
    let (name, metric) = result.unwrap();
    assert!(name == "log/append/100rec_1024B");
    assert!((metric.ns_per_iter - 95_979.0).abs() < f64::EPSILON);
    assert!((metric.variance_ns - 1_377_729.0).abs() < f64::EPSILON);
}

#[test]
fn ignores_non_matching_lines() {
    let result = parse_bencher_line("running 29 tests").unwrap();
    assert!(result.is_none());

    let result = parse_bencher_line("test result: ok. 29 passed").unwrap();
    assert!(result.is_none());
}

#[test]
fn parses_full_29_sample_benchmark_output() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("log-engine.txt");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(SAMPLE_CRITERION_BENCHER_OUTPUT.as_bytes())
        .unwrap();

    let benchmarks = parse_benchmark_dir(dir.path()).unwrap();
    assert!(benchmarks.len() == 29);

    let append_metric = &benchmarks["log/append/100rec_1024B"];
    assert!((append_metric.ns_per_iter - 97_083.0).abs() < f64::EPSILON);
    assert!((append_metric.variance_ns - 1_369_686.0).abs() < f64::EPSILON);

    let zero_variance_metric = &benchmarks["log/read/past_end_returns_empty"];
    assert!((zero_variance_metric.ns_per_iter - 22.0).abs() < f64::EPSILON);
    assert!((zero_variance_metric.variance_ns - 0.0).abs() < f64::EPSILON);

    let accessor_metric = &benchmarks["log/accessors/log_end_offset"];
    assert!((accessor_metric.ns_per_iter - 15.0).abs() < f64::EPSILON);
    assert!((accessor_metric.variance_ns - 1.0).abs() < f64::EPSILON);
}

#[test]
fn parses_multiple_txt_files() {
    let dir = tempdir().unwrap();

    let f1_path = dir.path().join("file1.txt");
    fs::write(
        &f1_path,
        "test bench_a ... bench: 100 ns/iter (+/- 5)\ntest bench_b ... bench: 200 ns/iter (+/- \
         10)\n",
    )
    .unwrap();

    let f2_path = dir.path().join("file2.txt");
    fs::write(
        &f2_path,
        "test bench_c ... bench: 300 ns/iter (+/- 15)\ntest bench_d ... bench: 400 ns/iter (+/- \
         20)\n",
    )
    .unwrap();

    let benchmarks = parse_benchmark_dir(dir.path()).unwrap();
    assert!(benchmarks.len() == 4);
    assert!(benchmarks.contains_key("bench_a"));
    assert!(benchmarks.contains_key("bench_b"));
    assert!(benchmarks.contains_key("bench_c"));
    assert!(benchmarks.contains_key("bench_d"));
}

#[test]
fn fails_when_results_directory_does_not_exist() {
    let missing_path = PathBuf::from("nonexistent-dir-for-tests-12345");
    let err = parse_benchmark_dir(&missing_path).unwrap_err();

    match err {
        ParseBenchesError::DirectoryNotFound(p) => assert!(p == missing_path),
        other => panic!("expected DirectoryNotFound, got {other:?}"),
    }
}

#[test]
fn fails_when_no_txt_files_in_directory() {
    let dir = tempdir().unwrap();
    let err = parse_benchmark_dir(dir.path()).unwrap_err();

    match err {
        ParseBenchesError::NoTxtFiles(p) => assert!(p == dir.path()),
        other => panic!("expected NoTxtFiles, got {other:?}"),
    }
}

#[test]
fn fails_when_txt_files_contain_no_benchmark_lines() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("empty.txt");
    fs::write(
        &file_path,
        "Compiling crabka-log v0.4.0\nFinished bench profile\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::NoMetricsParsed(p) => assert!(p == dir.path()),
        other => panic!("expected NoMetricsParsed, got {other:?}"),
    }
}

#[test]
fn fails_on_duplicate_benchmark_in_same_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("dups.txt");
    fs::write(
        &file_path,
        "test log/append/1rec_64B ... bench: 100 ns/iter (+/- 5)\ntest log/append/1rec_64B ... \
         bench: 120 ns/iter (+/- 6)\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::DuplicateBenchmark { name, file } => {
            assert!(name == "log/append/1rec_64B");
            assert!(file == file_path);
        }
        other => panic!("expected DuplicateBenchmark, got {other:?}"),
    }
}

#[test]
fn fails_on_duplicate_benchmark_across_files() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("part1.txt"),
        "test shared_bench ... bench: 100 ns/iter (+/- 5)\n",
    )
    .unwrap();
    let part2_path = dir.path().join("part2.txt");
    fs::write(
        &part2_path,
        "test shared_bench ... bench: 150 ns/iter (+/- 8)\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::DuplicateBenchmark { name, file } => {
            assert!(name == "shared_bench");
            assert!(file == part2_path);
        }
        other => panic!("expected DuplicateBenchmark, got {other:?}"),
    }
}

#[test]
fn formats_rfc3339_utc_timestamp_accurately() {
    // 2026-08-27T10:25:37Z in unix epoch seconds: 1787826337
    let sample_time = UNIX_EPOCH + Duration::from_secs(1_787_826_337);
    let formatted = format_rfc3339_utc(sample_time);
    assert!(formatted == "2026-08-27T10:25:37Z");

    // Non-zero subsecond input is truncated to whole seconds per YYYY-MM-DDTHH:MM:SSZ specification
    let subsecond_time = UNIX_EPOCH + Duration::new(1_787_826_337, 188_979_529);
    assert!(format_rfc3339_utc(subsecond_time) == "2026-08-27T10:25:37Z");

    // 1970-01-01T00:00:00Z
    assert!(format_rfc3339_utc(UNIX_EPOCH) == "1970-01-01T00:00:00Z");
}

#[test]
fn resolves_commit_sha_appropriately() {
    // Explicit CLI argument takes precedence over environment
    assert!(resolve_commit_sha(Some("abcdef123456"), None) == "abcdef12");
    assert!(resolve_commit_sha(Some("abcdef123456"), Some("fedcba987654")) == "abcdef12");
    assert!(resolve_commit_sha(Some("short"), None) == "short");

    // Fallback to environment SHA when CLI argument is absent or blank
    assert!(resolve_commit_sha(None, Some("fedcba987654")) == "fedcba98");
    assert!(resolve_commit_sha(Some(""), Some("fedcba987654")) == "fedcba98");
    assert!(resolve_commit_sha(Some("   "), Some("fedcba987654")) == "fedcba98");

    // Default to "unknown" when neither provides a non-blank value
    assert!(resolve_commit_sha(None, None) == "unknown");
    assert!(resolve_commit_sha(Some(""), None) == "unknown");
    assert!(resolve_commit_sha(None, Some("")) == "unknown");
    assert!(resolve_commit_sha(Some("   "), Some("   ")) == "unknown");

    // Multi-byte UTF-8 string truncation safety
    assert!(resolve_commit_sha(Some("🦀crabka123"), None) == "🦀crabka1");
}

#[test]
fn runs_end_to_end_writing_valid_json() {
    let dir = tempdir().unwrap();
    let results_dir = dir.path().join("bench-results");
    fs::create_dir_all(&results_dir).unwrap();

    let bench_file = results_dir.join("log-engine.txt");
    fs::write(&bench_file, SAMPLE_CRITERION_BENCHER_OUTPUT).unwrap();

    let out_json = results_dir.join("broker-benchmarks.json");

    let args = Args {
        results_dir: results_dir.clone(),
        output: Some(out_json.clone()),
        suite: "krabka-broker".to_string(),
        commit: Some("deadbeef999".to_string()),
    };

    let summary = run_from_args(&args).unwrap();
    assert!(summary.suite == "krabka-broker");
    assert!(summary.commit == "deadbeef");
    assert!(summary.benchmarks.len() == 29);
    assert!(out_json.exists());

    let json_content = fs::read_to_string(&out_json).unwrap();
    let parsed_back: BenchmarkSummary = serde_json::from_str(&json_content).unwrap();
    assert!(parsed_back == summary);
}
