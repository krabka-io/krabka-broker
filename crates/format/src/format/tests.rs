//! Whole-format tests: the exit code each argv produces, and the files a run
//! leaves behind.
//!
//! These drive the command end to end through [`crate::run_from_args`], which
//! is the only way the mode selection, the voter-set validation, and the
//! writers become observable. The tests that pin one parser or one resolution
//! rule live beside that code in the sibling modules.

use assert2::check;

use super::*;
use crate::format::output::ZERO_CHECKPOINT_NAME;

/// The exit code `run` returns for each argv it can be given.
///
/// Neither `run` nor `run_from_args` had a unit test, so a mutant making
/// either return a constant survived -- and with them the whole of
/// `is_dynamic_format` and `build_initial_voters`, whose only visible
/// effect is which of these codes comes back.
#[tokio::test]
async fn exit_code_for_each_argv() {
    const STANDALONE: &[&str] = &[
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "controller-1:9093",
    ];
    // (what it is, extra argv, expected exit)
    let cases: &[(&str, &[&str], i32)] = &[
        ("static, no flags at all", &[], EXIT_OK),
        ("standalone", STANDALONE, EXIT_OK),
        (
            "no-initial-controllers",
            &["--no-initial-controllers"],
            EXIT_OK,
        ),
        // is_dynamic_format: the kraft.version rules.
        (
            "kraft.version=1 with no quorum flag",
            &["--feature", "kraft.version=1"],
            EXIT_INVALID_FEATURE,
        ),
        (
            "standalone with kraft.version=0",
            &[
                "--standalone",
                "--node-id",
                "1",
                "--controller-listener",
                "c:9093",
                "--feature",
                "kraft.version=0",
            ],
            EXIT_INVALID_FEATURE,
        ),
        (
            "kraft.version given twice",
            &[
                "--no-initial-controllers",
                "--feature",
                "kraft.version=1",
                "--feature",
                "kraft.version=1",
            ],
            EXIT_INVALID_FEATURE,
        ),
        (
            "kraft.version above its range",
            &["--feature", "kraft.version=2"],
            EXIT_INVALID_FEATURE,
        ),
        // build_initial_voters: every way the standalone voter can be wrong.
        (
            "standalone without --node-id",
            &["--standalone", "--controller-listener", "c:9093"],
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "standalone without --controller-listener",
            &["--standalone", "--node-id", "1"],
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "listener with no port",
            &[
                "--standalone",
                "--node-id",
                "1",
                "--controller-listener",
                "hostonly",
            ],
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "listener with an empty host",
            &[
                "--standalone",
                "--node-id",
                "1",
                "--controller-listener",
                ":9093",
            ],
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "listener on port zero",
            &[
                "--standalone",
                "--node-id",
                "1",
                "--controller-listener",
                "c:0",
            ],
            EXIT_BOOTSTRAP_FAIL,
        ),
    ];

    for (what, extra, want) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log_dir = tmp.path().join("data");
        let mut argv = vec![
            "krabka-format".to_owned(),
            "--log-dir".to_owned(),
            log_dir.display().to_string(),
        ];
        argv.extend(extra.iter().map(|a| (*a).to_owned()));
        let got = crate::run_from_args(argv).await;
        check!(got == *want, "{what}: exit {got}, want {want}");
    }
}

/// Formatting into a fresh directory, returning its path for inspection.
async fn format_into(tmp: &std::path::Path, extra: &[&str]) -> (i32, std::path::PathBuf) {
    let log_dir = tmp.join("data");
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
    ];
    argv.extend(extra.iter().map(|a| (*a).to_owned()));
    (crate::run_from_args(argv).await, log_dir)
}

fn checkpoint_len(log_dir: &std::path::Path) -> u64 {
    let path = krabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"))
        .join(ZERO_CHECKPOINT_NAME);
    std::fs::metadata(path).map_or(0, |m| m.len())
}

/// Any one of the three quorum flags selects a dynamic format, and their
/// absence selects the static one. The offset-zero checkpoint is written
/// only for a dynamic format, so its presence is the observable.
#[tokio::test]
async fn each_quorum_flag_on_its_own_selects_a_dynamic_format() {
    const STANDALONE: &[&str] = &[
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "c:9093",
    ];
    const EXPLICIT: &[&str] = &[
        "--node-id",
        "3",
        "--initial-controllers",
        "3@host:9093:00000000-0000-0000-0000-000000000003",
    ];
    // (what it is, argv, dynamic?)
    let cases: &[(&str, &[&str], bool)] = &[
        ("no quorum flag", &[], false),
        ("--standalone", STANDALONE, true),
        ("--initial-controllers", EXPLICIT, true),
        (
            "--no-initial-controllers",
            &["--no-initial-controllers"],
            true,
        ),
    ];
    for (what, argv, dynamic) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (code, log_dir) = format_into(tmp.path(), argv).await;
        check!(code == EXIT_OK, "{what}: exit {code}");
        check!(
            (checkpoint_len(&log_dir) > 0) == *dynamic,
            "{what}: checkpoint present should be {dynamic}"
        );
    }
}

/// The voter set rides in the checkpoint only when there is one. Both of
/// these formats are dynamic, so both write a checkpoint -- what separates
/// them is whether it also carries voters, and the one that does is bigger.
#[tokio::test]
async fn the_checkpoint_carries_voters_only_when_the_quorum_has_them() {
    let tmp_with = tempfile::tempdir().expect("tempdir");
    let (code, with_voters) = format_into(
        tmp_with.path(),
        &[
            "--standalone",
            "--node-id",
            "1",
            "--controller-listener",
            "c:9093",
        ],
    )
    .await;
    check!(code == EXIT_OK);

    let tmp_without = tempfile::tempdir().expect("tempdir");
    let (code, without_voters) =
        format_into(tmp_without.path(), &["--no-initial-controllers"]).await;
    check!(code == EXIT_OK);

    let (a, b) = (
        checkpoint_len(&with_voters),
        checkpoint_len(&without_voters),
    );
    check!(
        a > b,
        "checkpoint with voters ({a}) should exceed one without ({b})"
    );
}

/// The explicit quorum must name each controller once and must include
/// this node.
#[tokio::test]
async fn an_explicit_quorum_is_checked_for_duplicates_and_for_this_node() {
    const A: &str = "1@host-a:9093:00000000-0000-0000-0000-000000000001";
    const B: &str = "2@host-b:9093:00000000-0000-0000-0000-000000000002";
    // Same id as A on a different host, and same directory id as A on a
    // different node: each is rejected by its own check.
    const DUP_ID: &str = "1@host-c:9093:00000000-0000-0000-0000-00000000000c";
    const DUP_DIR: &str = "3@host-d:9093:00000000-0000-0000-0000-000000000001";

    let cases: &[(&str, &str, &str, i32)] = &[
        ("a well-formed pair", "1", &joined(A, B), EXIT_OK),
        (
            "a repeated node id",
            "1",
            &joined(A, DUP_ID),
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "a repeated directory id",
            "1",
            &joined(A, DUP_DIR),
            EXIT_BOOTSTRAP_FAIL,
        ),
        (
            "a quorum without this node",
            "9",
            &joined(A, B),
            EXIT_BOOTSTRAP_FAIL,
        ),
    ];
    for (what, node_id, controllers, want) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (code, _) = format_into(
            tmp.path(),
            &["--node-id", node_id, "--initial-controllers", controllers],
        )
        .await;
        check!(code == *want, "{what}: exit {code}, want {want}");
    }
}

/// `--initial-controllers` takes one comma-separated value.
fn joined(a: &str, b: &str) -> String {
    format!("{a},{b}")
}

/// `--directory-id` is only checked against the quorum entry when it was
/// given, and only rejected when the two disagree.
#[tokio::test]
async fn an_explicit_directory_id_must_match_this_node_s_quorum_entry() {
    const CONTROLLER: &str = "1@host-a:9093:00000000-0000-0000-0000-000000000001";
    // (what it is, --directory-id, expected exit)
    let cases: &[(&str, &str, i32)] = &[
        (
            "matching the quorum entry",
            "00000000-0000-0000-0000-000000000001",
            EXIT_OK,
        ),
        (
            "disagreeing with the quorum entry",
            "00000000-0000-0000-0000-0000000000ff",
            EXIT_BOOTSTRAP_FAIL,
        ),
    ];
    for (what, directory_id, want) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (code, _) = format_into(
            tmp.path(),
            &[
                "--node-id",
                "1",
                "--initial-controllers",
                CONTROLLER,
                "--directory-id",
                directory_id,
            ],
        )
        .await;
        check!(code == *want, "{what}: exit {code}, want {want}");
    }
}

/// The SCRAM iteration floor is inclusive: the minimum itself is allowed
/// and one below it is not.
#[tokio::test]
async fn scram_iterations_are_checked_against_an_inclusive_minimum() {
    let min = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    for (iterations, want) in [(min, EXIT_OK), (min - 1, EXIT_LOW_ITERATIONS)] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let spec = format!("SCRAM-SHA-256=[name=alice,password=hunter2,iterations={iterations}]");
        let (code, _) = format_into(tmp.path(), &["--add-scram", &spec]).await;
        check!(
            code == want,
            "iterations={iterations}: exit {code}, want {want}"
        );
    }
}

/// A directory holding anything at all is refused rather than overwritten.
#[tokio::test]
async fn a_non_empty_log_dir_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    std::fs::create_dir_all(&log_dir).expect("mkdir");
    std::fs::write(log_dir.join("someone-elses.txt"), b"x").expect("write");

    let code =
        crate::run_from_args(["krabka-format", "--log-dir", &log_dir.display().to_string()]).await;
    check!(code == EXIT_DIRTY_LOG_DIR);
}

/// The writers are only observable through the files they leave, so a
/// mutant emptying one out to `Ok(())` survives until something reads the
/// directory. A boot needs all of these.
#[tokio::test]
async fn a_standalone_format_writes_what_a_boot_reads() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");

    let code = crate::run_from_args([
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "controller-1:9093",
    ])
    .await;
    check!(code == EXIT_OK);

    for name in [
        "meta.properties.json",
        "bootstrap.records.bin",
        "bootstrap.json",
    ] {
        let path = log_dir.join(name);
        let len = std::fs::metadata(&path).map_or(0, |m| m.len());
        check!(len > 0, "{name} should exist and carry bytes, got {len}");
    }

    // KIP-853 dynamic quorum: the voter set lives in the offset-zero
    // checkpoint, not in the bootstrap record stream.
    let checkpoint = krabka_raft::kraft::checkpoint_dir(&log_dir.join("__cluster_metadata"))
        .join(ZERO_CHECKPOINT_NAME);
    let len = std::fs::metadata(&checkpoint).map_or(0, |m| m.len());
    check!(len > 0, "offset-zero checkpoint should carry the voter set");
}

/// `--ignore-formatted` is what lets a Kubernetes init container run the
/// formatter unconditionally: the second run is a no-op that exits 0 and
/// leaves the first run's identity in place, while the same directory without
/// the flag is still refused.
#[tokio::test]
async fn ignore_formatted_makes_a_second_format_a_no_op() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    let dir = log_dir.display().to_string();
    let argv = |extra: &[&str]| {
        let mut argv = vec![
            "krabka-format".to_string(),
            "--log-dir".to_string(),
            dir.clone(),
            "--standalone".to_string(),
            "--node-id".to_string(),
            "1".to_string(),
            "--controller-listener".to_string(),
            "controller-1:9093".to_string(),
        ];
        argv.extend(extra.iter().map(|s| (*s).to_string()));
        argv
    };

    check!(crate::run_from_args(argv(&[])).await == EXIT_OK);
    let formatted = std::fs::read(log_dir.join(super::META_PROPERTIES)).expect("meta properties");

    // Without the flag the same directory is still a dirty log dir.
    check!(crate::run_from_args(argv(&[])).await == EXIT_DIRTY_LOG_DIR);

    // With it the run succeeds and rewrites nothing: a regenerated cluster or
    // directory id would strand the node's replicated identity.
    check!(crate::run_from_args(argv(&["--ignore-formatted"])).await == EXIT_OK);
    let after = std::fs::read(log_dir.join(super::META_PROPERTIES)).expect("meta properties");
    check!(after == formatted);
}

/// An unformatted directory is formatted normally under the flag: it means
/// "ignore an existing format", not "skip formatting".
#[tokio::test]
async fn ignore_formatted_still_formats_a_fresh_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");

    let code = crate::run_from_args([
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "controller-1:9093",
        "--ignore-formatted",
    ])
    .await;
    check!(code == EXIT_OK);
    check!(log_dir.join(super::META_PROPERTIES).is_file());
}
